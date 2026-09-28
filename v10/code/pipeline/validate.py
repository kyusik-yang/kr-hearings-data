"""validate.py - release-blocking validation suite for the v10 tables.

    python3 validate.py [--root DIR] [--turns GLOB] [--meetings PATH] [--dyads GLOB] ...
                        [--mode release|dev] [--out report.json] [--docs-numbers docs_numbers.json]

Exit status 1 when any check FAILs (or errors), 0 otherwise. The JSON report lists every check
with status (PASS / FAIL / WARN / SKIP), the number of offending rows, details and up to
N_EXAMPLES example rows. docs_numbers.json holds every number the documentation is rendered
from (check `docs_numbers` fails when a documentation template names a key that is not there).

Inputs (any can be a parquet path/glob/list, a CSV (calendar), the crawl sqlite, or - in tests -
a pandas DataFrame):
    turns            enriched turns (CONTRACT turn columns + roles/legislators/party/government)
    meetings         meetings table (CONTRACT)
    dyads            dyads.py output
    agenda, footer   build_turns tables
    coverage         build_turns internal coverage table (per-meeting text accounting)
    crosswalk        crosswalk.py meeting-level crosswalk
    crosswalk_turns  crosswalk.py turn-level alignment
    duplicate_turns  enriched turns of meetings marked meetings.duplicate_of (identical text of another
                     meeting; researcher decision 7): kept out of `turns` in the release
    duplicate_dyads  dyads of those meetings (kept out of `dyads`)
    duplicate_meetings  run_all duplicate decisions (one row per flagged pair: kind, kept, basis)
    universe         meeting universe (API meetings + id-gap meetings, source column)
    crawl            crawl_state.sqlite (table fetch) or a frame with conf_num, kind, status
    v9_meetings      v9_to_api_crosswalk.parquet (meeting_id, v9_source)
    v9_speeches      data/all_speeches_16_22_v9.parquet (meeting_id, speech_order; speech_text is read
                     only inside duckdb, for the XLSX source recount)
    calendar         president_calendar.csv
    lineage          party_lineage.csv (only label and satellite_of are read)

Raw sources (not a table): check coverage_source_sample re-reads a seeded sample of saved viewer
pages (v10/raw/viewer/view) and HWP files (v10/raw/hwp) through Params.raw_root.

Views: `turns` / `dyads` are the release tables plus the duplicate copies (every built meeting, so the
accounting, coverage, enrichment and dyad-recompute checks see all of them); `turns_release` /
`dyads_release` are the release tables alone (duplicate-content checks, docs numbers, duplicates_resolved).

Release files (Params.release_root): check release_no_local_paths scans every file of the release
directory (parquet string columns and metadata, JSON/CSV/text) for absolute local paths and the user name.

In mode 'release' a check whose input table or columns are missing FAILs; in mode 'dev' it is
SKIPped (or WARNs), so the suite can run while components are still being built.
Every check is SQL in duckdb (memory_limit 6GB, 4 threads); nothing loads text into pandas.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import glob as _glob
import json
import os
import re
import sqlite3
import sys
import time
import traceback
from pathlib import Path
from typing import Callable, Optional, Sequence

import duckdb
import pandas as pd
import pyarrow as pa

HERE = Path(__file__).resolve().parent
V10 = HERE.parents[1]
REPO = V10.parent
INTERIM = V10 / "interim"
PIPE = INTERIM / "pipeline"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import legacy_rules as LR  # noqa: E402

SEED = 8374
N_EXAMPLES = 10
VALIDATOR_VERSION = "1.0"

# ----------------------------------------------------------------------------- contract types
TURN_TYPES = {
    "conf_num": "BIGINT", "turn_seq": "INTEGER", "source": "VARCHAR", "speaker_label_raw": "VARCHAR",
    "speaker_pos": "VARCHAR", "speaker_name": "VARCHAR", "speaker_mem_id": "BIGINT", "speaker_area": "VARCHAR",
    "text_raw": "VARCHAR", "text": "VARCHAR", "has_stage": "BOOLEAN", "stage_kinds": "VARCHAR[]",
    "n_fragments": "SMALLINT", "agenda_ordinal": "INTEGER", "agenda_text": "VARCHAR", "time_hhmm": "VARCHAR",
    "speech_date": "VARCHAR",
    # CONTRACT turn boundary / sitting fields (build_turns 1.7)
    "after_end_marker": "BOOLEAN", "sitting_seq": "SMALLINT", "sitting_how": "VARCHAR", "label_how": "VARCHAR",
    "label_confidence": "VARCHAR",
}
LABEL_CONFIDENCE_VALUES = ("high", "medium", "low", "unrated")
ENRICH_COLS = {
    "roles": ("role", "role_group", "role_rule", "role_v9_compat", "affiliation_raw", "person_title"),
    "legislators": ("naas_cd", "leg_name_hangul", "leg_name_hanja", "gender", "birth_date", "district",
                    "elect_type", "seniority", "id_method", "id_confidence"),
    "party_timeline": ("party", "party_lineage", "ruling_status", "presidency_state", "president",
                       "president_party", "party_method"),
    "government": ("ministry_normalized", "minister_panel_id", "dual_office", "admin", "admin_ideology",
                   "link_method"),
}
ENRICH_TYPES = {"role": "VARCHAR", "role_group": "VARCHAR", "naas_cd": "VARCHAR", "party": "VARCHAR",
                "ruling_status": "VARCHAR", "presidency_state": "VARCHAR", "admin": "VARCHAR",
                "admin_ideology": "VARCHAR", "dual_office": "BOOLEAN", "ministry_normalized": "VARCHAR"}
INT_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"}
MEETING_TYPES = {
    "conf_num": "BIGINT", "conf_id": "VARCHAR", "v9_meeting_id": "VARCHAR", "term": "SMALLINT",
    "class_name": "VARCHAR", "hearing_type": "VARCHAR", "is_subcommittee": "BOOLEAN", "committee_raw": "VARCHAR",
    "subcommittee": "VARCHAR", "committee_key": "VARCHAR", "session_no": "SMALLINT", "session_type": "VARCHAR",
    "sitting": "VARCHAR", "date": "VARCHAR", "date_end": "VARCHAR", "audit_year": "SMALLINT",
    "audited_agencies": "VARCHAR[]", "source": "VARCHAR", "n_turns": "INT", "title": "VARCHAR",
}
DYAD_TYPES = {
    "conf_num": "BIGINT", "leg_turn_seq": "INTEGER", "wit_turn_seq": "INTEGER", "direction": "VARCHAR",
    "leg_is_chair": "BOOLEAN", "leg_is_procedural": "BOOLEAN", "wit_is_legislator_title": "BOOLEAN",
    "leg_text": "VARCHAR", "wit_text": "VARCHAR", "leg_text_raw": "VARCHAR", "wit_text_raw": "VARCHAR",
}
CROSSWALK_TYPES = {
    "v9_meeting_id": "VARCHAR", "v9_source": "VARCHAR", "v9_hearing_type": "VARCHAR", "v9_committee": "VARCHAR",
    "v9_date": "VARCHAR", "conf_num": "BIGINT", "conf_id": "VARCHAR", "relation": "VARCHAR",
    "relation_basis": "VARCHAR", "content_verified": "BOOLEAN", "content_conf_num": "BIGINT",
    "duplicate_of_v9_meeting_id": "VARCHAR", "is_second_copy": "BOOLEAN",
}
CROSSWALK_TURN_TYPES = {
    "v9_meeting_id": "VARCHAR", "v9_speech_order": "VARCHAR", "v9_order_num": "INTEGER", "conf_num": "BIGINT",
    "turn_seq": "INTEGER", "match_type": "VARCHAR", "similarity": "DOUBLE",
}
MEETING_LEVEL_DYAD_COLS = ("term", "date", "hearing_type", "class_name", "committee_key", "is_subcommittee")
# Slim release dyads (researcher decision 8, 2026-09-26): column -> SQL type, in file order
SLIM_DYAD_TYPES = {
    "conf_num": "BIGINT", "term": "SMALLINT", "date": "VARCHAR", "hearing_type": "VARCHAR", "class_name": "VARCHAR",
    "committee_key": "VARCHAR", "is_subcommittee": "BOOLEAN", "sitting_seq": "SMALLINT", "leg_turn_seq": "INTEGER",
    "wit_turn_seq": "INTEGER", "direction": "VARCHAR", "speech_date": "VARCHAR", "leg_naas_cd": "VARCHAR",
    "leg_name": "VARCHAR", "leg_role": "VARCHAR", "leg_is_chair": "BOOLEAN", "leg_party": "VARCHAR",
    "leg_party_camp": "VARCHAR", "leg_ruling_status": "VARCHAR", "presidency_state": "VARCHAR",
    "leg_seniority": "INT", "leg_gender": "VARCHAR", "wit_name": "VARCHAR", "wit_role": "VARCHAR",
    "wit_role_group": "VARCHAR", "wit_title_raw": "VARCHAR", "wit_ministry_normalized": "VARCHAR",
    "wit_minister_panel_id": "VARCHAR", "wit_dual_office": "BOOLEAN", "admin": "VARCHAR", "admin_ideology": "VARCHAR",
    "leg_text": "VARCHAR", "wit_text": "VARCHAR", "leg_is_procedural": "BOOLEAN", "wit_is_legislator_title": "BOOLEAN",
    "any_after_end_marker": "BOOLEAN", "any_low_label_confidence": "BOOLEAN", "any_time_regress": "BOOLEAN",
    "any_label_inconsistent": "BOOLEAN",
}
# Independent statement of where every slim dyad attribute comes from (validated against the turns /
# meetings rows, full, in dyads_attributes): column -> SQL over L (legislator turn), W (witness turn),
# M (meetings row). Pair keys and the three flags are checked by their own checks.
LABEL_INCONSISTENT_COLS = ("label_inconsistent", "speaker_label_inconsistent", "label_inconsistent_in_meeting",
                           "title_inconsistent", "label_title_inconsistent")
DYAD_ATTRIBUTE_SOURCES = {
    "term": "M.term", "date": "M.date", "hearing_type": "M.hearing_type", "class_name": "M.class_name",
    "committee_key": "M.committee_key", "is_subcommittee": "M.is_subcommittee",
    "sitting_seq": "L.sitting_seq", "speech_date": "L.speech_date", "leg_naas_cd": "L.naas_cd",
    "leg_name": "coalesce(L.leg_name_hangul, L.speaker_name)", "leg_role": "L.role", "leg_party": "L.party",
    "leg_party_camp": "L.party_camp", "leg_ruling_status": "L.ruling_status", "presidency_state": "L.presidency_state",
    "leg_seniority": "L.seniority", "leg_gender": "L.gender", "wit_name": "W.speaker_name", "wit_role": "W.role",
    "wit_role_group": "W.role_group", "wit_title_raw": "W.title_raw", "wit_ministry_normalized": "W.ministry_normalized",
    "wit_minister_panel_id": "W.minister_panel_id", "wit_dual_office": "W.dual_office", "admin": "L.admin",
    "admin_ideology": "L.admin_ideology", "leg_text": "L.text", "wit_text": "W.text",
    "any_after_end_marker": "(coalesce(L.after_end_marker, false) OR coalesce(W.after_end_marker, false))",
    "any_low_label_confidence": "(coalesce(L.label_confidence = 'low', false) OR coalesce(W.label_confidence = 'low', false))",
    "any_time_regress": "(coalesce(L.time_regress, false) OR coalesce(W.time_regress, false))",
    "any_label_inconsistent": "(coalesce(L.{lic}, false) OR coalesce(W.{lic}, false))",
}
DYAD_KEY_AND_FLAG_COLS = ("conf_num", "leg_turn_seq", "wit_turn_seq", "direction", "leg_is_chair",
                          "leg_is_procedural", "wit_is_legislator_title")
DYAD_CHUNK_TURNS = 1_500_000             # dyads_attributes / dyads_flags: turns per conf_num chunk

HEARING_TYPES = ("상임위원회", "국정감사", "인사청문특별위원회", "예산결산특별위원회", "국회본회의", "국정조사",
                 "특별위원회", "전원위원회")
CLASS_NAMES = ("상임위원회", "특별위원회", "예산결산특별위원회", "전원위원회", "국회본회의", "국정감사", "국정조사")
RELATIONS = ("same", "v9_wrong_content", "duplicate", "v9_only", "v10_only")
MATCH_TYPES = ("source_row", "exact", "normalized", "similar", "merge", "split", "v9_unmatched", "v10_unmatched")
# 'partyless' (researcher decision 6, 2026-09-26): president in office without a party; 'vacant' is kept in the
# domain (a vacancy without an acting president) although the calendar has none
PRESIDENCY_STATES = ("normal", "partyless", "acting", "vacant", "suspended")
PARTYLESS_RULES = ("last_president_party", "null")
# committee_key domain: the v9 standing-committee keys, the special hearing-type keys and the v10 additions
STANDING_COMMITTEE_KEYS = tuple(sorted(set(LR.COMMITTEE_KEY_MAP_STANDING.values())))
HEARING_TYPE_KEYS = dict(LR.SPECIAL_HEARING_TYPE_TO_KEY, **{"특별위원회": "special_committee", "전원위원회": "committee_of_whole"})
COMMITTEE_KEYS = tuple(sorted(set(STANDING_COMMITTEE_KEYS) | set(HEARING_TYPE_KEYS.values())))
# absolute local path prefixes that must not appear in a release file (the user name is added at run time)
LOCAL_PATH_PATTERNS = ("/Users/", "/home/", "/private/", "/var/folders/", "/tmp/", "/Volumes/", "C:\\Users", "C:/Users")
TERM_WINDOWS = dict(LR.TERM_DATE_RANGES_DEEP_AUDIT)   # constitutional 4-year terms (16: 2000-05-30..)
# v9 meetings matched to the Open API by date/committee instead of by id (v9_to_api_crosswalk)
ID_EXEMPT_METHODS = "('A04_pdf_url_viewer_title_ok', 'B_date_committee_unique')"

# whitespace exactly as Python's str \s (verified equal over all code points in test_validate)
WS_CLASS_RE2 = (r"[\t\n\x{0B}\x{0C}\r\x{1C}-\x{1F} \x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}"
                r"\x{202F}\x{205F}\x{3000}]")
WS_CLASS_PY = "[\t\n\x0b\x0c\r\x1c-\x1f \x85\xa0  -     　]"
NORM_KEEP_RE2 = r"[^가-힣0-9A-Za-z]"      # fingerprint normalisation: Hangul syllables, digits, Latin
SENT_SPLIT_RE2 = r"[.?!。…\n]+"
DUP_MEETING_CHUNK = 1000                 # dup_meeting_text: meetings per whole-meeting fingerprint query
DATE_RE =r"^\d{4}-\d{2}-\d{2}$"
HHMM_RE = r"^([01][0-9]|2[0-9]):[0-5][0-9]$"
STAFF_POS_RE = (r"(수석전문위원|전문위원|입법조사관|입법심의관|입법조사연구관|의사국장|의사과장|의안과장|속기"
                r"|사무처|행정실장|위원회\s*조사관)")
LEG_TITLE_POS_RE = (r"(부?의장|(소|분과)?위원장(대리|직무대행|직무대리)?|조정위원장|위원|의원|간사|반장"
                    r"|副?議長|委員長|委員|議員)")


def nows_len_sql(expr: str) -> str:
    """Number of non-whitespace characters (Python len(re.sub(r'\\s+', '', s)))."""
    return f"length(regexp_replace(coalesce({expr}, ''), '{WS_CLASS_RE2}', '', 'g'))"


def norm_sql(expr: str) -> str:
    return f"regexp_replace(coalesce({expr}, ''), '{NORM_KEEP_RE2}', '', 'g')"


def _s(x: str) -> str:
    return "'" + str(x).replace("'", "''") + "'"


def _q(c: str) -> str:
    return '"' + c.replace('"', '""') + '"'


# ----------------------------------------------------------------------------- parameters
@dataclasses.dataclass
class Params:
    mode: str = "release"                       # 'release' or 'dev'
    max_missing: int = 0                        # universe meetings with no build and no confirmed no_xml
    dup_containment: float = 0.5
    dup_long_min_chars: int = 100               # normalized chars for a 'long' turn
    dup_long_min_shared: int = 3
    dup_max_df: int = 20                        # fingerprints in more meetings are boilerplate, ignored
    dup_shingle_len: int = 10
    dup_shingle_min_shared: int = 5
    dup_shingle_min_n: int = 30                 # smaller meeting needs >= this many non-boilerplate shingles
    dup_allowlist: tuple = ()                   # ((conf_num_a, conf_num_b, reason), ...)
    wit_title_max_share: float = 0.01           # overall share of dyads with a legislator title on the witness side
    wit_title_max_share_by_type: float = 0.02   # same, per hearing_type
    link_thresholds: dict = dataclasses.field(default_factory=lambda: {16: .97, 17: .97, 18: .97, 19: .99,
                                                                        20: .99, 21: .99, 22: .99})
    # share of legislator-side turns with a party, per term (a party can only be known for a linked turn)
    party_thresholds: dict = dataclasses.field(default_factory=lambda: {16: .97, 17: .97, 18: .97, 19: .99,
                                                                         20: .99, 21: .99, 22: .99})
    max_linked_party_null: int = 0              # linked legislator turns (naas_cd set) without a party
    dup_meeting_min_chars: int = 1              # whole-meeting fingerprint: meetings with >= this many normalized chars
    coverage_sample_xml: int = 200              # built XML meetings re-read from the raw viewer page (seeded)
    coverage_sample_hwp: int = 20               # built HWP meetings re-parsed from the raw HWP file (seeded)
    raw_root: str = str(V10 / "raw")
    build_state: str = str(PIPE / "build_turns" / "state.sqlite")   # read-only: parser version recorded per built meeting
    speech_date_end_slack_days: int = 1         # speech_date may exceed the term end by this many days
    max_meeting_span_days: int = 10             # speech_date <= meeting date + this
    max_label_only_turns: int = 200             # turns with no text_raw (speech printed inside the label) before FAIL
    # researcher decision 6 (2026-09-26): 'last_president_party' = in a partyless-president window the
    # president's most recent party and its lineage successors are ruling, presidency_state 'partyless';
    # 'null' = the earlier rule (ruling_status null, presidency_state 'normal')
    partyless_rule: str = "last_president_party"
    allow_partyless_president_null: bool = False  # derived from partyless_rule ('null' -> True); kept for callers
    suspended_admin: str = "president"          # admin during impeachment suspension ('president' or 'acting')
    procedural_sample: int = 0                  # 0 = re-flag EVERY dyad (default); n > 0 = a seeded sample of n
    dyad_chunk_turns: int = DYAD_CHUNK_TURNS    # dyads_attributes / dyads_flags work in conf_num chunks
    release_root: Optional[str] = None          # release directory scanned by release_no_local_paths
    user_names: tuple = ()                      # extra user names that must not appear (the login name is always added)
    docs_templates: tuple = ()                  # template files with {{ key }} placeholders
    # dyads built with dyads.build_dyads_file(exclude_after_end_marker=...) (config.yaml dyads section)
    dyads_exclude_after_end_marker: bool = False
    # label_how_weak_share: 'weak' = label_confidence 'low', plus any label_how listed here
    label_weak_rules: tuple = ()
    label_weak_max_share: float = 0.001         # per source; above it the check WARNs (never blocks)


    def __post_init__(self):
        if self.partyless_rule not in PARTYLESS_RULES:
            raise ValueError(f"partyless_rule {self.partyless_rule!r} not in {PARTYLESS_RULES}")
        if self.allow_partyless_president_null and self.partyless_rule == "last_president_party":
            self.partyless_rule = "null"          # legacy callers: allow_partyless_president_null=True means 'null'
        if self.partyless_rule == "null":
            self.allow_partyless_president_null = True


class Skip(Exception):
    pass


@dataclasses.dataclass
class Result:
    id: str
    status: str
    doc: str
    n_bad: int = 0
    details: dict = dataclasses.field(default_factory=dict)
    examples: list = dataclasses.field(default_factory=list)
    seconds: float = 0.0


CHECKS: list = []


def check(cid: str, doc: str, needs: Sequence[str] = (), release_required: bool = True):
    def deco(fn):
        CHECKS.append((cid, fn, doc, tuple(needs), release_required))
        return fn
    return deco


# ----------------------------------------------------------------------------- validator
class Validator:
    TABLES = ("turns", "meetings", "dyads", "agenda", "footer", "coverage", "crosswalk", "crosswalk_turns",
              "universe", "crawl", "v9_meetings", "v9_speeches", "calendar", "lineage",
              "duplicate_turns", "duplicate_dyads", "duplicate_meetings")
    # registered under another name; `turns` / `dyads` become release + duplicate copies (see module docstring)
    _RENAME = {"turns": "turns_release", "dyads": "dyads_release", "duplicate_turns": "turns_duplicate",
               "duplicate_dyads": "dyads_duplicate"}

    def __init__(self, tables: dict, params: Optional[Params] = None,
                 con: Optional[duckdb.DuckDBPyConnection] = None, memory_limit: str = "6GB", threads: int = 4):
        self.params = params or Params()
        self.con = con or duckdb.connect()
        self.con.execute(f"SET memory_limit='{memory_limit}'")
        self.con.execute(f"SET threads={threads}")
        self.con.execute("SET enable_progress_bar=false")
        self.con.execute("SET preserve_insertion_order=false")
        self.inputs: dict = {}
        self._cols: dict = {}
        self._cache: dict = {}
        for name in self.TABLES:
            self._register(self._RENAME.get(name, name), tables.get(name))
        for name, rel, dup in (("turns", "turns_release", "turns_duplicate"), ("dyads", "dyads_release", "dyads_duplicate")):
            if self.has(rel) and self.has(dup):
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {rel} UNION ALL BY NAME SELECT * FROM {dup}")
                self.inputs[name] = {"kind": "view", "of": [rel, dup], "rows_duplicate": int(self.scalar(f"SELECT count(*) FROM {dup}"))}
            elif self.has(rel):
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {rel}")
                self.inputs[name] = {"kind": "view", "of": [rel]}
            else:
                self.inputs[name] = self.inputs.get(rel)
        if self.has("calendar"):
            self._build_calendar()

    # -- registration
    def _register(self, name: str, src):
        if src is None:
            self.inputs[name] = None
            return
        if isinstance(src, (pd.DataFrame, pa.Table)):
            self.con.register(f"_df_{name}", src)
            self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM _df_{name}")
            self.inputs[name] = {"kind": "dataframe", "rows": int(len(src))}
        else:
            paths = [str(p) for p in (src if isinstance(src, (list, tuple)) else [src])]
            files = sorted({f for p in paths for f in (_glob.glob(p) if any(ch in p for ch in "*?[") else [p])
                            if os.path.exists(f)})
            if not files:
                self.inputs[name] = {"kind": "missing", "pattern": paths}
                return
            if files[0].endswith(".sqlite"):
                c = sqlite3.connect(f"file:{files[0]}?mode=ro", uri=True)
                df = pd.read_sql_query("SELECT conf_num, kind, status FROM fetch", c)
                c.close()
                self.con.register(f"_df_{name}", df)
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM _df_{name}")
            elif files[0].endswith(".csv"):
                lst = "[" + ", ".join(_s(f) for f in files) + "]"
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_csv({lst}, all_varchar=true, header=true)")
            else:
                lst = "[" + ", ".join(_s(f) for f in files) + "]"
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({lst}, union_by_name=true)")
            self.inputs[name] = {"kind": "files", "n_files": len(files), "pattern": paths}
        self.inputs[name]["columns"] = len(self.cols(name))

    def has(self, name: str) -> bool:
        return bool(self.inputs.get(name)) and self.inputs[name].get("kind") != "missing"

    def cols(self, name: str) -> dict:
        if name not in self._cols:
            if not self.has(name):
                self._cols[name] = {}
            else:
                self._cols[name] = {r[0]: r[1] for r in self.con.execute(f"DESCRIBE SELECT * FROM {name}").fetchall()}
        return self._cols[name]

    def need(self, *names: str):
        miss = [n for n in names if not self.has(n)]
        if miss:
            raise Skip("input table(s) missing: " + ", ".join(miss))

    def need_cols(self, table: str, *cols: str):
        self.need(table)
        miss = [c for c in cols if c not in self.cols(table)]
        if miss:
            raise Skip(f"{table} lacks column(s): " + ", ".join(miss))

    def q(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).fetchdf()

    def one(self, sql: str) -> dict:
        cur = self.con.execute(sql)
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

    def scalar(self, sql: str):
        return self.con.execute(sql).fetchone()[0]

    def examples(self, sql: str, n: int = N_EXAMPLES) -> list:
        df = self.q(f"SELECT * FROM ({sql}) LIMIT {n}")
        return json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))

    # -- helpers
    def _lineage_successors(self) -> dict:
        """{label: [(successor_from, successor)]} from party_lineage.csv renames and mergers (whitespace removed)."""
        out = {}
        if not (self.has("lineage") and all(c in self.cols("lineage") for c in ("label", "successor", "successor_from", "kind"))):
            return out
        for r in self.q("SELECT label, successor, successor_from, kind FROM lineage").itertuples():
            if isinstance(r.successor, str) and r.successor.strip() and isinstance(r.successor_from, str) \
                    and r.successor_from.strip() and r.kind in ("rename", "merger_into"):
                out.setdefault(re.sub(r"\s", "", r.label), []).append((r.successor_from.strip(), re.sub(r"\s", "", r.successor)))
        return out

    def _build_calendar(self):
        """Interval table of the presidency on every date (independent of the enrichment modules).
        Researcher decisions 3 and 6: 'suspended' during an impeachment suspension (the president's party stays
        ruling), 'acting' after a removal (no ruling party), 'partyless' while the president in office has no
        party (Params.partyless_rule 'last_president_party': his most recent party, carried through its lineage
        renames / mergers (party_lineage.csv) to the date, counts as ruling). president_last_party = the formal
        party, else (partyless or suspended without a party) the most recent party (calendar pres_last_party,
        else derived from the earlier rows of the same president); null in acting windows. ruling_ref = the label
        that counts as ruling on the date (rows are split at lineage transitions)."""
        cal = self.q("SELECT * FROM calendar ORDER BY start")
        succ = self._lineage_successors()
        last_by_pres = {}
        rows = []
        for i, r in cal.iterrows():
            st = r["status"]
            acting = r.get("acting_president") or None
            pres = r.get("president") or None
            camp = r.get("camp") or None
            formal = r.get("pres_party_formal") or None
            formal = formal if isinstance(formal, str) and formal.strip() else None
            if formal and pres:
                last_by_pres[pres] = formal
            if st == "in_office":
                state = "normal" if (formal or self.params.partyless_rule == "null") else "partyless"
            elif st == "suspended_impeachment":
                state = "suspended"
            elif st == "vacant_after_removal":
                state = "acting" if acting else "vacant"
            else:
                raise ValueError(f"unknown calendar status {st!r}")
            ideol = {"progressive": "Progressive", "conservative": "Conservative"}.get(camp)
            if state in ("normal", "partyless"):
                admin, adm_ideol = pres, ideol
            elif state == "suspended":
                if self.params.suspended_admin == "acting":
                    admin, adm_ideol = f"권한대행({acting})", None
                else:
                    admin, adm_ideol = pres, ideol
            elif state == "acting":
                admin, adm_ideol = f"권한대행({acting})", None
            else:
                admin, adm_ideol = None, None
            last = None
            if state in ("normal", "partyless", "suspended"):
                given = r.get("pres_last_party") if "pres_last_party" in cal.columns else None
                given = given if isinstance(given, str) and given.strip() else None
                last = formal or given or last_by_pres.get(pres)
            end = r["end"] if isinstance(r["end"], str) and r["end"] else "9999-12-31"
            base = {"presidency_state": state, "admin": admin, "admin_ideology": adm_ideol,
                    "president": pres if state in ("normal", "partyless", "suspended") else None,
                    "president_party": formal, "president_last_party": last}
            # the label counting as ruling: the formal party; without one (partyless rule) the last party carried
            # through the lineage to each date of the row
            if formal:
                segs = [(r["start"], end, re.sub(r"\s", "", formal))]
            elif last and self.params.partyless_rule == "last_president_party" and state in ("partyless", "suspended"):
                segs, cur, a0 = [], re.sub(r"\s", "", last), r["start"]
                for _ in range(30):
                    nxt = [(d, n) for d, n in succ.get(cur, []) if a0 < d <= end]
                    if not nxt:
                        break
                    d, n = min(nxt)
                    segs.append((a0, (_dt.date.fromisoformat(d) - _dt.timedelta(days=1)).isoformat(), cur))
                    cur, a0 = n, d
                segs.append((a0, end, cur))
            else:
                segs = [(r["start"], end, None)]
            for a, b, ref in segs:
                rows.append(dict(base, start=a, end=b, ruling_ref=ref))
        df = pd.DataFrame(rows)
        # contiguity of the calendar itself
        for a, b in zip(df.itertuples(), df.iloc[1:].itertuples()):
            nxt = (_dt.date.fromisoformat(a.end) + _dt.timedelta(days=1)).isoformat()
            if nxt != b.start:
                raise ValueError(f"president calendar not contiguous at {a.end} -> {b.start}")
        self.con.register("_cal_df", df)
        self.con.execute("CREATE OR REPLACE TABLE cal AS SELECT * FROM _cal_df")

    def turn_date_expr(self) -> str:
        """SQL expr giving the date of a turn t (speech_date, else meeting date via m)."""
        return "coalesce(t.speech_date, m.date)" if self.has("meetings") else "t.speech_date"

    def built_expr(self, alias: str = "m") -> str:
        c = self.cols("meetings")
        if "is_built" in c:
            return f"coalesce({alias}.is_built, false)"
        return f"({alias}.n_turns IS NOT NULL)"

    # -- run
    def run(self, only: Optional[Sequence[str]] = None) -> dict:
        results = []
        for cid, fn, doc, needs, req in CHECKS:
            if only and cid not in only:
                continue
            t0 = time.time()
            try:
                res = fn(self)
                res.id, res.doc = cid, doc
            except Skip as e:
                st = "FAIL" if (self.params.mode == "release" and req) else "SKIP"
                res = Result(cid, st, doc, details={"reason": str(e)})
            except Exception as e:  # an error in a check blocks the release
                res = Result(cid, "FAIL", doc, details={"error": repr(e)[:500], "traceback": traceback.format_exc()[-2000:]})
            res.seconds = round(time.time() - t0, 2)
            results.append(res)
        summ = {s: sum(1 for r in results if r.status == s) for s in ("PASS", "FAIL", "WARN", "SKIP")}
        return {
            "validator_version": VALIDATOR_VERSION, "mode": self.params.mode,
            "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
            "params": {k: (list(v) if isinstance(v, tuple) else v) for k, v in dataclasses.asdict(self.params).items()},
            "inputs": self.inputs, "summary": summ, "ok": summ["FAIL"] == 0,
            "checks": [dataclasses.asdict(r) for r in results],
        }


def _res(ok_bad: int, details: dict = None, examples: list = None, warn: bool = False) -> Result:
    st = "PASS" if ok_bad == 0 else ("WARN" if warn else "FAIL")
    return Result("", st, "", n_bad=int(ok_bad), details=details or {}, examples=examples or [])


def _type_ok(actual: str, expected: str) -> bool:
    if expected == "INT":
        return actual in INT_TYPES
    return actual == expected


def _schema(v: Validator, table: str, types: dict) -> tuple:
    c = v.cols(table)
    missing = [k for k in types if k not in c]
    wrong = {k: {"expected": t, "actual": c[k]} for k, t in types.items() if k in c and not _type_ok(c[k], t)}
    return missing, wrong


# ============================================================================= checks
@check("schema_turns", "turns have every CONTRACT column with the CONTRACT type; enrichment columns present")
def c_schema_turns(v: Validator) -> Result:
    v.need("turns")
    missing, wrong = _schema(v, "turns", TURN_TYPES)
    c = v.cols("turns")
    miss_enrich = {k: [x for x in cols if x not in c] for k, cols in ENRICH_COLS.items()}
    miss_enrich = {k: x for k, x in miss_enrich.items() if x}
    wrong_enrich = {k: {"expected": t, "actual": c[k]} for k, t in ENRICH_TYPES.items() if k in c and c[k] != t}
    if "seniority" in c and c["seniority"] not in INT_TYPES:
        wrong_enrich["seniority"] = {"expected": "integer", "actual": c["seniority"]}
    bad = len(missing) + len(wrong) + len(wrong_enrich)
    if v.params.mode == "release":
        bad += sum(len(x) for x in miss_enrich.values())
    r = _res(bad, {"missing": missing, "wrong_type": wrong, "missing_enrichment": miss_enrich,
                   "wrong_enrichment_type": wrong_enrich})
    if bad == 0 and miss_enrich:
        r.status = "WARN"
    return r


@check("schema_meetings", "meetings have every CONTRACT column with the CONTRACT type")
def c_schema_meetings(v: Validator) -> Result:
    v.need("meetings")
    missing, wrong = _schema(v, "meetings", MEETING_TYPES)
    return _res(len(missing) + len(wrong), {"missing": missing, "wrong_type": wrong})


@check("schema_dyads", "dyads are the slim release layout (researcher decision 8): exactly the SLIM_DYAD_TYPES columns, in that order, with those types; no copied turn attribute block and no double prefix (leg_leg_*, wit_leg_*)")
def c_schema_dyads(v: Validator) -> Result:
    v.need("dyads_release")
    missing, wrong = _schema(v, "dyads_release", SLIM_DYAD_TYPES)
    dc = list(v.cols("dyads_release"))
    extra = [c for c in dc if c not in SLIM_DYAD_TYPES]
    double = [c for c in dc if re.match(r"^(leg|wit)_(leg|wit)_", c)]
    order_ok = [c for c in dc if c in SLIM_DYAD_TYPES] == [c for c in SLIM_DYAD_TYPES if c in dc]
    det = {"missing": missing, "wrong_type": wrong, "extra_columns": extra[:50], "n_extra_columns": len(extra),
           "double_prefixed": double, "column_order_ok": order_ok, "n_columns": len(dc)}
    if v.has("dyads_duplicate"):
        dd = list(v.cols("dyads_duplicate"))
        det["duplicate_dyads_columns_differ"] = dd != dc
    bad = len(missing) + len(wrong) + len(extra) + len(double) + (0 if order_ok else 1) + int(det.get("duplicate_dyads_columns_differ", False))
    return _res(bad, det)


@check("dyads_meeting_fields", "every meeting-level column carried in the dyads equals the meetings table value for that conf_num (and every dyad's meeting exists)")
def c_dy_meeting(v: Validator) -> Result:
    v.need("dyads", "meetings")
    dc, mc = v.cols("dyads"), v.cols("meetings")
    cols = [c for c in MEETING_LEVEL_DYAD_COLS if c in dc and c in mc]
    if not cols:
        raise Skip("dyads carry no meeting-level column")
    det = {"columns_compared": cols,
           "dyad_meeting_missing": int(v.scalar("SELECT count(*) FROM dyads d ANTI JOIN meetings m USING (conf_num)"))}
    diff = " + ".join(f"(d.{_q(c)} IS DISTINCT FROM m.{_q(c)})::INT" for c in cols)
    sql = f"SELECT d.conf_num, {', '.join(f'd.{_q(c)} AS {_q(c)}, m.{_q(c)} AS {_q(c + '_meetings')}' for c in cols)} FROM dyads d JOIN meetings m USING (conf_num) WHERE {diff} > 0"
    det["by_column"] = v.one("SELECT " + ", ".join(f"count(*) FILTER (WHERE d.{_q(c)} IS DISTINCT FROM m.{_q(c)}) AS {_q(c)}" for c in cols)
                             + " FROM dyads d JOIN meetings m USING (conf_num)")
    det["dyads_differing"] = int(v.scalar(f"SELECT count(*) FROM ({sql})"))
    return _res(det["dyad_meeting_missing"] + det["dyads_differing"], det, v.examples(sql))


@check("schema_crosswalk", "crosswalk tables have their documented columns and types")
def c_schema_crosswalk(v: Validator) -> Result:
    v.need("crosswalk", "crosswalk_turns")
    m1, w1 = _schema(v, "crosswalk", CROSSWALK_TYPES)
    m2, w2 = _schema(v, "crosswalk_turns", CROSSWALK_TURN_TYPES)
    return _res(len(m1) + len(w1) + len(m2) + len(w2), {"crosswalk_missing": m1, "crosswalk_wrong_type": w1,
                                                         "crosswalk_turns_missing": m2, "crosswalk_turns_wrong_type": w2})


DOMAINS = (
    ("turns", "source", ("xml", "hwp", "xlsx"), False),
    ("turns", "role_group", ("legislator", "nonlegislator", "excluded"), False),
    ("turns", "role", tuple(sorted(LR.ALL_ROLES)), False),
    ("turns", "ruling_status", ("ruling", "opposition", "independent"), True),
    ("turns", "presidency_state", PRESIDENCY_STATES, True),
    ("turns", "label_confidence", LABEL_CONFIDENCE_VALUES, False),
    ("turns", "sitting_how", ("parser", "end_open_markers"), False),
    ("meetings", "class_name", CLASS_NAMES, True),
    ("meetings", "hearing_type", HEARING_TYPES, True),
    ("meetings", "source", ("xml", "hwp", "xlsx"), True),
    ("meetings", "committee_key", COMMITTEE_KEYS, True),
    ("dyads", "direction", ("question", "answer"), False),
    ("dyads", "hearing_type", HEARING_TYPES, True),
    ("dyads", "class_name", CLASS_NAMES, True),
    ("dyads", "committee_key", COMMITTEE_KEYS, True),
    ("dyads", "presidency_state", PRESIDENCY_STATES, True),
    ("dyads", "leg_ruling_status", ("ruling", "opposition", "independent"), True),
    ("dyads", "wit_role_group", ("nonlegislator",), False),
    ("crosswalk", "relation", RELATIONS, False),
    ("crosswalk_turns", "match_type", MATCH_TYPES, False),
)
FORMATS = (("turns", "speech_date", "date"), ("turns", "time_hhmm", "hhmm"), ("meetings", "date", "date"),
           ("meetings", "date_end", "date"))
# identifier / categorical columns that must hold NULL, never '' or 'nan', when a value is missing (D10);
# free-text columns (text, agenda_text, title) are not listed
SENTINEL_COLS = (
    ("turns", ("speaker_pos", "speaker_name", "speaker_label_raw", "naas_cd", "party", "party_lineage", "president",
               "president_party", "ruling_status", "presidency_state", "role", "role_group", "ministry_normalized",
               "minister_panel_id", "admin", "admin_ideology", "source", "speech_date", "time_hhmm")),
    ("meetings", ("conf_id", "v9_meeting_id", "class_name", "hearing_type", "committee_raw", "subcommittee",
                  "committee_key", "source", "date", "date_end")),
    ("dyads", ("direction", "leg_naas_cd", "leg_party", "leg_ruling_status", "wit_role")),
    ("crosswalk", ("v9_meeting_id", "conf_id", "relation")),
)


@check("domains", "categorical columns take only documented values (committee_key in the key list and consistent with hearing_type, presidency_state incl. partyless); is_subcommittee never null; dates are valid YYYY-MM-DD, times HH:MM")
def c_domains(v: Validator) -> Result:
    bad, det, ex = 0, {}, []
    checked = 0
    for t, c, allowed, nullable in DOMAINS:
        if not v.has(t) or c not in v.cols(t):
            continue
        checked += 1
        lst = ", ".join(_s(a) for a in allowed)
        cond = f"{_q(c)} NOT IN ({lst})" + ("" if nullable else f" OR {_q(c)} IS NULL")
        if t == "meetings" and c in ("class_name", "hearing_type", "committee_key"):   # built meetings must carry them
            cond = f"({cond}) OR ({_q(c)} IS NULL AND {v.built_expr('meetings')})"
        n = v.scalar(f"SELECT count(*) FROM {t} WHERE {cond}")
        if n:
            det[f"{t}.{c}"] = {"n_bad": int(n), "values": v.q(f"SELECT {_q(c)} AS value, count(*) n FROM {t} WHERE {cond} GROUP BY 1 ORDER BY 2 DESC LIMIT 10").to_dict("records")}
            bad += int(n)
    for t, c, kind in FORMATS:
        if not v.has(t) or c not in v.cols(t):
            continue
        checked += 1
        if kind == "date":
            cond = f"{_q(c)} IS NOT NULL AND (NOT regexp_full_match({_q(c)}, '{DATE_RE}') OR try_strptime({_q(c)}, '%Y-%m-%d') IS NULL)"
        else:
            cond = f"{_q(c)} IS NOT NULL AND NOT regexp_full_match({_q(c)}, '{HHMM_RE}')"
        n = v.scalar(f"SELECT count(*) FROM {t} WHERE {cond}")
        if n:
            det[f"{t}.{c}"] = {"n_bad": int(n), "values": v.q(f"SELECT {_q(c)} AS value, count(*) n FROM {t} WHERE {cond} GROUP BY 1 ORDER BY 2 DESC LIMIT 10").to_dict("records")}
            bad += int(n)
    # booleans that must never be null (audit D 2026-09-26: a null is_subcommittee passed every check)
    for t, c in (("meetings", "is_subcommittee"), ("dyads", "is_subcommittee")):
        if not v.has(t) or c not in v.cols(t):
            continue
        checked += 1
        n = v.scalar(f"SELECT count(*) FROM {t} WHERE {_q(c)} IS NULL")
        if n:
            det[f"{t}.{c}.null"] = {"n_bad": int(n)}
            bad += int(n)
    # committee_key agrees with hearing_type: the special types have one key each, standing committees and
    # audits a standing-committee key
    for t in ("meetings", "dyads"):
        if not v.has(t) or not {"committee_key", "hearing_type"} <= set(v.cols(t)):
            continue
        checked += 1
        special = " ".join(f"WHEN hearing_type = {_s(h)} THEN committee_key IS DISTINCT FROM {_s(k)}"
                           for h, k in HEARING_TYPE_KEYS.items())
        std = ", ".join(_s(x) for x in STANDING_COMMITTEE_KEYS)
        cond = (f"committee_key IS NOT NULL AND hearing_type IS NOT NULL AND (CASE {special} "
                f"ELSE committee_key NOT IN ({std}) END)")
        n = v.scalar(f"SELECT count(*) FROM {t} WHERE {cond}")
        if n:
            det[f"{t}.committee_key_vs_hearing_type"] = {"n_bad": int(n), "values": v.q(
                f"SELECT hearing_type, committee_key, count(*) n FROM {t} WHERE {cond} GROUP BY ALL ORDER BY n DESC LIMIT 10").to_dict("records")}
            bad += int(n)
    for t, cols in SENTINEL_COLS:        # D10: '' / 'nan' / 'None' stored instead of NULL
        if not v.has(t):
            continue
        for c in cols:
            if c not in v.cols(t):
                continue
            checked += 1
            n = v.scalar(f"SELECT count(*) FROM {t} WHERE {_q(c)} IS NOT NULL AND {blank_sql(_q(c))}")
            if n:
                det[f"{t}.{c}.sentinel_string"] = {"n_bad": int(n), "values": v.q(
                    f"SELECT {_q(c)} AS value, count(*) n FROM {t} WHERE {_q(c)} IS NOT NULL AND {blank_sql(_q(c))} GROUP BY 1 ORDER BY 2 DESC LIMIT 10").to_dict("records")}
                bad += int(n)
    if checked == 0:
        raise Skip("no domain column available")
    det["n_columns_checked"] = checked
    return _res(bad, det)


KEYS = (
    ("turns", ("conf_num", "turn_seq"), None),
    ("meetings", ("conf_num",), None),
    ("meetings", ("conf_id",), "conf_id IS NOT NULL"),
    ("dyads", ("conf_num", "leg_turn_seq", "wit_turn_seq"), None),
    ("agenda", ("conf_num", "ordinal"), None),
    ("footer", ("conf_num", "section_seq", "coalesce(group_seq, -1)", "coalesce(item_seq, -1)"), None),
    ("crosswalk", ("v9_meeting_id",), "v9_meeting_id IS NOT NULL"),
    ("crosswalk", ("conf_num",), "relation = 'v10_only'"),
    ("crosswalk_turns", ("v9_meeting_id", "coalesce(v9_speech_order, '')", "coalesce(conf_num, -1)",
                         "coalesce(turn_seq, -1)"), None),
)
NOT_NULL = (("turns", ("conf_num", "turn_seq")), ("dyads", ("conf_num", "leg_turn_seq", "wit_turn_seq")),
            ("meetings", ("conf_num",)), ("agenda", ("conf_num", "ordinal")))


@check("keys_unique", "primary keys are unique and non-null in every table")
def c_keys(v: Validator) -> Result:
    bad, det = 0, {}
    ex = []
    for t, cols, where in KEYS:
        if not v.has(t):
            continue
        plain = [re.sub(r"coalesce\((\w+),.*\)", r"\1", c) for c in cols]
        if any(p not in v.cols(t) for p in plain):
            det[f"{t}({','.join(plain)})"] = "columns absent"
            if v.params.mode == "release" and t in ("turns", "meetings", "dyads"):
                bad += 1
            continue
        k = ", ".join(cols)
        w = f"WHERE {where}" if where else ""
        n = v.scalar(f"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) n FROM {t} {w} GROUP BY {k} HAVING count(*) > 1)")
        det[f"{t}({','.join(plain)})"] = int(n)
        if n:
            bad += int(n)
            ex += v.examples(f"SELECT '{t}' AS tbl, {k}, count(*) n FROM {t} {w} GROUP BY {k} HAVING count(*) > 1", 3)
    for t, cols in NOT_NULL:
        if not v.has(t) or any(c not in v.cols(t) for c in cols):
            continue
        n = v.scalar(f"SELECT count(*) FROM {t} WHERE " + " OR ".join(f"{c} IS NULL" for c in cols))
        det[f"{t}.null_keys"] = int(n)
        bad += int(n)
    if not det:
        raise Skip("no keyed table available")
    return _res(bad, det, ex)


@check("turns_contiguous", "turn_seq is exactly 1..n within every meeting", needs=("turns",))
def c_contig(v: Validator) -> Result:
    v.need_cols("turns", "conf_num", "turn_seq")
    sql = """SELECT conf_num, count(*) n, count(DISTINCT turn_seq) nd, min(turn_seq) mn, max(turn_seq) mx
             FROM turns GROUP BY 1 HAVING min(turn_seq) <> 1 OR max(turn_seq) <> count(*) OR count(DISTINCT turn_seq) <> count(*)"""
    n = v.scalar(f"SELECT count(*) FROM ({sql})")
    return _res(n, {"n_meetings": int(v.scalar("SELECT count(DISTINCT conf_num) FROM turns")),
                    "n_meetings_noncontiguous": int(n)}, v.examples(sql))


@check("turns_meetings_consistency", "turns and meetings agree: every turn's meeting exists and is built, n_turns equals the turn count, one source per meeting, built meetings carry term/class/type/date")
def c_turns_meetings(v: Validator) -> Result:
    v.need("turns", "meetings")
    b = v.built_expr("m")
    det = {}
    det["turns_without_meeting"] = int(v.scalar("SELECT count(*) FROM turns t ANTI JOIN meetings m USING (conf_num)"))
    cnt = "(SELECT conf_num, count(*) n, count(DISTINCT source) ns, min(source) s FROM turns GROUP BY 1)"
    det["n_turns_mismatch"] = int(v.scalar(f"SELECT count(*) FROM meetings m JOIN {cnt} c USING (conf_num) WHERE m.n_turns IS DISTINCT FROM c.n"))
    det["turns_for_unbuilt_meeting"] = int(v.scalar(f"SELECT count(*) FROM meetings m JOIN {cnt} c USING (conf_num) WHERE NOT {b}"))
    det["built_with_turns_expected_but_none"] = int(v.scalar(f"SELECT count(*) FROM meetings m ANTI JOIN {cnt} c USING (conf_num) WHERE {b} AND coalesce(m.n_turns, 0) > 0"))
    det["meetings_with_multiple_sources"] = int(v.scalar(f"SELECT count(*) FROM {cnt} WHERE ns > 1"))
    det["source_differs_from_meetings"] = int(v.scalar(f"SELECT count(*) FROM meetings m JOIN {cnt} c USING (conf_num) WHERE m.source IS DISTINCT FROM c.s AND c.ns = 1")) \
        if "source" in v.cols("meetings") else 0
    need = [c for c in ("term", "class_name", "hearing_type", "date") if c in v.cols("meetings")]
    det["built_missing_core_fields"] = int(v.scalar(
        f"SELECT count(*) FROM meetings m WHERE {b} AND (" + " OR ".join(f"m.{c} IS NULL" for c in need) + ")")) if need else 0
    bad = sum(det.values())
    ex = v.examples(f"SELECT m.conf_num, m.n_turns, c.n AS n_turn_rows, m.source, c.s AS turn_source FROM meetings m JOIN {cnt} c USING (conf_num) WHERE m.n_turns IS DISTINCT FROM c.n OR NOT {b}")
    return _res(bad, det, ex)


@check("turns_text", "text_raw non-null except turns whose printed speech sits in the label (WARN); text non-null except turns "
                     "that are stage directions only; text never has more non-whitespace characters than text_raw")
def c_turns_text(v: Validator) -> Result:
    v.need_cols("turns", "text_raw", "text")
    stage = "coalesce(has_stage, false)" if "has_stage" in v.cols("turns") else "false"
    r = v.one(f"""SELECT count(*) FILTER (WHERE text_raw IS NULL OR nr = 0) AS text_raw_null_or_empty,
                         count(*) FILTER (WHERE text IS NULL AND text_raw IS NOT NULL AND {stage}) AS text_null_stage_only,
                         count(*) FILTER (WHERE text IS NULL AND text_raw IS NOT NULL AND NOT {stage}) AS text_null_unexplained,
                         count(*) FILTER (WHERE nt > nr) AS text_longer_than_raw
                  FROM (SELECT text, text_raw, has_stage, {nows_len_sql('text')} AS nt, {nows_len_sql('text_raw')} AS nr
                        FROM (SELECT text, text_raw, {stage} AS has_stage FROM turns))""")
    det = {k: int(x or 0) for k, x in r.items()}
    bad = det["text_null_unexplained"] + det["text_longer_than_raw"]
    warn_n = det["text_raw_null_or_empty"]
    blocking = bad + max(0, warn_n - v.params.max_label_only_turns)
    ex = v.examples(f"""SELECT conf_num, turn_seq, left(speaker_label_raw, 60) AS label, left(text, 60) AS text_head,
                               left(text_raw, 60) AS text_raw_head FROM turns
                        WHERE (text IS NULL AND text_raw IS NOT NULL AND NOT {stage}) OR text_raw IS NULL
                           OR {nows_len_sql('text')} > {nows_len_sql('text_raw')}""") if (bad or warn_n) else []
    det["max_label_only_turns"] = v.params.max_label_only_turns
    return _res(blocking, det, ex) if blocking else _res(warn_n, det, ex, warn=True)


def universe_status_sql(v: Validator) -> str:
    """One row per universe meeting with status built / built_empty / no_xml / missing(+reason)."""
    b = v.built_expr("m")
    if v.has("crawl"):
        crawl = """(SELECT conf_num,
                      max(CASE WHEN kind='view' THEN status END) AS view_status,
                      max(CASE WHEN kind='hwp' THEN status END) AS hwp_status
                    FROM crawl GROUP BY 1)"""
    else:
        crawl = "(SELECT NULL::BIGINT AS conf_num, NULL::VARCHAR AS view_status, NULL::VARCHAR AS hwp_status WHERE false)"
    return f"""
SELECT u.CONFER_NUM AS conf_num, u.DAE_NUM AS term, u.CLASS_NAME_unified AS class_name,
       (m.conf_num IS NOT NULL) AS in_meetings, c.view_status, c.hwp_status,
       CASE WHEN m.conf_num IS NULL THEN 'absent_from_meetings'
            WHEN {b} AND coalesce(m.n_turns, 0) > 0 THEN 'built'
            WHEN {b} THEN 'built_empty'
            WHEN c.view_status IN ('no_xml', 'not_found') AND c.hwp_status IN ('no_xml', 'not_found') THEN 'no_xml'
            ELSE 'missing' END AS status,
       CASE WHEN m.conf_num IS NULL OR {b} THEN NULL
            WHEN c.view_status IN ('no_xml', 'not_found') AND c.hwp_status IN ('no_xml', 'not_found') THEN NULL
            WHEN c.view_status = 'ok' OR c.hwp_status = 'ok' THEN 'source_fetched_not_built'
            WHEN c.view_status = 'failed' OR c.hwp_status = 'failed' THEN 'fetch_failed'
            WHEN c.view_status IN ('no_xml', 'not_found') THEN 'no_xml_hwp_not_fetched'
            ELSE 'not_fetched' END AS missing_reason
FROM universe u LEFT JOIN meetings m ON m.conf_num = u.CONFER_NUM LEFT JOIN {crawl} c ON c.conf_num = u.CONFER_NUM"""


def outside_universe_sql(v: Validator) -> str:
    """Meetings absent from meeting_universe_api.parquet (e.g. found by the id-gap scan), with the facts
    that stand in for the Open API: in_universe flag, printed date, and whether a source was fetched."""
    mc = v.cols("meetings")
    b = v.built_expr("m")
    iu = "m.in_universe" if "in_universe" in mc else "NULL::BOOLEAN"
    dp = "m.date_printed" if "date_printed" in mc else "NULL::VARCHAR"
    if v.has("crawl"):
        fetched = "(SELECT DISTINCT conf_num FROM crawl WHERE status = 'ok' AND kind IN ('view', 'hwp'))"
        fetched_expr = "(f.conf_num IS NOT NULL)"
    else:
        fetched = "(SELECT NULL::BIGINT AS conf_num WHERE false)"
        fetched_expr = "NULL::BOOLEAN"
    return f"""SELECT m.conf_num, m.conf_id, m.date, {dp} AS date_printed, {iu} AS in_universe, {b} AS is_built,
                      {'m.source' if 'source' in mc else 'NULL'} AS source, {fetched_expr} AS source_fetched
               FROM meetings m ANTI JOIN universe u ON u.CONFER_NUM = m.conf_num
               LEFT JOIN {fetched} f ON f.conf_num = m.conf_num"""


@check("universe_accounted", "every universe meeting is in meetings with a status (built / built_empty / no_xml / missing); missing meetings block the release; a meeting outside the Open API universe (id-gap scan) must carry in_universe=false, no conf_id, a fetched source and a date equal to its printed date")
def c_universe(v: Validator) -> Result:
    v.need("universe", "meetings")
    sql = universe_status_sql(v)
    counts = v.q(f"SELECT status, count(*) n FROM ({sql}) GROUP BY 1 ORDER BY 1")
    st = {r.status: int(r.n) for r in counts.itertuples()}
    reasons = v.q(f"SELECT missing_reason, count(*) n FROM ({sql}) WHERE status='missing' GROUP BY 1").to_dict("records")
    by_term = v.q(f"SELECT term, status, count(*) n FROM ({sql}) GROUP BY ALL ORDER BY ALL").to_dict("records")
    absent = st.get("absent_from_meetings", 0)
    missing = st.get("missing", 0)
    # meetings outside the universe: no API row to compare conf_id / date with, so they must pass these instead
    osql = outside_universe_sql(v)
    o = v.one(f"""SELECT count(*) AS meetings_outside_universe,
                         count(*) FILTER (WHERE is_built) AS outside_built,
                         count(*) FILTER (WHERE NOT is_built) AS outside_not_built,
                         count(*) FILTER (WHERE in_universe IS DISTINCT FROM false) AS outside_in_universe_flag_not_false,
                         count(*) FILTER (WHERE conf_id IS NOT NULL) AS outside_with_conf_id,
                         count(*) FILTER (WHERE is_built AND source_fetched IS DISTINCT FROM true) AS outside_built_source_not_fetched,
                         count(*) FILTER (WHERE date IS NULL OR date_printed IS NULL OR date IS DISTINCT FROM date_printed) AS outside_date_unverified
                  FROM ({osql})""")
    o = {k: int(x or 0) for k, x in o.items()}
    inside_flag = int(v.scalar("SELECT count(*) FROM meetings m SEMI JOIN universe u ON u.CONFER_NUM = m.conf_num WHERE m.in_universe IS DISTINCT FROM true")) \
        if "in_universe" in v.cols("meetings") else 0
    outside_bad = (o["outside_in_universe_flag_not_false"] + o["outside_with_conf_id"] + o["outside_built_source_not_fetched"]
                   + o["outside_date_unverified"])
    over = max(0, missing + o["outside_not_built"] - v.params.max_missing)
    bad = absent + inside_flag + outside_bad + (over if v.params.mode == "release" else 0)
    det = {"status_counts": st, "n_universe": int(sum(st.values())), "missing_reasons": reasons,
           "max_missing": v.params.max_missing, "by_term": by_term, "crawl_available": v.has("crawl"),
           "inside_in_universe_flag_not_true": inside_flag, **o}
    ex = v.examples(f"SELECT 'universe' AS kind, conf_num, status, missing_reason, NULL AS date, NULL AS date_printed FROM ({sql}) WHERE status IN ('absent_from_meetings','missing')", 5)
    ex += v.examples(f"""SELECT 'outside_universe' AS kind, conf_num, conf_id, in_universe, is_built, source_fetched, date, date_printed
                         FROM ({osql}) ORDER BY conf_num""", 5)
    r = _res(bad, det, ex)
    if r.status == "PASS" and missing + o["outside_not_built"] > v.params.max_missing:
        r.status = "WARN"
    return r


@check("coverage_builder", "the builder's per-meeting text accounting (coverage.ok_all) holds for every built meeting")
def c_cov_builder(v: Validator) -> Result:
    v.need("coverage", "meetings")
    b = v.built_expr("m")
    det = {
        "built_without_coverage_row": int(v.scalar(f"SELECT count(*) FROM meetings m ANTI JOIN coverage c USING (conf_num) WHERE {b}")),
        "ok_all_false": int(v.scalar("SELECT count(*) FROM coverage WHERE ok_all = false")),
        "ok_all_null": int(v.scalar("SELECT count(*) FROM coverage WHERE ok_all IS NULL")),
        "by_source": v.q("SELECT source, count(*) n, count(*) FILTER (WHERE ok_all) n_ok, count(*) FILTER (WHERE NOT ok_all) n_not_ok FROM coverage GROUP BY 1 ORDER BY 1").to_dict("records"),
    }
    bad = det["built_without_coverage_row"] + det["ok_all_false"]
    return _res(bad, det, v.examples("SELECT * EXCLUDE (detail_json) FROM coverage WHERE ok_all = false" if "detail_json" in v.cols("coverage") else "SELECT * FROM coverage WHERE ok_all = false"))


@check("coverage_recompute", "independent recount from the turns table equals the builder's recorded n_turns and non-whitespace text_raw characters; a null recorded character count, or a meeting with turns but no coverage row, is an error")
def c_cov_recompute(v: Validator) -> Result:
    v.need("coverage", "turns")
    v.need_cols("coverage", "n_turns", "turn_text_raw_chars")
    sql = f"""WITH r AS (SELECT conf_num, count(*) n, sum({nows_len_sql('text_raw')}) ch FROM turns GROUP BY 1)
              SELECT c.conf_num, c.n_turns, r.n AS recount_turns, c.turn_text_raw_chars, r.ch AS recount_chars
              FROM coverage c LEFT JOIN r USING (conf_num)
              WHERE c.n_turns IS DISTINCT FROM coalesce(r.n, 0)
                 OR c.turn_text_raw_chars IS DISTINCT FROM coalesce(r.ch, 0)"""
    n = int(v.scalar(f"SELECT count(*) FROM ({sql})"))
    tot = v.one(f"SELECT count(*) n_meetings, sum(turn_text_raw_chars) recorded_chars FROM coverage")
    tot["recount_chars"] = int(v.scalar(f"SELECT sum({nows_len_sql('text_raw')}) FROM turns SEMI JOIN coverage USING (conf_num)") or 0)
    det = dict(tot, n_meetings_mismatch=n,
               recorded_chars_null=int(v.scalar("SELECT count(*) FROM coverage WHERE turn_text_raw_chars IS NULL")),
               turn_meetings_without_coverage_row=int(v.scalar("SELECT count(*) FROM (SELECT DISTINCT conf_num FROM turns) t ANTI JOIN coverage c USING (conf_num)")))
    bad = n + det["turn_meetings_without_coverage_row"]      # a null recorded count is part of n (IS DISTINCT FROM)
    return _res(bad, det, v.examples(sql))


def _raw_view_path(root: Path, n: int) -> Optional[Path]:
    """Saved viewer page of meeting n: the crawler's store, else the saved samples."""
    for p in (root / "viewer" / "view" / f"{n // 1000:03d}" / f"{n}.html.gz",
              root / "samples" / f"{n}_view.html.gz", root / "samples_random" / f"{n}_view.html.gz"):
        if p.exists():
            return p
    return None


def _raw_hwp_path(root: Path, n: int) -> Optional[Path]:
    p = root / "hwp" / f"{n // 1000:03d}" / f"{n}.hwp"
    return p if p.exists() else None


_PY_WS = re.compile(r"\s+")


def _nows_len(x: str) -> int:
    return len(_PY_WS.sub("", x or ""))


def dom_speech_chars(page: bytes) -> int:
    """Non-whitespace characters of the spoken text in a viewer page: every div.txt directly under the
    div.talk of a speaker div in the minutes body (the text the turns' text_raw must hold in full)."""
    import lxml.html as LH
    t = LH.fromstring(page)
    body = t.xpath('//div[@id="minutes"]/div[contains(@class,"minutes_body")]')
    if not body:
        return 0
    tot = 0
    for d in body[0].xpath('.//div[contains(concat(" ",normalize-space(@class)," ")," speaker ")]'):
        tot += sum(_nows_len(x.text_content()) for x in d.xpath('./div[@class="talk"]/div[@class="txt"]'))
    return tot


@check("coverage_source_sample", "text is not lost between the raw source and the turns: every built XLSX meeting's turns hold exactly the non-whitespace characters of its v9 speech_text rows; a seeded sample of built XML meetings is re-read from the saved viewer page (speaker div.txt) and of built HWP meetings re-parsed from the saved HWP file (per-turn text_raw), and must match the stored turns; the raw file must be the one the builder read (raw_sha1)")
def c_cov_source(v: Validator) -> Result:
    import gzip
    import hashlib
    v.need("turns", "meetings")
    v.need_cols("meetings", "source")
    p = v.params
    root = Path(p.raw_root)
    b = v.built_expr("m")
    det, ex, bad = {}, [], 0
    # --- XLSX: all built meetings, v9 speech_text vs turns (inside duckdb)
    n_xlsx = int(v.scalar(f"SELECT count(*) FROM meetings m WHERE {b} AND m.source = 'xlsx'"))
    det["xlsx_built"] = n_xlsx
    if n_xlsx:
        ok_inputs = (v.has("v9_speeches") and "speech_text" in v.cols("v9_speeches") and "v9_meeting_id" in v.cols("meetings"))
        if not ok_inputs:
            det["xlsx_unverifiable"] = n_xlsx
            det["xlsx_reason"] = "v9_speeches.speech_text or meetings.v9_meeting_id not available"
            bad += n_xlsx if p.mode == "release" else 0
        else:
            q = f"""WITH x AS (SELECT m.conf_num, m.v9_meeting_id FROM meetings m WHERE {b} AND m.source = 'xlsx'),
                    s AS (SELECT x.conf_num, count(s.meeting_id) n, sum({nows_len_sql('s.speech_text')}) ch
                          FROM x LEFT JOIN v9_speeches s ON s.meeting_id = x.v9_meeting_id GROUP BY 1),
                    r AS (SELECT conf_num, count(*) n, sum({nows_len_sql('text_raw')}) ch FROM turns SEMI JOIN x USING (conf_num) GROUP BY 1)
                    SELECT s.conf_num, s.n AS v9_rows, r.n AS turns, s.ch AS v9_chars, r.ch AS turn_chars
                    FROM s LEFT JOIN r USING (conf_num)
                    WHERE s.n = 0 OR coalesce(s.ch, 0) IS DISTINCT FROM coalesce(r.ch, 0)"""
            k = int(v.scalar(f"SELECT count(*) FROM ({q})"))
            det["xlsx_meetings_differ"] = k
            bad += k
            ex += [dict(source="xlsx", **r) for r in v.examples(q, 5)]
    # --- XML / HWP samples (seeded), read in Python from the saved raw files
    has_sha = "raw_sha1" in v.cols("meetings")
    recorded = {}          # conf_num -> parser sha8 recorded by the builder ('<version>+<parser sha8>+<adapter sha8>')
    if p.build_state and Path(p.build_state).exists():
        try:
            c = sqlite3.connect(f"file:{p.build_state}?mode=ro", uri=True)
            recorded = {int(n): (bv.split("+")[1] if isinstance(bv, str) and bv.count("+") >= 2 else None)
                        for n, bv in c.execute("SELECT conf_num, builder_version FROM built WHERE source = 'hwp'")}
            c.close()
        except sqlite3.Error:
            recorded = {}
    cur_hwp_sha8 = hashlib.sha1((HERE / "hwp_parser.py").read_bytes()).hexdigest()[:8] if (HERE / "hwp_parser.py").exists() else None
    det["hwp_parser_sha8_current"] = cur_hwp_sha8
    for src, n_s in (("xml", p.coverage_sample_xml), ("hwp", p.coverage_sample_hwp)):
        n_built = int(v.scalar(f"SELECT count(*) FROM meetings m WHERE {b} AND m.source = '{src}'"))
        det[f"{src}_built"] = n_built
        if not n_built or n_s <= 0:
            continue
        samp = v.q(f"""SELECT m.conf_num, {'m.raw_sha1' if has_sha else 'NULL'} AS raw_sha1 FROM meetings m
                       WHERE {b} AND m.source = '{src}' ORDER BY hash(m.conf_num, {SEED}) LIMIT {int(n_s)}""")
        ids = [int(x) for x in samp.conf_num]
        v.con.execute("CREATE OR REPLACE TEMP TABLE _cov_ids AS SELECT unnest($1::BIGINT[]) AS conf_num", [ids])
        stored = v.q(f"""SELECT conf_num, turn_seq, {nows_len_sql('text_raw')} AS ch FROM turns
                         WHERE conf_num IN (SELECT conf_num FROM _cov_ids) ORDER BY conf_num, turn_seq""")
        by = {c: g for c, g in stored.groupby("conf_num")}
        c_missing = c_sha = c_diff = 0
        for cn, sh in zip(ids, samp.raw_sha1):
            path = _raw_view_path(root, cn) if src == "xml" else _raw_hwp_path(root, cn)
            if path is None:
                c_missing += 1
                ex.append({"source": src, "conf_num": cn, "problem": "raw file missing"})
                continue
            data = path.read_bytes()
            if src == "xml":
                data = gzip.decompress(data) if path.name.endswith(".gz") else data
            if has_sha and isinstance(sh, str) and len(sh) == 40 and hashlib.sha1(data).hexdigest() != sh:
                c_sha += 1
                ex.append({"source": src, "conf_num": cn, "problem": "raw file differs from the one built (raw_sha1)"})
                continue
            g = by.get(cn)
            got = [] if g is None else [int(x) for x in g.ch]
            if src == "xml":
                exp_total = dom_speech_chars(data)
                if exp_total != sum(got):
                    c_diff += 1
                    ex.append({"source": src, "conf_num": cn, "raw_speech_chars": exp_total, "turn_chars": sum(got)})
            else:
                import importlib
                hp = importlib.import_module("hwp_parser")
                res = hp.parse_hwp(data, conf_num=cn)
                exp = [_nows_len(t.get("text_raw")) for t in res.get("turns") or []]
                if exp != got:
                    c_diff += 1
                    rec = recorded.get(cn)
                    why = ("parser_changed_since_build" if rec and cur_hwp_sha8 and rec != cur_hwp_sha8
                           else "text_differs" if rec else "text_differs_parser_version_unknown")
                    det[f"hwp_differ_{why}"] = det.get(f"hwp_differ_{why}", 0) + 1
                    first = next((i for i, (x, y) in enumerate(zip(exp, got)) if x != y), min(len(exp), len(got)))
                    ex.append({"source": src, "conf_num": cn, "why": why, "raw_turns": len(exp), "stored_turns": len(got),
                               "raw_chars": sum(exp), "turn_chars": sum(got), "first_differing_turn_seq": first + 1})
        det[f"{src}_sampled"] = len(ids)
        det[f"{src}_raw_missing"] = c_missing
        det[f"{src}_raw_changed_since_build"] = c_sha
        det[f"{src}_differ"] = c_diff
        bad += c_missing + c_sha + c_diff
    return _res(bad, det, ex[:N_EXAMPLES])


@check("dyads_adjacent", "every dyad pairs numerically adjacent turns (|leg_turn_seq - wit_turn_seq| = 1)")
def c_dy_adj(v: Validator) -> Result:
    v.need_cols("dyads", "leg_turn_seq", "wit_turn_seq")
    sql = "SELECT conf_num, leg_turn_seq, wit_turn_seq FROM dyads WHERE abs(leg_turn_seq - wit_turn_seq) <> 1 OR leg_turn_seq IS NULL OR wit_turn_seq IS NULL"
    n = int(v.scalar(f"SELECT count(*) FROM ({sql})"))
    return _res(n, {"n_dyads": int(v.scalar("SELECT count(*) FROM dyads")), "n_nonadjacent": n}, v.examples(sql))


@check("dyads_endpoints", "both dyad endpoints exist in the same meeting's turns, with role_group legislator / nonlegislator, and carry those turns' texts verbatim (no cross-meeting or misaligned pairs)")
def c_dy_ends(v: Validator) -> Result:
    v.need("dyads", "turns")
    v.need_cols("turns", "role_group")
    dc = v.cols("dyads")
    # compare md5 digests, not texts, so the join never holds two copies of the corpus (md5(NULL) is NULL)
    tsel, dsel, cmp = [], [], []
    for side, alias in (("leg", "L"), ("wit", "W")):
        for c in ("text", "text_raw"):
            if f"{side}_{c}" in dc and c in v.cols("turns"):
                dsel.append(f"md5(d.{side}_{c}) AS {side}_{c}_h")
                cmp.append(f"d.{side}_{c}_h IS DISTINCT FROM {alias}.{c}_h")
    for c in ("text", "text_raw"):
        if c in v.cols("turns"):
            tsel.append(f"md5({c}) AS {c}_h")
    tk = f"(SELECT conf_num, turn_seq, role_group{''.join(', ' + x for x in tsel)} FROM turns)"
    dd = f"(SELECT d.conf_num, d.leg_turn_seq, d.wit_turn_seq{''.join(', ' + x for x in dsel)} FROM dyads d)"
    sql = f"""SELECT d.conf_num, d.leg_turn_seq, d.wit_turn_seq,
                     (L.turn_seq IS NULL) AS leg_missing, (W.turn_seq IS NULL) AS wit_missing,
                     L.role_group AS leg_group, W.role_group AS wit_group,
                     ({' OR '.join(cmp) if cmp else 'false'}) AS text_differs
              FROM {dd} d
              LEFT JOIN {tk} L ON L.conf_num = d.conf_num AND L.turn_seq = d.leg_turn_seq
              LEFT JOIN {tk} W ON W.conf_num = d.conf_num AND W.turn_seq = d.wit_turn_seq"""
    det = v.one(f"""SELECT count(*) FILTER (WHERE leg_missing OR wit_missing) AS endpoint_missing,
                           count(*) FILTER (WHERE NOT leg_missing AND leg_group IS DISTINCT FROM 'legislator') AS leg_not_legislator,
                           count(*) FILTER (WHERE NOT wit_missing AND wit_group IS DISTINCT FROM 'nonlegislator') AS wit_not_nonlegislator,
                           count(*) FILTER (WHERE NOT leg_missing AND NOT wit_missing AND text_differs) AS text_differs
                    FROM ({sql})""")
    det = {k: int(x or 0) for k, x in det.items()}
    bad = sum(det.values())
    ex = v.examples(f"SELECT * FROM ({sql}) WHERE leg_missing OR wit_missing OR leg_group IS DISTINCT FROM 'legislator' OR wit_group IS DISTINCT FROM 'nonlegislator' OR text_differs") if bad else []
    return _res(bad, det, ex)


def expected_dyads_sql(v: Optional["Validator"] = None) -> str:
    """Independent dyad recomputation with a LEAD window (not the self-join used by dyads.py). Both turns
    must have the same non-null sitting_seq (when turns have the column); with
    Params.dyads_exclude_after_end_marker a turn after a meeting-end marker counts as excluded."""
    cols = v.cols("turns") if v is not None and v.has("turns") else {}
    sit = "sitting_seq" if "sitting_seq" in cols else "1"
    grp = "role_group"
    if v is not None and v.params.dyads_exclude_after_end_marker and "after_end_marker" in cols:
        grp = "(CASE WHEN coalesce(after_end_marker, false) THEN 'excluded_after_end' ELSE role_group END)"
    return f"""
WITH s AS (SELECT conf_num, turn_seq, {grp} AS g, {sit} AS sit,
                  lead(turn_seq) OVER w AS nseq, lead({grp}) OVER w AS ng, lead({sit}) OVER w AS nsit
           FROM turns WINDOW w AS (PARTITION BY conf_num ORDER BY turn_seq))
SELECT conf_num,
       CASE WHEN g = 'legislator' THEN turn_seq ELSE nseq END AS leg,
       CASE WHEN g = 'legislator' THEN nseq ELSE turn_seq END AS wit,
       CASE WHEN g = 'legislator' THEN 'question' ELSE 'answer' END AS direction
FROM s WHERE nseq = turn_seq + 1 AND nsit = sit
  AND ((g = 'legislator' AND ng = 'nonlegislator') OR (g = 'nonlegislator' AND ng = 'legislator'))"""


@check("dyads_recompute", "the dyad set equals an independent LEAD-window recomputation from turns (count and every key)")
def c_dy_recompute(v: Validator) -> Result:
    v.need("dyads", "turns")
    v.need_cols("turns", "role_group")
    e = expected_dyads_sql(v)
    det = v.one(f"""SELECT (SELECT count(*) FROM ({e})) AS n_expected, (SELECT count(*) FROM dyads) AS n_dyads,
        (SELECT count(*) FROM (SELECT conf_num, leg, wit, direction FROM ({e}) EXCEPT ALL
                               SELECT conf_num, leg_turn_seq, wit_turn_seq, direction FROM dyads)) AS missing,
        (SELECT count(*) FROM (SELECT conf_num, leg_turn_seq, wit_turn_seq, direction FROM dyads EXCEPT ALL
                               SELECT conf_num, leg, wit, direction FROM ({e}))) AS extra""")
    det = {k: int(x or 0) for k, x in det.items()}
    bad = det["missing"] + det["extra"] + (0 if det["n_expected"] == det["n_dyads"] else 1)
    ex = v.examples(f"""(SELECT 'extra' AS kind, conf_num, leg_turn_seq AS leg, wit_turn_seq AS wit FROM dyads
                         EXCEPT SELECT 'extra', conf_num, leg, wit FROM ({e}))
                        UNION ALL (SELECT 'missing', conf_num, leg, wit FROM ({e})
                         EXCEPT SELECT 'missing', conf_num, leg_turn_seq, wit_turn_seq FROM dyads)""")
    return _res(bad, det, ex)


@check("dyads_direction", "direction is 'question' exactly when the legislator turn comes first")
def c_dy_dir(v: Validator) -> Result:
    v.need_cols("dyads", "direction", "leg_turn_seq", "wit_turn_seq")
    sql = """SELECT conf_num, leg_turn_seq, wit_turn_seq, direction FROM dyads
             WHERE direction IS DISTINCT FROM (CASE WHEN leg_turn_seq < wit_turn_seq THEN 'question' ELSE 'answer' END)"""
    n = int(v.scalar(f"SELECT count(*) FROM ({sql})"))
    return _res(n, {"n_inconsistent": n}, v.examples(sql))


@check("dyads_sitting", "no dyad pairs two turns of different sittings: both endpoints have the same non-null sitting_seq in turns (and leg_sitting_seq = wit_sitting_seq when the dyads carry them)")
def c_dy_sitting(v: Validator) -> Result:
    v.need("dyads", "turns")
    v.need_cols("turns", "sitting_seq")
    sql = """SELECT d.conf_num, d.leg_turn_seq, d.wit_turn_seq, L.sitting_seq AS leg_sitting, W.sitting_seq AS wit_sitting
             FROM dyads d
             LEFT JOIN turns L ON L.conf_num = d.conf_num AND L.turn_seq = d.leg_turn_seq
             LEFT JOIN turns W ON W.conf_num = d.conf_num AND W.turn_seq = d.wit_turn_seq
             WHERE L.sitting_seq IS NULL OR W.sitting_seq IS NULL OR L.sitting_seq <> W.sitting_seq"""
    det = {"dyads_across_sittings_or_null": int(v.scalar(f"SELECT count(*) FROM ({sql})"))}
    dc = v.cols("dyads")
    if "sitting_seq" in dc:
        det["carried_sitting_differs"] = int(v.scalar(
            """SELECT count(*) FROM dyads d LEFT JOIN turns L ON L.conf_num = d.conf_num AND L.turn_seq = d.leg_turn_seq
               WHERE d.sitting_seq IS NULL OR d.sitting_seq IS DISTINCT FROM L.sitting_seq"""))
    elif "leg_sitting_seq" in dc and "wit_sitting_seq" in dc:
        det["carried_sitting_differs"] = int(v.scalar(
            "SELECT count(*) FROM dyads WHERE leg_sitting_seq IS DISTINCT FROM wit_sitting_seq OR leg_sitting_seq IS NULL"))
    det["meetings_several_sittings"] = int(v.scalar(
        "SELECT count(*) FROM (SELECT conf_num FROM turns GROUP BY 1 HAVING count(DISTINCT sitting_seq) > 1)"))
    det["dyads_in_meetings_several_sittings"] = int(v.scalar(
        "SELECT count(*) FROM dyads WHERE conf_num IN (SELECT conf_num FROM turns GROUP BY 1 HAVING count(DISTINCT sitting_seq) > 1)"))
    bad = det["dyads_across_sittings_or_null"] + det.get("carried_sitting_differs", 0)
    return _res(bad, det, v.examples(sql) if bad else [])


@check("turns_sittings", "sitting_seq and after_end_marker are well formed in every meeting: non-null, sitting_seq starts at 1 and never decreases or skips along turn_seq, after_end_marker never turns back to false; sitting_how / label_how / label_confidence are non-null")
def c_turn_sittings(v: Validator) -> Result:
    v.need_cols("turns", "sitting_seq", "after_end_marker")
    tc = v.cols("turns")
    s = """WITH s AS (SELECT conf_num, turn_seq, sitting_seq, after_end_marker,
                         lag(sitting_seq) OVER w AS psit, lag(after_end_marker) OVER w AS pae,
                         row_number() OVER w AS rn
                  FROM turns WINDOW w AS (PARTITION BY conf_num ORDER BY turn_seq))"""
    det = v.one(f"""{s} SELECT
        count(*) FILTER (WHERE sitting_seq IS NULL) AS sitting_seq_null,
        count(*) FILTER (WHERE after_end_marker IS NULL) AS after_end_marker_null,
        count(*) FILTER (WHERE rn = 1 AND sitting_seq IS DISTINCT FROM 1) AS first_turn_sitting_not_1,
        count(*) FILTER (WHERE rn > 1 AND (sitting_seq < psit OR sitting_seq > psit + 1)) AS sitting_step_bad,
        count(*) FILTER (WHERE rn > 1 AND pae AND NOT after_end_marker) AS after_end_reset
        FROM s""")
    det = {k: int(x or 0) for k, x in det.items()}
    for c in ("sitting_how", "label_how", "label_confidence"):
        if c in tc:
            det[f"{c}_null"] = int(v.scalar(f"SELECT count(*) FROM turns WHERE {c} IS NULL"))
        elif v.params.mode == "release":
            det[f"{c}_missing_column"] = 1
    bad = sum(det.values())
    ex = v.examples(f"""{s} SELECT conf_num, turn_seq, sitting_seq, psit, after_end_marker, pae FROM s
        WHERE sitting_seq IS NULL OR after_end_marker IS NULL OR (rn = 1 AND sitting_seq IS DISTINCT FROM 1)
           OR (rn > 1 AND (sitting_seq < psit OR sitting_seq > psit + 1)) OR (rn > 1 AND pae AND NOT after_end_marker)""") if bad else []
    return _res(bad, det, ex)


@check("turns_after_end_marker", "turns printed after a meeting-end marker are counted and reported by source (turns, meetings, dyads with such a turn on either side); WARN when any exist (kept and flagged by default, config.yaml)", release_required=True)
def c_after_end(v: Validator) -> Result:
    v.need_cols("turns", "after_end_marker")
    src = "source" if "source" in v.cols("turns") else "'?'"
    by = v.q(f"""SELECT {src} AS source, count(*) AS turns, count(*) FILTER (WHERE after_end_marker) AS turns_after_end,
                        count(DISTINCT conf_num) FILTER (WHERE after_end_marker) AS meetings_with_turns_after_end
                 FROM turns GROUP BY 1 ORDER BY 1""")
    det = {"by_source": json.loads(by.to_json(orient="records", force_ascii=False)),
           "turns_after_end": int(by.turns_after_end.sum()) if len(by) else 0,
           "after_end_marker_null": int(v.scalar("SELECT count(*) FROM turns WHERE after_end_marker IS NULL"))}
    if "sitting_seq" in v.cols("turns"):
        # after an end marker with no re-opening in sitting 1 (trailing material) vs in a re-opened later sitting
        det["turns_after_end_in_sitting_1"] = int(v.scalar("SELECT count(*) FROM turns WHERE after_end_marker AND sitting_seq = 1"))
        det["turns_after_end_in_later_sittings"] = int(v.scalar("SELECT count(*) FROM turns WHERE after_end_marker AND sitting_seq > 1"))
    if v.has("dyads"):
        dc = v.cols("dyads")
        if "any_after_end_marker" in dc:
            det["dyads_with_after_end_turn"] = int(v.scalar("SELECT count(*) FROM dyads WHERE any_after_end_marker"))
            det["dyads_straddling_end_marker"] = int(v.scalar(
                """SELECT count(*) FROM dyads d JOIN turns L ON L.conf_num = d.conf_num AND L.turn_seq = d.leg_turn_seq
                   JOIN turns W ON W.conf_num = d.conf_num AND W.turn_seq = d.wit_turn_seq
                   WHERE coalesce(L.after_end_marker, false) <> coalesce(W.after_end_marker, false)"""))
        elif "leg_after_end_marker" in dc and "wit_after_end_marker" in dc:
            det["dyads_with_after_end_turn"] = int(v.scalar(
                "SELECT count(*) FROM dyads WHERE coalesce(leg_after_end_marker, false) OR coalesce(wit_after_end_marker, false)"))
            det["dyads_straddling_end_marker"] = int(v.scalar(
                "SELECT count(*) FROM dyads WHERE coalesce(leg_after_end_marker, false) <> coalesce(wit_after_end_marker, false)"))
    det["dyads_exclude_after_end_marker"] = v.params.dyads_exclude_after_end_marker
    ex = v.examples("""SELECT conf_num, count(*) AS turns_after_end, min(turn_seq) AS first_turn_seq FROM turns
                       WHERE after_end_marker GROUP BY 1 ORDER BY 2 DESC, 1""") if det["turns_after_end"] else []
    r = _res(det["after_end_marker_null"], det, ex)
    if r.status == "PASS" and det["turns_after_end"]:
        r.status = "WARN"
    return r


@check("label_how_weak_share", "every turn records how its speaker label was found (label_how, label_confidence non-null); the share of weak labels (label_confidence 'low' or label_how in Params.label_weak_rules) is reported by source and by label_how, WARN above label_weak_max_share or when a label_how is unrated")
def c_label_how(v: Validator) -> Result:
    v.need_cols("turns", "label_how", "label_confidence")
    src = "source" if "source" in v.cols("turns") else "'?'"
    rules = ", ".join(_s(x) for x in v.params.label_weak_rules)
    weak = "label_confidence = 'low'" + (f" OR label_how IN ({rules})" if rules else "")
    by = v.q(f"""SELECT {src} AS source, count(*) AS turns, count(*) FILTER (WHERE {weak}) AS weak,
                        count(*) FILTER (WHERE label_confidence = 'unrated') AS unrated,
                        count(*) FILTER (WHERE label_how IS NULL) AS label_how_null,
                        count(*) FILTER (WHERE label_confidence IS NULL) AS label_confidence_null
                 FROM turns GROUP BY 1 ORDER BY 1""")
    by["weak_share"] = (by.weak / by.turns).round(6)
    rules_by = v.q(f"""SELECT {src} AS source, label_how, label_confidence, count(*) AS n FROM turns
                       GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC""")
    det = {"by_source": json.loads(by.to_json(orient="records", force_ascii=False)),
           "by_label_how": json.loads(rules_by.to_json(orient="records", force_ascii=False)),
           "weak_definition": weak, "label_weak_max_share": v.params.label_weak_max_share}
    nulls = int(by.label_how_null.sum() + by.label_confidence_null.sum())
    over = by[by.weak_share > v.params.label_weak_max_share].source.tolist()
    det["sources_over_max_share"] = over
    det["unrated"] = int(by.unrated.sum())
    ex = v.examples(f"SELECT conf_num, turn_seq, {src} AS source, label_how, label_confidence, speaker_label_raw FROM turns WHERE {weak} OR label_confidence = 'unrated' ORDER BY hash(conf_num, turn_seq, {SEED})") if (over or det["unrated"]) else []
    r = _res(nulls, det, ex)
    if r.status == "PASS" and (over or det["unrated"]):
        r.status = "WARN"
    return r


def _chunks_by_conf(v: Validator, table: str, max_rows: int) -> list:
    """conf_num ranges [(lo, hi)] of at most max_rows rows of `table` each (a larger meeting is its own range)."""
    per = v.con.execute(f"SELECT conf_num, count(*) FROM {table} WHERE conf_num IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()
    out, cur, lo, prev = [], 0, None, None
    for cn, n in per:
        if lo is None:
            lo = cn
        if cur and cur + n > max_rows:
            out.append((lo, prev))
            lo, cur = cn, 0
        cur += n
        prev = cn
    if lo is not None:
        out.append((lo, prev))
    return out


def _dyad_turn_pass(v: Validator) -> dict:
    """One chunked pass over every dyad joined to both endpoint turns and its meetings row: per column, the
    number of dyads whose value differs from the value derived from the turns (DYAD_ATTRIBUTE_SOURCES), and the
    three flags recomputed (leg_is_chair from the turn role; leg_is_procedural from the legislator turn's text
    with the procedural definition in dyads.py; wit_is_legislator_title from the witness turn's position or
    label). Cached for dyads_attributes and dyads_flags."""
    if "_dyad_pass" in v._cache:
        return v._cache["_dyad_pass"]
    import dyads as DY
    v.need("dyads", "turns")
    dc, tc = v.cols("dyads"), v.cols("turns")
    mc = v.cols("meetings") if v.has("meetings") else {}
    lic = next((c for c in LABEL_INCONSISTENT_COLS if c in tc), None)
    attrs, src_missing = {}, []
    for col, expr in DYAD_ATTRIBUTE_SOURCES.items():
        if col not in dc:
            continue
        e = expr.format(lic=lic) if "{lic}" in expr else expr
        refs = re.findall(r"\b([LWM])\.(\w+)", e) if not (col == "any_label_inconsistent" and lic is None) else [("L", "__none__")]
        if not v.has("meetings"):        # the builder then takes meeting columns from the legislator turn
            e = re.sub(r"\bM\.", "L.", e)
            refs = [("L" if a == "M" else a, c) for a, c in refs]
        absent = [f"{a}.{c}" for a, c in refs if (a == "M" and c not in mc) or (a in "LW" and c not in tc)]
        if absent:
            # the builder writes null when a source column is absent; a partly absent coalesce keeps the rest
            present = [f"{a}.{c}" for a, c in refs if f"{a}.{c}" not in absent]
            e = present[0] if (col == "leg_name" and present) else "NULL"
            src_missing.append(col)
        attrs[col] = e
    need_t = sorted({c for e in attrs.values() for a, c in re.findall(r"\b([LW])\.(\w+)", e)}
                    | {c for c in ("text", "role", "speaker_pos", "speaker_label_raw") if c in tc})
    chair = ("coalesce(L.role = 'chair', false)" if "role" in tc else
             (f"coalesce(regexp_full_match(coalesce(L.speaker_pos, ''), {_s(DY.CHAIR_POS_RE)}), false)" if "speaker_pos" in tc else "false"))
    wpos = "W.speaker_pos" if "speaker_pos" in tc else "NULL"
    wlab = "W.speaker_label_raw" if "speaker_label_raw" in tc else "NULL"
    wtitle = (f"(CASE WHEN {wpos} IS NOT NULL AND trim({wpos}) <> '' THEN regexp_full_match(trim({wpos}), {_s(LEG_TITLE_POS_RE)}) "
              f"ELSE coalesce(regexp_full_match(trim({wlab}), {_s(DY.LEG_TITLE_LABEL_RE)}), false) END)")
    codes = DY.procedural_codes_sql("L.text") if "text" in tc else "NULL"
    proc = DY.procedural_from_codes_sql("pc", "chair_exp")
    mjoin = "LEFT JOIN meetings M ON M.conf_num = d.conf_num" if v.has("meetings") else ""
    cnt = {"n_dyads": 0, "endpoint_missing": 0, "flag_null": 0, "chair_mismatch": 0, "procedural_mismatch": 0,
           "wit_title_mismatch": 0, "procedural_true": 0}
    by_col = {c: 0 for c in attrs}
    ex = []
    chunks = _chunks_by_conf(v, "turns", max(1, int(v.params.dyad_chunk_turns)))
    tsel = ", ".join(["conf_num", "turn_seq"] + [_q(c) for c in need_t if c not in ("conf_num", "turn_seq")])
    for lo, hi in chunks:
        v.con.execute(f"CREATE OR REPLACE TEMP TABLE _dp_t AS SELECT {tsel} FROM turns WHERE conf_num BETWEEN {int(lo)} AND {int(hi)}")
        v.con.execute(f"""CREATE OR REPLACE TEMP TABLE _dp AS
            SELECT d.*, (L.turn_seq IS NULL OR W.turn_seq IS NULL) AS _missing, {chair} AS chair_exp,
                   {codes} AS pc, {wtitle} AS wtitle_exp,
                   {', '.join(f'({e}) AS {_q("exp_" + c)}' for c, e in attrs.items())}
            FROM (SELECT * FROM dyads WHERE conf_num BETWEEN {int(lo)} AND {int(hi)}) d
            LEFT JOIN _dp_t L ON L.conf_num = d.conf_num AND L.turn_seq = d.leg_turn_seq
            LEFT JOIN _dp_t W ON W.conf_num = d.conf_num AND W.turn_seq = d.wit_turn_seq
            {mjoin}""")
        agg = ", ".join([f"count(*) FILTER (WHERE NOT _missing AND {_q(c)} IS DISTINCT FROM {_q('exp_' + c)}) AS {_q(c)}" for c in attrs])
        r = v.one(f"""SELECT count(*) AS n_dyads, count(*) FILTER (WHERE _missing) AS endpoint_missing,
                  count(*) FILTER (WHERE leg_is_chair IS NULL OR leg_is_procedural IS NULL OR wit_is_legislator_title IS NULL) AS flag_null,
                  count(*) FILTER (WHERE NOT _missing AND leg_is_chair IS DISTINCT FROM chair_exp) AS chair_mismatch,
                  count(*) FILTER (WHERE NOT _missing AND leg_is_procedural IS DISTINCT FROM {proc}) AS procedural_mismatch,
                  count(*) FILTER (WHERE NOT _missing AND wit_is_legislator_title IS DISTINCT FROM wtitle_exp) AS wit_title_mismatch,
                  count(*) FILTER (WHERE leg_is_procedural) AS procedural_true
                  {', ' + agg if agg else ''} FROM _dp""")
        for k in cnt:
            cnt[k] += int(r[k] or 0)
        for c in attrs:
            k = int(r[c] or 0)
            by_col[c] += k
            if k and len(ex) < N_EXAMPLES:
                ex += v.examples(f"""SELECT 'attribute' AS kind, conf_num, leg_turn_seq, wit_turn_seq, {_s(c)} AS column,
                                            left(CAST({_q(c)} AS VARCHAR), 80) AS value, left(CAST({_q('exp_' + c)} AS VARCHAR), 80) AS expected
                                     FROM _dp WHERE NOT _missing AND {_q(c)} IS DISTINCT FROM {_q('exp_' + c)}""", 3)
        for k, cond in (("chair_mismatch", "leg_is_chair IS DISTINCT FROM chair_exp"),
                        ("procedural_mismatch", f"leg_is_procedural IS DISTINCT FROM {proc}"),
                        ("wit_title_mismatch", "wit_is_legislator_title IS DISTINCT FROM wtitle_exp")):
            if int(r[k] or 0) and len(ex) < N_EXAMPLES:
                ex += v.examples(f"""SELECT {_s(k)} AS kind, conf_num, leg_turn_seq, wit_turn_seq, leg_is_chair, leg_is_procedural,
                                            wit_is_legislator_title FROM _dp WHERE NOT _missing AND {cond}""", 3)
    v.con.execute("DROP TABLE IF EXISTS _dp")
    v.con.execute("DROP TABLE IF EXISTS _dp_t")
    res = {"counts": cnt, "by_column": by_col, "columns_compared": list(attrs), "source_absent_expect_null": src_missing,
           "label_inconsistent_source": lic, "n_chunks": len(chunks), "examples": ex[:N_EXAMPLES]}
    v._cache["_dyad_pass"] = res
    return res


@check("dyads_attributes", "every slim dyad attribute (meeting columns, sitting_seq, speech_date, legislator and witness attributes, texts, any_* flags) equals the value derived from its endpoint turns and meetings row, for EVERY dyad (chunked, vectorized); an attribute whose source column is absent must be null")
def c_dy_attrs(v: Validator) -> Result:
    v.need("dyads", "turns")
    r = _dyad_turn_pass(v)
    dc = v.cols("dyads")
    uncovered = [c for c in dc if c not in DYAD_ATTRIBUTE_SOURCES and c not in DYAD_KEY_AND_FLAG_COLS]
    if not r["columns_compared"]:
        raise Skip("dyads carry no slim attribute column")
    det = {"n_dyads": r["counts"]["n_dyads"], "endpoint_missing": r["counts"]["endpoint_missing"],
           "by_column": {k: x for k, x in r["by_column"].items() if x}, "n_columns_compared": len(r["columns_compared"]),
           "columns_without_rule": uncovered, "source_absent_expect_null": r["source_absent_expect_null"],
           "label_inconsistent_source": r["label_inconsistent_source"], "n_chunks": r["n_chunks"]}
    bad = sum(r["by_column"].values()) + r["counts"]["endpoint_missing"] + len(uncovered)
    return _res(bad, det, [e for e in r["examples"] if e.get("kind") == "attribute"])


@check("dyads_flags", "flags are non-null; leg_is_chair equals the legislator turn's role = 'chair'; leg_is_procedural equals the procedural definition (dyads.py) recomputed from the legislator turn's text for EVERY dyad; wit_is_legislator_title equals the legislator-title rule on the witness turn's position / label")
def c_dy_flags(v: Validator) -> Result:
    v.need_cols("dyads", "leg_is_chair", "leg_is_procedural", "wit_is_legislator_title")
    if "role" not in v.cols("turns") and v.params.mode == "release":
        raise Skip("turns lack role")
    import dyads as DY
    r = _dyad_turn_pass(v)
    c = r["counts"]
    det = {"n_dyads": c["n_dyads"], "flag_null": c["flag_null"], "chair_mismatch": c["chair_mismatch"],
           "procedural_mismatch": c["procedural_mismatch"], "procedural_recomputed": c["n_dyads"] - c["endpoint_missing"],
           "wit_title_mismatch": c["wit_title_mismatch"], "procedural_true": c["procedural_true"],
           "procedural_longer_than_max": int(v.scalar(f"SELECT count(*) FROM dyads WHERE leg_is_procedural AND length(leg_text) > {DY.PROCEDURAL_MAX_CHARS}"))
           if "leg_text" in v.cols("dyads") else 0}
    bad = det["flag_null"] + det["chair_mismatch"] + det["procedural_mismatch"] + det["wit_title_mismatch"] + det["procedural_longer_than_max"]
    return _res(bad, det, [e for e in r["examples"] if e.get("kind") != "attribute"])


def _term_windows_sql() -> str:
    return "(VALUES " + ", ".join(f"({t}, '{a}', '{b}')" for t, (a, b) in sorted(TERM_WINDOWS.items())) + ") AS tw(term, t_start, t_end)"


@check("dates_term_window", "meeting dates and speech dates fall inside the meeting's term window (a speech may run past the term end by the slack)")
def c_term_window(v: Validator) -> Result:
    v.need_cols("meetings", "term", "date")
    tw = _term_windows_sql()
    det = {}
    det["meeting_date_outside_term"] = int(v.scalar(
        f"SELECT count(*) FROM meetings m JOIN {tw} ON tw.term = m.term WHERE m.date IS NOT NULL AND (m.date < tw.t_start OR m.date > tw.t_end)"))
    det["meeting_term_unknown"] = int(v.scalar(f"SELECT count(*) FROM meetings m ANTI JOIN {tw} ON tw.term = m.term WHERE m.term IS NOT NULL"))
    ex = v.examples(f"SELECT m.conf_num, m.term, m.date, tw.t_start, tw.t_end FROM meetings m JOIN {tw} ON tw.term = m.term WHERE m.date < tw.t_start OR m.date > tw.t_end")
    if v.has("turns") and "speech_date" in v.cols("turns"):
        sl = v.params.speech_date_end_slack_days
        det["speech_date_outside_term"] = int(v.scalar(
            f"""SELECT count(*) FROM turns t JOIN meetings m USING (conf_num) JOIN {tw} ON tw.term = m.term
                WHERE t.speech_date IS NOT NULL AND (t.speech_date < tw.t_start
                  OR t.speech_date > strftime(CAST(tw.t_end AS DATE) + INTERVAL {sl} DAY, '%Y-%m-%d'))"""))
    return _res(sum(det.values()), det, ex)


@check("dates_meeting", "meeting date equals the Open API date; date_end >= date - 1 day; every speech_date lies in "
                        "[date - 1 day, date + max span] (overnight sittings opened late on the previous day, printed "
                        "dates one day off) or is the printed date of an appended record in a later sitting (WARN)")
def c_dates_meeting(v: Validator) -> Result:
    v.need_cols("meetings", "date")
    det, warn = {}, {}
    if v.has("universe"):
        det["date_differs_from_api"] = int(v.scalar(
            "SELECT count(*) FROM meetings m JOIN universe u ON u.CONFER_NUM = m.conf_num WHERE m.date IS DISTINCT FROM u.CONF_DATE"))
    elif v.params.mode == "release":
        raise Skip("universe missing")
    if "date_end" in v.cols("meetings"):
        det["date_end_before_date_minus_1"] = int(v.scalar(
            "SELECT count(*) FROM meetings WHERE CAST(date_end AS DATE) < CAST(date AS DATE) - INTERVAL 1 DAY"))
        warn["date_end_one_day_before_date"] = int(v.scalar(
            "SELECT count(*) FROM meetings WHERE date_end < date AND CAST(date_end AS DATE) >= CAST(date AS DATE) - INTERVAL 1 DAY"))
    span = v.params.max_meeting_span_days
    if v.has("turns") and "speech_date" in v.cols("turns"):
        tc = v.cols("turns")
        appended = ("(t.sitting_seq > 1 AND t.speech_date_how = 'sub_cover')"
                    if {"sitting_seq", "speech_date_how"} <= set(tc) else "false")
        lo = "strftime(CAST(m.date AS DATE) - INTERVAL 1 DAY, '%Y-%m-%d')"
        hi = f"strftime(CAST(m.date AS DATE) + INTERVAL {span} DAY, '%Y-%m-%d')"
        det["speech_outside_allowed_range"] = int(v.scalar(
            f"SELECT count(*) FROM turns t JOIN meetings m USING (conf_num) WHERE (t.speech_date < {lo} OR t.speech_date > {hi}) AND NOT {appended}"))
        warn["speech_on_day_before_meeting_date"] = int(v.scalar(
            f"SELECT count(*) FROM turns t JOIN meetings m USING (conf_num) WHERE t.speech_date < m.date AND t.speech_date >= {lo}"))
        warn["speech_dated_by_appended_record"] = int(v.scalar(
            f"SELECT count(*) FROM turns t JOIN meetings m USING (conf_num) WHERE (t.speech_date < {lo} OR t.speech_date > {hi}) AND {appended}"))
    info = {}
    if "date_mismatch" in v.cols("meetings"):
        info["printed_date_differs_from_api (info)"] = int(v.scalar("SELECT count(*) FROM meetings WHERE date_mismatch"))
    ex = []
    if v.has("turns") and "speech_date" in v.cols("turns"):
        ex = v.examples(f"""SELECT t.conf_num, m.date, m.date_end, min(t.speech_date) min_speech_date, max(t.speech_date) max_speech_date, count(*) n_turns
                            FROM turns t JOIN meetings m USING (conf_num)
                            WHERE t.speech_date < m.date OR t.speech_date > strftime(CAST(m.date AS DATE) + INTERVAL {span} DAY, '%Y-%m-%d')
                            GROUP BY 1, 2, 3 ORDER BY n_turns DESC""")
    bad = sum(det.values())
    out = dict(det, **{"warn." + k: x for k, x in warn.items()}, **info)
    return _res(bad, out, ex) if bad else _res(sum(warn.values()), out, ex, warn=True)


def _overlap_pairs_sql(v: Validator) -> Optional[str]:
    """(a, b) pairs recorded as partial overlaps in meetings.overlap_with (kept and flagged, researcher
    decision 7); None when the meetings table has no overlap_with column."""
    if not (v.has("meetings") and "overlap_with" in v.cols("meetings")):
        return None
    return """SELECT DISTINCT least(m.conf_num, o) AS a, greatest(m.conf_num, o) AS b
              FROM (SELECT conf_num, unnest(overlap_with) AS o FROM meetings WHERE overlap_with IS NOT NULL) m"""


def _allow_sql(v: Validator) -> str:
    """Pairs excepted from the duplicate-content checks: config dup_allowlist plus meetings.overlap_with."""
    parts = []
    al = v.params.dup_allowlist
    if al:
        parts.append("SELECT * FROM (VALUES " + ", ".join(f"({min(int(a), int(b))}, {max(int(a), int(b))})" for a, b, *_ in al) + ") AS x(a, b)")
    ov = _overlap_pairs_sql(v)
    if ov:
        parts.append(ov)
    if not parts:
        return "(SELECT NULL::BIGINT a, NULL::BIGINT b WHERE false) AS al"
    return "(" + " UNION ".join(parts) + ") AS al"


def _dup_counts(v: Validator, tbl: str) -> dict:
    """pairs flagged / allowlisted (config) / recorded as overlaps (meetings.overlap_with) / blocking."""
    cfg = ("(VALUES " + ", ".join(f"({min(int(a), int(b))}, {max(int(a), int(b))})" for a, b, *_ in v.params.dup_allowlist) + ") AS c(a, b)") \
        if v.params.dup_allowlist else "(SELECT NULL::BIGINT a, NULL::BIGINT b WHERE false) AS c"
    ov = _overlap_pairs_sql(v)
    n_all = int(v.scalar(f"SELECT count(*) FROM {tbl}"))
    n_cfg = int(v.scalar(f"SELECT count(*) FROM {tbl} d SEMI JOIN {cfg} ON c.a = d.a AND c.b = d.b"))
    n_ov = int(v.scalar(f"SELECT count(*) FROM {tbl} d SEMI JOIN ({ov}) o ON o.a = d.a AND o.b = d.b")) if ov else 0
    n_block = int(v.scalar(f"SELECT count(*) FROM {tbl} d ANTI JOIN {_allow_sql(v)} ON al.a = d.a AND al.b = d.b"))
    return {"pairs_flagged": n_all, "pairs_allowlisted": n_cfg, "pairs_recorded_overlap": n_ov, "pairs_blocking": n_block}


@check("dup_long_turn", "no two meetings share >= containment of their long turns (exact md5 of normalized text); boilerplate (df > max_df) ignored; allowlisted pairs excepted")
def c_dup_long(v: Validator) -> Result:
    v.need_cols("turns_release", "text")
    p = v.params
    v.con.execute(f"""CREATE OR REPLACE TEMP TABLE _dup_long_l AS
WITH L0 AS (SELECT DISTINCT conf_num, md5({norm_sql('text')}) h FROM turns_release WHERE length({norm_sql('text')}) >= {p.dup_long_min_chars}),
DF AS (SELECT h, count(*) df FROM L0 GROUP BY 1)
SELECT L0.conf_num, L0.h, DF.df FROM L0 JOIN DF USING (h) WHERE DF.df <= {p.dup_max_df}""")
    # per-meeting count of non-boilerplate long turns (also read by dup_meeting_text for its threshold report)
    v.con.execute("CREATE OR REPLACE TEMP TABLE _dup_long_n AS SELECT conf_num, count(*) n FROM _dup_long_l GROUP BY 1")
    v.con.execute(f"""CREATE OR REPLACE TEMP TABLE _dup_long AS
WITH P AS (SELECT a.conf_num a, b.conf_num b, count(*) shared FROM _dup_long_l a JOIN _dup_long_l b ON a.h = b.h AND a.conf_num < b.conf_num
           WHERE a.df >= 2 GROUP BY 1, 2)
SELECT P.a, P.b, P.shared, na.n AS n_a, nb.n AS n_b, P.shared / least(na.n, nb.n) AS containment
FROM P JOIN _dup_long_n na ON na.conf_num = P.a JOIN _dup_long_n nb ON nb.conf_num = P.b
WHERE P.shared >= {p.dup_long_min_shared} AND P.shared / least(na.n, nb.n) >= {p.dup_containment}""")
    det = _dup_counts(v, "_dup_long")
    n = det["pairs_blocking"]
    det["n_long_turn_meetings"] = int(v.scalar(f"SELECT count(DISTINCT conf_num) FROM turns_release WHERE length({norm_sql('text')}) >= {p.dup_long_min_chars}"))
    return _res(n, det, v.examples(f"SELECT * FROM _dup_long d ANTI JOIN {_allow_sql(v)} ON al.a = d.a AND al.b = d.b ORDER BY containment DESC"))


def sentence_shingles_sql(src: str, key: str, text: str, k: int) -> str:
    """Distinct (key, h) of 10-char (k-char) shingles anchored at sentence starts: the first k
    normalized characters of every sentence with >= k normalized characters."""
    return f"""SELECT DISTINCT {key}, hash(left(ns, {k})) h FROM (
                 SELECT {key}, {norm_sql('s')} ns FROM (
                   SELECT {key}, unnest(regexp_split_to_array(coalesce({text}, ''), '{SENT_SPLIT_RE2}')) s FROM {src}))
               WHERE length(ns) >= {k}"""


@check("dup_shingle", "no two meetings have 10-char sentence-anchored shingle containment >= threshold (catches re-segmented copies the md5 check misses)")
def c_dup_shingle(v: Validator) -> Result:
    v.need_cols("turns_release", "text")
    p = v.params
    sh = sentence_shingles_sql("turns_release", "conf_num", "text", p.dup_shingle_len)
    v.con.execute(f"""CREATE OR REPLACE TEMP TABLE _dup_sh_h AS
WITH H0 AS ({sh}), DF AS (SELECT h, count(*) df FROM H0 GROUP BY 1)
SELECT H0.conf_num, H0.h, DF.df FROM H0 JOIN DF USING (h) WHERE DF.df <= {p.dup_max_df}""")
    v.con.execute("CREATE OR REPLACE TEMP TABLE _dup_sh_n AS SELECT conf_num, count(*) n FROM _dup_sh_h GROUP BY 1")
    v.con.execute(f"""CREATE OR REPLACE TEMP TABLE _dup_sh AS
WITH P AS (SELECT a.conf_num a, b.conf_num b, count(*) shared FROM _dup_sh_h a JOIN _dup_sh_h b ON a.h = b.h AND a.conf_num < b.conf_num
           WHERE a.df >= 2 GROUP BY 1, 2)
SELECT P.a, P.b, P.shared, na.n AS n_a, nb.n AS n_b, P.shared / least(na.n, nb.n) AS containment
FROM P JOIN _dup_sh_n na ON na.conf_num = P.a JOIN _dup_sh_n nb ON nb.conf_num = P.b
WHERE P.shared >= {p.dup_shingle_min_shared} AND least(na.n, nb.n) >= {p.dup_shingle_min_n}
  AND P.shared / least(na.n, nb.n) >= {p.dup_containment}""")
    det = _dup_counts(v, "_dup_sh")
    n = det["pairs_blocking"]
    return _res(n, det, v.examples(f"SELECT * FROM _dup_sh d ANTI JOIN {_allow_sql(v)} ON al.a = d.a AND al.b = d.b ORDER BY containment DESC"))


@check("dup_meeting_text", "no two meetings have identical whole-meeting text (normalized text of all turns in turn order, boilerplate included; invariant to re-segmentation), so a verbatim copy of a meeting of any size is caught, including small meetings below the dup_long_turn / dup_shingle thresholds; allowlisted pairs excepted")
def c_dup_meeting(v: Validator) -> Result:
    v.need_cols("turns_release", "text", "turn_seq")
    p = v.params
    # whole-meeting strings are built DUP_MEETING_CHUNK meetings at a time (conf_num ranges): one string_agg
    # over the full release (15M turns) exceeds the duckdb memory limit. The hashes are the same.
    fp_sql = f"""SELECT conf_num, md5(t) h, length(t) n_chars, n_turns FROM (
          SELECT conf_num, string_agg({norm_sql('text')}, '' ORDER BY turn_seq) t, count(*) n_turns FROM turns_release
          WHERE {{where}} GROUP BY 1)
        WHERE length(t) >= {int(p.dup_meeting_min_chars)}"""
    v.con.execute(f"CREATE OR REPLACE TEMP TABLE _dup_mt_h AS {fp_sql.format(where='false')}")
    ids = [r[0] for r in v.con.execute("SELECT DISTINCT conf_num FROM turns_release WHERE conf_num IS NOT NULL ORDER BY 1").fetchall()]
    step = max(1, int(DUP_MEETING_CHUNK))
    for i in range(0, len(ids), step):
        lo, hi = ids[i], ids[min(i + step, len(ids)) - 1]
        v.con.execute(f"INSERT INTO _dup_mt_h {fp_sql.format(where=f'conf_num BETWEEN {int(lo)} AND {int(hi)}')}")
    v.con.execute("""CREATE OR REPLACE TEMP TABLE _dup_mt AS
        SELECT a.conf_num a, b.conf_num b, a.n_chars, a.n_turns n_turns_a, b.n_turns n_turns_b
        FROM _dup_mt_h a JOIN _dup_mt_h b ON a.h = b.h AND a.conf_num < b.conf_num""")
    det = _dup_counts(v, "_dup_mt")
    n = det["pairs_blocking"]
    det.update({"n_meetings_fingerprinted": int(v.scalar("SELECT count(*) FROM _dup_mt_h")),
                "n_meetings_below_min_chars": int(v.scalar("SELECT count(DISTINCT conf_num) FROM turns_release")) - int(v.scalar("SELECT count(*) FROM _dup_mt_h"))})
    tabs = {r[0] for r in v.con.execute("SELECT table_name FROM duckdb_tables() WHERE temporary").fetchall()}
    if {"_dup_long_n", "_dup_sh_n"} <= tabs:
        # meetings the near-duplicate checks cannot flag (fewer shared long turns / shingles than their
        # thresholds allow): only this exact whole-meeting check covers them
        det["n_meetings_below_near_duplicate_thresholds"] = int(v.scalar(f"""
            SELECT count(*) FROM (SELECT DISTINCT conf_num FROM turns_release) m
            LEFT JOIN _dup_long_n l USING (conf_num) LEFT JOIN _dup_sh_n s USING (conf_num)
            WHERE coalesce(l.n, 0) < {p.dup_long_min_shared} AND coalesce(s.n, 0) < {p.dup_shingle_min_n}"""))
    return _res(n, det, v.examples(f"SELECT * FROM _dup_mt d ANTI JOIN {_allow_sql(v)} ON al.a = d.a AND al.b = d.b ORDER BY n_chars DESC"))


@check("duplicates_resolved", "researcher decision 7: a meeting marked meetings.duplicate_of has whole-meeting text identical to the kept copy (which exists and is not itself a duplicate); its turns and dyads are only in duplicate_turns / duplicate_dyads, never in the release turns / dyads, and all of its turns are there; overlap_with targets exist and are not duplicates")
def c_dups_resolved(v: Validator) -> Result:
    v.need("meetings", "turns_release")
    v.need_cols("meetings", "duplicate_of", "overlap_with")
    dup = "(SELECT conf_num, duplicate_of FROM meetings WHERE duplicate_of IS NOT NULL)"
    det = {
        "n_duplicates": int(v.scalar(f"SELECT count(*) FROM {dup}")),
        "duplicate_of_self": int(v.scalar(f"SELECT count(*) FROM {dup} WHERE duplicate_of = conf_num")),
        "duplicate_target_missing": int(v.scalar(f"SELECT count(*) FROM {dup} d ANTI JOIN meetings m ON m.conf_num = d.duplicate_of")),
        "duplicate_target_is_duplicate": int(v.scalar(f"SELECT count(*) FROM {dup} d JOIN meetings m ON m.conf_num = d.duplicate_of WHERE m.duplicate_of IS NOT NULL")),
        "release_turns_of_duplicates": int(v.scalar(f"SELECT count(*) FROM turns_release WHERE conf_num IN (SELECT conf_num FROM {dup})")),
    }
    if v.has("dyads_release"):
        det["release_dyads_of_duplicates"] = int(v.scalar(f"SELECT count(*) FROM dyads_release WHERE conf_num IN (SELECT conf_num FROM {dup})"))
    n_turns = "m.n_turns" if "n_turns" in v.cols("meetings") else "NULL"
    if v.has("turns_duplicate"):
        det["duplicate_turns_not_marked"] = int(v.scalar(f"SELECT count(*) FROM turns_duplicate WHERE conf_num NOT IN (SELECT conf_num FROM {dup})"))
        det["duplicate_turns_count_mismatch"] = int(v.scalar(f"""SELECT count(*) FROM meetings m JOIN {dup} d USING (conf_num)
            LEFT JOIN (SELECT conf_num, count(*) n FROM turns_duplicate GROUP BY 1) c USING (conf_num)
            WHERE coalesce(c.n, 0) IS DISTINCT FROM coalesce({n_turns}, 0)"""))
    else:
        det["duplicates_with_turns_but_no_duplicate_turns_table"] = int(v.scalar(
            f"SELECT count(*) FROM meetings m JOIN {dup} d USING (conf_num) WHERE coalesce({n_turns}, 0) > 0"))
    if v.has("dyads_duplicate"):
        det["duplicate_dyads_not_marked"] = int(v.scalar(f"SELECT count(*) FROM dyads_duplicate WHERE conf_num NOT IN (SELECT conf_num FROM {dup})"))
    # identical text: whole-meeting normalized text (turn order) of the copy and of the kept meeting
    if det["n_duplicates"] and "text" in v.cols("turns"):
        v.con.execute(f"""CREATE OR REPLACE TEMP TABLE _dr_fp AS SELECT conf_num, md5(string_agg({norm_sql('text')}, '' ORDER BY turn_seq)) h
                          FROM turns WHERE conf_num IN (SELECT conf_num FROM {dup} UNION SELECT duplicate_of FROM {dup}) GROUP BY 1""")
        det["duplicate_text_not_identical"] = int(v.scalar(f"""SELECT count(*) FROM {dup} d LEFT JOIN _dr_fp a ON a.conf_num = d.conf_num
            LEFT JOIN _dr_fp b ON b.conf_num = d.duplicate_of WHERE a.h IS NULL OR b.h IS NULL OR a.h <> b.h"""))
    ov = "(SELECT conf_num, unnest(overlap_with) AS o FROM meetings WHERE overlap_with IS NOT NULL)"
    det["overlap_target_missing"] = int(v.scalar(f"SELECT count(*) FROM {ov} x ANTI JOIN meetings m ON m.conf_num = x.o"))
    det["overlap_target_is_duplicate"] = int(v.scalar(f"SELECT count(*) FROM {ov} x JOIN meetings m ON m.conf_num = x.o WHERE m.duplicate_of IS NOT NULL"))
    det["overlap_not_symmetric"] = int(v.scalar(f"""SELECT count(*) FROM {ov} x WHERE NOT EXISTS (
        SELECT 1 FROM {ov} y WHERE y.conf_num = x.o AND y.o = x.conf_num)"""))
    info = {"n_meetings_with_overlap": int(v.scalar("SELECT count(*) FROM meetings WHERE overlap_with IS NOT NULL AND len(overlap_with) > 0"))}
    extra = [c for c in ("duplicate_basis", "overlap_kind") if c in v.cols("meetings")]
    info["pairs"] = v.examples(f"""SELECT conf_num, duplicate_of, overlap_with{''.join(', ' + c for c in extra)} FROM meetings
                                   WHERE duplicate_of IS NOT NULL OR (overlap_with IS NOT NULL AND len(overlap_with) > 0) ORDER BY conf_num""", 50)
    if v.has("duplicate_meetings"):
        info["duplicate_meetings_rows"] = int(v.scalar("SELECT count(*) FROM duplicate_meetings"))
    bad = sum(x for k, x in det.items() if k != "n_duplicates")
    return _res(bad, dict(det, **info))


def _user_names(v: Validator) -> list:
    import getpass
    names = set(x for x in v.params.user_names if x)
    for f in (getpass.getuser, lambda: Path.home().name):
        try:
            names.add(f())
        except Exception:
            pass
    return sorted(n for n in names if n and len(n) >= 3 and n not in ("root", "user", "runner"))


def local_path_regex(names: Sequence[str]) -> str:
    """RE2 pattern (case-insensitive) matching an absolute local path prefix or one of the user names."""
    alts = [re.escape(x) for x in LOCAL_PATH_PATTERNS] + [re.escape(n) for n in names]
    return "(?i)(" + "|".join(alts) + ")"


RELEASE_SCAN_SKIP = ("validation_report.json", "docs_numbers.json", "MANIFEST.json")   # written after validation


def scan_file_for_local_paths(con, path: Path, pattern: str) -> dict:
    """{'hits': [...]} for one release file: parquet schema/key-value metadata and every VARCHAR / list column
    (cast to text), or the whole text of any other file."""
    rx = re.compile(pattern)
    hits = []

    def kind(m):      # never echo the matched text (the report itself must stay clean)
        return "user_name" if not any(m.group(0).lower() == x.lower() for x in LOCAL_PATH_PATTERNS) else "local_path_prefix"
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq
        md = pq.ParquetFile(path).schema_arrow.metadata or {}
        for k, x in md.items():
            t = (k + b"=" + x).decode("utf-8", "replace")
            if rx.search(t):
                hits.append({"where": "parquet_metadata", "key": rx.sub("<local>", k.decode("utf-8", "replace")), "match": kind(rx.search(t))})
        cols = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_s(str(path))})").fetchall()]
        sc = [c for c, t in cols if t == "VARCHAR" or t.endswith("[]")]
        if sc:
            agg = ", ".join(f"count(*) FILTER (WHERE regexp_matches(CAST({_q(c)} AS VARCHAR), {_s(pattern)})) AS {_q(c)}" for c in sc)
            r = con.execute(f"SELECT {agg} FROM read_parquet({_s(str(path))})").fetchone()
            for c, n in zip(sc, r):
                if n:
                    ex = con.execute(f"""SELECT left(CAST({_q(c)} AS VARCHAR), 120) FROM read_parquet({_s(str(path))})
                                         WHERE regexp_matches(CAST({_q(c)} AS VARCHAR), {_s(pattern)}) LIMIT 1""").fetchone()[0]
                    hits.append({"where": "column", "column": c, "rows": int(n), "example": rx.sub("<local>", ex or "")})
        if rx.search(path.name):
            hits.append({"where": "file_name"})
    else:
        t = path.read_bytes().decode("utf-8", "replace")
        m = rx.search(t)
        if m:
            hits.append({"where": "text", "count": len(rx.findall(t)), "match": kind(m),
                         "context": rx.sub("<local>", t[max(0, m.start() - 40): m.end() + 40])})
    return {"hits": hits}


@check("release_no_local_paths", "no release file holds an absolute local path (/Users/, /home/, /private/, /tmp/, ...) or the user name: parquet string columns and metadata, JSON / CSV / text files of Params.release_root (validation_report, docs_numbers and MANIFEST are written after validation and scanned by run_all); without a release root, the string columns of the release tables are scanned")
def c_release_paths(v: Validator) -> Result:
    names = _user_names(v)
    pattern = local_path_regex(names)
    det = {"patterns": "validate.LOCAL_PATH_PATTERNS + the login / home directory name", "n_user_names": len(names)}
    bad, ex = 0, []
    root = v.params.release_root
    if root:
        root = Path(root)
        if not root.exists():
            raise Skip(f"release root missing")
        files = sorted(p for p in root.rglob("*") if p.is_file() and p.name not in RELEASE_SCAN_SKIP)
        det["files_scanned"] = len(files)
        det["files_skipped"] = [p for p in RELEASE_SCAN_SKIP if (root / p).exists()]
        for f in files:
            h = scan_file_for_local_paths(v.con, f, pattern)["hits"]
            if h:
                bad += len(h)
                ex.append({"file": re.sub(pattern, "<local>", str(f.relative_to(root))), "hits": h[:3]})
    else:
        tabs = [t for t in ("turns_release", "meetings", "dyads_release", "agenda", "footer", "crosswalk", "crosswalk_turns",
                            "turns_duplicate", "dyads_duplicate", "duplicate_meetings") if v.has(t)]
        if not tabs:
            raise Skip("no release root and no release table")
        det["tables_scanned"] = tabs
        for t in tabs:
            sc = [c for c, ty in v.cols(t).items() if ty == "VARCHAR" or ty.endswith("[]")]
            if not sc:
                continue
            agg = ", ".join(f"count(*) FILTER (WHERE regexp_matches(CAST({_q(c)} AS VARCHAR), {_s(pattern)})) AS {_q(c)}" for c in sc)
            r = v.con.execute(f"SELECT {agg} FROM {t}").fetchone()
            for c, n in zip(sc, r):
                if n:
                    bad += 1
                    ex.append({"table": t, "column": c, "rows": int(n)})
    return _res(bad, det, ex[:N_EXAMPLES])


@check("ids_namespace", "conf_num/conf_id pairs equal the Open API; conf_id keeps its zeros and prefix; v9 ids follow their namespace (CONF_ID without leading zero, CONFER_NUM for v6 HTML); turns/dyads/agenda/footer refer to meetings by conf_num")
def c_ids(v: Validator) -> Result:
    v.need("meetings", "universe")
    det = {}
    det["conf_id_differs_from_api"] = int(v.scalar(
        "SELECT count(*) FROM meetings m JOIN universe u ON u.CONFER_NUM = m.conf_num WHERE m.conf_id IS DISTINCT FROM u.CONF_ID"))
    det["conf_id_bad_format"] = int(v.scalar(
        "SELECT count(*) FROM meetings WHERE conf_id IS NOT NULL AND NOT regexp_full_match(CAST(conf_id AS VARCHAR), '^N?[0-9]{6}$')"))
    det["conf_id_known_to_api_under_other_conf_num"] = int(v.scalar(
        "SELECT count(*) FROM meetings m JOIN universe u ON u.CONF_ID = CAST(m.conf_id AS VARCHAR) WHERE u.CONFER_NUM <> m.conf_num"))
    for t in ("turns", "dyads", "agenda", "footer"):
        if v.has(t) and "conf_num" in v.cols(t):
            det[f"{t}_conf_num_not_in_meetings"] = int(v.scalar(f"SELECT count(*) FROM (SELECT DISTINCT conf_num FROM {t}) x ANTI JOIN meetings m USING (conf_num)"))
    ex = v.examples("SELECT m.conf_num, m.conf_id, u.CONF_ID AS api_conf_id FROM meetings m JOIN universe u ON u.CONFER_NUM = m.conf_num WHERE m.conf_id IS DISTINCT FROM u.CONF_ID") \
        if det["conf_id_differs_from_api"] else []
    # info: CONF_IDs that collide once the 'N' prefix and leading zeros are stripped (legitimately different
    # meetings, e.g. 053840 vs N053840): any join on an integer-cast CONF_ID would merge them
    info_twins = int(v.scalar("""SELECT count(*) FROM (SELECT ltrim(CAST(conf_id AS VARCHAR), 'N0') k FROM meetings
                                  WHERE conf_id IS NOT NULL GROUP BY 1 HAVING count(*) > 1)"""))

    def ns_violations(tbl: str, src_expr: str, mm_expr: str) -> str:
        """v9 ids: CONF_ID verbatim or without its leading zero (never an N-prefixed CONF_ID) for
        XLSX/v7/v8; CONFER_NUM for v6 HTML. Rows matched by date/committee (not by id) are exempt."""
        return f"""SELECT x.v9_meeting_id, x.conf_num, x.conf_id, {src_expr} AS v9_source, {mm_expr} AS match_method FROM {tbl} x
                   WHERE x.v9_meeting_id IS NOT NULL AND x.conf_num IS NOT NULL
                     AND coalesce({mm_expr}, '') NOT IN {ID_EXEMPT_METHODS} AND (
                     ({src_expr} = 'v6_html' AND x.v9_meeting_id <> CAST(x.conf_num AS VARCHAR))
                  OR (coalesce({src_expr}, '') <> 'v6_html' AND (x.conf_id IS NULL
                        OR (x.v9_meeting_id <> CAST(x.conf_id AS VARCHAR) AND x.v9_meeting_id <> ltrim(CAST(x.conf_id AS VARCHAR), '0'))
                        OR CAST(x.conf_id AS VARCHAR) LIKE 'N%')))"""
    if v.has("crosswalk") and all(c in v.cols("crosswalk") for c in ("v9_meeting_id", "conf_num", "conf_id", "v9_source")):
        mm = "x.v9_match_method" if "v9_match_method" in v.cols("crosswalk") else "NULL"
        s = ns_violations("crosswalk", "x.v9_source", mm)
        det["crosswalk_v9_namespace_violations"] = int(v.scalar(f"SELECT count(*) FROM ({s})"))
        ex += v.examples(s, 5)
    if v.has("v9_meetings") and "v9_meeting_id" in v.cols("meetings"):
        mm = "x.match_method" if "match_method" in v.cols("v9_meetings") else "NULL"
        mmc = ", w.match_method" if "match_method" in v.cols("v9_meetings") else ""
        s = ns_violations(f"(SELECT m.v9_meeting_id, m.conf_num, m.conf_id, w.v9_source{mmc} FROM meetings m JOIN v9_meetings w ON w.meeting_id = m.v9_meeting_id)", "x.v9_source", mm)
        det["meetings_v9_namespace_violations"] = int(v.scalar(f"SELECT count(*) FROM ({s})"))
        det["meetings_v9_id_unknown"] = int(v.scalar("SELECT count(*) FROM meetings m ANTI JOIN v9_meetings w ON w.meeting_id = m.v9_meeting_id WHERE m.v9_meeting_id IS NOT NULL"))
        ex += v.examples(s, 5)
    return _res(sum(det.values()), dict(det, **{"info_conf_id_integer_twins": info_twins}), ex)


@check("roles_staff", "no committee staff (or staff title) on the legislator side; role and role_group agree with the v9 taxonomy sets; dyad sides carry legislator / nonlegislator roles")
def c_roles(v: Validator) -> Result:
    v.need_cols("turns", "role_group")
    c = v.cols("turns")
    det = {}
    if "speaker_pos" in c:
        det["legislator_side_staff_title"] = int(v.scalar(
            f"SELECT count(*) FROM turns WHERE role_group = 'legislator' AND regexp_matches(coalesce(speaker_pos, ''), '{STAFF_POS_RE}')"))
    if "role" in c:
        leg = ", ".join(_s(x) for x in sorted(LR.LEG_ROLES))
        non = ", ".join(_s(x) for x in sorted(LR.NONLEG_ROLES))
        exc = ", ".join(_s(x) for x in sorted(LR.EXCLUDED_ROLES))
        det["role_group_inconsistent_with_role"] = int(v.scalar(f"""SELECT count(*) FROM turns WHERE NOT (
            (role IN ({leg}) AND role_group = 'legislator') OR (role IN ({non}) AND role_group = 'nonlegislator')
            OR (role IN ({exc}) AND role_group = 'excluded'))"""))
        det["committee_staff_not_excluded"] = int(v.scalar("SELECT count(*) FROM turns WHERE role = 'committee_staff' AND role_group IS DISTINCT FROM 'excluded'"))
        if v.has("dyads") and "leg_role" in v.cols("dyads") and "wit_role" in v.cols("dyads"):
            det["dyad_leg_role_not_legislator"] = int(v.scalar(f"SELECT count(*) FROM dyads WHERE leg_role NOT IN ({leg}) OR leg_role IS NULL"))
            det["dyad_wit_role_not_nonlegislator"] = int(v.scalar(f"SELECT count(*) FROM dyads WHERE wit_role NOT IN ({non}) OR wit_role IS NULL"))
    elif v.params.mode == "release":
        raise Skip("turns lack role")
    ex = v.examples(f"SELECT conf_num, turn_seq, speaker_label_raw, speaker_pos, role_group FROM turns WHERE role_group = 'legislator' AND regexp_matches(coalesce(speaker_pos, ''), '{STAFF_POS_RE}')") if "speaker_pos" in c and "speaker_label_raw" in c else []
    return _res(sum(det.values()), det, ex)


@check("roles_wit_title_share", "share of dyads with a legislator title on the witness side is below the threshold, overall and per hearing_type")
def c_wit_title(v: Validator) -> Result:
    v.need_cols("dyads", "wit_is_legislator_title")
    p = v.params
    tot = v.one("SELECT count(*) n, count(*) FILTER (WHERE wit_is_legislator_title) k FROM dyads")
    n, k = int(tot["n"] or 0), int(tot["k"] or 0)
    share = k / n if n else 0.0
    by = []
    ht_src = ("dyads" if "hearing_type" in v.cols("dyads") else
              "(SELECT d.wit_is_legislator_title, m.hearing_type FROM dyads d LEFT JOIN meetings m USING (conf_num))"
              if v.has("meetings") and "hearing_type" in v.cols("meetings") else None)
    if ht_src:
        by = v.q(f"SELECT hearing_type, count(*) n, count(*) FILTER (WHERE wit_is_legislator_title) k FROM {ht_src} GROUP BY 1 ORDER BY 1").to_dict("records")
        for r in by:
            r["share"] = round(r["k"] / r["n"], 5) if r["n"] else 0.0
    over = [r for r in by if r["share"] > p.wit_title_max_share_by_type]
    bad = int(share > p.wit_title_max_share) + len(over)
    return _res(bad, {"n_dyads": n, "n_wit_legislator_title": k, "share": round(share, 5),
                      "max_share": p.wit_title_max_share, "by_hearing_type": by,
                      "types_over_threshold": [r["hearing_type"] for r in over]})


SENTINELS = ("", "nan", "NaN", "NAN", "None", "none", "null", "NULL", "Null", "<NA>", "NA", "N/A", "NaT")


def blank_sql(expr: str) -> str:
    """True when a VARCHAR holds a missing-value sentinel instead of NULL (D10: '' and 'nan')."""
    lst = ", ".join(_s(x) for x in SENTINELS)
    return f"(trim(CAST({expr} AS VARCHAR)) IN ({lst}))"


def present_sql(expr: str) -> str:
    """Non-null and not a missing-value sentinel."""
    return f"({expr} IS NOT NULL AND NOT {blank_sql(expr)})"


@check("legislator_links", "share of legislator-side turns linked to a NAAS_CD meets the threshold in every term; a blank or 'nan' naas_cd counts as unlinked and is itself an error")
def c_links(v: Validator) -> Result:
    v.need_cols("turns", "role_group", "naas_cd")
    term_expr = "t.term" if "term" in v.cols("turns") else "m.term"
    join = "" if "term" in v.cols("turns") else "JOIN meetings m USING (conf_num)"
    by = v.q(f"""SELECT {term_expr} AS term, count(*) n, count(*) FILTER (WHERE {present_sql('t.naas_cd')}) linked,
                        count(*) FILTER (WHERE t.naas_cd IS NOT NULL AND {blank_sql('t.naas_cd')}) blank
                 FROM turns t {join} WHERE t.role_group = 'legislator' GROUP BY 1 ORDER BY 1""").to_dict("records")
    bad = []
    for r in by:
        r["share"] = round(r["linked"] / r["n"], 5) if r["n"] else None
        thr = v.params.link_thresholds.get(int(r["term"])) if r["term"] is not None else None
        r["threshold"] = thr
        if thr is None or r["share"] is None or r["share"] < thr:
            bad.append(r["term"])
    n_blank = int(v.scalar(f"SELECT count(*) FROM turns t WHERE t.naas_cd IS NOT NULL AND {blank_sql('t.naas_cd')}"))
    ex = v.examples(f"SELECT conf_num, turn_seq, speaker_label_raw, role_group, naas_cd FROM turns t WHERE t.naas_cd IS NOT NULL AND {blank_sql('t.naas_cd')}") \
        if n_blank and "speaker_label_raw" in v.cols("turns") else []
    return _res(len(bad) + n_blank, {"by_term": by, "terms_below_threshold": bad, "naas_cd_blank_string": n_blank}, ex)


@check("party_coverage", "legislator-side turns carry a party: no linked turn (naas_cd set) lacks a party, no party is a blank/'nan' string, and the share with a party meets the threshold in every term")
def c_party_cov(v: Validator) -> Result:
    v.need_cols("turns", "role_group", "party", "naas_cd")
    term_expr = "t.term" if "term" in v.cols("turns") else "m.term"
    join = "" if "term" in v.cols("turns") else "JOIN meetings m USING (conf_num)"
    base = f"FROM turns t {join} WHERE t.role_group = 'legislator'"
    by = v.q(f"""SELECT {term_expr} AS term, count(*) n,
                        count(*) FILTER (WHERE {present_sql('t.party')}) with_party,
                        count(*) FILTER (WHERE {present_sql('t.naas_cd')}) linked,
                        count(*) FILTER (WHERE {present_sql('t.naas_cd')} AND NOT {present_sql('t.party')}) linked_party_missing
                 {base} GROUP BY 1 ORDER BY 1""").to_dict("records")
    low = []
    for r in by:
        r["share"] = round(r["with_party"] / r["n"], 5) if r["n"] else None
        thr = v.params.party_thresholds.get(int(r["term"])) if r["term"] is not None else None
        r["threshold"] = thr
        if thr is None or r["share"] is None or r["share"] < thr:
            low.append(r["term"])
    det = {
        "linked_party_missing": int(sum(r["linked_party_missing"] for r in by)),
        "party_blank_string": int(v.scalar(f"SELECT count(*) FROM turns t WHERE t.party IS NOT NULL AND {blank_sql('t.party')}")),
        "terms_below_threshold": low, "max_linked_party_null": v.params.max_linked_party_null, "by_term": by,
    }
    if "party_method" in v.cols("turns"):
        det["legislator_party_method"] = v.q(f"SELECT t.party_method, count(*) n {base} GROUP BY 1 ORDER BY 2 DESC").to_dict("records")
    bad = max(0, det["linked_party_missing"] - v.params.max_linked_party_null) + det["party_blank_string"] + len(low)
    ex = v.examples(f"""SELECT t.conf_num, t.turn_seq, t.naas_cd, t.party, t.speech_date {base}
                        AND {present_sql('t.naas_cd')} AND NOT {present_sql('t.party')}""")
    return _res(bad, det, ex)


@check("ruling_null", "ruling_status is null only when presidency_state is acting/vacant, the party is missing, or (partyless_rule 'null' only) the president has no party; never set during acting/vacant")
def c_ruling(v: Validator) -> Result:
    v.need_cols("turns", "role_group", "ruling_status", "presidency_state", "party")
    has_pp = "president_party" in v.cols("turns")
    allow_pp = (f"OR (president_party IS NULL AND presidency_state IN ('normal','suspended','partyless'))"
                if (has_pp and v.params.partyless_rule == "null") else "")
    base = "FROM turns WHERE role_group = 'legislator'"
    det = {
        "null_without_reason": int(v.scalar(f"""SELECT count(*) {base} AND ruling_status IS NULL AND NOT (
            coalesce(presidency_state IN ('acting','vacant'), false) OR party IS NULL {allow_pp})""")),
        "set_during_acting_or_vacant": int(v.scalar(f"SELECT count(*) {base} AND ruling_status IS NOT NULL AND presidency_state IN ('acting','vacant')")),
        "presidency_state_null": int(v.scalar(f"SELECT count(*) {base} AND presidency_state IS NULL")),
    }
    info = v.q(f"""SELECT presidency_state, (party IS NULL) AS party_null, {'(president_party IS NULL)' if has_pp else 'NULL'} AS president_partyless,
                   count(*) n FROM turns WHERE role_group = 'legislator' AND ruling_status IS NULL GROUP BY ALL ORDER BY ALL""").to_dict("records")
    ex = v.examples(f"""SELECT conf_num, turn_seq, party, presidency_state, ruling_status {', president_party' if has_pp else ''} {base}
        AND ((ruling_status IS NULL AND NOT (coalesce(presidency_state IN ('acting','vacant'), false) OR party IS NULL {allow_pp}))
             OR (ruling_status IS NOT NULL AND presidency_state IN ('acting','vacant')))""")
    return _res(sum(det.values()), dict(det, null_breakdown=info, partyless_rule=v.params.partyless_rule), ex)


def _satellite_sql(v: Validator) -> str:
    """(label, parent) of satellite parties from party_lineage.csv; a satellite counts as its parent."""
    if v.has("lineage") and all(c in v.cols("lineage") for c in ("label", "satellite_of")):
        return ("(SELECT DISTINCT regexp_replace(label, '\\s', '', 'g') AS label, regexp_replace(satellite_of, '\\s', '', 'g') AS parent "
                "FROM lineage WHERE satellite_of IS NOT NULL AND trim(satellite_of) <> '')")
    return "(SELECT NULL::VARCHAR AS label, NULL::VARCHAR AS parent WHERE false)"


def expected_ruling_sql(v: Validator) -> str:
    """Independent recomputation of ruling_status for legislator-side turns from party, the turn date
    and the president calendar (researcher decisions 2026-09-25 and decision 6 of 2026-09-26): null during
    acting/vacant or when the party is missing; 'independent' for 무소속; 'ruling' when the party (a satellite
    counts as its parent) equals the label counting as ruling on that date (cal.ruling_ref: the president's
    formal party, or in a partyless window his most recent party carried through its lineage), whitespace
    ignored; else 'opposition'. With partyless_rule 'null' a partyless date gives null."""
    d = v.turn_date_expr()
    join = "LEFT JOIN meetings m USING (conf_num)" if v.has("meetings") else ""
    pp_null = "NULL" if v.params.partyless_rule == "null" else "'opposition'"
    return f"""
WITH x AS (SELECT t.conf_num, t.turn_seq, {d} AS d, t.party, t.ruling_status,
                  regexp_replace(t.party, '\\s', '', 'g') AS p, c.presidency_state AS st,
                  c.ruling_ref AS pp, c.president_party
           FROM turns t {join} LEFT JOIN cal c ON {d} BETWEEN c.start AND c."end"
           WHERE t.role_group = 'legislator'),
y AS (SELECT x.*, coalesce(s.parent, x.p) AS camp FROM x LEFT JOIN {_satellite_sql(v)} s ON s.label = x.p)
SELECT conf_num, turn_seq, d, party, president_party, pp AS ruling_reference, st AS presidency_state_expected, ruling_status,
       CASE WHEN st IS NULL THEN NULL
            WHEN st IN ('acting', 'vacant') THEN NULL
            WHEN NOT {present_sql('party')} THEN NULL
            WHEN p = '무소속' THEN 'independent'
            WHEN pp IS NULL THEN {pp_null}
            WHEN camp = pp THEN 'ruling'
            ELSE 'opposition' END AS expected
FROM y"""


@check("ruling_recompute", "ruling_status of every legislator-side turn equals an independent recomputation from party, turn date and the president calendar (catches inverted or per-term constant ruling status, D7)")
def c_ruling_recompute(v: Validator) -> Result:
    v.need("calendar")
    v.need_cols("turns", "role_group", "party", "ruling_status")
    if v.params.mode == "release":
        v.need("lineage")
    e = expected_ruling_sql(v)
    v.con.execute(f"CREATE OR REPLACE TEMP TABLE _rul AS {e}")
    det = v.one("""SELECT count(*) AS n_legislator_turns,
                          count(*) FILTER (WHERE ruling_status IS DISTINCT FROM expected) AS n_mismatch,
                          count(*) FILTER (WHERE d IS NULL) AS turn_date_unknown,
                          count(*) FILTER (WHERE d IS NOT NULL AND presidency_state_expected IS NULL) AS date_outside_calendar
                   FROM _rul""")
    det = {k: int(x or 0) for k, x in det.items()}
    det["expected_distribution"] = v.q("SELECT expected, count(*) n FROM _rul GROUP BY 1 ORDER BY 1").to_dict("records")
    det["mismatch_pairs"] = v.q("""SELECT ruling_status, expected, count(*) n FROM _rul WHERE ruling_status IS DISTINCT FROM expected
                                   GROUP BY ALL ORDER BY n DESC LIMIT 20""").to_dict("records")
    det["mismatch_by_party"] = v.q("""SELECT party, ruling_reference, ruling_status, expected, count(*) n FROM _rul
                                      WHERE ruling_status IS DISTINCT FROM expected GROUP BY ALL ORDER BY n DESC LIMIT 20""").to_dict("records")
    det["lineage_available"] = v.has("lineage")
    bad = det["n_mismatch"] + det["turn_date_unknown"] + det["date_outside_calendar"]
    return _res(bad, det, v.examples("SELECT * FROM _rul WHERE ruling_status IS DISTINCT FROM expected OR d IS NULL ORDER BY conf_num, turn_seq"))


@check("presidency_by_date", "presidency_state equals the president calendar on the turn date (speech_date, else meeting date) for every turn; a null presidency_state or an unknown turn date is an error")
def c_presidency(v: Validator) -> Result:
    v.need("calendar")
    v.need_cols("turns", "presidency_state")
    d = v.turn_date_expr()
    join = "LEFT JOIN meetings m USING (conf_num)" if v.has("meetings") else ""
    sql = f"""SELECT t.conf_num, t.turn_seq, {d} AS d, t.presidency_state, c.presidency_state AS expected
              FROM turns t {join} LEFT JOIN cal c ON {d} BETWEEN c.start AND c."end"
              WHERE t.presidency_state IS DISTINCT FROM c.presidency_state OR {d} IS NULL"""
    det = v.one(f"""SELECT count(*) AS n_mismatch,
                           count(*) FILTER (WHERE presidency_state IS NULL AND expected IS NOT NULL) AS null_where_calendar_has_state,
                           count(*) FILTER (WHERE d IS NULL) AS turn_date_unknown,
                           count(*) FILTER (WHERE d IS NOT NULL AND expected IS NULL) AS date_outside_calendar
                    FROM ({sql})""")
    det = {k: int(x or 0) for k, x in det.items()}
    det["n_checked"] = int(v.scalar("SELECT count(*) FROM turns"))
    if "role_group" in v.cols("turns"):
        det["mismatch_by_role_group"] = v.q(f"""SELECT t.role_group, count(*) n FROM turns t {join} LEFT JOIN cal c ON {d} BETWEEN c.start AND c."end"
            WHERE t.presidency_state IS DISTINCT FROM c.presidency_state OR {d} IS NULL GROUP BY 1 ORDER BY 1""").to_dict("records")
    return _res(det["n_mismatch"], det, v.examples(sql))


@check("president_by_date", "president_party, president and president_last_party equal the president calendar on the turn date for every turn (the formal party after renames, null while the president has no party, during acting presidencies and vacancies; president_last_party = the formal party, else in a partyless window or a suspension without a party his most recent party, null in acting windows)")
def c_president(v: Validator) -> Result:
    v.need("calendar")
    v.need_cols("turns", "president_party")
    d = v.turn_date_expr()
    join = "LEFT JOIN meetings m USING (conf_num)" if v.has("meetings") else ""
    tc = v.cols("turns")
    has_pres = "president" in tc
    if not has_pres and v.params.mode == "release":
        raise Skip("turns lack president")
    has_last = "president_last_party" in tc
    if not has_last and v.params.mode == "release" and v.params.partyless_rule == "last_president_party":
        raise Skip("turns lack president_last_party (researcher decision 6)")
    pres_cmp = "OR t.president IS DISTINCT FROM c.president" if has_pres else ""
    last_cmp = "OR t.president_last_party IS DISTINCT FROM c.president_last_party" if has_last else ""
    sql = f"""SELECT t.conf_num, t.turn_seq, {d} AS d, t.president_party, c.president_party AS expected_president_party
                     {', t.president, c.president AS expected_president' if has_pres else ''}
                     {', t.president_last_party, c.president_last_party AS expected_president_last_party' if has_last else ''}
              FROM turns t {join} LEFT JOIN cal c ON {d} BETWEEN c.start AND c."end"
              WHERE {d} IS NULL OR t.president_party IS DISTINCT FROM c.president_party {pres_cmp} {last_cmp}"""
    n = int(v.scalar(f"SELECT count(*) FROM ({sql})"))
    by = v.q(f"""SELECT president_party, expected_president_party{', president_last_party, expected_president_last_party' if has_last else ''}, count(*) n
                 FROM ({sql}) GROUP BY ALL ORDER BY n DESC LIMIT 20""").to_dict("records")
    return _res(n, {"n_mismatch": n, "n_checked": int(v.scalar("SELECT count(*) FROM turns")), "mismatch_pairs": by,
                    "president_checked": has_pres, "president_last_party_checked": has_last}, v.examples(sql))


@check("partyless_windows", "researcher decision 6: in every partyless-president window (calendar rows of a president in office or suspended without a party) presidency_state is 'partyless' (or 'suspended' during a suspension), legislator ruling_status follows the last-party rule (the president's most recent party and its lineage successors ruling, 무소속 independent, others opposition), and 'partyless' never appears outside those windows; reported window by window")
def c_partyless(v: Validator) -> Result:
    v.need("calendar")
    v.need_cols("turns", "presidency_state", "role_group", "ruling_status", "party")
    if v.params.mode == "release" and v.params.partyless_rule == "last_president_party":
        v.need("lineage")
    d = v.turn_date_expr()
    join = "LEFT JOIN meetings m USING (conf_num)" if v.has("meetings") else ""
    wins = v.q("""SELECT start, "end", presidency_state, president, president_last_party, ruling_ref FROM cal
                  WHERE president_party IS NULL AND presidency_state IN ('partyless', 'suspended', 'normal')
                    AND president IS NOT NULL ORDER BY start""")
    v.con.execute(f"CREATE OR REPLACE TEMP TABLE _rul_pl AS {expected_ruling_sql(v)}")
    by, bad = [], 0
    for w in wins.itertuples():
        r = v.one(f"""SELECT count(*) AS turns,
                             count(*) FILTER (WHERE t.presidency_state IS DISTINCT FROM {_s(w.presidency_state)}) AS state_mismatch
                      FROM turns t {join} WHERE {d} BETWEEN {_s(w.start)} AND {_s(w.end)}""")
        q = v.one(f"""SELECT count(*) AS legislator_turns,
                             count(*) FILTER (WHERE ruling_status = 'ruling') AS ruling,
                             count(*) FILTER (WHERE ruling_status = 'opposition') AS opposition,
                             count(*) FILTER (WHERE ruling_status = 'independent') AS independent,
                             count(*) FILTER (WHERE ruling_status IS NULL) AS null_status,
                             count(*) FILTER (WHERE ruling_status IS DISTINCT FROM expected) AS ruling_mismatch
                      FROM _rul_pl WHERE d BETWEEN {_s(w.start)} AND {_s(w.end)}""")
        row = {"start": w.start, "end": w.end, "presidency_state": w.presidency_state, "president": w.president,
               "president_last_party": w.president_last_party, "ruling_reference": w.ruling_ref,
               **{k: int(x or 0) for k, x in r.items()}, **{k: int(x or 0) for k, x in q.items()}}
        bad += row["state_mismatch"] + row["ruling_mismatch"]
        by.append(row)
    outside = int(v.scalar(f"""SELECT count(*) FROM turns t {join} LEFT JOIN cal c ON {d} BETWEEN c.start AND c."end"
                                WHERE t.presidency_state = 'partyless' AND c.presidency_state IS DISTINCT FROM 'partyless'"""))
    bad += outside
    ex = v.examples(f"""SELECT * FROM _rul_pl r WHERE r.ruling_status IS DISTINCT FROM r.expected
                        AND EXISTS (SELECT 1 FROM cal c WHERE r.d BETWEEN c.start AND c."end" AND c.president_party IS NULL
                                    AND c.president IS NOT NULL) ORDER BY conf_num, turn_seq""")
    return _res(bad, {"partyless_rule": v.params.partyless_rule, "windows": by,
                      "partyless_state_outside_windows": outside}, ex)


@check("admin_by_date", "admin and admin_ideology equal the president calendar on the turn date (a suspended president stays the administration)")
def c_admin(v: Validator) -> Result:
    v.need("calendar")
    v.need_cols("turns", "admin")
    d = v.turn_date_expr()
    join = "LEFT JOIN meetings m USING (conf_num)" if v.has("meetings") else ""
    ideol = "t.admin_ideology" if "admin_ideology" in v.cols("turns") else "c.admin_ideology"
    sql = f"""SELECT t.conf_num, t.turn_seq, {d} AS d, t.admin, c.admin AS expected_admin, {ideol} AS admin_ideology,
                     c.admin_ideology AS expected_ideology
              FROM turns t {join} LEFT JOIN cal c ON {d} BETWEEN c.start AND c."end"
              WHERE {d} IS NOT NULL AND (t.admin IS DISTINCT FROM c.admin OR {ideol} IS DISTINCT FROM c.admin_ideology)"""
    n = int(v.scalar(f"SELECT count(*) FROM ({sql})"))
    by = v.q(f"SELECT t.admin, expected_admin, count(*) n FROM ({sql}) t GROUP BY ALL ORDER BY n DESC LIMIT 20").to_dict("records")
    return _res(n, {"n_mismatch": n, "mismatch_pairs": by}, v.examples(sql))


@check("agenda_integrity", "agenda rows belong to meetings, after_turn_seq lies in 0..n_turns, and every turn agenda_ordinal refers to an agenda row")
def c_agenda(v: Validator) -> Result:
    v.need("agenda", "meetings")
    det = {"agenda_without_meeting": int(v.scalar("SELECT count(*) FROM agenda a ANTI JOIN meetings m USING (conf_num)"))}
    if "after_turn_seq" in v.cols("agenda"):
        det["after_turn_seq_out_of_range"] = int(v.scalar(
            "SELECT count(*) FROM agenda a JOIN meetings m USING (conf_num) WHERE a.after_turn_seq IS NOT NULL AND (a.after_turn_seq < 0 OR a.after_turn_seq > coalesce(m.n_turns, 0))"))
    ex = []
    if v.has("turns") and "agenda_ordinal" in v.cols("turns"):
        s = "SELECT t.conf_num, t.turn_seq, t.agenda_ordinal FROM turns t ANTI JOIN agenda a ON a.conf_num = t.conf_num AND a.ordinal = t.agenda_ordinal WHERE t.agenda_ordinal IS NOT NULL"
        det["turn_agenda_ordinal_dangling"] = int(v.scalar(f"SELECT count(*) FROM ({s})"))
        ex = v.examples(s)
    return _res(sum(det.values()), det, ex)


@check("footer_integrity", "footer rows belong to built meetings")
def c_footer(v: Validator) -> Result:
    v.need("footer", "meetings")
    b = v.built_expr("m")
    det = {"footer_without_meeting": int(v.scalar("SELECT count(*) FROM footer f ANTI JOIN meetings m USING (conf_num)")),
           "footer_for_unbuilt_meeting": int(v.scalar(f"SELECT count(*) FROM footer f JOIN meetings m USING (conf_num) WHERE NOT {b}"))}
    return _res(sum(det.values()), det)


@check("crosswalk_integrity", "crosswalk has every v9 meeting exactly once and every universe or meetings-table meeting linked or v10_only (v10_only = no v9 row carries its transcript); every content meeting carried by several v9 rows has exactly one primary and all other rows point to it; relation-specific fields are consistent")
def c_cw(v: Validator) -> Result:
    v.need("crosswalk")
    v.need_cols("crosswalk", "v9_meeting_id", "conf_num", "relation", "content_conf_num", "duplicate_of_v9_meeting_id")
    cc = v.cols("crosswalk")
    subs = {}      # name -> SQL selecting the offending rows (count + examples from the same query)
    if v.has("v9_meetings"):
        subs["v9_meeting_missing"] = "SELECT w.meeting_id AS v9_meeting_id FROM (SELECT DISTINCT meeting_id FROM v9_meetings) w ANTI JOIN crosswalk c ON c.v9_meeting_id = w.meeting_id"
        subs["v9_meeting_unknown"] = "SELECT c.v9_meeting_id, c.conf_num, c.relation FROM crosswalk c ANTI JOIN v9_meetings w ON w.meeting_id = c.v9_meeting_id WHERE c.v9_meeting_id IS NOT NULL"
    elif v.params.mode == "release":
        raise Skip("v9_meetings missing")
    subs["v9_meeting_repeated"] = "SELECT v9_meeting_id, n FROM (SELECT v9_meeting_id, count(*) n FROM crosswalk WHERE v9_meeting_id IS NOT NULL GROUP BY 1 HAVING count(*) > 1)"
    if v.has("universe"):
        subs["universe_meeting_absent"] = """SELECT u.CONFER_NUM AS conf_num FROM universe u WHERE NOT EXISTS (
            SELECT 1 FROM crosswalk c WHERE c.conf_num = u.CONFER_NUM OR c.content_conf_num = u.CONFER_NUM)"""
    if v.has("meetings"):
        subs["meetings_row_absent"] = """SELECT m.conf_num FROM meetings m WHERE NOT EXISTS (
            SELECT 1 FROM crosswalk c WHERE c.conf_num = m.conf_num OR c.content_conf_num = m.conf_num)"""
        subs["conf_num_not_in_meetings"] = """SELECT c.v9_meeting_id, c.conf_num, c.content_conf_num, c.relation, c.relation_basis FROM crosswalk c WHERE
            (c.conf_num IS NOT NULL AND c.conf_num NOT IN (SELECT conf_num FROM meetings))
            OR (c.content_conf_num IS NOT NULL AND c.content_conf_num NOT IN (SELECT conf_num FROM meetings))""" \
            if "relation_basis" in cc else """SELECT c.v9_meeting_id, c.conf_num, c.content_conf_num, c.relation FROM crosswalk c WHERE
            (c.conf_num IS NOT NULL AND c.conf_num NOT IN (SELECT conf_num FROM meetings))
            OR (c.content_conf_num IS NOT NULL AND c.content_conf_num NOT IN (SELECT conf_num FROM meetings))"""
    subs["v10_only_with_v9_id"] = "SELECT v9_meeting_id, conf_num FROM crosswalk WHERE relation = 'v10_only' AND v9_meeting_id IS NOT NULL"
    subs["v10_only_also_linked"] = """SELECT o.conf_num FROM crosswalk o WHERE o.relation = 'v10_only' AND EXISTS (
        SELECT 1 FROM crosswalk c WHERE c.relation <> 'v10_only' AND c.content_conf_num = o.conf_num)"""
    subs["same_without_conf_num"] = "SELECT v9_meeting_id, relation FROM crosswalk WHERE relation IN ('same', 'duplicate', 'v10_only') AND conf_num IS NULL"
    subs["same_content_elsewhere"] = "SELECT v9_meeting_id, conf_num, content_conf_num, relation FROM crosswalk WHERE relation IN ('same', 'duplicate') AND content_conf_num IS DISTINCT FROM conf_num"
    subs["wrong_content_equal_label"] = "SELECT v9_meeting_id, conf_num, content_conf_num FROM crosswalk WHERE relation = 'v9_wrong_content' AND content_conf_num IS NOT NULL AND content_conf_num = conf_num"
    subs["v9_only_with_content"] = "SELECT v9_meeting_id, content_conf_num FROM crosswalk WHERE relation = 'v9_only' AND content_conf_num IS NOT NULL"
    # double carriage: one primary per content meeting, every other carrier points to it
    subs["duplicate_without_target"] = "SELECT v9_meeting_id, content_conf_num FROM crosswalk WHERE relation = 'duplicate' AND duplicate_of_v9_meeting_id IS NULL"
    subs["duplicate_target_invalid"] = """SELECT d.v9_meeting_id, d.relation, d.content_conf_num, d.duplicate_of_v9_meeting_id FROM crosswalk d
        WHERE d.duplicate_of_v9_meeting_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM crosswalk s WHERE s.v9_meeting_id = d.duplicate_of_v9_meeting_id AND s.duplicate_of_v9_meeting_id IS NULL
          AND s.relation IN ('same', 'v9_wrong_content') AND s.content_conf_num = d.content_conf_num)"""
    subs["same_marked_as_copy"] = "SELECT v9_meeting_id, duplicate_of_v9_meeting_id FROM crosswalk WHERE relation = 'same' AND duplicate_of_v9_meeting_id IS NOT NULL"
    subs["multi_carrier_without_single_primary"] = """SELECT content_conf_num, count(*) n_carriers,
            count(*) FILTER (WHERE duplicate_of_v9_meeting_id IS NULL) n_primary, list(v9_meeting_id ORDER BY v9_meeting_id) v9_ids
        FROM crosswalk WHERE v9_meeting_id IS NOT NULL AND content_conf_num IS NOT NULL
        GROUP BY 1 HAVING count(*) FILTER (WHERE duplicate_of_v9_meeting_id IS NULL) <> 1"""
    if "is_second_copy" in cc:
        subs["second_copy_flag_inconsistent"] = """SELECT v9_meeting_id, is_second_copy, duplicate_of_v9_meeting_id FROM crosswalk
            WHERE v9_meeting_id IS NOT NULL AND content_conf_num IS NOT NULL
              AND is_second_copy IS DISTINCT FROM (duplicate_of_v9_meeting_id IS NOT NULL)"""
    elif v.params.mode == "release":
        raise Skip("crosswalk lacks is_second_copy")
    det, ex = {}, []
    for k, q in subs.items():
        n = int(v.scalar(f"SELECT count(*) FROM ({q})"))
        det[k] = n
        if n:
            ex += [dict(check=k, **r) for r in v.examples(q, 3)]
    rel = v.q("SELECT relation, count(*) n FROM crosswalk GROUP BY 1 ORDER BY 1").to_dict("records")
    info = {"relation_counts": rel}
    if "is_second_copy" in cc:
        info["second_copies_by_relation"] = v.q("SELECT relation, count(*) n FROM crosswalk WHERE is_second_copy GROUP BY 1 ORDER BY 1").to_dict("records")
    if "v10_status" in cc:
        info["v10_status_counts"] = v.q("SELECT v10_status, count(*) n FROM crosswalk GROUP BY 1 ORDER BY 1").to_dict("records")
    return _res(sum(det.values()), dict(det, **info), ex[:N_EXAMPLES * 2])


@check("crosswalk_turns_integrity", "turn alignment links existing v9 speeches and v10 turns, covers every row of each aligned meeting once, and preserves order")
def c_cwt(v: Validator) -> Result:
    v.need("crosswalk_turns")
    v.need_cols("crosswalk_turns", "v9_meeting_id", "v9_speech_order", "v9_order_num", "conf_num", "turn_seq", "match_type")
    det = {}
    if v.has("turns"):
        det["v10_turn_not_found"] = int(v.scalar("SELECT count(*) FROM crosswalk_turns x ANTI JOIN turns t ON t.conf_num = x.conf_num AND t.turn_seq = x.turn_seq WHERE x.turn_seq IS NOT NULL"))
        det["v10_turn_not_covered"] = int(v.scalar("""SELECT count(*) FROM turns t SEMI JOIN (SELECT DISTINCT conf_num FROM crosswalk_turns) a USING (conf_num)
            ANTI JOIN crosswalk_turns x ON x.conf_num = t.conf_num AND x.turn_seq = t.turn_seq"""))
    if v.has("v9_speeches"):
        det["v9_row_not_found"] = int(v.scalar("SELECT count(*) FROM crosswalk_turns x ANTI JOIN v9_speeches s ON s.meeting_id = x.v9_meeting_id AND CAST(s.speech_order AS VARCHAR) = x.v9_speech_order WHERE x.v9_speech_order IS NOT NULL"))
        det["v9_row_not_covered"] = int(v.scalar("""SELECT count(*) FROM (SELECT meeting_id, CAST(speech_order AS VARCHAR) so FROM v9_speeches
            WHERE meeting_id IN (SELECT DISTINCT v9_meeting_id FROM crosswalk_turns)) s
            ANTI JOIN crosswalk_turns x ON x.v9_meeting_id = s.meeting_id AND x.v9_speech_order = s.so"""))
    elif v.params.mode == "release":
        raise Skip("v9_speeches missing")
    det["one_to_one_repeated"] = int(v.scalar("""SELECT count(*) FROM (
        SELECT v9_meeting_id, v9_speech_order FROM crosswalk_turns WHERE match_type IN ('source_row','exact','normalized','similar')
        GROUP BY ALL HAVING count(*) > 1)"""))
    det["order_violations"] = int(v.scalar("""SELECT count(*) FROM (
        SELECT v9_meeting_id, v9_order_num, turn_seq,
               max(turn_seq) OVER (PARTITION BY v9_meeting_id, conf_num ORDER BY v9_order_num, turn_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
        FROM crosswalk_turns WHERE v9_order_num IS NOT NULL AND turn_seq IS NOT NULL) WHERE turn_seq < prev_max"""))
    mt = v.q("SELECT match_type, count(*) n FROM crosswalk_turns GROUP BY 1 ORDER BY 1").to_dict("records")
    return _res(sum(det.values()), dict(det, match_type_counts=mt))


# ----------------------------------------------------------------------------- docs numbers
def docs_numbers(v: Validator, report: Optional[dict] = None) -> dict:
    """Every number the documentation quotes, computed from the tables. Flat keys."""
    out = {}

    def put(prefix, df, key_cols, val="n"):
        for r in df.to_dict("records"):
            k = ".".join(str(r[c]) for c in key_cols)
            out[f"{prefix}.{k}"] = int(r[val]) if r[val] is not None and not pd.isna(r[val]) else None

    if v.has("universe"):
        out["universe.meetings"] = int(v.scalar("SELECT count(*) FROM universe"))
        put("universe.by_term", v.q("SELECT DAE_NUM term, count(*) n FROM universe GROUP BY 1"), ["term"])
        put("universe.by_class", v.q("SELECT CLASS_NAME_unified c, count(*) n FROM universe GROUP BY 1"), ["c"])
        if v.has("meetings"):
            put("universe.status", v.q(f"SELECT status, count(*) n FROM ({universe_status_sql(v)}) GROUP BY 1"), ["status"])
            out["meetings.outside_universe"] = int(v.scalar("SELECT count(*) FROM meetings m ANTI JOIN universe u ON u.CONFER_NUM = m.conf_num"))
    if v.has("meetings"):
        b = v.built_expr("m")
        out["meetings.rows"] = int(v.scalar("SELECT count(*) FROM meetings"))
        out["meetings.built"] = int(v.scalar(f"SELECT count(*) FROM meetings m WHERE {b}"))
        put("meetings.built_by_term", v.q(f"SELECT term, count(*) n FROM meetings m WHERE {b} GROUP BY 1"), ["term"])
        if "hearing_type" in v.cols("meetings"):
            put("meetings.built_by_hearing_type", v.q(f"SELECT hearing_type h, count(*) n FROM meetings m WHERE {b} GROUP BY 1"), ["h"])
        if "source" in v.cols("meetings"):
            put("meetings.built_by_source", v.q(f"SELECT source s, count(*) n FROM meetings m WHERE {b} GROUP BY 1"), ["s"])
        if "is_subcommittee" in v.cols("meetings"):
            out["meetings.built_subcommittee"] = int(v.scalar(f"SELECT count(*) FROM meetings m WHERE {b} AND is_subcommittee"))
        dr = v.q(f"SELECT term, min(date) mn, max(date) mx FROM meetings m WHERE {b} GROUP BY 1").to_dict("records")
        for r in dr:
            out[f"meetings.date_min.{r['term']}"] = r["mn"]
            out[f"meetings.date_max.{r['term']}"] = r["mx"]
    if v.has("meetings") and "duplicate_of" in v.cols("meetings"):
        out["meetings.duplicate_of"] = int(v.scalar("SELECT count(*) FROM meetings WHERE duplicate_of IS NOT NULL"))
        if "overlap_with" in v.cols("meetings"):
            out["meetings.with_overlap"] = int(v.scalar("SELECT count(*) FROM meetings WHERE overlap_with IS NOT NULL AND len(overlap_with) > 0"))
    if v.has("turns_duplicate"):
        out["duplicate_turns.rows"] = int(v.scalar("SELECT count(*) FROM turns_duplicate"))
    if v.has("dyads_duplicate"):
        out["duplicate_dyads.rows"] = int(v.scalar("SELECT count(*) FROM dyads_duplicate"))
    if v.has("turns_release"):
        out["turns.rows"] = int(v.scalar("SELECT count(*) FROM turns_release"))
        if "source" in v.cols("turns_release"):
            put("turns.by_source", v.q("SELECT source s, count(*) n FROM turns_release GROUP BY 1"), ["s"])
        if v.has("meetings"):
            put("turns.by_term", v.q("SELECT m.term, count(*) n FROM turns_release t JOIN meetings m USING (conf_num) GROUP BY 1"), ["term"])
        if "role_group" in v.cols("turns_release"):
            put("turns.by_role_group", v.q("SELECT coalesce(role_group, 'null') g, count(*) n FROM turns_release GROUP BY 1"), ["g"])
        if "naas_cd" in v.cols("turns_release"):
            out["legislators.distinct_naas_cd"] = int(v.scalar("SELECT count(DISTINCT naas_cd) FROM turns_release"))
        tc = v.cols("turns_release")
        if "after_end_marker" in tc:
            out["turns.after_end_marker"] = int(v.scalar("SELECT count(*) FROM turns_release WHERE after_end_marker"))
            out["turns.meetings_with_after_end_marker"] = int(v.scalar(
                "SELECT count(DISTINCT conf_num) FROM turns_release WHERE after_end_marker"))
        if "sitting_seq" in tc:
            out["turns.meetings_several_sittings"] = int(v.scalar(
                "SELECT count(*) FROM (SELECT conf_num FROM turns_release GROUP BY 1 HAVING count(DISTINCT sitting_seq) > 1)"))
            out["turns.in_later_sittings"] = int(v.scalar("SELECT count(*) FROM turns_release WHERE sitting_seq > 1"))
        if "label_confidence" in tc and "source" in tc:
            put("turns.label_confidence", v.q("SELECT source s, coalesce(label_confidence, 'null') c, count(*) n FROM turns_release GROUP BY 1, 2"), ["s", "c"])
    if v.has("dyads_release"):
        out["dyads.rows"] = int(v.scalar("SELECT count(*) FROM dyads_release"))
        dc = v.cols("dyads_release")
        put("dyads.by_direction", v.q("SELECT direction d, count(*) n FROM dyads_release GROUP BY 1"), ["d"])
        for f in ("leg_is_chair", "leg_is_procedural", "wit_is_legislator_title", "any_after_end_marker",
                  "any_low_label_confidence", "any_time_regress", "any_label_inconsistent"):
            if f in dc:
                out[f"dyads.{f}"] = int(v.scalar(f"SELECT count(*) FROM dyads_release WHERE {f}"))
        if "hearing_type" in dc:
            put("dyads.by_hearing_type", v.q("SELECT hearing_type h, count(*) n FROM dyads_release GROUP BY 1"), ["h"])
        if "term" in dc:
            put("dyads.by_term", v.q("SELECT term, count(*) n FROM dyads_release GROUP BY 1"), ["term"])
    if v.has("crosswalk") and "relation" in v.cols("crosswalk"):
        put("crosswalk.relation", v.q("SELECT relation r, count(*) n FROM crosswalk GROUP BY 1"), ["r"])
        cc = v.cols("crosswalk")
        if "is_second_copy" in cc:
            out["crosswalk.second_copies"] = int(v.scalar("SELECT count(*) FROM crosswalk WHERE is_second_copy"))
            put("crosswalk.second_copies_by_relation", v.q("SELECT relation r, count(*) n FROM crosswalk WHERE is_second_copy GROUP BY 1"), ["r"])
        if "content_overlap" in cc:
            put("crosswalk.content_overlap", v.q("SELECT coalesce(content_overlap, 'null') r, count(*) n FROM crosswalk WHERE v9_meeting_id IS NOT NULL GROUP BY 1"), ["r"])
    if v.has("crosswalk_turns") and "match_type" in v.cols("crosswalk_turns"):
        put("crosswalk_turns.match_type", v.q("SELECT match_type r, count(*) n FROM crosswalk_turns GROUP BY 1"), ["r"])
    if report is not None:
        for k, x in report["summary"].items():
            out[f"validation.{k}"] = x
    return dict(sorted(out.items()))


PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z0-9_.\-가-힣]+)\s*\}\}")


@check("docs_numbers", "docs_numbers.json can be generated and every {{ key }} in the documentation templates resolves to it", release_required=False)
def c_docs(v: Validator) -> Result:
    nums = docs_numbers(v)
    unresolved = {}
    for t in v.params.docs_templates:
        keys = PLACEHOLDER_RE.findall(Path(t).read_text(encoding="utf-8"))
        miss = sorted({k for k in keys if k not in nums})
        if miss:
            unresolved[str(t)] = miss
    n = sum(len(x) for x in unresolved.values())
    return _res(n, {"n_numbers": len(nums), "templates": [str(t) for t in v.params.docs_templates],
                    "unresolved": unresolved})


def render_template(text: str, numbers: dict) -> str:
    """Render {{ key }} placeholders (thousands separators for ints). Raises on a missing key."""
    def rep(m):
        k = m.group(1)
        if k not in numbers:
            raise KeyError(k)
        x = numbers[k]
        return f"{x:,}" if isinstance(x, int) else str(x)
    return PLACEHOLDER_RE.sub(rep, text)


# ----------------------------------------------------------------------------- CLI
def default_tables(root: Path = PIPE) -> dict:
    return {
        "turns": [str(root / "turns_enriched" / "**" / "*.parquet")] if list((root / "turns_enriched").glob("**/*.parquet"))
                 else [str(root / "turns" / "*" / "*" / "*.parquet")],
        "meetings": str(root / "meetings" / "meetings.parquet"),
        "dyads": str(root / "dyads" / "dyads.parquet"),
        "agenda": str(root / "agenda" / "*" / "*" / "*.parquet"),
        "footer": str(root / "footer" / "*" / "*" / "*.parquet"),
        "coverage": str(root / "build_turns" / "coverage" / "*" / "*" / "*.parquet"),
        "crosswalk": str(root / "crosswalk" / "crosswalk_meetings.parquet"),
        "crosswalk_turns": str(root / "crosswalk" / "crosswalk_turns.parquet"),
        "universe": str(INTERIM / "meeting_universe_api.parquet"),
        "crawl": str(INTERIM / "crawl_state.sqlite"),
        "v9_meetings": str(INTERIM / "v9_to_api_crosswalk.parquet"),
        "v9_speeches": str(REPO / "data" / "all_speeches_16_22_v9.parquet"),
        "calendar": str(INTERIM / "president_calendar.csv"),
        "lineage": str(INTERIM / "party_lineage.csv"),
        "duplicate_turns": str(root / "duplicate_turns" / "*" / "*.parquet"),
        "duplicate_dyads": str(root / "duplicate_dyads" / "*" / "*.parquet"),
        "duplicate_meetings": str(root / "duplicate_meetings.parquet"),
    }


def relativize(obj, repo: Path = REPO):
    """Replace absolute local paths in a JSON-able object: a path under the repository becomes repository-relative
    ('v10/interim/...'), any other absolute path '<external>/<file name>' (release files never carry local paths)."""
    rs = str(Path(repo).resolve())
    alts = sorted({rs, str(Path(repo))}, key=len, reverse=True)

    def one(x: str) -> str:
        for r in alts:
            x = x.replace(r + "/", "").replace(r, ".")
        return re.sub(r"(?<![\w.])(/(?:Users|home|private|var|tmp|Volumes)/[^\s'\",\]\}]*)",
                      lambda m: "<external>/" + Path(m.group(1)).name, x)
    if isinstance(obj, dict):
        return {relativize(k, repo) if isinstance(k, str) else k: relativize(x, repo) for k, x in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [relativize(x, repo) for x in obj]
    if isinstance(obj, Path):
        return one(str(obj))
    if isinstance(obj, str):
        return one(obj)
    return obj


def run_validation(tables: dict, params: Params, out: Optional[Path] = None, docs_out: Optional[Path] = None,
                   only: Optional[Sequence[str]] = None) -> dict:
    v = Validator(tables, params)
    rep = relativize(v.run(only=only))
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(json.dumps(rep, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    if docs_out:
        nums = docs_numbers(v, rep)
        Path(docs_out).parent.mkdir(parents=True, exist_ok=True)
        Path(docs_out).write_text(json.dumps(nums, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    return rep


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="v10 release validation (exit 1 on FAIL)")
    ap.add_argument("--root", default=str(PIPE))
    for t in Validator.TABLES:
        ap.add_argument(f"--{t.replace('_', '-')}", dest=t, nargs="+", default=None)
    ap.add_argument("--mode", choices=("release", "dev"), default="release")
    ap.add_argument("--out", default=str(PIPE / "validate" / "validation_report.json"))
    ap.add_argument("--docs-numbers", default=str(PIPE / "validate" / "docs_numbers.json"))
    ap.add_argument("--docs-templates", nargs="*", default=[])
    ap.add_argument("--only", nargs="*", default=None)
    ap.add_argument("--dup-allowlist", default=None, help="CSV with columns a,b,reason")
    ap.add_argument("--release-root", default=None, help="release directory scanned by release_no_local_paths")
    ap.add_argument("--partyless-rule", choices=PARTYLESS_RULES, default="last_president_party")
    a = ap.parse_args(argv)
    tables = default_tables(Path(a.root))
    for t in Validator.TABLES:
        x = getattr(a, t)
        if x:
            tables[t] = x if len(x) > 1 else x[0]
    allow = ()
    if a.dup_allowlist:
        al = pd.read_csv(a.dup_allowlist)
        allow = tuple((int(r.a), int(r.b), str(r.reason)) for r in al.itertuples())
    params = Params(mode=a.mode, docs_templates=tuple(a.docs_templates), dup_allowlist=allow,
                    release_root=a.release_root, partyless_rule=a.partyless_rule)
    rep = run_validation(tables, params, Path(a.out), Path(a.docs_numbers), a.only)
    for c in rep["checks"]:
        print(f"{c['status']:4s}  {c['id']:28s} n_bad={c['n_bad']:<8d} {c['seconds']:>7.1f}s")
    print(json.dumps(rep["summary"]), "->", a.out)
    return 0 if rep["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
