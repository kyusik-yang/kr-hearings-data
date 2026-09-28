"""build_turns.py - turn raw minutes sources into the v10 CONTRACT tables at scale.

Production sources (one per meeting, precedence xml > hwp, except meetings listed in the
XML-vs-HWP source override file, which put hwp first):
  xml   viewer view page  raw/viewer/view/{bucket}/{id}.html.gz, parsed by ../parse_viewer.py
  hwp   HWP file          raw/hwp/{bucket}/{id}.hwp, parsed by hwp_parser.parse_hwp (imported lazily);
        all 4,270 18대 meetings (researcher decision 2026-09-26, section 8.5)
The XLSX adapter (v9 speeches rows, data/all_speeches_16_22_v9.parquet) is kept for the v9 crosswalk
and validation only (--xlsx-compare, written to xlsx_compare/); it is no longer a production source.
A meeting built earlier from XLSX is rebuilt from its HWP file (or dropped and counted when it has
none).

Outputs (default root v10/interim/pipeline/):
  turns/ agenda/ agenda_header/ events/ footer/ rollcall/ rollcall_groups/ attendance/
      {table}/{source}/t{term}/{batch_key}.parquet   (source and term are also columns)
  meetings/meetings.parquet                            (one row per meeting of the universe)
  build_turns/state.sqlite                             (manifest: which meeting is in which batch)
  build_turns/headers/..., build_turns/coverage/...    (parsed headers, per-meeting text accounting)
  build_turns/run_{run_id}.json, build_turns/build_turns.log

Incremental: a meeting is built once. It is rebuilt when its best available source changes
(e.g. a view page appears), its raw file hash changes, its adapter version changes (parser file,
adapter code, SOURCE_VERSION), or --rebuild is given. Files of a batch are written first and the
manifest is committed afterwards; the previous rows of a rebuilt meeting are removed only after
that commit (pending_drops), so a re-parse that fails leaves the previous build in place (counted
in the run summary). Batch files that are not in the manifest (an interrupted run) are moved to
build_turns/_orphans/ at the next start, never deleted. One run at a time (build_turns/run.lock).

CLI:
  python build_turns.py --sources xml,hwp --incremental [--workers 10] [--limit N]
  python build_turns.py --status
  python build_turns.py --xlsx-compare          # all v9 XLSX meetings -> xlsx_compare/ (resource)
"""
from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import gzip
import hashlib
import html as _html
import json
import logging
import multiprocessing as mp
import os
import re
import shutil
import sqlite3
import sys
import time
import traceback
import types
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent            # v10/code/pipeline
CODE = HERE.parent                                # v10/code
V10 = CODE.parent
REPO = V10.parent
for _p in (str(CODE), str(HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _sha8_bytes(b):
    return hashlib.sha1(b).hexdigest()[:8]


def _load_pinned(name, path):
    """Import module `name` from ONE read of `path`, so the recorded sha8 (__pinned_sha8__) is the
    hash of exactly the code that runs (a plain import followed by a later file hash can record
    the hash of a file edited in between). An already imported module of that name (imported by
    another component in this process) is left in sys.modules untouched."""
    path = Path(path)
    src = path.read_bytes()
    mod = types.ModuleType(name)
    mod.__file__ = str(path)
    mod.__pinned_sha8__ = _sha8_bytes(src)
    registered = name not in sys.modules
    if registered:
        sys.modules[name] = mod          # dataclasses and pickling look the module up by name
    try:
        exec(compile(src, str(path), "exec"), mod.__dict__)
    except BaseException:
        if registered:
            sys.modules.pop(name, None)
        raise
    return mod


pv = _load_pinned("parse_viewer", CODE / "parse_viewer.py")
import legacy_rules as lr  # noqa: E402

BUILDER_VERSION = "1.2"
# per-source adapter versions: a built meeting whose stored version differs is rebuilt
# xml 1.1: div.txt leading text nodes and comment tails kept in text_raw (_speaker_block_v10)
# xml 1.2 / hwp 1.1: parenthetical lines in vote / attendance name lists -> note (not a name);
#   xml name rows printed as p.tit_sm labels read as names (method xml_footer_label_rows)
# xml 1.3: member-attendance names printed as free lines -> attendance item_kind line_name
# xml 1.4: fused 16-17대 labels split by FUSED_SPLITS (label_split v10_*), else left unsplit
# xml 1.5: FUSED_SPLITS also tried on the printed label; footer check uses the footer element the
#   parser used (nested-in-body footers flagged in coverage.detail_json)
# 1.6 (all sources, review fixes 2026-09-26): Hanja classes written as \u escapes (an NFC save had
#   turned U+F900 into U+8C48, so the class covered Hangul); fused labels split with a surname check
#   and compatibility-ideograph folding; name_from_label override restricted; re-votes in one
#   section get their own vote_seq; more attendance titles typed; XLSX time markers and
#   '(24시 경과)' roll-overs applied to time_hhmm / speech_date.
# 1.7 (all sources, 2026-09-26): turns carry after_end_marker, sitting_seq / sitting_how, label_how and
#   label_confidence (hwp: from hwp_parser when it supplies them, else from the time markers / the
#   LABEL_CONFIDENCE table; xml / xlsx: from the printed time markers, see assign_sittings()).
# 1.8 (all sources, R1 2026-09-26): after_end_marker resets at a new sitting, after_final_end_marker;
#   text_label_prefix_stripped (printed label repeated at the start of the text removed from `text`);
#   label_has_text / label_misattributed flags, fused labels rated 'low'; speaker_name_norm /
#   speaker_pos_norm; NULL instead of empty strings; footer / attendance row_seq and
#   attendance.duplicate_of_row_seq; audited agencies split outside parentheses (xml) and read from
#   HWP covers.
SOURCE_VERSION = {"xml": "1.8", "xlsx": "1.8", "hwp": "1.8"}
PRODUCTION_SOURCES = ("xml", "hwp")
# The stored version is '<SOURCE_VERSION>+<parser sha8>+<adapter code sha8>':
#   parser sha8  - the parser module the adapter runs, hashed from the bytes that were executed
#                  (xml, xlsx: parse_viewer.py; hwp: hwp_parser.py)
#   adapter sha8 - this file up to the '# ==== worker' marker (all adapter code and schemas), so an
#                  adapter edit triggers a rebuild without a manual SOURCE_VERSION bump. Planner,
#                  state and meetings-table code below the marker is not part of the hash.
PARSER_FILE = {"xml": CODE / "parse_viewer.py", "xlsx": CODE / "parse_viewer.py", "hwp": HERE / "hwp_parser.py"}
_ADAPTER_MARKER = b"\n# " + b"=" * 76 + b" worker\n"


def _adapter_code_sha8():
    b = Path(__file__).read_bytes()
    i = b.find(_ADAPTER_MARKER)
    if i < 0:
        raise RuntimeError("build_turns.py: adapter marker line '# ==== worker' not found")
    return _sha8_bytes(b[:i])


ADAPTER_SHA8 = _adapter_code_sha8()   # read once at import: the code this process runs


def _file_sha8(path):
    path = Path(path)
    return _sha8_bytes(path.read_bytes()) if path.exists() else "missing"


def _module_sha8(mod):
    """sha8 of the code a module object runs: pinned hash, else a hash of its file (test stubs)."""
    sha = getattr(mod, "__pinned_sha8__", None)
    if sha:
        return sha
    f = getattr(mod, "__file__", None)
    return _file_sha8(f) if f else "unknown"


def adapter_version(source, parser_sha8=None):
    """'<SOURCE_VERSION>+<parser sha8>+<adapter sha8>'. Workers pass the sha8 of the parser module
    they executed. Without it (planner in the main process): xml/xlsx use the pinned parse_viewer;
    hwp the hwp_parser module already loaded in this process if any, else the current
    hwp_parser.py file (a meeting whose recorded parser differs is rebuilt)."""
    if parser_sha8 is None:
        if source in ("xml", "xlsx"):
            parser_sha8 = pv.__pinned_sha8__
        elif _HWP is not None:
            parser_sha8 = _module_sha8(_HWP)
        else:
            parser_sha8 = _file_sha8(PARSER_FILE[source])
    return f"{SOURCE_VERSION[source]}+{parser_sha8}+{ADAPTER_SHA8}"


_HWP = None     # hwp_parser module, loaded lazily by _import_hwp_parser()


SEED = 8374
LOG = logging.getLogger("build_turns")

# ============================================================================ configuration


@dataclasses.dataclass
class Config:
    out: Path = V10 / "interim" / "pipeline"
    raw: Path = V10 / "raw"
    crawl_db: Path = V10 / "interim" / "crawl_state.sqlite"
    # API universe (26,077) + 187 id-gap meetings (CONF_ID null, source 'gap_scan'); researcher decision 1
    universe: Path = V10 / "interim" / "meeting_universe_v10.parquet"
    crosswalk: Path = V10 / "interim" / "v9_to_api_crosswalk.parquet"
    v9_speeches: Path = REPO / "data" / "all_speeches_16_22_v9.parquet"
    # per-meeting source overrides from the XML-vs-HWP cross-check (conf_num, source, reason);
    # a missing file means no overrides
    source_override: Path = V10 / "interim" / "pipeline" / "xml_hwp_crosscheck" / "source_override.parquet"
    duckdb_memory: str = "6GB"
    duckdb_threads: int = 4

    @property
    def state_dir(self):
        return self.out / "build_turns"

    @property
    def state_db(self):
        return self.state_dir / "state.sqlite"


def view_path(cfg, n):
    return cfg.raw / "viewer" / "view" / f"{n // 1000:03d}" / f"{n}.html.gz"


def hwp_path(cfg, n):
    return cfg.raw / "hwp" / f"{n // 1000:03d}" / f"{n}.hwp"


# ============================================================================ schemas

S, I16, I32, I64, B = pa.string(), pa.int16(), pa.int32(), pa.int64(), pa.bool_()
LS = pa.list_(pa.string())

CONTRACT_TURN_COLUMNS = [
    "conf_num", "turn_seq", "source", "speaker_label_raw", "speaker_pos", "speaker_name",
    "speaker_mem_id", "speaker_area", "text_raw", "text", "has_stage", "stage_kinds",
    "n_fragments", "agenda_ordinal", "agenda_text", "time_hhmm", "speech_date"]

SCHEMAS = {
    "turns": pa.schema([
        ("conf_num", I64), ("turn_seq", I32), ("source", S), ("speaker_label_raw", S),
        ("speaker_pos", S), ("speaker_name", S), ("speaker_mem_id", I64), ("speaker_area", S),
        ("text_raw", S), ("text", S), ("has_stage", B), ("stage_kinds", LS), ("n_fragments", I16),
        ("agenda_ordinal", I32), ("agenda_text", S), ("time_hhmm", S), ("speech_date", S),
        # additions (not in the CONTRACT list)
        ("term", I16), ("spk_id", S), ("label_split", S), ("label_fused", B),
        ("name_from_label", B), ("name_has_hanja", B), ("profile_slug", S), ("profile_term", I16),
        ("agenda_item", S), ("agenda_top_text", S), ("time_hhmm_start", S), ("time_marker", S),
        ("speech_date_end", S), ("n_sentences", I32), ("n_stage_sentences", I32),
        ("n_oath_signature", I32), ("n_embedded", I32), ("stage_texts", LS), ("interjections", S),
        ("inline_stage_parens", LS), ("source_member_id", S), ("source_speech_order", S),
        ("text_rule", S),
        # 1.7 (CONTRACT, appended 2026-09-26): turn boundary / sitting fields, see assign_sittings()
        ("after_end_marker", B), ("sitting_seq", I16), ("sitting_how", S), ("label_how", S),
        ("label_confidence", S),
        # hwp_parser extras carried when the parser supplies them (null otherwise and for xml / xlsx):
        # label_lex_count (occurrences of the label in the document's own label lexicon, a label-confidence
        # signal), speech_date_how (how the turn's date was set), time_regress (clock went backwards)
        ("label_lex_count", I32), ("speech_date_how", S), ("time_regress", B),
        # 1.8 (R1): document-level end flag, printed label repeated at the start of the text, label
        # sanity flags, matching forms of the printed name / position (see docs/CODEBOOK.md)
        ("after_final_end_marker", B), ("text_label_prefix_stripped", B), ("text_label_prefix_match", S),
        ("label_has_text", B), ("label_misattributed", B), ("speaker_name_norm", S), ("speaker_pos_norm", S)]),
    "agenda": pa.schema([
        ("conf_num", I64), ("ordinal", I32), ("anchor", S), ("level", S), ("text", S),
        ("bill_id", S), ("bill_no", S), ("is_continued", B), ("after_turn_seq", I32),
        ("bill_url", S), ("match_rule", S), ("source", S), ("term", I16)]),
    "agenda_header": pa.schema([
        ("conf_num", I64), ("item_seq", I32), ("section", S), ("head_id", S), ("level", S),
        ("num", S), ("text", S), ("target", S), ("page", I32), ("source", S), ("term", I16)]),
    "events": pa.schema([
        ("conf_num", I64), ("event_seq", I32), ("kind", S), ("text", S), ("after_turn_seq", I32),
        ("hhmm", S), ("action", S), ("within_turn", B), ("new_date", S), ("tag", S), ("cls", S),
        ("source", S), ("term", I16)]),
    "footer": pa.schema([
        ("conf_num", I64), ("section_seq", I32), ("section_title", S), ("group_seq", I32),
        ("group_label", S), ("item_seq", I32), ("item_kind", S), ("pos", S), ("name", S),
        ("org", S), ("line_text", S), ("extra", S), ("profile_url", S), ("table_seq", I32),
        ("row_idx", I32), ("source", S), ("term", I16),
        # 1.8: 1..n in document order within the meeting; (conf_num, row_seq) is the unique key
        ("row_seq", I32)]),
    "rollcall": pa.schema([
        ("conf_num", I64), ("vote_seq", I32), ("vote_title", S), ("vote_section_title", S),
        ("vote_group", S), ("group_label", S), ("n_reported", I32), ("name_seq", I32),
        ("name", S), ("pos", S), ("profile_url", S), ("method", S), ("source", S), ("term", I16)]),
    "rollcall_groups": pa.schema([
        ("conf_num", I64), ("vote_seq", I32), ("vote_title", S), ("vote_group", S),
        ("group_label", S), ("n_reported", I32), ("n_names", I32), ("method", S), ("source", S),
        ("term", I16), ("note", S)]),
    "attendance": pa.schema([
        ("conf_num", I64), ("section_seq", I32), ("section_title", S), ("category", S),
        ("n_reported", I32), ("group_label", S), ("item_kind", S), ("org", S), ("pos", S),
        ("name", S), ("line_text", S), ("profile_url", S), ("source", S), ("term", I16),
        # 1.8: 1..n in document order within the meeting ((conf_num, row_seq) is the unique key);
        # duplicate_of_row_seq = row_seq of the first identical row of the meeting (NULL for a first one)
        ("row_seq", I32), ("duplicate_of_row_seq", I32)]),
    # internal tables
    "headers": pa.schema([
        ("conf_num", I64), ("source", S), ("term", I16), ("parse_status", S), ("h_title", S),
        ("h_term", I16), ("h_session", I32), ("h_session_type", S), ("h_sitting", S),
        ("h_committee_full", S), ("h_committee", S), ("h_subcommittee", S), ("h_is_audit", B),
        ("h_audit_year", I16), ("h_date", S), ("h_doc_title", S), ("h_turn", S), ("h_doc_no", S),
        ("h_author", S), ("h_fields_json", S), ("audited_agencies", LS), ("is_provisional", B),
        ("date_end", S), ("n_turns", I32), ("n_agenda", I32), ("n_agenda_header", I32),
        ("n_events", I32), ("n_footer_rows", I32), ("n_rollcall_votes", I32),
        ("agenda_confirmation_hit", B), ("stats_json", S), ("raw_sha1", S), ("raw_bytes", I64),
        ("extra_json", S),
        # 1.8: where audited_agencies come from, and the printed value(s) they were split from
        ("audited_agencies_how", S), ("audited_agencies_raw", S)]),
    "coverage": pa.schema([
        ("conf_num", I64), ("source", S), ("term", I16), ("n_turns", I32), ("n_div_speaker", I32),
        ("sum_fragments", I32), ("turn_seq_contiguous", B), ("dom_spk_sub_chars", I64),
        ("parsed_sub_chars", I64), ("turn_text_raw_chars", I64), ("dom_txt_chars", I64),
        ("embedded_chars", I64), ("dom_body_chars", I64), ("accounted_body_chars", I64),
        ("dom_footer_chars", I64), ("footer_rows_chars", I64),
        ("ok_spk_sub_vs_text_raw", B), ("ok_sentences", B), ("ok_txt", B), ("ok_body", B),
        ("ok_footer", B), ("ok_all", B), ("detail_json", S)]),
}
PUBLIC_TABLES = ["turns", "agenda", "agenda_header", "events", "footer", "rollcall",
                 "rollcall_groups", "attendance"]
INTERNAL_TABLES = ["headers", "coverage"]
ALL_TABLES = PUBLIC_TABLES + INTERNAL_TABLES


def table_dir(cfg, table):
    return (cfg.state_dir if table in INTERNAL_TABLES else cfg.out) / table


# ============================================================================ small helpers

WS_RE = re.compile(r"\s+")
HANJA_RE = pv.HANJA_RE
# Character classes are written as \u escapes: a literal U+F900 is changed to U+8C48 by Unicode
# normalization (canonical decomposition) when a file is saved as NFC, which silently widened the
# class to cover every Hangul syllable (review 2026-09-26). test_char_classes_code_points pins them.
HANJA_CLS = "㐀-䶿一-鿿豈-﫿"
HANGUL_CLS = "가-힣"
CJK_NAME_RE = re.compile(f"^[{HANGUL_CLS}{HANJA_CLS}\\s]+$")
# CJK compatibility ideographs (U+F900-U+FAFF) -> unified form, one char to one char, so positions
# are preserved. Sources print e.g. 理 as U+F9E4, 金 as U+F90A, 李 as U+F9E1.
_COMPAT_TR = {}
for _cp in range(0xF900, 0xFB00):
    _n = unicodedata.normalize("NFC", chr(_cp))
    if len(_n) == 1 and _n != chr(_cp):
        _COMPAT_TR[_cp] = ord(_n)


def fold_compat(s):
    """Map CJK compatibility ideographs to their unified form (length-preserving)."""
    return s.translate(_COMPAT_TR) if s else s


# ============================================================================ sittings / labels
# Meeting-end and (re)opening time-marker actions, compared on the whitespace-free action text as
# printed ('(12시16분 산회)' -> '산회'). Exact comparison: '비공개감사종료', '투표종료', '회의중지' are
# not ends (the XML adapter's event 'action' drops the '비공개' prefix, so the action is re-read from
# the marker text). hwp_parser.END_ACTIONS_EXACT also ends at 폐식/閉式/유회/流會 (HWP turns carry the
# parser's own after_end_marker / after_final_end_marker / sitting_seq).
END_ACTIONS = frozenset({"산회", "폐회", "감사종료", "조사종료", "散會", "閉會"})
# a continuation marker printed after an end marker ('(13시17분 감사종료)' ... '(14시40분 감사계속)', 24614) also
# re-opens: the meeting resumed after a printed end
OPEN_ACTIONS = frozenset({"개의", "계속개의", "속개", "개회", "감사개시", "조사개시", "감사계속", "조사계속", "회의계속",
                          "開議", "續開", "開會", "繼續開議"})
_ACTION_RE = re.compile(r"^[\(（](?:\d{1,2}[월月]\d{1,2}[일日])?\d{1,2}[시時](?:\d{1,2}[분分])?(?P<act>[^()（）]*)[\)）]$")


def marker_action(text, action=None):
    """Exact action of a time marker ('(9월26일 01시15분 산회)' -> '산회'), from the printed text;
    falls back to the parser's action field. None for a bare time ('(10시05분)')."""
    m = _ACTION_RE.match(nows(text))
    if m:
        return m.group("act") or None
    a = nows(action)
    return a or None


def assign_sittings(turns, marks, counters=None, parser_sitting=False, parser_after_end=False,
                    parser_after_final=False):
    """Set `after_end_marker`, `after_final_end_marker` and `sitting_seq` / `sitting_how` on turn dicts
    (document order).

    marks: [(after_turn_seq, action)] in document order; after_turn_seq k means the marker was printed
    after turn k began (inside turn k when the turn continues after it), so it precedes turn j iff
    k < j. Rule (all sources):
      sitting_seq            = 1 + number of (re)opening markers (OPEN_ACTIONS) that follow an end
                               marker with no opening in between and precede the turn ('(16시10분 산회)'
                               ... '(16시37분 개의)' starts sitting 2);
      after_end_marker       = a meeting-end marker (END_ACTIONS) precedes the turn within the turn's own
                               sitting: it is reset when a new sitting starts, i.e. at a (re)opening or
                               resumption marker (개의, 속개, 계속개의, 감사계속, 조사계속, 회의계속, ...)
                               printed after the end marker (researcher decision 2026-09-26);
      after_final_end_marker = the turn starts after the document's last meeting-end marker.
    parser_sitting / parser_after_end / parser_after_final: the parser supplied the value (hwp_parser);
    it is kept, and a disagreement with this rule is counted (sitting_parser_vs_markers_differ,
    after_end_parser_vs_markers_differ, after_final_parser_vs_markers_differ), never silently changed."""
    c = counters if counters is not None else Counter()
    marks = sorted(((int(k) if k is not None else 0), i, a) for i, (k, a) in enumerate(marks))
    last_end = max((k for k, _, a in marks if a in END_ACTIONS), default=None)
    j = 0
    ended = pending = False
    sitting = 1
    derived = []
    for t in turns:
        seq = t["turn_seq"]
        while j < len(marks) and marks[j][0] < seq:
            a = marks[j][2]
            if a in END_ACTIONS:
                ended = pending = True
            elif a in OPEN_ACTIONS and pending:
                sitting += 1
                pending = ended = False
            j += 1
        derived.append((ended, sitting, last_end is not None and last_end < seq))
    for t, (ae, si, fe) in zip(turns, derived):
        if parser_after_end:
            ae_p = bool(t.get("after_end_marker"))
            if ae_p != ae:
                c["after_end_parser_vs_markers_differ"] += 1
            t["after_end_marker"] = ae_p
        else:
            t["after_end_marker"] = ae
        if parser_after_final:
            fe_p = bool(t.get("after_final_end_marker"))
            if fe_p != fe:
                c["after_final_parser_vs_markers_differ"] += 1
            t["after_final_end_marker"] = fe_p
        else:
            t["after_final_end_marker"] = fe
        if parser_sitting and t.get("sitting_seq") is not None:
            sp = int(t["sitting_seq"])
            if sp != si:
                c["sitting_parser_vs_markers_differ"] += 1
            t["sitting_seq"], t["sitting_how"] = sp, "parser"
        else:
            t["sitting_seq"], t["sitting_how"] = si, "end_open_markers"
        c["turns_after_end_marker"] += int(bool(t["after_end_marker"]))
        c["turns_after_final_end_marker"] += int(bool(t["after_final_end_marker"]))
    n_sit = max((t["sitting_seq"] for t in turns), default=1)
    if n_sit > 1:
        c["meetings_several_sittings"] += 1
        c["turns_in_later_sittings"] += sum(1 for t in turns if t["sitting_seq"] > 1)
    if any(t["after_end_marker"] for t in turns):
        c["meetings_with_turns_after_end_marker"] += 1
    if any(t["after_final_end_marker"] for t in turns):
        c["meetings_with_turns_after_final_end_marker"] += 1
    return c


# label_how -> label_confidence (default rating of how a turn's speaker label was found; the
# parser's own value wins when it supplies one). 'low': the review found the rule wrong on most hits
# (single_space_pos_name, right on 3 of 63 in 18대 HWP) or the label is not a label (no label,
# implausible split). 'medium': label found through the document's own label lexicon or a
# single-space / whole-line shape. 'high': label set off by the source's own structure (viewer
# checkbox label / data attributes, XLSX speaker column, HWP double space or tab). A label_how not
# listed here is 'unrated' (counted).
LABEL_CONFIDENCE = {
    # xml (viewer)
    "in_chk_label": "high", "in_chk_label_nosuffix": "high", "attr_fallback": "high",
    # xlsx (v9 speaker column)
    "xlsx_speaker_column": "high",
    # hwp (hwp_parser._speaker_line)
    "sep": "high", "sep_joined_pos_name": "high", "sep_trimmed_by_lexicon": "high",
    "lexicon_prefix": "medium", "lexicon_prefix_punct": "medium", "lexicon_prefix_fused": "medium",
    "single_space_name_pos": "medium", "label_only": "medium", "label_joined_next_line": "medium",
    "single_space_pos_name": "low", "sep_implausible": "low", "label_missing": "low",
}


def label_confidence(how, parser_value=None, counters=None):
    if parser_value is not None and str(parser_value).strip():
        if counters is not None:
            counters["label_confidence_from_parser"] += 1
        return str(parser_value)
    r = LABEL_CONFIDENCE.get(how)
    if r is None:
        r = "unrated"
        if counters is not None:
            counters["label_how_unrated_" + str(how)] += 1
    return r
# ---------------------------------------------------------------------------- R1 helpers (1.8)
# Matching forms (speaker_name_norm / speaker_pos_norm). Text columns stay verbatim; these forms are for
# matching only: separators U+2024 ONE DOT LEADER, U+2027 HYPHENATION POINT, U+318D HANGUL LETTER ARAEA
# (and the katakana middle dots U+30FB / U+FF65) -> U+00B7 MIDDLE DOT, before and after NFKC (NFKC alone
# turns U+2024 into '.' and U+318D into a conjoining jamo); NFKC folds CJK compatibility ideographs
# (U+F9E4 -> U+7406) and full-width forms; whitespace runs -> one space.
_SEP_TR = {0x2024: 0xB7, 0x2027: 0xB7, 0x318D: 0xB7, 0x30FB: 0xB7, 0xFF65: 0xB7}


def norm_match(s):
    if s is None:
        return None
    t = unicodedata.normalize("NFKC", str(s).translate(_SEP_TR)).translate(_SEP_TR)
    t = WS_RE.sub(" ", t).strip()
    return t or None


AGENCY_SEP_CHARS = "|․·,，"     # the separators parse_viewer splits the viewer's 피감사기관 field on


def split_agencies(value):
    """Audited agencies from a printed value, split at AGENCY_SEP_CHARS outside parentheses
    ('韓國銀行全北本部(光州全南․大田忠南 本部 포함)' stays one item). Same rule as hwp_parser."""
    out, cur, depth = [], [], 0
    for ch in value or "":
        if ch in "(（":
            depth += 1
        elif ch in ")）" and depth:
            depth -= 1
        if ch in AGENCY_SEP_CHARS and depth == 0:
            out.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur))
    return [x.strip() for x in out if x.strip()]


# A printed label repeated at the start of the spoken text ('金鍾河委員  그러면 …' in div.txt after the
# label '金鍾河委員'). The repeat is followed by the label separator the sources print (2+ whitespace
# characters, a tab, a line break or an ideographic space) or ends the sentence; with a single space the
# next word must not end a self-introduction ('금융위원회 금융정책과 양병권 사무관입니다.' is speech); a repeat
# followed directly by a letter is a word of the speech ('김태년 위원입니다'). The name part alone counts
# only with a strong separator.
SELF_INTRO_WORD_RE = re.compile(r"^\S*(?:입니다|이다|입니다만)[.!,]?$")


def _ws_match_end(raw, part):
    """Index in `raw` right after `part` matched at its start, ignoring whitespace; None if no match."""
    p = nows(part)
    if not p:
        return None
    i = j = 0
    n = len(raw)
    while j < len(p):
        while i < n and raw[i].isspace():
            i += 1
        if i >= n or raw[i] != p[j]:
            return None
        i += 1
        j += 1
    return i


def label_prefix_match(first_raw, label, name):
    """('label' | 'name', end) when the printed label (or the name part) is repeated at the start of
    first_raw (the turn's first spoken sentence as printed, whitespace kept), else (None, None). `end`
    is the index in first_raw after the repeat (the separator whitespace is not included)."""
    if not first_raw:
        return None, None
    raw = first_raw.lstrip()
    off = len(first_raw) - len(raw)
    for kind, part in (("label", label), ("name", name)):
        if not part or (kind == "name" and nows(part) == nows(label or "")):
            continue
        if kind == "name" and len(nows(part)) < 2:
            continue
        end = _ws_match_end(raw, part)
        if end is None:
            continue
        rest = raw[end:]
        ws = rest[:len(rest) - len(rest.lstrip())]
        if not rest.strip():
            ok = kind == "label"             # the repeat is the whole sentence (maybe with trailing spaces)
        elif not ws:
            ok = False                       # glued to a word of the speech
        elif len(ws) >= 2 or any(ch in ws for ch in "\t\n\u3000"):
            ok = True                        # the source's label separator
        else:
            nxt = rest.lstrip().split(None, 1)[0]
            ok = kind == "label" and not SELF_INTRO_WORD_RE.match(nxt)
        if ok:
            return kind, off + end
    return None, None


def strip_label_prefix(t, first_raw, first_line, counters):
    """Apply label_prefix_match to turn dict t whose spoken text starts with first_line (the
    normalized form of first_raw). Sets text_label_prefix_stripped / text_label_prefix_match and
    removes the repeat from t['text'] (text_raw is never changed)."""
    t["text_label_prefix_stripped"], t["text_label_prefix_match"] = False, None
    text = t.get("text")
    if not text or first_raw is None or first_line is None or t.get("label_fused"):
        return
    lines = text.split("\n")
    if lines[0] != first_line:
        return
    kind, end = label_prefix_match(first_raw, t.get("speaker_label_raw"), t.get("speaker_name"))
    if kind is None:
        return
    new_first = pv.norm(first_raw[end:])
    if nows(first_raw[:end]) + nows(new_first) != nows(first_line):   # never lose a character
        counters["text_label_prefix_mismatch_skipped"] += 1
        return
    t["text"] = "\n".join(([new_first] if new_first else []) + lines[1:])
    t["text_label_prefix_stripped"], t["text_label_prefix_match"] = True, kind
    counters["text_label_prefix_stripped_" + kind] += 1
    counters["text_label_prefix_stripped_chars"] += len(nows(first_raw[:end]))


# A label that holds speech text ('田溶鶴 委員그런데 법사위에 가서 … 보내야 됩니다.'): a Hangul / Hanja
# character followed by sentence punctuation, or a comma and a space. Latin initials ('Kenneth C.
# Crawford', 'Mr. Michael Richter') do not count.
LABEL_TEXT_RE = re.compile(f"[{HANGUL_CLS}{HANJA_CLS}][.?!…](?:\\s|$|[◯○])|[{HANGUL_CLS}{HANJA_CLS}][,，]\\s")
MARKERS_IN_LABEL = "◯○"


def label_flags(turns, counters):
    """label_fused (a speaker marker inside the printed label: 'A◯B'), label_has_text (the label holds
    speech text), label_misattributed (speech text, then a marker and another label inside the printed
    label, as in 25519 turn 29 '田溶鶴 委員그런데 … 됩니다. ◯首席專門委員 尙元鍾': the speaker fields come
    from the part before the marker while the text belongs to the speaker after it). Any of the three
    rates the label 'low' (label_confidence), counted."""
    for t in turns:
        lab = t.get("speaker_label_raw") or ""
        cut = max(lab.rfind(m) for m in MARKERS_IN_LABEL)
        fused = cut > 0
        t["label_fused"] = bool(t.get("label_fused")) or fused
        t["label_has_text"] = bool(LABEL_TEXT_RE.search(lab))
        t["label_misattributed"] = bool(fused and LABEL_TEXT_RE.search(lab[:cut]))
        for k in ("label_fused", "label_has_text", "label_misattributed"):
            counters[k] += int(t[k])
        if (t["label_fused"] or t["label_has_text"]) and t.get("label_confidence") != "low":
            counters["label_confidence_low_from_" + str(t.get("label_confidence"))] += 1
            t["label_confidence"] = "low"


def speaker_norm_fields(turns):
    for t in turns:
        t["speaker_name_norm"] = norm_match(t.get("speaker_name"))
        t["speaker_pos_norm"] = norm_match(t.get("speaker_pos"))


def null_policy(tables, counters):
    """'' and whitespace-only values of every string column -> NULL (per table.column counter
    null_policy_<table>.<column>); list columns are left as they are."""
    for tb, rows in tables.items():
        if tb not in SCHEMAS or not rows:
            continue
        cols = [f.name for f in SCHEMAS[tb] if f.type == S]
        for r in rows:
            for col in cols:
                v = r.get(col)
                if isinstance(v, str) and not v.strip():
                    r[col] = None
                    counters[f"null_policy_{tb}.{col}"] += 1


def number_rows(rows):
    """row_seq 1..n in document order (footer, attendance)."""
    for i, r in enumerate(rows, 1):
        r["row_seq"] = i
    return rows


ATT_KEY_COLS = ("section_seq", "section_title", "category", "n_reported", "group_label", "item_kind", "org",
                "pos", "name", "line_text", "profile_url")


def mark_attendance_duplicates(rows, counters):
    """duplicate_of_row_seq: the row_seq of the first row of the meeting with identical content (every
    attendance column except row_seq), NULL for a first occurrence; counted, never removed."""
    first = {}
    for r in rows:
        k = tuple(r.get(c) for c in ATT_KEY_COLS)
        if k in first:
            r["duplicate_of_row_seq"] = first[k]
            counters["attendance_exact_duplicate_rows"] += 1
        else:
            first[k] = r["row_seq"]
            r["duplicate_of_row_seq"] = None
    return rows


def finalize_tables(tables, counters):
    """Steps shared by the adapters (1.8): row_seq for footer / attendance, attendance duplicates,
    label flags, matching forms, NULL policy."""
    if "footer" in tables:
        number_rows(tables["footer"])
    if "attendance" in tables:
        mark_attendance_duplicates(number_rows(tables["attendance"]), counters)
    if "turns" in tables:
        label_flags(tables["turns"], counters)
        speaker_norm_fields(tables["turns"])
    null_policy(tables, counters)
    return tables


LABEL_RE = re.compile(r'<label for="spk_cnt_chk_(\d+)">(.*?)</label>', re.S)
SPK_XP = './/div[contains(concat(" ",normalize-space(@class)," ")," speaker ")]'


def nows(s):
    return WS_RE.sub("", (s or "").replace("\xa0", " "))


def _int_or_none(x):
    try:
        if x is None or (isinstance(x, float) and x != x):
            return None
        return int(x)
    except (TypeError, ValueError):
        return None


def _json(x):
    return json.dumps(x, ensure_ascii=False, default=str) if x not in (None, [], {}) else None


def _b(x):
    """bool for possibly-missing values (None, NaN, pd.NA -> False)."""
    try:
        return bool(x) if x is not None and x == x else False
    except TypeError:
        return False



# ============================================================================ footer typing

VOTE_GROUP_RE = re.compile(
    r"^\s*[◯○]?\s*(투표|찬성|반대|기권|投票|贊成|反對|棄權)\s*(?:의원|議員|위원|委員)\s*"
    r"(?:[\(（]\s*(\d+)\s*[인人名명]\s*[\)）])?\s*$")
VOTE_GROUP_NORM = {"投票": "투표", "贊成": "찬성", "反對": "반대", "棄權": "기권"}
COUNT_RE = re.compile(r"[\(（]\s*(\d+)\s*[인人名명]\s*[\)）]")


def vote_group_match(s):
    """VOTE_GROUP_RE on the compatibility-folded text (sources print some Hanja as U+F9xx)."""
    return VOTE_GROUP_RE.match(fold_compat(s or ""))


ATT_RULES = [
    # '委員아닌出席議員', '小委員아닌出席委員', '委員이아닌出席議員', '監査委員아닌出席議員', '委員아닌출석의원'
    ("present_nonmember", re.compile(r"^(小委員|監査委員|委員|소위원|감사위원|위원)이?아닌(出席|출석|參席|참석)(議員|의원|委員|위원)")),
    ("present", re.compile(r"^(출석위원|出席委員|출석의원|出席議員|출석감사위원|出席監査委員|출석조사위원|出席調査委員"
                           r"|출석소위원|出席小委員|감사출석위원|監査出席委員|出席監事委員|참석의원|參席議員|出席國監委員|출석국감위원)")),
    ("excused", re.compile(r"^(청가위원|請暇委員|청가의원|請暇議員|청가감사위원|請暇監査委員|청가소위원|請暇小委員"
                           r"|請暇國監委員|청가국감위원)")),
    ("official_travel", re.compile(r"^(출장위원|出張委員|출장의원|出張議員|출장감사위원|出張監査委員|出張國監委員|출장국감위원)")),
    ("seated_at_opening", re.compile(r"^(개의시재석|개의시출석의원)")),
    # '속개시재석의원', '20시38분속개시재석의원', '속개(14시06분)시출석의원'
    ("seated_at_resumption", re.compile(r"^(\d{1,2}시\d{1,2}분)?속개(\(\d{1,2}시\d{1,2}분\))?시(재석|출석)의원")),
    ("seated_during_meeting", re.compile(r"^회의중(\(\d{1,2}시\d{1,2}분\))?재석의원")),
    ("seated_at_adjournment", re.compile(r"^산회시재석")),
    # '※코로나19방역관련권고에따라출석하지않은의원' (a list of member names)
    ("absent_listed", re.compile(r"(출석|참석)하지않은의원$")),
    ("committee_staff", re.compile(r"^(출석전문위원|出席專門委員|출석수석전문위원|出席立法調査官|출석입법조사관"
                                   r"|出席立法審議官|출석입법심의관|出席審議官|출석심의관)")),
    ("assembly_staff", re.compile(r"^(국회참석자|국회측참석자|國會側參席者|國會參席者)")),
    ("cabinet", re.compile(r"^(출석국무위원|出席國務委員|出席國務總理|출석국무총리)")),
    ("government", re.compile(r"^(출석정부위원|出席政府委員|정부측참석자|政府側參席者|정부측및기타참석자|政府側및其他參席者"
                              r"|政府側參席委員|出席政府議員)")),
    ("witness", re.compile(r"^(출석증인|出席證人|참석증인|參席證人|일반증인|기관증인|증인명단)")),
    ("reference", re.compile(r"^(출석참고인|出席參考人|참석참고인|參席參考人|참고인명단)|(出席參考人|출석참고인)$")),
    ("statement", re.compile(r"^(출석진술인|出席陳述人|참석진술인|參席陳述人)")),
    # '출석공직후보자', '出席公職候補者', '出席大法官候補者', '出席憲法裁判所裁判官候補者'
    ("nominee", re.compile(r"^(공직후보자|公職候補者)|^(출석|出席)\S{0,20}(후보자|候補者)$")),
    ("advisor", re.compile(r"^(출석자문위원|出席諮問委員)")),
    ("other_attendee", re.compile(r"^(기타참석자|其他參席者|其他出席者|出席參席人)")),
    ("agency_attendee", re.compile(r"(참석자|參席者|출석자|出席者)$")),
]
# a section title that looks like an attendance list but has no category is counted
# (counter attendance_like_title_uncategorized), never silently dropped from the count
ATT_LIKE_RE = re.compile(r"출석|出席|청가|請暇|출장|出張|참석|參席|재석|在席")


def _title_core(title):
    """'【보고사항】◯출석 위원(30인)' -> '출석위원'. Compatibility ideographs are folded; a leading
    Latin 'O' used as the circle mark ('O出席委員') is dropped."""
    t = fold_compat(nows(title))
    t = re.sub(r"^.*】", "", t)
    t = t.lstrip("◯○")
    t = re.sub(r"^[Oo](?=[^\x00-\x7f])", "", t)
    t = COUNT_RE.sub("", t)
    return t


def attendance_category(title):
    if not title:
        return None
    core = _title_core(title)
    for cat, rx in ATT_RULES:
        if rx.search(core):
            return cat
    return None


def attendance_like_uncategorized(title):
    """True for a short section title that mentions attendance but maps to no category."""
    if not title or attendance_category(title) is not None:
        return False
    core = _title_core(title)
    return len(core) <= 30 and bool(ATT_LIKE_RE.search(core))


def _n_reported(s):
    m = COUNT_RE.search(s or "")
    return int(m.group(1)) if m else None


def _vote_title(section_title):
    t = pv.norm(re.sub(r"^.*】", "", section_title or ""))
    return t.lstrip("◯○").strip() or None


def flatten_footer_xml(sections):
    """parse_viewer footer sections -> long rows. Order inside a group: org headings, names,
    free lines, tables (the parser keeps these in separate lists)."""
    rows = []
    for si, s in enumerate(sections, 1):
        base = {"section_seq": si, "section_title": s.get("title")}
        groups = s.get("groups") or []
        if not groups:
            rows.append(dict(base, group_seq=None, group_label=None, item_seq=None, item_kind="title_only"))
            continue
        for gi, g in enumerate(groups, 1):
            gb = dict(base, group_seq=gi, group_label=g.get("label"))
            k = 0
            items = []
            for o in g.get("orgs", []) or []:
                items.append({"item_kind": "org", "line_text": o, "org": o})
            for n in g.get("names", []) or []:
                items.append({"item_kind": "name", "pos": n.get("pos"), "name": n.get("name"),
                              "org": n.get("org"), "extra": n.get("extra"),
                              "profile_url": n.get("profile_url"),
                              "line_text": n.get("extra") or pv.norm(f"{n.get('pos') or ''} {n.get('name') or ''}")})
            for ln in g.get("lines", []) or []:
                items.append({"item_kind": "line", "line_text": ln})
            for ti, tb in enumerate(g.get("tables", []) or [], 1):
                if tb.get("caption"):
                    items.append({"item_kind": "table_caption", "line_text": tb["caption"], "table_seq": ti})
                for ri, r in enumerate(tb.get("rows", []) or [], 1):
                    items.append({"item_kind": "table_row", "line_text": "\t".join(r), "table_seq": ti,
                                  "row_idx": ri})
            if not items:
                rows.append(dict(gb, item_seq=None, item_kind="label_only"))
            for it in items:
                k += 1
                rows.append(dict(gb, item_seq=k, **it))
    return rows


def footer_rows_chars(rows):
    """Whitespace-free characters represented by footer rows (titles and labels counted once),
    computed the way test_parse_viewer.py counts the DOM footer."""
    seen_s, seen_g, n = set(), set(), 0
    for r in rows:
        if r["section_seq"] not in seen_s:
            seen_s.add(r["section_seq"])
            n += len(nows(r.get("section_title")))
        key = (r["section_seq"], r.get("group_seq"))
        if r.get("group_seq") is not None and key not in seen_g:
            seen_g.add(key)
            n += len(nows(r.get("group_label")))
        kind = r.get("item_kind")
        if kind == "name":
            n += len(nows(r["extra"])) if r.get("extra") else len(nows(r.get("pos"))) + len(nows(r.get("name")))
        elif kind == "table_row":
            n += len(nows(r.get("line_text")))
        elif kind in ("org", "line", "table_caption"):
            n += len(nows(r.get("line_text")))
    return n


# a whole line in parentheses inside a name list is an editorial note, e.g.
# '(권오을․이방호 의원 버튼 미조작. 실제 투표의원 211인, 찬성 의원 207인, 기권 의원 4인임)'
PAREN_LINE_RE = re.compile(r"^\s*[\(（].*[\)）]\s*$", re.S)
# a whole line in angle brackets or starting with a reference mark is a note too ('<2차 투표>')
ANGLE_LINE_RE = re.compile(r"^\s*[<〈＜《].*[>〉＞》]\s*$", re.S)
REFMARK_LINE_RE = re.compile(r"^\s*※")
# a separator between two votes printed in one section ('<2차 투표>', '〈재투표〉')
VOTE_SEP_RE = re.compile(r"^\s*[<〈＜《\[【]\s*[^<>〈〉＜＞《》\[\]【】]{0,20}투표[^<>〈〉＜＞《》\[\]【】]{0,20}[>〉＞》\]】]\s*$")


def is_note_line(ln):
    ln = ln or ""
    return bool(PAREN_LINE_RE.match(ln) or ANGLE_LINE_RE.match(ln) or REFMARK_LINE_RE.match(ln)
                or VOTE_SEP_RE.match(ln))


# a group label that is itself a row of names ('강기정 강봉균 강재섭 강혜숙', '박종근 박 진 박찬석')
NAME_LIST_RE = re.compile(f"^[{HANGUL_CLS}{HANJA_CLS}]{{1,4}}(?:\\s+[{HANGUL_CLS}{HANJA_CLS}]{{1,4}})*$")
MEMBER_ATT_CATS = ("present", "excused", "official_travel", "present_nonmember",
                   "seated_at_opening", "seated_at_resumption", "seated_during_meeting",
                   "seated_at_adjournment", "absent_listed")


def _label_names(label):
    """Names from a space-separated name row; two adjacent 1-char tokens are one spaced-out
    name ('박 진' -> '박진')."""
    toks = pv.norm(label).split(" ")
    out, i = [], 0
    while i < len(toks):
        if len(toks[i]) == 1 and i + 1 < len(toks) and len(toks[i + 1]) == 1:
            out.append(toks[i] + toks[i + 1])
            i += 2
            continue
        out.append(toks[i])
        i += 1
    return out


class _VoteSeq:
    """vote_seq assignment. A new vote starts when the context (section) changes, after a
    separator such as '<2차 투표>', when a 투표 group follows groups of the current vote, or when
    a group repeats inside the current vote (a re-vote printed in the same section)."""

    def __init__(self):
        self.n, self.key, self.groups, self.force, self.splits = 0, object(), set(), False, 0

    def next(self, key, grp):
        if key != self.key or self.force or grp in self.groups or (grp == "투표" and self.groups):
            if key == self.key:
                self.splits += 1
            self.n += 1
            self.key, self.groups, self.force = key, set(), False
        self.groups.add(grp)
        return self.n


def _xml_vote_blocks(s):
    """Vote groups of one footer section: [(match, label, names[dict], notes[str], method, sep)].
    Names are the group's li items; a following group whose label is a row of names and that
    has no items of its own continues the current vote group (27528 prints names as p.tit_sm).
    `sep` is a separator label or line ('<2차 투표>') printed before the group."""
    blocks, cur, sep = [], None, None
    for g in s.get("groups") or []:
        lab = g.get("label") or ""
        m = vote_group_match(lab)
        if m:
            cur = [m, g.get("label"), list(g.get("names") or []), [], "xml_footer", sep]
            sep = None
            blocks.append(cur)
        elif lab and VOTE_SEP_RE.match(lab):
            sep, cur = pv.norm(lab), None
            continue
        elif cur is not None and lab and NAME_LIST_RE.match(pv.norm(lab)) and not g.get("names"):
            cur[2].extend({"name": x, "pos": None, "profile_url": None} for x in _label_names(lab))
            cur[4] = "xml_footer_label_rows"
        elif cur is not None and not lab and not g.get("names") and not g.get("lines"):
            continue
        elif cur is not None and lab:
            cur = None
        for ln in g.get("lines") or []:
            if VOTE_SEP_RE.match(ln):
                sep = pv.norm(ln)
            elif cur is not None and PAREN_LINE_RE.match(ln):
                cur[3].append(ln)
    return blocks


def rollcall_from_xml_sections(sections, counters=None):
    """Electronic-vote name lists: sections that contain groups labelled 투표/찬성/반대/기권 의원.
    One vote per section, split further by _VoteSeq (re-votes printed in one section)."""
    groups, names = [], []
    vs = _VoteSeq()
    for si, s in enumerate(sections, 1):
        blocks = _xml_vote_blocks(s)
        if not blocks:
            continue
        title = _vote_title(s.get("title"))
        for m, lab, nm, notes, method, sep in blocks:
            grp = VOTE_GROUP_NORM.get(m.group(1), m.group(1))
            nrep = int(m.group(2)) if m.group(2) else None
            if sep:
                vs.force = True
            seq = vs.next(("sec", si), grp)
            groups.append({"vote_seq": seq, "vote_title": title, "vote_group": grp,
                           "group_label": lab, "n_reported": nrep, "n_names": len(nm),
                           "note": " | ".join(([sep] if sep else []) + notes) or None, "method": method})
            for j, n in enumerate(nm, 1):
                names.append({"vote_seq": seq, "vote_title": title, "vote_section_title": s.get("title"),
                              "vote_group": grp, "group_label": lab, "n_reported": nrep,
                              "name_seq": j, "name": n.get("name"), "pos": n.get("pos"),
                              "profile_url": n.get("profile_url"), "method": method})
    if counters is not None:
        counters["rollcall_vote_split_in_section"] += vs.splits
    return vs.n, groups, names


def attendance_from_xml_sections(sections, counters=None):
    out = []
    for si, s in enumerate(sections, 1):
        cat = attendance_category(s.get("title"))
        if cat is None:
            if counters is not None and attendance_like_uncategorized(s.get("title")):
                counters["attendance_like_title_uncategorized"] += 1
            continue
        for g in s.get("groups") or []:
            nrep = _n_reported(s.get("title")) or _n_reported(g.get("label"))
            lab = g.get("label")
            if (cat in MEMBER_ATT_CATS and lab and not g.get("names") and not _n_reported(lab)
                    and NAME_LIST_RE.match(pv.norm(lab))):
                # member names printed as a p.tit_sm row
                for x in _label_names(lab):
                    out.append({"section_seq": si, "section_title": s.get("title"), "category": cat,
                                "n_reported": nrep, "group_label": lab, "item_kind": "label_name",
                                "name": x})
            for n in g.get("names") or []:
                out.append({"section_seq": si, "section_title": s.get("title"), "category": cat,
                            "n_reported": nrep, "group_label": lab, "item_kind": "name",
                            "org": n.get("org"), "pos": n.get("pos"), "name": n.get("name"),
                            "line_text": n.get("extra"), "profile_url": n.get("profile_url")})
            for ln in g.get("lines") or []:
                if cat in MEMBER_ATT_CATS and NAME_LIST_RE.match(pv.norm(ln)):
                    # member names printed as free lines (42214: one name per line)
                    for x in _label_names(ln):
                        out.append({"section_seq": si, "section_title": s.get("title"), "category": cat,
                                    "n_reported": nrep, "group_label": lab, "item_kind": "line_name",
                                    "name": x, "line_text": ln})
                    continue
                out.append({"section_seq": si, "section_title": s.get("title"), "category": cat,
                            "n_reported": nrep, "group_label": lab,
                            "item_kind": "note" if PAREN_LINE_RE.match(ln) else "line", "line_text": ln})
    return out


# ============================================================================ confirmation hearings

CONF_HEARING_RE = re.compile(r"후보자\s*[\(（][^()（）]{1,80}[\)）].{0,30}?인사\s*청문회\s*$")


def is_confirmation_agenda(text):
    """True for an agenda item that is itself a nominee hearing ('...후보자(홍길동) 인사청문회',
    '...후보자(X) 인사청문요청안 심사를 위한 인사청문회'), not '...실시계획서 채택의 건'."""
    if not text:
        return False
    t = re.sub(r"\s*\(계속\)\s*$", "", pv.norm(text))
    return bool(CONF_HEARING_RE.search(t))


# ============================================================================ XML adapter

def _labels_from_page(page_bytes):
    s = page_bytes.decode("utf-8", errors="replace")
    out, conflicts = {}, 0
    for m in LABEL_RE.finditer(s):
        lab = pv.norm(_html.unescape(re.sub(r"<[^>]+>", "", m.group(2))))
        k = m.group(1)
        if k in out and out[k] != lab:
            conflicts += 1
            continue
        out.setdefault(k, lab)
    return out, conflicts


_HJ = f"[{HANJA_CLS}]"
# title characters a fused position ends with (unified forms; labels are compatibility-folded)
POS_SUFFIX_CHARS = frozenset("長官人員理裁事監使士將臣")
HANGUL_POS_SUFFIX_CHARS = frozenset("원장관인리사감")
# Hanja surnames used to find where a fused name starts. Derived 2026-09-26 from the data: the first
# characters of NAAS_CH_NM in v10/interim/members_allnamember_16_22.parquet (90) plus the first
# characters of Hanja data-name values on built 16-17대 viewer pages that are surnames (37; '松',
# '協', '首', '小', '柱', '榮', '非', '下', '心' there are given names or source mis-splits and were
# left out, as was '司', which starts '司令官'). Disjoint from POS_SUFFIX_CHARS (tested), so at most
# one split point can qualify.
HANJA_SURNAMES = frozenset(
    "丁丘任余偰元全兪具劉千卓南卜卞印吉吳呂周咸嚴天太夫奇奉姜孔孟孫安宋宣尙尹崔康庾廉延張徐愈愼慶成"
    "房承文方明曺朱朴李林柳柴桂梁楊權段殷池沈河洪潘片牟玄玉王琴田申異白皇盧睦石禹秋秦程章粱羅董蔡蔣"
    "薛蘇表裴裵許諸賈賓趙車辛邊邢郭都鄭金錢閔陰陳陸鞠韓頓馬高魏魚魯黃龍")
HANJA_COMPOUND_SURNAMES = frozenset({"南宮", "皇甫", "鮮于", "諸葛", "司空", "獨孤", "西門", "東方"})
# a name candidate ending in a title word is a title fragment ('次長', '理事', '代理'), never a name
TITLE_TAILS = ("長官", "次官", "令官", "議官", "理事", "監事", "判事", "檢事", "委員", "議員", "局長", "室長",
               "課長", "部長", "院長", "廳長", "處長", "社長", "會長", "所長", "館長", "團長", "總長", "次長",
               "總裁", "大使", "將軍", "事長", "代理")
HANGUL_TITLE_WORDS = frozenset({
    "대리", "직무대리", "대행", "직무대행", "위원", "위원장", "의원", "의장", "장관", "차관", "차관보", "증인",
    "참고인", "진술인", "이사장", "사장", "원장", "청장", "처장", "국장", "실장", "과장", "이사", "총장", "부장",
    "회장", "대표", "총리", "부총리", "감사", "소장", "단장", "본부장", "위원회", "후보자"})
MEMBER_ROLES_HANJA = ("委員長", "委員", "議員")
NAME_ROLE_SPACED_RE = re.compile(rf"^(?P<n1>{_HJ}{{1,2}})\s?(?P<n2>{_HJ}{{1,2}})\s?(?P<pos>委員長|委員|議員)$")
HANJA_POS_HANGUL_NAME_RE = re.compile(rf"^(?P<pos>.*{_HJ})\s?(?P<name>[{HANGUL_CLS}]{{2,4}})$")
HANGUL_POS_HANJA_NAME_RE = re.compile(rf"^(?P<pos>[{HANGUL_CLS}][{HANGUL_CLS}\s]*[{HANGUL_CLS}])\s?(?P<name>{_HJ}{{2,4}})$")
HANGUL_NAME_RE = re.compile(f"[{HANGUL_CLS}]{{2,4}}")


def is_hanja_name(name):
    """2-4 Hanja characters (compatibility-folded) that start with a known surname (4 only with a
    compound surname) and do not end in a title word."""
    name = fold_compat(name or "")
    n = len(name)
    if not 2 <= n <= 4 or not re.fullmatch(f"{_HJ}+", name) or name.endswith(TITLE_TAILS):
        return False
    if n == 4:
        return name[:2] in HANJA_COMPOUND_SURNAMES
    return name[0] in HANJA_SURNAMES or name[:2] in HANJA_COMPOUND_SURNAMES


def is_hangul_name(name):
    return bool(name and HANGUL_NAME_RE.fullmatch(name) and name not in HANGUL_TITLE_WORDS
                and not name.endswith(("대리", "대행")))


def split_fused_label(label):
    """(pos, name, rule) for a fused 16-17대 label (parse_viewer left it unsplit: data-name empty,
    whole label in data-pos), or (None, None, None). Position-only labels ('委員長代理',
    '民主平和統一諮問會議事務處長') are never split. Matching is done on the compatibility-folded
    label; pos and name are returned as printed. Rules, in order:
      v10_name_role_spaced       '薛 勳委員', '尹景湜 議員' (spaced Hanja name + member role)
      v10_hanja_pos_hangul_name  '證人김동호', '副總理兼財政經濟部長官진념' (script boundary; the
                                 position ends in a title character)
      v10_hangul_pos_hanja_name  '수석전문위원姜長錫'
      v10_space_pos_name         '委員長 李嬿淑' (title character, space, Hanja name)
      v10_space_pos_name_dup     '金龍學委員 金龍學' (name printed twice; position = member role)
      v10_hanja_suffix_name      '委員長田瑢源', '서울特別市長高建' (title character + 2-4 char
                                 Hanja name starting with a surname; the split point is unique)
      v10_hanja_suffix_name_spaced '韓國勞動敎育院長李 銑' (surname and given name spaced)"""
    s0 = pv.norm(label)
    if not s0:
        return None, None, None
    s = fold_compat(s0)                       # same length as s0
    m = NAME_ROLE_SPACED_RE.match(s)
    if m and is_hanja_name(m.group("n1") + m.group("n2")):
        return (s0[m.start("pos"):], s0[m.start("n1"):m.end("n1")] + s0[m.start("n2"):m.end("n2")],
                "v10_name_role_spaced")
    m = HANJA_POS_HANGUL_NAME_RE.match(s)
    if m:
        pos = m.group("pos").rstrip()
        if len(pos) >= 2 and pos[-1] in POS_SUFFIX_CHARS and is_hangul_name(m.group("name")):
            return s0[:len(pos)], s0[m.start("name"):], "v10_hanja_pos_hangul_name"
    m = HANGUL_POS_HANJA_NAME_RE.match(s)
    if m:
        pos = m.group("pos").rstrip()
        if len(pos) >= 2 and pos[-1] in HANGUL_POS_SUFFIX_CHARS and is_hanja_name(m.group("name")):
            return s0[:len(pos)], s0[m.start("name"):], "v10_hangul_pos_hanja_name"
    idx = [i for i, ch in enumerate(s) if ch != " "]
    t = "".join(s[i] for i in idx)
    found = [k for k in (2, 3, 4) if len(t) - k >= 2 and t[-k - 1] in POS_SUFFIX_CHARS and is_hanja_name(t[-k:])]
    if len(found) != 1:
        return None, None, None
    b = idx[len(t) - found[0]]                # where the name starts in s0
    pos, name = s0[:b].rstrip(), s0[b:]
    if " " in name:
        head, _, tail = fold_compat(name).partition(" ")
        if " " in tail or not (head in HANJA_SURNAMES or head in HANJA_COMPOUND_SURNAMES):
            return None, None, None
        return pos, name.replace(" ", ""), "v10_hanja_suffix_name_spaced"
    if b > 0 and s0[b - 1] == " ":
        fp, fn = fold_compat(pos), fold_compat(name)
        if fp.startswith(fn) and fp[len(fn):] in MEMBER_ROLES_HANJA:
            return pos[len(name):], name, "v10_space_pos_name_dup"
        return pos, name, "v10_space_pos_name"
    return pos, name, "v10_hanja_suffix_name"


# Hangul surnames for splitting a Hangul name off a Hangul position (labels whose data-name is empty)
HANGUL_SURNAMES = frozenset("김이박최정강조윤장임한오서신권황안송류유전홍고문양손배백허남심노하곽성차주우구민진나지엄채원천방"
                            "공현함변염여추도소석선설마길연위표명기반왕금옥육인맹제모탁국어은편용예경봉사부가복태목형피두감음"
                            "빈동온호범좌팽승간상시갈단견당화창")
HANGUL_COMPOUND_SURNAMES = frozenset({"남궁", "황보", "선우", "제갈", "사공", "독고", "서문", "동방"})


def is_hangul_person_name(name):
    """A 2-4 syllable Hangul name that starts with a Hangul surname (stricter than is_hangul_name)."""
    if not is_hangul_name(name) or not 2 <= len(name) <= 4:
        return False
    return name[:2] in HANGUL_COMPOUND_SURNAMES or name[0] in HANGUL_SURNAMES


def split_hangul_label(label):
    """(pos, name, rule) for a Hangul label whose viewer data-name is empty, or (None, None, None).
      v10_space_pos_hangul_name  '건설교통부장관 추병직' (position ending in a title character, a
                                 space, then a 2-4 syllable Hangul person name)
    Labels with the name glued to the position ('통일부장관정동영') are NOT split: a split rule
    without a separator mis-splits position strings ('산업통상자원부장관' -> '부장관'), measured on
    the 23,858 distinct positions of the 2026-09-26 build (569 would split)."""
    s0 = pv.norm(label)
    if not s0 or not re.search("[가-힣]", s0):
        return None, None, None
    head, sep, tail = s0.rpartition(" ")
    if sep and head and head[-1] in HANGUL_POS_SUFFIX_CHARS and is_hangul_person_name(tail):
        return head, tail, "v10_space_pos_hangul_name"
    return None, None, None


LATIN_NAME_RE = re.compile(r"[A-Za-z][A-Za-z .'\-]{0,60}")


def _label_name_override(rest, dn, after_pos):
    """The printed label minus data-pos (`rest`) is longer than data-name `dn` and starts with it.
    Returns (outcome, pos_extension, name). Accepted: a spaced-out or longer name ('陳 稔' for
    data-name '陳', 5 chars at most), a Latin name, or - label printed as 'POS TITLE NAME' with
    data-name = TITLE - the title moved to the position ('金融監督 委員長 李瑾榮'). Rejected (data-name
    kept): the name printed twice ('張在植 張在植') and label text that runs into speech."""
    r = pv.norm(rest)
    toks = r.split(" ")
    if len(toks) > 1 and all(nows(x) == dn for x in toks):
        return "rejected_duplicate", None, None
    if CJK_NAME_RE.match(r):
        if len(nows(r)) <= 5 and len(toks) <= 3 and all(1 <= len(x) <= 4 for x in toks):
            return "spaced_name", None, nows(r)
        if after_pos and len(toks) >= 2:
            title, nm = " ".join(toks[:-1]), toks[-1]
            ft = fold_compat(title)
            if (ft[-1] in POS_SUFFIX_CHARS or ft[-1] in HANGUL_POS_SUFFIX_CHARS) and (
                    is_hanja_name(nm) or is_hangul_name(nm)):
                return "pos_extended", title, nm
        return "rejected_not_name", None, None
    if LATIN_NAME_RE.fullmatch(r):
        return "latin_name", None, r
    return "rejected_not_name", None, None


def _speaker_fields(sp, label):
    """speaker_label_raw / speaker_pos / speaker_name for a viewer turn.
    speaker_label_raw is the printed label from the turn's checkbox label ('김한정 위원 선택',
    '위원장 윤관석 선택') minus ' 선택'; fallback 'pos name' from data attributes.
    Returns (raw, pos, name, how, fused, from_label, override_outcome)."""
    pos, name = sp.get("pos_norm"), sp.get("name_norm")
    how = "in_chk_label"
    if label:
        raw = label[:-3].strip() if label.endswith(" 선택") else label
        if not label.endswith(" 선택"):
            how = "in_chk_label_nosuffix"
    else:
        raw = pv.norm(f"{sp.get('pos') or ''} {sp.get('name') or ''}")
        how = "attr_fallback"
    fused = any(ch in (x or "") for x in (raw, sp.get("pos"), sp.get("name")) for ch in "◯○")
    from_label, outcome = False, None
    p = (sp.get("pos") or "").strip()
    if raw and name and p and not fused:
        rest, after_pos = None, False
        if raw.startswith(p):
            rest, after_pos = raw[len(p):], True
        elif raw.endswith(p):
            rest = raw[:-len(p)]
        if rest is not None:
            rn, dn = nows(rest), nows(name)
            if rn != dn and len(rn) > len(dn) and rn.startswith(dn):
                outcome, pos_ext, new_name = _label_name_override(rest, dn, after_pos)
                if new_name is not None:
                    name, from_label = new_name, True
                    if pos_ext:
                        pos = pv.norm(f"{pos or ''} {pos_ext}")
    if name is None and sp.get("label_split") == "unsplit" and not fused:
        # the fused string is in data-pos on most pages, only in the printed label on some
        # (data-pos '薛', label '薛 勳委員')
        for src_s in (sp.get("pos"), raw):
            p2, n2, rule = split_fused_label(src_s)
            if not rule:
                p2, n2, rule = split_hangul_label(src_s)
            if rule:
                pos, name = p2, n2
                sp["label_split"] = rule
                break
    return raw or None, pos, name, how, fused, from_label, outcome


_PV_SPEAKER_BLOCK = pv._speaker_block


def _speaker_block_v10(div):
    """parse_viewer._speaker_block plus two text nodes the original leaves out of `sentences`
    (it keeps the first only in embedded_other and drops the second):
      - the leading text node of a div.txt (e.g. 17대 pages written without span.spk_sub,
        27616: the first line of 95 turns),
      - the tail text of a comment / processing-instruction child of div.txt.
    Both become sentence entries at their document position (embedded='textnode' /
    'comment_tail'). Every other entry is produced exactly as in parse_viewer (same loop), plus the
    un-normalized node text in 'raw' (used to detect a label repeated at the start of the text)."""
    sp = _PV_SPEAKER_BLOCK(div)
    sents, other = [], []
    added = Counter()
    for tx in div.xpath('./div[@class="talk"]/div[@class="txt"]'):
        pos = 0
        if pv.norm(tx.text):
            other.append({"kind": "textnode", "text": pv.norm(tx.text), "pos": pos})
            sents.append({"sub_id": None, "text": pv.norm(tx.text), "is_note": False, "embedded": "textnode",
                          "raw": tx.text})
            added["textnode"] += 1
        for c in tx:
            if not isinstance(c.tag, str):
                if pv.norm(c.tail):
                    other.append({"kind": "comment_tail", "text": pv.norm(c.tail), "pos": pos})
                    sents.append({"sub_id": None, "text": pv.norm(c.tail), "is_note": False,
                                  "embedded": "comment_tail", "raw": c.tail})
                    added["comment_tail"] += 1
                continue
            ccls = pv._cls(c)
            if c.tag == "span" and "spk_sub" in ccls:
                raw_sub = c.text_content()
                sents.append({"sub_id": c.get("id"), "text": pv.norm(raw_sub),
                              "is_note": bool(c.xpath('.//div[contains(@class,"taR")]')), "raw": raw_sub})
                pos += 1
            elif c.tag in ("br",) or (c.tag == "div" and ("line_dot" in ccls or "line_solid" in ccls)):
                if c.tag == "div":
                    sents.append({"sub_id": None, "text": "", "is_note": False, "separator": "line_dot"})
            else:
                txt = pv.norm(c.text_content())
                kind = "table" if c.xpath("self::table|.//table") else ("img" if c.xpath("self::img|.//img") else c.tag)
                if txt or kind == "img":
                    other.append({"kind": kind, "text": txt, "pos": pos, "cls": " ".join(ccls) or None})
                    sents.append({"sub_id": None, "text": txt, "is_note": False, "embedded": kind,
                                  "raw": c.text_content()})
            if pv.norm(c.tail):
                other.append({"kind": "tailtext", "text": pv.norm(c.tail), "pos": pos})
                sents.append({"sub_id": None, "text": pv.norm(c.tail), "is_note": False, "embedded": "tailtext",
                              "raw": c.tail})
    sp["sentences"], sp["embedded_other"] = sents, other
    sp["v10_added"] = dict(added)
    return sp


def parse_view_v10(page):
    """parse_viewer.parse_view with _speaker_block_v10 (swapped in for this call only)."""
    pv._speaker_block = _speaker_block_v10
    try:
        return pv.parse_view(page)
    finally:
        pv._speaker_block = _PV_SPEAKER_BLOCK


def xml_extract(conf_num, page, term=None, sha1=None):
    """One viewer view page -> dict(status, tables{name: [records]}, header, coverage)."""
    from lxml import html as LH
    if isinstance(page, (bytes, bytearray)) and page[:2] == b"\x1f\x8b":
        page = gzip.decompress(page)
    res = parse_view_v10(page)
    status = res["status"]
    out = {"conf_num": conf_num, "source": "xml", "status": status, "tables": {}, "header": None,
           "coverage": None, "counters": Counter()}
    m = res.get("meeting") or {}
    # audited agencies: the header field '피감사기관' split outside parentheses (parse_viewer splits
    # inside them too, '韓國銀行全北本部(光州全南․忠北 本部 포함)')
    aa_raw = [f["value"] for f in (m.get("fields") or [])
              if f.get("key") and f["key"].replace(" ", "") == "피감사기관" and f.get("value")]
    aa = [x for v in aa_raw for x in split_agencies(v)] or None
    if aa is not None and aa != m.get("audited_agencies"):
        out["counters"]["xml_audited_agencies_resplit"] += 1
    header = {
        "conf_num": conf_num, "source": "xml", "term": term, "parse_status": status,
        "h_title": m.get("title"), "h_term": m.get("term"), "h_session": m.get("session"),
        "h_session_type": m.get("session_type"), "h_sitting": m.get("sitting"),
        "h_committee_full": m.get("committee_full"), "h_committee": m.get("committee"),
        "h_subcommittee": m.get("subcommittee"), "h_is_audit": m.get("is_audit"),
        "h_audit_year": m.get("audit_year"), "h_date": m.get("date"),
        "h_doc_title": m.get("doc_title"), "h_turn": m.get("turn"), "h_doc_no": m.get("doc_no"),
        "h_author": m.get("author"), "h_fields_json": _json(m.get("fields")),
        "audited_agencies": aa, "audited_agencies_how": "viewer_field_피감사기관" if aa else None,
        "audited_agencies_raw": " | ".join(aa_raw) or None,
        "is_provisional": b'class="bg_tmp"' in page if isinstance(page, (bytes, bytearray)) else 'class="bg_tmp"' in page,
        "stats_json": _json(res.get("stats")), "raw_sha1": sha1, "raw_bytes": res.get("bytes"),
    }
    out["header"] = header
    null_policy({"headers": [header]}, out["counters"])
    if status not in ("ok", "ok_no_speeches"):
        return out
    labels, label_conflicts = _labels_from_page(page if isinstance(page, (bytes, bytearray)) else page.encode())
    out["counters"]["label_conflicts"] += label_conflicts
    turns = []
    for sp in res["speeches"]:
        for k, v in (sp.get("v10_added") or {}).items():
            out["counters"]["xml_added_" + k] += v
            out["counters"]["xml_turns_with_added_" + k] += 1
        num = (sp.get("spk_id") or "").split("_")[-1]
        raw, pos, name, how, fused, from_label, override = _speaker_fields(sp, labels.get(num))
        out["counters"]["label_" + how] += 1
        out["counters"]["label_fused"] += int(fused)
        out["counters"]["name_from_label"] += int(from_label)
        if override:
            out["counters"]["name_from_label_" + override] += 1
        out["counters"]["label_split_" + (sp.get("label_split") or "none")] += 1
        mem = (sp.get("mem_id") or "").strip()
        # the first sentence with text, as printed, when the spoken text starts with it
        first = next((se for se in sp["sentences"] if se.get("text")), None)
        if first is not None and not first.get("is_stage") and not first.get("is_oath_signature"):
            first_raw, first_line = first.get("raw"), first["text"]
        else:
            first_raw = first_line = None
        turns.append({
            "conf_num": conf_num, "turn_seq": sp["speech_seq"], "source": "xml",
            "speaker_label_raw": raw, "speaker_pos": pos, "speaker_name": name,
            "speaker_mem_id": int(mem) if mem.isdigit() and int(mem) != 0 else None,
            "speaker_area": sp.get("area") or None,
            "text_raw": sp["text"], "text": sp["text_spoken"], "has_stage": bool(sp["has_stage"]),
            "stage_kinds": list(sp["stage_kinds"]), "n_fragments": sp["n_fragments"],
            "agenda_ordinal": sp["agenda_ordinal"] or None, "agenda_text": sp.get("agenda_text"),
            "time_hhmm": sp.get("time_hhmm_end") or sp.get("time_hhmm"),
            "speech_date": sp.get("speech_date"),
            "term": term, "spk_id": sp.get("spk_id"), "label_split": sp.get("label_split"),
            "label_fused": fused, "name_from_label": from_label,
            "name_has_hanja": bool(HANJA_RE.search(name or "")),
            "profile_slug": sp.get("profile_slug"), "profile_term": sp.get("profile_term"),
            "agenda_item": sp.get("agenda_item"), "agenda_top_text": sp.get("agenda_top_text"),
            "time_hhmm_start": sp.get("time_hhmm"), "time_marker": sp.get("time_marker"),
            "speech_date_end": sp.get("speech_date_end"), "n_sentences": sp["n_sentences"],
            "n_stage_sentences": len(sp["stage_texts"]), "n_oath_signature": sp["n_oath_signature"],
            "n_embedded": len(sp["embedded_other"]), "stage_texts": list(sp["stage_texts"]),
            "interjections": _json(sp["interjections"]),
            "inline_stage_parens": list(sp["inline_stage_parens"]),
            "source_member_id": None, "source_speech_order": None, "text_rule": "xml_sentences",
            "label_how": how, "label_confidence": label_confidence(how, None, out["counters"])})
        strip_label_prefix(turns[-1], first_raw, first_line, out["counters"])
    agenda = [{"conf_num": conf_num, "ordinal": a["ordinal"], "anchor": a.get("anchor"),
               "level": a.get("level"), "text": a.get("text"), "bill_id": a.get("bill_id"),
               "bill_no": a.get("bill_no"), "is_continued": bool(a.get("is_continued")),
               "after_turn_seq": a.get("after_speech_seq"), "bill_url": a.get("bill_url"),
               "match_rule": "xml_anchor", "source": "xml", "term": term} for a in res["agenda"]]
    agenda_header = [{"conf_num": conf_num, "item_seq": i, "section": a.get("section"),
                      "head_id": a.get("head_id"), "level": a.get("level"), "num": a.get("num"),
                      "text": a.get("text"), "target": a.get("target"), "page": None,
                      "source": "xml", "term": term} for i, a in enumerate(res["agenda_header"], 1)]
    events = [{"conf_num": conf_num, "event_seq": i, "kind": e.get("kind"), "text": e.get("text"),
               "after_turn_seq": e.get("after_speech_seq"), "hhmm": e.get("hhmm"),
               "action": e.get("action"), "within_turn": bool(e.get("within_turn", False)),
               "new_date": e.get("new_date"), "tag": e.get("tag"), "cls": e.get("cls"),
               "source": "xml", "term": term} for i, e in enumerate(res["events"], 1)]
    assign_sittings(turns, [(e["after_turn_seq"], marker_action(e.get("text"), e.get("action")))
                            for e in events if e.get("kind") == "time"], out["counters"])
    frows = flatten_footer_xml(res["footer"])
    footer = [dict(r, conf_num=conf_num, source="xml", term=term) for r in frows]
    nvotes, rgroups, rnames = rollcall_from_xml_sections(res["footer"], out["counters"])
    att = attendance_from_xml_sections(res["footer"], out["counters"])
    out["tables"] = {
        "turns": turns, "agenda": agenda, "agenda_header": agenda_header, "events": events,
        "footer": footer,
        "rollcall": [dict(r, conf_num=conf_num, source="xml", term=term) for r in rnames],
        "rollcall_groups": [dict(r, conf_num=conf_num, source="xml", term=term) for r in rgroups],
        "attendance": [dict(r, conf_num=conf_num, source="xml", term=term) for r in att],
    }
    finalize_tables(out["tables"], out["counters"])
    dates = [t["speech_date"] for t in turns if t["speech_date"]] + \
            [t["speech_date_end"] for t in turns if t["speech_date_end"]] + \
            [e["new_date"] for e in events if e["new_date"]]
    header.update({
        "date_end": max(dates) if dates else m.get("date"), "n_turns": len(turns),
        "n_agenda": len(agenda), "n_agenda_header": len(agenda_header), "n_events": len(events),
        "n_footer_rows": len(footer), "n_rollcall_votes": nvotes,
        "agenda_confirmation_hit": any(is_confirmation_agenda(a["text"]) for a in agenda + agenda_header)})
    null_policy({"headers": [header]}, Counter())      # date_end etc. added after the first pass
    # -------- exact text accounting against an independent DOM pass
    t = LH.fromstring(page if isinstance(page, (bytes, bytearray)) else page.encode("utf-8"))
    body = t.xpath('//div[@id="minutes"]/div[contains(@class,"minutes_body")]')
    body = body[0]
    divs = body.xpath(SPK_XP)
    # one local pass per speaker div (a body-wide 'speaker//span' XPath is quadratic on long
    # plenary pages). Outermost span.spk_sub only, so a nested span is never counted twice.
    dom_sub = dom_txt = meta = nested_spk = 0
    for d in divs:
        dom_sub += sum(len(nows(x.text_content())) for x in
                       d.xpath('.//span[@class="spk_sub"][not(ancestor::span[@class="spk_sub"])]'))
        dom_txt += sum(len(nows(x.text_content())) for x in d.xpath('./div[@class="talk"]/div[@class="txt"]'))
        meta += sum(len(nows(x.text_content())) for x in d.xpath('./div[@class="in_chk"]|./div[@class="man"]'))
        nested_spk += len(d.xpath(SPK_XP))
    out["counters"]["xml_nested_speaker_divs"] += nested_spk
    btn = sum(len(nows(x.text_content())) for x in body.xpath('.//p[contains(@class,"angun")]//a[contains(@class,"btn_move")]'))
    total = len(nows(body.text_content()))
    parsed_sub = sum(len(nows(se["text"])) for s in res["speeches"] for se in s["sentences"] if se["sub_id"])
    tr = sum(len(nows(x["text_raw"])) for x in turns)
    ag = sum(len(nows(a["text"])) for a in agenda)
    ev = sum(len(nows(e["text"])) for e in events)
    acc = meta + tr + ag + btn + ev
    # the footer element parse_view used (first .minutes_footer under #minutes, any depth)
    ftr = t.xpath('//div[@id="minutes"]')[0].find_class("minutes_footer")
    dom_f = len(nows(ftr[0].text_content())) if ftr else 0
    footer_in_body = bool(ftr) and any(a is body for a in ftr[0].iterancestors())
    if footer_in_body:
        # its text is also a body event (cls minutes_footer), so it is counted in both tables
        out["counters"]["xml_footer_nested_in_body"] += 1
    got_f = footer_rows_chars(frows)
    seqs = [x["turn_seq"] for x in turns]
    cov = {"conf_num": conf_num, "source": "xml", "term": term, "n_turns": len(turns),
           "n_div_speaker": len(divs), "sum_fragments": sum(x["n_fragments"] for x in turns),
           "turn_seq_contiguous": seqs == list(range(1, len(turns) + 1)),
           "dom_spk_sub_chars": dom_sub, "parsed_sub_chars": parsed_sub, "turn_text_raw_chars": tr,
           "dom_txt_chars": dom_txt, "embedded_chars": dom_txt - dom_sub, "dom_body_chars": total,
           "accounted_body_chars": acc, "dom_footer_chars": dom_f, "footer_rows_chars": got_f,
           "ok_spk_sub_vs_text_raw": dom_sub == tr, "ok_sentences": dom_sub == parsed_sub,
           "ok_txt": dom_txt == tr, "ok_body": total == acc, "ok_footer": dom_f == got_f}
    cov["ok_all"] = bool(cov["ok_sentences"] and cov["ok_txt"] and cov["ok_body"] and cov["ok_footer"]
                         and cov["turn_seq_contiguous"] and cov["n_div_speaker"] == cov["sum_fragments"])
    cov["detail_json"] = _json({"footer_nested_in_body": True}) if footer_in_body else None
    out["coverage"] = cov
    return out


# ============================================================================ XLSX adapter

XLSX_SUFFIX_ROLE_RE = re.compile(r"^(위원장|위원|의원|참고인대리|委員長|委員|議員)(\([^()]*\))?$")
XLSX_TITLE_ONLY_RE = re.compile(r"^(제\d+차관|차관|장관|제\d+)$")
PAREN_SEG_RE = re.compile(r"\((?:[^()]|\([^()]*\))*\)")
SENT_END = set(".?!…」』”’\"')")
STAGE_LEX = re.compile("|".join(rx.pattern for k, rx in pv.STAGE_KINDS if k != "appendix_note"))
# a whole parenthetical that is a time marker ('(14시30분 회의중지)', '(9월26일 01시15분 산회)',
# '(22시 유회)') or a day roll-over ('(24시 경과)', '(3월19일 24시 경과)'), as printed in v9 speech text
XLSX_TIME_MARK_RE = re.compile(
    r"^\(\s*(?P<md>\d{1,2}\s*월\s*\d{1,2}\s*일\s*)?(?P<h>\d{1,2})\s*시\s*(?:(?P<m>\d{1,2})\s*분)?"
    r"\s*(?P<act>[가-힣][가-힣\s·ㆍ]{0,30})?\)$")


def split_xlsx_label(label):
    """v9 speaker string -> (speaker_pos, speaker_name, method).
    '박영선 위원' -> ('위원','박영선'); '위원장 류선호' -> ('위원장','류선호');
    '국방부장관 김태영' -> ('국방부장관','김태영'); '이수진(비) 위원' -> ('위원','이수진(비)');
    '최경환 위원(국)' -> ('위원','최경환(국)'); '증인 존 리' -> ('증인','존 리');
    '산업통상자원부 제1차관' -> (label, None); '진재문' -> (None,'진재문'); '육군본부' -> ('육군본부', None)."""
    s = pv.norm(label)
    if not s:
        return None, None, "empty"
    toks = s.split(" ")
    if len(toks) == 1:
        m = pv.NAME_ROLE_RE.match(s)
        if m:
            return m.group("role"), m.group("name"), "fused_name+role"
        if re.fullmatch(r"[가-힣]{2,4}", s):
            return None, s, "name_only"
        return s, None, "pos_only"
    m = XLSX_SUFFIX_ROLE_RE.match(toks[-1])
    if m and len(toks) == 2:
        return m.group(1), toks[0] + (m.group(2) or ""), "name+pos"
    if XLSX_TITLE_ONLY_RE.match(toks[-1]):
        return s, None, "pos_only_title"
    return toks[0], " ".join(toks[1:]), "pos+name"


def xlsx_stage_split(text):
    """Stage directions inside a one-line XLSX speech. A parenthetical that stands alone
    (whitespace or text boundary on both sides) is a stage direction when it opens a sentence
    (text start or after . ? ! … closing quote or ')') - kind by the parse_viewer lexicon or
    'other' - or, mid-sentence, when it matches the lexicon. Returns
    (spoken_text, stage_texts, stage_kinds, interjections, inline_stage_parens)."""
    if not text:
        return text, [], [], [], []
    keep, stage, kinds, interj = [], [], [], []
    last = 0
    for m in PAREN_SEG_RE.finditer(text):
        s, e = m.span()
        body = m.group(0)
        right_ok = e == len(text) or text[e].isspace()
        left_ok = s == 0 or text[s - 1].isspace()
        # a time marker glued to the end of the previous sentence ('습니다.(24시 경과)') is a
        # stage direction as well
        if not left_ok and right_ok and text[s - 1] in SENT_END and XLSX_TIME_MARK_RE.match(body):
            left_ok = True
        if not (left_ok and right_ok):
            continue
        before = text[:s].rstrip()
        sent_level = before == "" or before[-1] in SENT_END
        im = pv.INTERJ_RE.match(body)
        if im:
            kind = "interjection"
            who = im.group("who").strip()
            wm = pv.INTERJ_WHERE_RE.search(who)
            interj.append({"who": who[:wm.start()].strip() if wm else who,
                           "where": wm.group(1) if wm else None, "text": im.group("txt").strip()})
        else:
            kind = next((k for k, rx in pv.STAGE_KINDS if rx.search(body)), None)
            if kind is None and sent_level:
                kind = "other"
        if kind is None:
            continue
        keep.append(text[last:s])
        last = e
        stage.append(body)
        kinds.append(kind)
    keep.append(text[last:])
    spoken = WS_RE.sub(" ", "".join(keep)).strip() if stage else text
    inline = [p for p in pv.PAREN_RE.findall(spoken) if STAGE_LEX.search(p)]
    return spoken, stage, sorted(set(kinds)), interj, inline


def xlsx_apply_time_markers(turns, base_date, counters):
    """time_hhmm / speech_date for XLSX turns from the markers printed inside the v9 speech text
    (the v9 conversion kept the minutes' time lines as parentheticals in the neighbouring speech,
    sometimes glued to it: '습니다.(24시 경과)'), with parse_viewer's semantics: '(24시 경과)' or
    '(M월D일 24시 경과)' moves the date of what follows to the next day; '(HH시MM분 ...)' sets the
    time, and the date when it prints one. Per turn: speech_date and time_hhmm_start / time_marker
    are the state at the turn start, time_hhmm the last marker before or within the turn, and
    speech_date_end the date after a change that has spoken text after it in the same turn."""
    try:
        date = dt.date.fromisoformat(str(base_date)[:10]) if base_date else None
    except ValueError:
        date = None
        counters["xlsx_date_unparsed"] += 1
    hhmm = marker = None
    for t in turns:
        start_date, start_hhmm, start_marker = date, hhmm, marker
        mid_date = None
        txt = t["text_raw"] or ""
        for m in PAREN_SEG_RE.finditer(txt):
            body = m.group(0)
            if not XLSX_TIME_MARK_RE.match(body):
                continue
            md = pv.MD_RE.search(body)
            new_date = None
            if pv.ROLLOVER_RE.search(body):
                base = pv._md_to_date(md, date) if md else date
                if base is not None:
                    new_date = base + dt.timedelta(days=1)
                counters["xlsx_day_rollover"] += 1
            else:
                tm = pv.TIME_RE.search(body)
                if not tm:
                    counters["xlsx_time_marker_without_minutes"] += 1
                    continue
                hhmm, marker = f"{int(tm.group(1)):02d}:{int(tm.group(2)):02d}", pv.norm(body)
                counters["xlsx_time_marker"] += 1
                if md:
                    new_date = pv._md_to_date(md, date)
            if new_date is not None and new_date != date:
                date = new_date
                counters["xlsx_date_changes"] += 1
                if txt[m.end():].strip():
                    mid_date = date
        t["speech_date"] = start_date.isoformat() if start_date else t.get("speech_date")
        t["time_hhmm_start"], t["time_marker"], t["time_hhmm"] = start_hhmm, start_marker, hhmm
        t["speech_date_end"] = date.isoformat() if mid_date is not None else None


def xlsx_meeting_tables(conf_num, term, rows, v9_meeting_id=None):
    """rows: list of dicts of ONE v9 meeting (speaker, member_id, speech_order, agenda,
    speech_text, date, committee, session, sub_session), in any order."""
    def _order(r):
        try:
            return (0, int(str(r["speech_order"]).strip()))
        except (TypeError, ValueError):
            return (1, str(r["speech_order"]))  # never seen in v9 (0 non-numeric); kept, sorted last
    rows = sorted(rows, key=_order)
    c = Counter()
    turns, agenda = [], []
    prev_ag, ordinal, seen = object(), 0, set()
    for i, r in enumerate(rows, 1):
        ag = r.get("agenda")
        if ag != prev_ag:
            if ag is not None and str(ag).strip():
                ordinal += 1
                agenda.append({"conf_num": conf_num, "ordinal": ordinal, "anchor": None, "level": None,
                               "text": ag, "bill_id": None, "bill_no": (pv.BILL_NO_RE.search(ag) or [None, None])[1],
                               "is_continued": ag in seen or "(계속)" in ag, "after_turn_seq": i - 1,
                               "bill_url": None, "match_rule": "xlsx_agenda_run", "source": "xlsx",
                               "term": term})
                seen.add(ag)
            prev_ag = ag
        pos, name, how = split_xlsx_label(r.get("speaker"))
        c["xlsx_split_" + how] += 1
        raw = r.get("speech_text")
        if raw is None:
            c["xlsx_null_text"] += 1
            raw = ""
        spoken, stage, kinds, interj, inline = xlsx_stage_split(raw)
        c["xlsx_stage_segments"] += len(stage)
        turns.append({
            "conf_num": conf_num, "turn_seq": i, "source": "xlsx",
            "speaker_label_raw": r.get("speaker"), "speaker_pos": pos, "speaker_name": name,
            "speaker_mem_id": None, "speaker_area": None, "text_raw": raw, "text": spoken,
            "has_stage": bool(stage), "stage_kinds": kinds, "n_fragments": 1,
            "agenda_ordinal": ordinal if (ag is not None and str(ag).strip()) else None,
            "agenda_text": ag, "time_hhmm": None, "speech_date": r.get("date"),
            "term": term, "spk_id": None, "label_split": how, "label_fused": "◯" in (r.get("speaker") or ""),
            "name_from_label": False, "name_has_hanja": bool(HANJA_RE.search(name or "")),
            "profile_slug": None, "profile_term": None, "agenda_item": None, "agenda_top_text": None,
            "time_hhmm_start": None, "time_marker": None, "speech_date_end": None,
            "n_sentences": None, "n_stage_sentences": len(stage), "n_oath_signature": None,
            "n_embedded": 0, "stage_texts": stage, "interjections": _json(interj),
            "inline_stage_parens": inline, "source_member_id": r.get("member_id"),
            "source_speech_order": str(r.get("speech_order")), "text_rule": "xlsx_paren_rule",
            "label_how": "xlsx_speaker_column", "label_confidence": label_confidence("xlsx_speaker_column", None, c)})
        if not stage:
            strip_label_prefix(turns[-1], spoken, spoken.split("\n")[0] if spoken else None, c)
        else:
            turns[-1]["text_label_prefix_stripped"], turns[-1]["text_label_prefix_match"] = False, None
    r0 = rows[0] if rows else {}
    xlsx_apply_time_markers(turns, r0.get("date"), c)
    # sittings / after_end_marker from the time markers printed inside the v9 speech text: a marker in
    # turn k's text precedes turn k+1 (after_turn_seq k)
    marks = [(t["turn_seq"], marker_action(m.group(0))) for t in turns
             for m in PAREN_SEG_RE.finditer(t["text_raw"] or "") if XLSX_TIME_MARK_RE.match(m.group(0))]
    assign_sittings(turns, marks, c)
    dates = [x for t in turns for x in (t["speech_date"], t["speech_date_end"]) if x]
    header = {"conf_num": conf_num, "source": "xlsx", "term": term, "parse_status": "ok" if turns else "ok_no_turns",
              "h_title": None, "h_term": r0.get("term"), "h_session": None, "h_session_type": None,
              "h_sitting": None, "h_committee_full": r0.get("committee"), "h_committee": r0.get("committee"),
              "h_subcommittee": None, "h_is_audit": r0.get("hearing_type") == "국정감사",
              "h_audit_year": None, "h_date": r0.get("date"), "h_doc_title": None, "h_turn": None,
              "h_doc_no": None, "h_author": None, "h_fields_json": None, "audited_agencies": None,
              "is_provisional": None, "date_end": max(dates) if dates else r0.get("date"), "n_turns": len(turns),
              "n_agenda": len(agenda), "n_agenda_header": 0, "n_events": 0, "n_footer_rows": 0,
              "n_rollcall_votes": 0,
              "agenda_confirmation_hit": any(is_confirmation_agenda(a["text"]) for a in agenda),
              "stats_json": None, "raw_sha1": "v9", "raw_bytes": None,
              "extra_json": _json({"v9_meeting_id": v9_meeting_id, "v9_session": r0.get("session"),
                                   "v9_sub_session": r0.get("sub_session"),
                                   "v9_hearing_type": r0.get("hearing_type")})}
    seqs = [t["turn_seq"] for t in turns]
    tr = sum(len(nows(t["text_raw"])) for t in turns)
    src = sum(len(nows(r.get("speech_text"))) for r in rows)
    cov = {"conf_num": conf_num, "source": "xlsx", "term": term, "n_turns": len(turns),
           "n_div_speaker": None, "sum_fragments": len(turns),
           "turn_seq_contiguous": seqs == list(range(1, len(turns) + 1)),
           "dom_spk_sub_chars": src, "parsed_sub_chars": src, "turn_text_raw_chars": tr,
           "dom_txt_chars": src, "embedded_chars": 0, "dom_body_chars": None, "accounted_body_chars": None,
           "dom_footer_chars": None, "footer_rows_chars": None,
           "ok_spk_sub_vs_text_raw": src == tr, "ok_sentences": src == tr, "ok_txt": src == tr,
           "ok_body": None, "ok_footer": None, "detail_json": _json(
               {"speech_order_min": _int_or_none(rows[0]["speech_order"]) if rows else None,
                "speech_order_max": _int_or_none(rows[-1]["speech_order"]) if rows else None,
                "speech_order_gaps": (_int_or_none(rows[-1]["speech_order"]) - _int_or_none(rows[0]["speech_order"]) + 1 - len(rows))
                if rows and _int_or_none(rows[0]["speech_order"]) is not None and _int_or_none(rows[-1]["speech_order"]) is not None else None})}
    cov["ok_all"] = bool(cov["ok_txt"] and cov["turn_seq_contiguous"])
    finalize_tables({"turns": turns, "agenda": agenda, "headers": [header]}, c)
    return {"conf_num": conf_num, "source": "xlsx", "status": header["parse_status"],
            "tables": {"turns": turns, "agenda": agenda}, "header": header, "coverage": cov,
            "counters": c}


# ============================================================================ HWP adapter

def _import_hwp_parser():
    """Lazy, pinned import (one read of hwp_parser.py; __pinned_sha8__ is the hash of the code
    that runs), so this module works before hwp_parser exists."""
    global _HWP
    if _HWP is None:
        _HWP = _load_pinned("hwp_parser", HERE / "hwp_parser.py")
    return _HWP


def _hwp_version():
    return adapter_version("hwp", _module_sha8(_import_hwp_parser()))


HWP_NAME_SPLIT_RE = re.compile(r"[ 　\xa0]{2,}|\t")


def _hwp_line_names(lines):
    out = []
    for ln in lines:
        toks = [x.strip() for x in HWP_NAME_SPLIT_RE.split(ln.strip()) if x.strip()]
        i = 0
        while i < len(toks):
            a = toks[i]
            if len(a) == 1 and i + 1 < len(toks) and len(toks[i + 1]) == 1:
                out.append(a + toks[i + 1])
                i += 2
                continue
            out.extend(a.split(" ") if re.fullmatch(r"(?:[가-힣]{2,4} )+[가-힣]{2,4}", a) else [a])
            i += 1
    return out


def flatten_footer_hwp(sections):
    """hwp_parser footer sections ({title, lines, tables, names?}) -> long rows. Text-bearing rows
    are lines and table cells; names derived by the HWP parser are extra rows of kind
    'name_derived' that do not count as text."""
    rows = []
    for si, s in enumerate(sections, 1):
        base = {"section_seq": si, "section_title": s.get("title"), "group_seq": 1, "group_label": None}
        k = 0
        items = [{"item_kind": "line", "line_text": ln} for ln in (s.get("lines") or [])]
        for ti, tb in enumerate(s.get("tables") or [], 1):
            byrow = defaultdict(list)
            for cell in tb.get("cells") or []:
                byrow[cell.get("row")].append(cell)
            for ri, (r, cells) in enumerate(sorted(byrow.items(), key=lambda x: (x[0] is None, x[0])), 1):
                cells = sorted(cells, key=lambda c: (c.get("col") is None, c.get("col")))
                items.append({"item_kind": "table_row", "line_text": "\t".join(c.get("text") or "" for c in cells),
                              "table_seq": ti, "row_idx": ri})
        for n in s.get("names") or []:
            items.append({"item_kind": "name_derived", "name": n})
        if not items:
            rows.append(dict(base, group_seq=None, item_seq=None, item_kind="title_only"))
        for it in items:
            k += 1
            rows.append(dict(base, item_seq=k, **it))
    return rows


def rollcall_attendance_from_hwp(sections, counters=None):
    """Vote name lists and attendance from HWP footer sections (line based). Note lines
    (parenthesized, angle-bracketed like '<2차 투표>', or starting with ※) are never names; a
    separator line containing 투표 starts a new vote (see _VoteSeq)."""
    groups, names, att = [], [], []
    ctx_title, ctx_si = None, None
    vs = _VoteSeq()
    for si, s in enumerate(sections, 1):
        title = s.get("title")
        lines = s.get("lines") or []
        tm = vote_group_match(title)
        blocks = []                       # (match, label, lines, separator before the group)
        sep_after = None
        if tm:
            # the section title itself is a vote group ('◯찬성 의원(164인)'): the vote is titled by
            # the last preceding non-vote section. A separator line in it ('<2차 투표>') is a note
            # of this group and starts a new vote at the next group.
            blocks.append([tm, title, list(lines), None])
            sep_after = next((ln for ln in lines if VOTE_SEP_RE.match(ln)), None)
            key, vt = ("ctx", ctx_si), _vote_title(ctx_title) if ctx_title else None
        else:
            # vote groups as lines inside a section titled by the bill
            sep = None
            for ln in lines:
                lm = vote_group_match(ln)
                if lm:
                    blocks.append([lm, ln, [], sep])
                    sep = None
                elif VOTE_SEP_RE.match(ln):
                    sep = pv.norm(ln)
                elif blocks:
                    blocks[-1][2].append(ln)
            if sep and blocks:            # a trailing separator with no group after it
                blocks[-1][2].append(sep)
            key, vt = ("sec", si), _vote_title(title)
        if blocks:
            for m, lab, blines, sep in blocks:
                grp = VOTE_GROUP_NORM.get(m.group(1), m.group(1))
                nrep = int(m.group(2)) if m.group(2) else None
                if sep:
                    vs.force = True
                seq = vs.next(key, grp)
                notes = ([sep] if sep else []) + [ln.strip() for ln in blines if is_note_line(ln)]
                nm = _hwp_line_names([ln for ln in blines if not is_note_line(ln)])
                groups.append({"vote_seq": seq, "vote_title": vt, "vote_group": grp, "group_label": lab,
                               "n_reported": nrep, "n_names": len(nm), "note": " | ".join(notes) or None,
                               "method": "hwp_lines"})
                for j, n in enumerate(nm, 1):
                    names.append({"vote_seq": seq, "vote_title": vt, "vote_section_title": title,
                                  "vote_group": grp, "group_label": lab, "n_reported": nrep, "name_seq": j,
                                  "name": n, "pos": None, "profile_url": None, "method": "hwp_lines"})
            if sep_after:
                vs.force = True
            continue
        ctx_title, ctx_si = title, si
        cat = attendance_category(title)
        if cat is None:
            if counters is not None and attendance_like_uncategorized(title):
                counters["attendance_like_title_uncategorized"] += 1
            continue
        nrep = _n_reported(title)
        derived = s.get("names")
        if derived is not None and cat in MEMBER_ATT_CATS:
            for n in derived:
                note = bool(PAREN_LINE_RE.match(n or ""))
                if counters is not None and not note and re.fullmatch(f"[{HANGUL_CLS}]{{5,}}", n or ""):
                    # hwp_parser output passed through unsplit (e.g. two names run together)
                    counters["hwp_att_name_derived_hangul_ge5"] += 1
                att.append({"section_seq": si, "section_title": title, "category": cat, "n_reported": nrep,
                            "group_label": None, "item_kind": "note" if note else "name_derived",
                            "name": None if note else n, "line_text": n if note else None})
        else:
            for ln in lines:
                att.append({"section_seq": si, "section_title": title, "category": cat, "n_reported": nrep,
                            "group_label": None, "item_kind": "line", "line_text": ln})
    if counters is not None:
        counters["rollcall_vote_split_in_section"] += vs.splits
    return vs.n, groups, names, att


def _hwp_turn(t, conf_num, term):
    return {
        "conf_num": conf_num, "turn_seq": t.get("turn_seq"), "source": "hwp",
        "speaker_label_raw": t.get("speaker_label_raw"), "speaker_pos": t.get("speaker_pos"),
        "speaker_name": t.get("speaker_name"), "speaker_mem_id": _int_or_none(t.get("speaker_mem_id")),
        "speaker_area": t.get("speaker_area"), "text_raw": t.get("text_raw"), "text": t.get("text"),
        "has_stage": bool(t.get("has_stage")), "stage_kinds": list(t.get("stage_kinds") or []),
        "n_fragments": t.get("n_fragments") or 1, "agenda_ordinal": _int_or_none(t.get("agenda_ordinal")),
        "agenda_text": t.get("agenda_text"),
        "time_hhmm": t.get("time_hhmm_end") or t.get("time_hhmm"), "speech_date": t.get("speech_date"),
        "term": term, "spk_id": None, "label_split": t.get("label_split"),
        "label_fused": any(ch in (t.get("speaker_label_raw") or "")[1:] for ch in "◯○"),
        "name_from_label": False,
        "name_has_hanja": bool(t.get("name_has_hanja")) if t.get("name_has_hanja") is not None
        else bool(HANJA_RE.search(t.get("speaker_name") or "")),
        "profile_slug": None, "profile_term": None, "agenda_item": None, "agenda_top_text": None,
        "time_hhmm_start": t.get("time_hhmm"), "time_marker": t.get("time_marker"),
        "speech_date_end": t.get("speech_date_end"), "n_sentences": _int_or_none(t.get("n_lines")),
        "n_stage_sentences": len(t.get("stage_texts") or []),
        "n_oath_signature": _int_or_none(t.get("n_oath_signature")),
        "n_embedded": len(t.get("embedded_tables") or []), "stage_texts": list(t.get("stage_texts") or []),
        "interjections": t["interjections"] if isinstance(t.get("interjections"), str) else _json(t.get("interjections")),
        "inline_stage_parens": list(t.get("inline_stage_parens") or []),
        "source_member_id": None, "source_speech_order": None, "text_rule": "hwp_lines",
        # carried from hwp_parser; assign_sittings() fills what the parser does not supply
        "after_end_marker": t.get("after_end_marker"), "after_final_end_marker": t.get("after_final_end_marker"),
        "sitting_seq": _int_or_none(t.get("sitting_seq")),
        "sitting_how": None, "label_how": t.get("label_how"),
        "label_confidence": _hwp_label_conf_parser(t),
        "label_lex_count": _int_or_none(t.get("label_lex_count")), "speech_date_how": t.get("speech_date_how"),
        "time_regress": None if t.get("time_regress") is None else bool(t.get("time_regress"))}


def _hwp_norm(s):
    """hwp_parser.norm: whitespace runs to one space, stripped (the parser's first text line)."""
    return " ".join((s or "").split())


def _hwp_label_conf_parser(t):
    """The parser's own label-confidence value, if it supplies one ('label_confidence', or a boolean
    'label_weak' read as 'low'); None otherwise (build_turns' LABEL_CONFIDENCE table is used)."""
    v = t.get("label_confidence")
    if v is not None and str(v).strip():
        return str(v)
    w = t.get("label_weak")
    if w is not None and w == w and bool(w):
        return "low"
    return None


def hwp_extract(conf_num, data, term=None, sha1=None):
    hp = _import_hwp_parser()
    res = hp.parse_hwp(data, conf_num=conf_num)
    status = res.get("status")
    out = {"conf_num": conf_num, "source": "hwp", "status": status, "tables": {}, "header": None,
           "coverage": None, "counters": Counter()}
    m = res.get("meeting") or {}
    raw_turns = res.get("turns")
    if raw_turns is not None and hasattr(raw_turns, "to_dict"):
        raw_turns = raw_turns.to_dict("records")
    turns = [_hwp_turn(t, conf_num, term) for t in (raw_turns or [])]
    for i, (t, rtn) in enumerate(zip(turns, raw_turns or []), 1):
        if t["turn_seq"] is None:
            t["turn_seq"] = i
            out["counters"]["hwp_turn_seq_filled"] += 1
        t["label_confidence"] = label_confidence(t["label_how"], t["label_confidence"], out["counters"])
        if t["label_how"] is None:
            out["counters"]["hwp_label_how_missing"] += 1
        fr = rtn.get("first_text_raw")
        strip_label_prefix(t, fr, _hwp_norm(fr) if fr is not None else None, out["counters"])
    rt = raw_turns or []
    parser_sitting = any("sitting_seq" in x for x in rt)
    parser_after_end = any("after_end_marker" in x for x in rt)
    parser_after_final = any("after_final_end_marker" in x for x in rt)
    if rt and not parser_sitting:
        out["counters"]["hwp_sitting_seq_not_in_parser_meetings"] += 1
    if rt and not parser_after_end:
        out["counters"]["hwp_after_end_marker_not_in_parser_meetings"] += 1
    header = {
        "conf_num": conf_num, "source": "hwp", "term": term, "parse_status": status,
        "h_title": m.get("doc_title"), "h_term": _int_or_none(m.get("term")),
        "h_session": _int_or_none(m.get("session_no") if m.get("session_no") is not None else m.get("session")),
        "h_session_type": m.get("session_type"), "h_sitting": m.get("sitting"),
        "h_committee_full": m.get("committee_raw") or m.get("committee"),
        "h_committee": m.get("committee_raw") or m.get("committee"), "h_subcommittee": m.get("subcommittee"),
        "h_is_audit": m.get("is_audit"), "h_audit_year": _int_or_none(m.get("audit_year")),
        "h_date": m.get("date"), "h_doc_title": m.get("doc_title"), "h_turn": None,
        "h_doc_no": None if m.get("doc_no") is None else str(m.get("doc_no")), "h_author": None,
        "h_fields_json": None, "audited_agencies": m.get("audited_agencies") or None, "is_provisional": None,
        "stats_json": _json(res.get("stats")), "raw_sha1": sha1, "raw_bytes": len(data),
        "extra_json": _json({"meeting": m, "reader": res.get("reader")}),
        "audited_agencies_how": ("hwp_cover_" + m["audited_agencies_label"]) if m.get("audited_agencies") else None,
        "audited_agencies_raw": " | ".join(m.get("audited_agencies_raw") or []) or None,
    }
    out["header"] = header
    null_policy({"headers": [header]}, out["counters"])
    if status not in ("ok", "ok_no_turns"):
        return out
    agenda = [{"conf_num": conf_num, "ordinal": _int_or_none(a.get("ordinal")) or i, "anchor": None,
               "level": a.get("level"), "text": a.get("text"), "bill_id": a.get("bill_id"),
               "bill_no": None if a.get("bill_no") is None else str(a.get("bill_no")),
               "is_continued": bool(a.get("is_continued")) if a.get("is_continued") is not None
               else "(계속)" in (a.get("text") or ""),
               "after_turn_seq": _int_or_none(a.get("after_turn_seq")), "bill_url": None,
               "match_rule": "hwp_" + (a.get("match") or "agenda"), "source": "hwp", "term": term}
              for i, a in enumerate(res.get("agenda") or [], 1)]
    agenda_header = [{"conf_num": conf_num, "item_seq": i, "section": a.get("section"), "head_id": None,
                      "level": a.get("level"), "num": a.get("num"), "text": a.get("text"), "target": None,
                      "page": _int_or_none(a.get("page")), "source": "hwp", "term": term}
                     for i, a in enumerate(res.get("agenda_header") or [], 1)]
    events = [{"conf_num": conf_num, "event_seq": i, "kind": e.get("kind"), "text": e.get("text"),
               "after_turn_seq": _int_or_none(e.get("after_turn_seq")), "hhmm": e.get("hhmm"),
               "action": e.get("action"), "within_turn": bool(e.get("within_turn", False)),
               "new_date": e.get("new_date"), "tag": e.get("tag"),
               "cls": None if e.get("table_id") is None else f"table_id={e.get('table_id')}",
               "source": "hwp", "term": term} for i, e in enumerate(res.get("events") or [], 1)]
    assign_sittings(turns, [(e["after_turn_seq"], marker_action(e.get("text"), e.get("action")))
                            for e in events if e.get("kind") == "time"], out["counters"],
                    parser_sitting=parser_sitting, parser_after_end=parser_after_end,
                    parser_after_final=parser_after_final)
    fsec = res.get("footer") or []
    frows = flatten_footer_hwp(fsec)
    nvotes, rgroups, rnames, att = rollcall_attendance_from_hwp(fsec, out["counters"])
    out["tables"] = {
        "turns": turns, "agenda": agenda, "agenda_header": agenda_header, "events": events,
        "footer": [dict(r, conf_num=conf_num, source="hwp", term=term) for r in frows],
        "rollcall": [dict(r, conf_num=conf_num, source="hwp", term=term) for r in rnames],
        "rollcall_groups": [dict(r, conf_num=conf_num, source="hwp", term=term) for r in rgroups],
        "attendance": [dict(r, conf_num=conf_num, source="hwp", term=term) for r in att]}
    finalize_tables(out["tables"], out["counters"])
    dates = [t["speech_date"] for t in turns if t["speech_date"]] + \
            [t["speech_date_end"] for t in turns if t["speech_date_end"]] + \
            [e["new_date"] for e in events if e["new_date"]]
    header.update({"date_end": max(dates) if dates else m.get("date"), "n_turns": len(turns),
                   "n_agenda": len(agenda), "n_agenda_header": len(agenda_header), "n_events": len(events),
                   "n_footer_rows": len(frows), "n_rollcall_votes": nvotes,
                   "agenda_confirmation_hit": any(is_confirmation_agenda(a["text"]) for a in agenda + agenda_header)})
    null_policy({"headers": [header]}, Counter())
    st = res.get("stats") or {}
    seqs = [t["turn_seq"] for t in turns]
    tr = sum(len(nows(t["text_raw"])) for t in turns)
    lab = sum(len(nows(t["speaker_label_raw"])) for t in turns)
    items = st.get("chars_turn_items")
    cat = st.get("chars_by_category") or {}
    # footer rows (section titles + lines + table cells) vs the parser's appendix items
    got_f = sum(len(nows(s.get("title"))) + sum(len(nows(x)) for x in (s.get("lines") or []))
                + sum(len(nows(c.get("text"))) for tb in (s.get("tables") or []) for c in (tb.get("cells") or []))
                for s in fsec)
    dom_f = cat.get("appendix", 0)
    # whole document: turn markers + labels + text_raw + agenda + events + footer + cover
    ag_c = sum(len(nows(a["text"])) for a in agenda)
    ev_c = sum(len(nows(e["text"])) for e in events)
    acc = len(turns) + lab + tr + ag_c + ev_c + got_f + cat.get("cover", 0)
    total = st.get("chars_total")
    cov = {"conf_num": conf_num, "source": "hwp", "term": term, "n_turns": len(turns), "n_div_speaker": None,
           "sum_fragments": sum(t["n_fragments"] for t in turns),
           "turn_seq_contiguous": seqs == list(range(1, len(turns) + 1)),
           "dom_spk_sub_chars": items, "parsed_sub_chars": None, "turn_text_raw_chars": tr,
           "dom_txt_chars": None, "embedded_chars": cat.get("table_in_turn", 0), "dom_body_chars": total,
           "accounted_body_chars": acc, "dom_footer_chars": dom_f, "footer_rows_chars": got_f,
           "ok_spk_sub_vs_text_raw": (items == tr + lab) if items is not None else None,
           "ok_sentences": None, "ok_txt": (items == tr + lab) if items is not None else None,
           "ok_body": (total == acc and st.get("n_unassigned") == 0) if total is not None else None,
           "ok_footer": dom_f == got_f,
           "detail_json": _json({"label_chars": lab, "chars_turns": st.get("chars_turns"),
                                 "n_unassigned": st.get("n_unassigned"), "chars_by_category": cat})}
    cov["ok_all"] = bool(cov["turn_seq_contiguous"] and cov["ok_txt"] is not False and cov["ok_body"] is not False
                         and cov["ok_footer"])
    out["coverage"] = cov
    return out


# ============================================================================ worker

def _work(task):
    """task = (source, conf_num, path, term, sha1). Runs in a pool worker. The adapter version is
    taken from the modules this process executes (pinned parser hash + adapter code hash), before
    parsing, so a failed parse is recorded with the version that produced it too."""
    source, conf_num, path, term, sha1 = task
    t0 = time.time()
    version = None
    try:
        if source == "xml":
            version = adapter_version("xml", pv.__pinned_sha8__)
        elif source == "hwp":
            version = _hwp_version()
        else:
            raise ValueError(source)
        data = Path(path).read_bytes()
        if source == "xml":
            r = xml_extract(conf_num, data, term=term, sha1=sha1)
        else:
            r = hwp_extract(conf_num, data, term=term, sha1=sha1)
        r["error"] = None
        r["raw_bytes_read"] = len(data)
    except Exception as e:  # recorded, never silent
        r = {"conf_num": conf_num, "source": source, "status": "exception", "tables": {}, "header": None,
             "coverage": None, "counters": Counter(),
             "error": f"{type(e).__name__}: {e}\n{traceback.format_exc()[-2000:]}", "raw_bytes_read": 0}
    r["adapter_version"] = version
    r["elapsed"] = time.time() - t0
    r["sha1"] = sha1
    r["term"] = term
    return r


# ============================================================================ state (manifest)

def open_state(cfg):
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(cfg.state_db, timeout=60)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS built(conf_num INTEGER PRIMARY KEY, source TEXT, term INTEGER,
        status TEXT, batch_key TEXT, raw_sha1 TEXT, n_turns INTEGER, built_at TEXT, run_id TEXT,
        builder_version TEXT);
    CREATE TABLE IF NOT EXISTS attempts(conf_num INTEGER, source TEXT, status TEXT, error TEXT,
        run_id TEXT, at TEXT, raw_sha1 TEXT);
    CREATE TABLE IF NOT EXISTS batches(batch_key TEXT PRIMARY KEY, source TEXT, term INTEGER,
        n_meetings INTEGER, run_id TEXT, written_at TEXT);
    CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY, started TEXT, finished TEXT,
        args TEXT, summary TEXT);
    -- rows of a rebuilt meeting still in its previous batch files; recorded in the same transaction
    -- that points `built` at the new batch, removed from the files right after (or at the next start)
    CREATE TABLE IF NOT EXISTS pending_drops(conf_num INTEGER, batch_key TEXT, run_id TEXT, at TEXT);
    """)
    cols = {r[1] for r in con.execute("PRAGMA table_info(attempts)")}
    if "builder_version" not in cols:
        with con:
            con.execute("ALTER TABLE attempts ADD COLUMN builder_version TEXT")
    return con


class RunLocked(RuntimeError):
    pass


@contextlib.contextmanager
def run_lock(cfg):
    """Exclusive lock on build_turns/run.lock for the whole run (flock, released by the OS if the
    process dies). A second run fails fast instead of moving the first run's uncommitted batch
    files to _orphans/."""
    import fcntl
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.state_dir / "run.lock"
    f = open(path, "a+")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.seek(0)
        holder = f.read().strip()
        f.close()
        raise RunLocked(f"another build_turns run holds {path}: {holder or 'unknown holder'}")
    try:
        f.seek(0)
        f.truncate()
        f.write(f"pid={os.getpid()} since={dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}\n")
        f.flush()
        yield
    finally:
        f.seek(0)
        f.truncate()                      # the holder line is cleared before the lock is released
        f.flush()
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()


def new_run_id():
    """UTC timestamp with microseconds (a second run in the same second no longer collides)."""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S_%f")


def built_map(con):
    return {r[0]: {"source": r[1], "term": r[2], "status": r[3], "batch_key": r[4], "raw_sha1": r[5],
                   "builder_version": r[6]}
            for r in con.execute("SELECT conf_num, source, term, status, batch_key, raw_sha1, builder_version FROM built")}


def _batch_files(cfg, batch_key):
    src, tdir = batch_key.split("/")[:2]
    out = []
    for tb in ALL_TABLES:
        p = table_dir(cfg, tb) / src / tdir / (batch_key.split("/")[-1] + ".parquet")
        if p.exists():
            out.append((tb, p))
    return out


def reconcile_orphans(cfg, con):
    """Batch files not registered in the manifest (interrupted run) are moved to _orphans/."""
    known = {r[0] for r in con.execute("SELECT batch_key FROM batches")}
    moved = 0
    for tb in ALL_TABLES:
        d = table_dir(cfg, tb)
        if not d.exists():
            continue
        for p in d.glob("*/*/*.parquet"):
            key = f"{p.parent.parent.name}/{p.parent.name}/{p.stem}"
            if key not in known:
                dest = cfg.state_dir / "_orphans" / tb / p.parent.parent.name / p.parent.name
                dest.mkdir(parents=True, exist_ok=True)
                shutil.move(str(p), str(dest / p.name))
                moved += 1
    for p in list(cfg.out.glob("*/*/*/.*.tmp")) + list(cfg.state_dir.glob("*/*/*/.*.tmp")):
        dest = cfg.state_dir / "_orphans" / "_tmp"
        dest.mkdir(parents=True, exist_ok=True)
        shutil.move(str(p), str(dest / (p.parent.parent.parent.name + "_" + p.name)))
        moved += 1
    return moved


def _write_table(path, table, rows):
    schema = SCHEMAS[table]
    cols = {f.name: [r.get(f.name) for r in rows] for f in schema}
    tbl = pa.Table.from_pydict(cols, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, path)
    return tbl.num_rows


def write_batch(cfg, con, run_id, source, term, key_seq, results):
    """Write all tables of a group of meetings, then commit the manifest rows. A meeting that was
    built before keeps its previous rows until this point: the same transaction that points
    `built` at the new batch records the old batch in pending_drops, and apply_pending_drops then
    removes the meeting's rows from the old files."""
    tdir = f"t{term}" if term is not None else "tNA"
    stem = f"b{run_id}_{key_seq:05d}"
    batch_key = f"{source}/{tdir}/{stem}"
    rows = defaultdict(list)
    for r in results:
        for tb, recs in r["tables"].items():
            rows[tb].extend(recs)
        if r.get("header"):
            rows["headers"].append(r["header"])
        if r.get("coverage"):
            rows["coverage"].append(r["coverage"])
    written = {}
    for tb in ALL_TABLES:
        if rows.get(tb):
            written[tb] = _write_table(table_dir(cfg, tb) / source / tdir / f"{stem}.parquet", tb, rows[tb])
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    confs = [r["conf_num"] for r in results]
    with con:
        old = []
        for i in range(0, len(confs), 500):
            part = confs[i:i + 500]
            old += con.execute(f"SELECT conf_num, batch_key FROM built WHERE conf_num IN ({','.join('?' * len(part))})",
                               part).fetchall()
        con.executemany("INSERT INTO pending_drops VALUES (?,?,?,?)",
                        [(n, k, run_id, now) for n, k in old if k != batch_key])
        con.execute("INSERT OR REPLACE INTO batches VALUES (?,?,?,?,?,?)",
                    (batch_key, source, term, len(results), run_id, now))
        con.executemany("INSERT OR REPLACE INTO built VALUES (?,?,?,?,?,?,?,?,?,?)",
                        [(r["conf_num"], source, term, r["status"], batch_key, r.get("sha1"),
                          len(r["tables"].get("turns", [])), now, run_id,
                          r.get("adapter_version") or adapter_version(source)) for r in results])
    apply_pending_drops(cfg, con)
    return batch_key, written


def _drop_from_batch(cfg, key, conf_nums):
    """Rewrite one batch's files without the given meetings (an emptied file is moved to
    _orphans/_emptied/, never deleted). Returns the number of rows removed."""
    removed = 0
    vs = pa.array(sorted(conf_nums), I64)
    for tb, p in _batch_files(cfg, key):
        t = pq.read_table(p)
        t2 = t.filter(pc.invert(pc.is_in(t["conf_num"], value_set=vs)))
        if t2.num_rows == t.num_rows:
            continue
        removed += t.num_rows - t2.num_rows
        tmp = p.parent / f".{p.name}.tmp"
        if t2.num_rows:
            pq.write_table(t2, tmp, compression="zstd")
            os.replace(tmp, p)
        else:
            dest = cfg.state_dir / "_orphans" / "_emptied" / tb / p.parent.parent.name / p.parent.name
            dest.mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), str(dest / p.name))
    return removed


def drop_meetings(cfg, con, run_id, conf_nums):
    """Remove built meetings (rows and manifest entries) whose source is no longer a production source
    and that have no other source: recorded in pending_drops in the same transaction that deletes the
    `built` row, then applied. Returns the number of rows removed (the run summary counts them)."""
    if not conf_nums:
        return 0
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    with con:
        for i in range(0, len(conf_nums), 500):
            part = conf_nums[i:i + 500]
            q = ",".join("?" * len(part))
            old = con.execute(f"SELECT conf_num, batch_key FROM built WHERE conf_num IN ({q})", part).fetchall()
            con.executemany("INSERT INTO pending_drops VALUES (?,?,?,?)", [(n, k, run_id, now) for n, k in old])
            con.execute(f"DELETE FROM built WHERE conf_num IN ({q})", part)
    return apply_pending_drops(cfg, con)


def apply_pending_drops(cfg, con):
    """Remove superseded rows recorded in pending_drops from their old batch files."""
    by_key = defaultdict(set)
    for n, k in con.execute("SELECT conf_num, batch_key FROM pending_drops").fetchall():
        by_key[k].add(n)
    removed = 0
    for k, ns in by_key.items():
        removed += _drop_from_batch(cfg, k, ns)
        with con:
            con.executemany("DELETE FROM pending_drops WHERE batch_key=? AND conf_num=?", [(k, n) for n in ns])
    return removed


# ============================================================================ reference data

def _duck(cfg):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{cfg.duckdb_memory}'; SET threads={cfg.duckdb_threads}; "
                "SET enable_progress_bar=false")
    return con


def load_universe(cfg):
    import pandas as pd
    d = _duck(cfg)
    u = d.execute(f"""SELECT CONFER_NUM, CONF_ID, DAE_NUM, CLASS_NAME, CLASS_NAME_unified, COMM_NAME,
        is_subcommittee_name, CONF_DATE, sess_num, sitting, TITLE, agenda, source AS api_source,
        v_CMIT_NM, v_SB_CMIT_NM, in_vconf_cfrm, in_vconf_ch, in_vconf_ph, in_vconf_jm
        FROM read_parquet('{cfg.universe}')""").df()
    u["CONFER_NUM"] = u["CONFER_NUM"].astype("int64")
    return u


def load_crosswalk(cfg):
    d = _duck(cfg)
    return d.execute(f"""SELECT meeting_id, term, hearing_type, committee, date, v9_source,
        match_method, api_CONFER_NUM, api_CONF_ID FROM read_parquet('{cfg.crosswalk}')""").df()


def load_crawl(cfg):
    """{(conf_num, kind): (status, sha1)} from the crawler's state (read-only)."""
    if not Path(cfg.crawl_db).exists():
        return {}
    for attempt in range(5):
        try:
            c = sqlite3.connect(f"file:{cfg.crawl_db}?mode=ro", uri=True, timeout=60)
            rows = c.execute("SELECT conf_num, kind, status, sha1 FROM fetch").fetchall()
            c.close()
            return {(int(r[0]), r[1]): (r[2], r[3]) for r in rows}
        except sqlite3.OperationalError:
            time.sleep(2 + 2 * attempt)
    raise RuntimeError("crawl_state.sqlite locked")


# ============================================================================ planning

def unusable_map(con):
    """{(conf_num, source): (status, raw_sha1, builder_version)} of the latest parse attempt per
    meeting and source whose status is a property of the raw file (not an exception). The planner
    treats the raw file as unusable only while both its hash and the adapter version are the ones
    that failed, so a parser or adapter change retries it."""
    out = {}
    for n, src, st, sha, ver in con.execute(
            "SELECT conf_num, source, status, raw_sha1, builder_version FROM attempts ORDER BY rowid"):
        out[(n, src)] = (st, sha, ver)
    return {k: v for k, v in out.items() if v[0] not in ("exception",)}


def load_source_override(cfg):
    """{conf_num: (source, reason)} from cfg.source_override, or {} when the file does not exist."""
    import pandas as pd
    p = Path(cfg.source_override) if getattr(cfg, "source_override", None) else None
    if p is None or not p.exists():
        return {}
    d = pd.read_parquet(p, columns=["conf_num", "source", "reason"])
    bad = set(d.source) - set(PRODUCTION_SOURCES)
    if bad:
        raise ValueError(f"source_override has non-production sources: {sorted(bad)}")
    if d.conf_num.duplicated().any():
        raise ValueError("source_override has duplicate conf_num rows")
    return {int(r.conf_num): (r.source, r.reason) for r in d.itertuples(index=False)}


def plan(cfg, universe, crosswalk, crawl, built, sources, rebuild=False, only=None, unusable=None,
         override=None):
    """Decide the source for every meeting. Returns (tasks, pending Counter, actions Counter).
    Precedence: xml (view ok), then hwp (hwp ok) (researcher decision 2026-09-26: HWP for every 18대
    meeting, XLSX no longer a production source; `crosswalk` is not used for the choice). A raw file
    whose parse with the current adapter version returned a non-ok status (e.g. a view page without
    minutes_body) is skipped while its hash and the version are unchanged; --rebuild retries it. If that
    file is the one the meeting is already built from, the previous build is kept (action
    keep_previous_build_*), never dropped. A meeting built from a source that is no longer a production
    source (XLSX) is rebuilt from its best source; with none available it gets a task with action
    'drop_non_production_source' (its rows are removed by run() and counted).
    `override` ({conf_num: (source, reason)}, from the XML-vs-HWP cross-check) puts the named source
    first for that meeting when its raw file is ok; otherwise the normal precedence applies and the
    miss is counted (override_source_unavailable)."""
    unusable = unusable or {}
    override = override or {}
    cur = {s: adapter_version(s) for s in ("xml", "xlsx", "hwp")}
    term_of = dict(zip(universe.CONFER_NUM, universe.DAE_NUM))
    ids = set(int(x) for x in universe.CONFER_NUM) | {k[0] for k in crawl}
    ids |= {n for n, b in built.items() if b.get("source") not in PRODUCTION_SOURCES}
    if only is not None:
        ids &= set(only)
    tasks, pending, actions = [], Counter(), Counter()
    if "xlsx" in sources:
        actions["xlsx_in_sources_not_production"] += 1
    for n in sorted(ids):
        in_u = n in term_of
        term = _int_or_none(term_of.get(n))
        v = crawl.get((n, "view"), (None, None))
        h = crawl.get((n, "hwp"), (None, None))
        b = built.get(n)

        def bad_now(src, raw):
            bad = unusable.get((n, src))
            return (not rebuild and raw[0] == "ok" and bad is not None and bad[1] == raw[1]
                    and len(bad) > 2 and bad[2] == cur[src])

        cands = []
        if v[0] == "ok":
            cands.append(("xml", v[1], bad_now("xml", v)))
        if h[0] == "ok":
            cands.append(("hwp", h[1], bad_now("hwp", h)))
        ov = override.get(n)
        if ov is not None:
            if any(c[0] == ov[0] for c in cands):
                cands.sort(key=lambda c: c[0] != ov[0])
                actions[f"override_{ov[0]}_{ov[1]}"] += 1
            else:
                actions["override_source_unavailable"] += 1
        best = sha = keep = None
        for src, sh, is_bad in cands:
            if not is_bad:
                best, sha = src, sh
                break
            if b is not None and b["source"] == src and b["raw_sha1"] == sh:
                keep = (src, unusable[(n, src)][0])
                break
        if keep:
            # the current adapter fails on the raw file this meeting is built from: keep the build
            actions[f"keep_previous_build_{keep[0]}_new_parse_{keep[1]}"] += 1
            continue
        if best is None and b is not None and b.get("source") not in PRODUCTION_SOURCES:
            # built from XLSX and no production source to rebuild it from: removed, counted
            actions[f"drop_non_production_source_{b.get('source')}"] += 1
            tasks.append({"conf_num": n, "source": b.get("source"), "term": b.get("term"), "sha1": None,
                          "path": None, "action": "drop_non_production_source"})
            continue
        if best is None:
            vbad, hbad = bad_now("xml", v), bad_now("hwp", h)
            vs = f"xml_unusable_{unusable[(n, 'xml')][0]}" if vbad else f"view_{v[0]}"
            hs = f"hwp_unusable_{unusable[(n, 'hwp')][0]}" if hbad else f"hwp_{h[0]}"
            if v[0] is None and h[0] is None:
                reason = "not_crawled_yet"
            elif h[0] is None:
                reason = f"{vs}_hwp_not_crawled"
            else:
                reason = f"{vs}_{hs}"
            pending[reason if in_u else "outside_universe_" + reason] += 1
            continue
        if best not in sources:
            pending[f"source_{best}_not_selected"] += 1
            continue
        if b is None:
            act = "build"
        elif rebuild:
            act = "rebuild"
        elif b["source"] != best:
            act = "rebuild_source_changed" if b["source"] in PRODUCTION_SOURCES else f"rebuild_from_{b['source']}_to_{best}"
        elif b["raw_sha1"] != sha:
            act = "rebuild_raw_changed"
        elif b.get("builder_version", cur[best]) != cur[best]:
            act = "rebuild_builder_version"
        else:
            actions["skip_built"] += 1
            continue
        actions[act] += 1
        path = view_path(cfg, n) if best == "xml" else hwp_path(cfg, n)
        if not path.exists():
            pending[f"{best}_ok_in_crawl_state_but_file_missing"] += 1
            actions[act] -= 1
            continue
        tasks.append({"conf_num": n, "source": best, "term": term, "sha1": sha, "path": path, "action": act})
    return tasks, pending, actions


# ============================================================================ XLSX pass

V9_COLS = "meeting_id, term, hearing_type, committee, date, session, sub_session, agenda, speaker, member_id, speech_order, speech_text"


def iter_v9_meetings(cfg, meeting_ids, chunk_rows=50_000):
    """Stream v9 rows of the given meeting ids, grouped by meeting (duckdb, no full load)."""
    d = _duck(cfg)
    ids = sorted(set(meeting_ids))
    d.execute("CREATE TEMP TABLE want(meeting_id VARCHAR)")
    d.executemany("INSERT INTO want VALUES (?)", [(m,) for m in ids])
    rel = d.execute(f"""SELECT {V9_COLS} FROM read_parquet('{cfg.v9_speeches}') s
        WHERE s.meeting_id IN (SELECT meeting_id FROM want)
        ORDER BY s.meeting_id, TRY_CAST(s.speech_order AS INTEGER)""")
    cols = [c[0] for c in rel.description]
    cur_id, buf = None, []
    while True:
        chunk = rel.fetchmany(chunk_rows)
        if not chunk:
            break
        for row in chunk:
            r = dict(zip(cols, row))
            if r["meeting_id"] != cur_id and buf:
                yield cur_id, buf
                buf = []
            cur_id = r["meeting_id"]
            buf.append(r)
    if buf:
        yield cur_id, buf


# ============================================================================ meetings table

NEW_STANDING_KEYS = {  # committees absent from the v9 map (2025 renames), mapped by lineage
    "기후에너지환경노동위원회": "environment_labor",
    "성평등가족위원회": "gender_family",
    "재정경제기획위원회": "finance",
}
V10_SPECIAL_KEYS = {"특별위원회": "special_committee", "전원위원회": "committee_of_whole"}
CONF_SPECIAL_RE = re.compile(r"인사청문특별위원회")


def hearing_type_for(class_name, committee_name):
    if class_name == "특별위원회":
        return "인사청문특별위원회" if committee_name and CONF_SPECIAL_RE.search(committee_name) else "특별위원회"
    return class_name


def committee_key_for(hearing_type, committee_raw):
    """(key, rule). Legacy v9 map first; v10 additions are reported by the caller."""
    if hearing_type in lr.SPECIAL_HEARING_TYPE_TO_KEY:
        return lr.SPECIAL_HEARING_TYPE_TO_KEY[hearing_type], "legacy_hearing_type"
    if hearing_type in V10_SPECIAL_KEYS:
        return V10_SPECIAL_KEYS[hearing_type], "v10_hearing_type"
    k = lr.harmonize_committee(committee_raw, hearing_type)
    if k is not None:
        return k, "legacy_map"
    if committee_raw:
        base = lr.SUBCOMMITTEE_SUFFIX_RE.sub("", str(committee_raw).strip())
        for nm in (str(committee_raw).strip(), base):
            if nm in NEW_STANDING_KEYS:
                return NEW_STANDING_KEYS[nm], "v10_new_standing"
    return None, "unmapped"


AUDIT_TEAM_RE = re.compile(r"^(?P<c>.+?)\s*(?:[-(（]\s*(?P<t>[^()（）]*반)\s*[)）]?)$")
# second COMM_NAME token / v_SB_CMIT_NM of 국정조사 rows is a document label, not a subcommittee
# ('한빛은행국정조사조사록', '국정조사록')
DOC_LABEL_RE = re.compile(r"조사록$")
# 안건조정위원회 (National Assembly Act art. 57-2) is a body formed inside a committee; the API
# does not flag it as a subcommittee. Kept in `subcommittee`, flagged separately.
AGENDA_ADJ_RE = re.compile(r"안건조정위원회")
SUBCOMMITTEE_NAME_RE = re.compile(r"소위|小委|안건조정위원회")


def _s(x):
    """str or None for possibly-missing scalars."""
    return x if isinstance(x, str) and x.strip() else None


def _audit_team(v_sb):
    if v_sb is None or (isinstance(v_sb, float) and v_sb != v_sb) or not str(v_sb).strip():
        return None
    s = str(v_sb).strip()
    return f"제{s}반" if s.isdigit() else s


# session_type: canonical Hangul value from the printed form ('臨時會․閉會中', '임시회·폐회중' ->
# '임시회(폐회중)'); the printed form is kept in session_type_raw.
SESSION_TYPE_HANJA = (("臨時會", "임시회"), ("定期會", "정기회"), ("特別會", "특별회"), ("閉會中", "폐회중"))
SESSION_TYPE_RE = re.compile(r"^(?P<base>임시회|정기회|특별회)(?:[·.\-]?\(?(?P<closed>폐회중)\)?)?$")


def canonical_session_type(raw):
    """Printed session type -> '임시회', '정기회', '임시회(폐회중)', '정기회(폐회중)' (or '특별회'); None when
    not recognised (the caller counts it)."""
    if raw is None:
        return None
    t = unicodedata.normalize("NFKC", str(raw).translate(_SEP_TR)).translate(_SEP_TR)
    t = nows(t)
    for a, b in SESSION_TYPE_HANJA:
        t = t.replace(a, b)
    m = SESSION_TYPE_RE.match(t)
    if not m:
        return None
    return m.group("base") + ("(폐회중)" if m.group("closed") else "")


def construct_title(term, hearing_type, session_no, sitting, committee_raw, subcommittee, audit_year,
                    audit_team, date):
    """Title in the viewer / Open API shapes for a meeting that has neither ('제18대국회 2011년도 국정감사
    지식경제위원회', '제18대 제278회 제1차 정무위원회 법안심사소위원회 (2008년 09월 11일)')."""
    if not term or not committee_raw:
        return None
    if hearing_type == "국정감사":
        parts = [f"제{term}대국회", f"{audit_year}년도 국정감사" if audit_year else "국정감사", committee_raw, audit_team]
        return " ".join(p for p in parts if p)
    parts = [f"제{term}대", f"제{session_no}회" if session_no else None, sitting, committee_raw, subcommittee]
    t = " ".join(p for p in parts if p)
    if isinstance(date, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        t += f" ({date[:4]}년 {date[5:7]}월 {date[8:]}일)"
    return t


def v9_id_namespace(v9_id, conf_id, conf_num):
    """Which id space a meeting's v9_meeting_id lives in: 'conf_id' (the Open API CONF_ID, with or
    without its leading zeros: v9 XLSX 16-20대 ids drop them, 21-22대 ids keep them; v7/v8 ids),
    'confer_num' (the viewer CONFER_NUM: v6 HTML meetings), 'none' (neither: the v9 id was matched on
    content, e.g. 41160 <- 48520); None without a v9 id. Never cast either id to int to join: many v9
    ids equal the conf_num of a different meeting."""
    if v9_id is None:
        return None
    v = str(v9_id)
    if conf_id is not None and (v == conf_id or (v.isdigit() and conf_id.isdigit()
                                                 and v.lstrip("0") == conf_id.lstrip("0"))):
        return "conf_id"
    if v == str(conf_num):
        return "confer_num"
    return "none"


def build_meetings(cfg, universe, crosswalk, con):
    """One row per meeting of the universe (plus built meetings outside it)."""
    import pandas as pd
    hdr_dir = table_dir(cfg, "headers")
    files = sorted(hdr_dir.glob("*/*/*.parquet")) if hdr_dir.exists() else []
    if files:
        parts = []
        for f in files:
            t = pq.read_table(f)
            parts.append(t.append_column("batch_key", pa.array([f"{f.parent.parent.name}/{f.parent.name}/{f.stem}"] * t.num_rows, S)))
        # header files written before an adapter version added columns lack them (null-filled)
        H = pa.concat_tables(parts, promote_options="default").to_pandas()
    else:
        H = pd.DataFrame(columns=[f.name for f in SCHEMAS["headers"]] + ["batch_key"])
    built = pd.read_sql_query("SELECT conf_num, source AS built_source, batch_key AS built_batch FROM built", con)
    H = H.merge(built, on="conf_num", how="inner")
    # only the header in the batch the manifest points at (a superseded one is still on disk
    # between the manifest commit and apply_pending_drops)
    H = H[(H.source == H.built_source) & (H.batch_key == H.built_batch)]
    H = H.drop_duplicates("conf_num", keep="last").set_index("conf_num")
    cw = crosswalk[crosswalk.api_CONFER_NUM.notna()].copy()
    cw["api_CONFER_NUM"] = cw.api_CONFER_NUM.astype("int64")
    pref = {"xlsx": 0, "v8_flagged": 1, "v8_unflagged": 1, "v7_pdf": 2, "v6_html": 3}
    cw["pref"] = cw.v9_source.map(pref).fillna(9)
    cw = cw.sort_values(["api_CONFER_NUM", "pref", "meeting_id"])
    v9g = cw.groupby("api_CONFER_NUM")
    v9first = v9g.first()
    v9all = v9g.meeting_id.apply(list)
    U = universe.set_index("CONFER_NUM")
    ovr = load_source_override(cfg)
    ids = sorted(set(U.index) | set(H.index))
    # v9 meetings whose crosswalk CONFER_NUM is neither in the universe nor built get no meetings
    # row; they are counted and listed in the run summary instead of being dropped silently
    cw_out = cw[~cw.api_CONFER_NUM.isin(set(ids))]
    outside = [{"meeting_id": m, "api_CONFER_NUM": int(c), "v9_source": vs, "term": _int_or_none(t)}
               for m, c, vs, t in zip(cw_out.meeting_id, cw_out.api_CONFER_NUM, cw_out.v9_source, cw_out.term)]
    rows, newkeys, mstats = [], Counter(), Counter()
    for n in ids:
        u = U.loc[n] if n in U.index else None
        h = H.loc[n] if n in H.index else None
        in_u = u is not None
        cls = u["CLASS_NAME_unified"] if in_u else None
        comm = u["COMM_NAME"] if in_u and isinstance(u["COMM_NAME"], str) else None
        if comm is None and in_u and isinstance(u["v_CMIT_NM"], str):
            comm = u["v_CMIT_NM"]
        api_parent, api_sub = (comm.split(" ", 1) + [None])[:2] if comm else (None, None)
        if cls == "국회본회의" and api_parent is None:
            api_parent = "국회본회의"
        h_comm = _s(h["h_committee"]) if h is not None else None
        h_sub = _s(h["h_subcommittee"]) if h is not None else None
        v_sb = _s(u["v_SB_CMIT_NM"]) if in_u else None
        doc_label = None
        if api_sub and DOC_LABEL_RE.search(api_sub):
            doc_label, api_sub = api_sub, None
        if v_sb and DOC_LABEL_RE.search(v_sb):
            doc_label, v_sb = doc_label or v_sb, None
        committee_raw = api_parent or h_comm
        audit_team = None
        if cls == "국정감사":
            audit_team = _audit_team(v_sb)
            subcommittee = None
        elif in_u:
            # API committee name is authoritative; the printed header value is kept in
            # subcommittee_printed (HWP covers can yield e.g. '임 시 회 의 록')
            subcommittee = api_sub or v_sb
        else:
            subcommittee = h_sub if h_sub and SUBCOMMITTEE_NAME_RE.search(h_sub) else None
        if h_comm and cls == "국정감사" and audit_team is None:
            am = AUDIT_TEAM_RE.match(h_comm)
            if am and am.group("t"):
                audit_team = am.group("t")
        is_agenda_adj = bool(subcommittee and AGENDA_ADJ_RE.search(subcommittee))
        api_sub_flag = bool(in_u and _b(u["is_subcommittee_name"]))
        is_sub = api_sub_flag or (bool(subcommittee) and not is_agenda_adj)
        if in_u and is_sub and not api_sub_flag:
            newkeys[("_is_subcommittee_from_non_api", cls, subcommittee, None)] += 1
        ht = hearing_type_for(cls, committee_raw) if cls else None
        key, rule = committee_key_for(ht, committee_raw) if ht else (None, "no_class")
        if rule.startswith("v10") or rule == "unmapped":
            newkeys[(ht, committee_raw, key, rule)] += 1
        # confirmation hearing flag: special committee, or the meeting's own agenda text
        # (parsed source: body anchors + header list; XLSX: v9 agenda column). The Open API
        # agenda string and in_vconf_cfrm list are kept as columns only (both carry
        # other meetings' items for some rows); the API agenda text is used only for a
        # meeting that is not built yet.
        api_cfrm = bool(in_u and _b(u["in_vconf_cfrm"]))
        agenda_hit = bool(h is not None and _b(h["agenda_confirmation_hit"]))
        api_agenda_hit = bool(in_u and isinstance(u["agenda"], str)
                              and any(is_confirmation_agenda(x) for x in u["agenda"].split("||")))
        rules = []
        if ht == "인사청문특별위원회":
            rules.append("special_committee")
        if agenda_hit:
            rules.append("source_agenda_text")
        if h is None and api_agenda_hit:
            rules.append("api_agenda_text_unbuilt")
        v9 = v9first.loc[n] if n in v9first.index else None
        date_api = u["CONF_DATE"] if in_u else None
        h_date = h["h_date"] if h is not None else None
        session_no = None
        sess_src = None
        if h is not None and _int_or_none(h["h_session"]) is not None:
            session_no, sess_src = _int_or_none(h["h_session"]), h["source"]
        elif in_u and _int_or_none(u["sess_num"]) is not None:
            session_no, sess_src = _int_or_none(u["sess_num"]), "api"
        sitting = (h["h_sitting"] if h is not None and isinstance(h["h_sitting"], str) else None) or \
                  (u["sitting"] if in_u and isinstance(u["sitting"], str) else None)
        audit_year = _int_or_none(h["h_audit_year"]) if h is not None else None
        if audit_year is None and ht == "국정감사":
            d0 = date_api or h_date
            audit_year = int(d0[:4]) if isinstance(d0, str) and d0[:4].isdigit() else None
        term = _int_or_none(u["DAE_NUM"]) if in_u else (_int_or_none(h["h_term"]) if h is not None else None)
        # session type: canonical Hangul value, printed form in session_type_raw
        st_raw = _s(h["h_session_type"]) if h is not None else None
        st_canon = canonical_session_type(st_raw)
        if st_raw is not None and st_canon is None:
            mstats["session_type_unrecognised"] += 1
            mstats["session_type_unrecognised_" + st_raw] += 1
        # title: viewer header > Open API TITLE > constructed from the meeting fields > printed cover title
        title, title_src = None, None
        for cand, src_name in (
                (_s(h["h_title"]) if h is not None and h["source"] == "xml" else None, "viewer_header"),
                (_s(u["TITLE"]) if in_u else None, "api"),
                (construct_title(term, ht, session_no, sitting, committee_raw, subcommittee, audit_year,
                                 audit_team, date_api or h_date), "constructed"),
                (_s(h["h_title"]) if h is not None else None, "printed_cover")):
            if cand:
                title, title_src = cand, src_name
                break
        mstats["title_source_" + str(title_src)] += 1
        aa = h["audited_agencies"] if h is not None else None
        aa = list(aa) if aa is not None and not (isinstance(aa, float)) else None
        aa = aa or None
        aa_how = _s(h["audited_agencies_how"]) if h is not None and "audited_agencies_how" in h.index else None
        if ht == "국정감사":
            mstats["audit_meetings"] += 1
            mstats["audit_meetings_built" if h is not None else "audit_meetings_unbuilt"] += 1
            if h is not None and aa is None:
                mstats["audit_meetings_built_without_agencies"] += 1
        v9_id = v9["meeting_id"] if v9 is not None else None
        v9_ns = v9_id_namespace(v9_id, u["CONF_ID"] if in_u and isinstance(u["CONF_ID"], str) else None, n)
        mstats["v9_id_namespace_" + str(v9_ns)] += 1
        rows.append({
            "conf_num": int(n), "conf_id": u["CONF_ID"] if in_u else None,
            "v9_meeting_id": v9_id,
            "term": term, "class_name": cls, "hearing_type": ht, "is_subcommittee": is_sub,
            "committee_raw": committee_raw, "subcommittee": subcommittee, "committee_key": key,
            "session_no": session_no, "session_type": st_canon,
            "sitting": sitting, "date": date_api or h_date,
            "date_end": (h["date_end"] if h is not None else None) or None,
            "audit_year": audit_year, "audited_agencies": aa,
            "source": h["source"] if h is not None else None,
            "n_turns": _int_or_none(h["n_turns"]) if h is not None else None, "title": title,
            # additions
            "is_confirmation_hearing": bool(rules), "confirmation_rule": ";".join(rules) or None,
            "api_in_vconf_cfrm": api_cfrm if in_u else None,
            "api_agenda_confirmation_hit": api_agenda_hit if in_u else None,
            "is_agenda_adjustment": is_agenda_adj, "api_is_subcommittee": api_sub_flag if in_u else None,
            "doc_label": doc_label,
            "audit_team": audit_team, "committee_key_rule": rule, "committee_printed": h_comm,
            "subcommittee_printed": h_sub, "api_comm_name": comm, "date_printed": h_date,
            "date_mismatch": bool(date_api and h_date and date_api != h_date),
            "session_no_source": sess_src,
            "is_provisional_minutes": (_b(h["is_provisional"]) if h is not None and h["source"] == "xml" else None),
            "in_universe": in_u, "is_built": h is not None,
            "parse_status": h["parse_status"] if h is not None else None,
            "v9_meeting_ids": list(v9all.loc[n]) if n in v9all.index else None,
            "v9_source": v9["v9_source"] if v9 is not None else None,
            "v9_hearing_type": v9["hearing_type"] if v9 is not None else None,
            "api_class_raw": u["CLASS_NAME"] if in_u else None,
            "api_in_vconf_ch": _b(u["in_vconf_ch"]) if in_u else None,
            "api_in_vconf_ph": _b(u["in_vconf_ph"]) if in_u else None,
            "api_in_vconf_jm": _b(u["in_vconf_jm"]) if in_u else None,
            "n_agenda": _int_or_none(h["n_agenda"]) if h is not None else None,
            "n_events": _int_or_none(h["n_events"]) if h is not None else None,
            "n_footer_rows": _int_or_none(h["n_footer_rows"]) if h is not None else None,
            "n_rollcall_votes": _int_or_none(h["n_rollcall_votes"]) if h is not None else None,
            "raw_sha1": h["raw_sha1"] if h is not None else None,
            # 1.8 (R1)
            "session_type_raw": st_raw, "title_source": title_src, "audited_agencies_how": aa_how,
            "v9_id_namespace": v9_ns,
            # why this meeting's production source was chosen (XML-vs-HWP cross-check overrides)
            "source_reason": (None if h is None else
                              f"override:{ovr[n][1]}" if n in ovr and ovr[n][0] == h["source"] else
                              "xml_default" if h["source"] == "xml" else "hwp_no_usable_xml"),
        })
    schema = pa.schema([
        ("conf_num", I64), ("conf_id", S), ("v9_meeting_id", S), ("term", I16), ("class_name", S),
        ("hearing_type", S), ("is_subcommittee", B), ("committee_raw", S), ("subcommittee", S),
        ("committee_key", S), ("session_no", I16), ("session_type", S), ("sitting", S), ("date", S),
        ("date_end", S), ("audit_year", I16), ("audited_agencies", LS), ("source", S), ("n_turns", I32),
        ("title", S), ("is_confirmation_hearing", B), ("confirmation_rule", S),
        ("api_in_vconf_cfrm", B), ("api_agenda_confirmation_hit", B), ("is_agenda_adjustment", B),
        ("api_is_subcommittee", B), ("doc_label", S), ("audit_team", S),
        ("committee_key_rule", S), ("committee_printed", S), ("subcommittee_printed", S),
        ("api_comm_name", S), ("date_printed", S), ("date_mismatch", B), ("session_no_source", S),
        ("is_provisional_minutes", B), ("in_universe", B), ("is_built", B), ("parse_status", S),
        ("v9_meeting_ids", LS), ("v9_source", S), ("v9_hearing_type", S), ("api_class_raw", S),
        ("api_in_vconf_ch", B), ("api_in_vconf_ph", B), ("api_in_vconf_jm", B), ("n_agenda", I32),
        ("n_events", I32), ("n_footer_rows", I32), ("n_rollcall_votes", I32), ("raw_sha1", S),
        ("session_type_raw", S), ("title_source", S), ("audited_agencies_how", S), ("v9_id_namespace", S),
        ("source_reason", S)])
    cols = {f.name: [r[f.name] for r in rows] for f in schema}
    for c in [f.name for f in schema if f.type == S]:
        vals = [None if (v is None or (isinstance(v, float) and v != v)) else str(v) for v in cols[c]]
        # NULL, never an empty string (counted)
        n_empty = sum(1 for v in vals if v is not None and not v.strip())
        if n_empty:
            mstats[f"null_policy_meetings.{c}"] += n_empty
        cols[c] = [None if (v is not None and not v.strip()) else v for v in vals]
    tbl = pa.Table.from_pydict(cols, schema=schema)
    p = cfg.out / "meetings" / "meetings.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.parent / f".{p.name}.tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, p)
    return tbl, newkeys, outside, mstats


# ============================================================================ readers

def read_table(table, out=None, where=None, columns="*"):
    """Read a built table with duckdb into pandas with CONTRACT nullable dtypes.
    e.g. read_table('turns', where="conf_num IN (56410)")."""
    import duckdb
    cfg = Config(out=Path(out)) if out else Config()
    d = table_dir(cfg, table)
    pat = str(d / "meetings.parquet") if table == "meetings" else str(d / "*" / "*" / "*.parquet")
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET threads=4; SET enable_progress_bar=false")
    q = f"SELECT {columns} FROM read_parquet('{pat}')" + (f" WHERE {where}" if where else "")
    rel = con.execute(q)
    tbl = rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()
    return tbl.to_pandas(types_mapper=_types_mapper)


def _types_mapper(t):
    import pandas as pd
    return {pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
            pa.string(): pd.StringDtype(), pa.large_string(): pd.StringDtype(),
            pa.bool_(): pd.BooleanDtype()}.get(t)


# ============================================================================ run

def _setup_logging(cfg):
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.INFO)
    if not LOG.handlers:
        fh = logging.FileHandler(cfg.state_dir / "build_turns.log")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        LOG.addHandler(fh)
        LOG.addHandler(sh)


def run(cfg, sources=("xml", "hwp"), workers=10, batch_size=100, limit=None, only=None,
        rebuild=False, build_meetings_table=True, max_batch_chars=150_000_000):
    """One build run under the run lock (a concurrent run raises RunLocked)."""
    with run_lock(cfg):
        return _run_locked(cfg, sources, workers, batch_size, limit, only, rebuild, build_meetings_table,
                           max_batch_chars)


def _run_locked(cfg, sources, workers, batch_size, limit, only, rebuild, build_meetings_table, max_batch_chars):
    run_id = new_run_id()
    t_start = time.time()
    con = open_state(cfg)
    moved = reconcile_orphans(cfg, con)
    drops_at_start = apply_pending_drops(cfg, con)   # left by a run interrupted after a commit
    universe = load_universe(cfg)
    crosswalk = load_crosswalk(cfg)
    crawl = load_crawl(cfg)
    built = built_map(con)
    override = load_source_override(cfg)
    tasks, pending, actions = plan(cfg, universe, crosswalk, crawl, built, set(sources), rebuild=rebuild, override=override,
                                   only=only, unusable=unusable_map(con))
    drops = [t for t in tasks if t["action"] == "drop_non_production_source"]
    tasks = [t for t in tasks if t["action"] != "drop_non_production_source"]
    if limit is not None:
        tasks = tasks[:limit]
    with con:
        con.execute("INSERT INTO runs(run_id, started, args) VALUES (?,?,?)",
                    (run_id, dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                     json.dumps({"sources": list(sources), "workers": workers, "batch_size": batch_size,
                                 "limit": limit, "rebuild": rebuild, "only_n": len(only) if only else None})))
    LOG.info("run %s: %d tasks %s pending=%s orphans_moved=%d", run_id, len(tasks), dict(Counter(t["source"] for t in tasks)),
             dict(pending), moved)
    # A rebuilt meeting keeps its previous rows until its new result is committed (write_batch);
    # a failed re-parse leaves the previous build in place and is counted below.
    dropped_rows = drop_meetings(cfg, con, run_id, [t["conf_num"] for t in drops])
    summary = {"run_id": run_id, "builder_version": BUILDER_VERSION, "orphans_moved": moved,
               "dropped_non_production_meetings": len(drops), "dropped_non_production_rows": dropped_rows,
               "dropped_non_production_examples": [t["conf_num"] for t in drops[:200]],
               "pending_drop_rows_applied_at_start": drops_at_start,
               "adapter_versions": {s: adapter_version(s) for s in ("xml", "xlsx", "hwp")},
               "plan_actions": dict(actions), "pending": dict(pending),
               "tasks_by_source": dict(Counter(t["source"] for t in tasks)),
               "results": Counter(), "rows_written": Counter(), "counters": Counter(),
               "coverage": Counter(), "errors": [], "raw_bytes": 0, "elapsed_by_source": Counter(),
               "meetings_by_source": Counter(), "rebuild_failed_kept_previous": Counter(),
               "rebuild_failed_kept_previous_examples": []}
    buffers = defaultdict(list)
    buf_chars = Counter()
    key_seq = [0]

    def flush(src, term, force=False):
        k = (src, term)
        if not buffers[k] or (not force and len(buffers[k]) < batch_size and buf_chars[k] < max_batch_chars):
            return
        key_seq[0] += 1
        key, written = write_batch(cfg, con, run_id, src, term, key_seq[0], buffers[k])
        for tb, n in written.items():
            summary["rows_written"][tb] += n
        buffers[k] = []
        buf_chars[k] = 0

    def record_attempt(conf_num, src, status, error, sha1, version):
        with con:
            con.execute("INSERT INTO attempts(conf_num, source, status, error, run_id, at, raw_sha1, builder_version) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        (conf_num, src, status, (error or "")[:4000], run_id,
                         dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), sha1, version))
        if conf_num in built:
            # the meeting stays as previously built (its rows were never removed)
            key = f"{src}:{status}"
            summary["rebuild_failed_kept_previous"][key] += 1
            if len(summary["rebuild_failed_kept_previous_examples"]) < 200:
                summary["rebuild_failed_kept_previous_examples"].append(
                    {"conf_num": conf_num, "new_source": src, "status": status,
                     "kept_source": built[conf_num]["source"], "kept_version": built[conf_num]["builder_version"]})
            LOG.warning("rebuild of %s from %s failed (%s): previous %s build kept", conf_num, src, status,
                        built[conf_num]["source"])

    def accept(r, src):
        summary["meetings_by_source"][src] += 1
        summary["results"][f"{src}:{r['status']}"] += 1
        summary["counters"].update(r.get("counters") or {})
        if r.get("elapsed"):
            summary["elapsed_by_source"][src] += r["elapsed"]
        summary["raw_bytes"] += r.get("raw_bytes_read") or 0
        ok_status = r["status"] in ("ok", "ok_no_speeches", "ok_no_turns")
        if not ok_status:
            record_attempt(r["conf_num"], src, r["status"], r.get("error"), r.get("sha1"), r.get("adapter_version"))
            if r.get("error"):
                summary["errors"].append({"conf_num": r["conf_num"], "source": src, "error": r["error"][:300]})
            return
        cov = r.get("coverage")
        if cov:
            for kk in ("ok_spk_sub_vs_text_raw", "ok_sentences", "ok_txt", "ok_body", "ok_footer",
                       "turn_seq_contiguous", "ok_all"):
                v = cov.get(kk)
                if v is not None:
                    summary["coverage"][f"{src}:{kk}:{bool(v)}"] += 1
            if cov.get("ok_all") is False:
                LOG.warning("coverage check failed for %s %s: %s", src, r["conf_num"],
                            {k: cov.get(k) for k in cov if k.startswith(("ok_", "dom_", "turn_", "parsed", "accounted", "footer"))})
        k = (src, r.get("term"))
        buffers[k].append(r)
        buf_chars[k] += sum(len(t.get("text_raw") or "") for t in r["tables"].get("turns", []))
        flush(src, r.get("term"))

    # ---- xml / hwp through the worker pool
    ptasks = [(t["source"], t["conf_num"], str(t["path"]), t["term"], t["sha1"]) for t in tasks
              if t["source"] in ("xml", "hwp")]
    t_pool = time.time()
    done = 0
    if ptasks:
        if workers and workers > 1:
            ctx = mp.get_context("spawn")
            with ctx.Pool(workers, maxtasksperchild=500) as pool:
                for r in pool.imap_unordered(_work, ptasks, chunksize=2):
                    accept(r, r["source"])
                    done += 1
                    if done % 500 == 0:
                        el = time.time() - t_pool
                        LOG.info("progress %d/%d meetings, %.2f meetings/s", done, len(ptasks), done / el)
        else:
            for tk in ptasks:
                r = _work(tk)
                accept(r, r["source"])
                done += 1
    pool_elapsed = time.time() - t_pool
    for (src, term) in list(buffers):
        flush(src, term, force=True)
    summary["timing"] = {"pool_seconds": round(pool_elapsed, 1), "pool_meetings": len(ptasks),
                         "pool_meetings_per_s": round(len(ptasks) / pool_elapsed, 2) if pool_elapsed > 0 and ptasks else None,
                         "workers": workers}
    if build_meetings_table:
        tbl, newkeys, outside, mstats = build_meetings(cfg, universe, crosswalk, con)
        summary["meetings_rows"] = tbl.num_rows
        summary["meetings_stats"] = dict(mstats)
        summary["new_committee_keys"] = [{"hearing_type": a, "committee_raw": b, "key": c, "rule": d, "n": n}
                                         for (a, b, c, d), n in sorted(newkeys.items(), key=lambda x: -x[1])]
        summary["crosswalk_rows_outside_universe_unbuilt"] = outside
    summary["total_seconds"] = round(time.time() - t_start, 1)
    for k in ("results", "rows_written", "counters", "coverage", "elapsed_by_source", "meetings_by_source",
              "rebuild_failed_kept_previous"):
        summary[k] = dict(summary[k])
    with con:
        con.execute("UPDATE runs SET finished=?, summary=? WHERE run_id=?",
                    (dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                     json.dumps(summary, ensure_ascii=False, default=str), run_id))
    (cfg.state_dir / f"run_{run_id}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str))
    LOG.info("run %s done in %.1fs: results=%s rows=%s", run_id, summary["total_seconds"], summary["results"],
             summary["rows_written"])
    con.close()
    return summary


def xlsx_compare(cfg, force=False, terms=None):
    """All v9 XLSX meetings (any term) -> xlsx_compare/t{term}/part.parquet. Comparison resource
    only; never merged into turns/. Skips terms already written unless force=True."""
    crosswalk = load_crosswalk(cfg)
    cw = crosswalk[(crosswalk.v9_source == "xlsx") & crosswalk.api_CONFER_NUM.notna()]
    out = {}
    for term, g in cw.groupby("term"):
        if terms and int(term) not in terms:
            continue
        p = cfg.out / "xlsx_compare" / f"t{int(term)}" / "part.parquet"
        if p.exists() and not force:
            out[int(term)] = "exists"
            continue
        m2c = {m: int(c) for m, c in zip(g.meeting_id, g.api_CONFER_NUM)}
        rows = []
        n_mt = 0
        writer = None
        tmp = p.parent / f".{p.name}.tmp"
        p.parent.mkdir(parents=True, exist_ok=True)
        acc = Counter()
        for mid, mrows in iter_v9_meetings(cfg, list(m2c)):
            r = xlsx_meeting_tables(m2c[mid], int(term), mrows, v9_meeting_id=mid)
            rows.extend(r["tables"]["turns"])
            n_mt += 1
            acc["meetings"] += 1
            acc["v9_rows"] += len(mrows)
            acc["turns"] += len(r["tables"]["turns"])
            acc["ok_all"] += int(bool(r["coverage"]["ok_all"]))
            acc["v9_text_chars_nows"] += r["coverage"]["dom_txt_chars"]
            acc["text_raw_chars_nows"] += r["coverage"]["turn_text_raw_chars"]
            acc.update({k: v for k, v in r["counters"].items()})
            if len(rows) >= 200_000:
                tb = pa.Table.from_pydict({f.name: [x.get(f.name) for x in rows] for f in SCHEMAS["turns"]},
                                          schema=SCHEMAS["turns"])
                writer = writer or pq.ParquetWriter(tmp, SCHEMAS["turns"], compression="zstd")
                writer.write_table(tb)
                rows = []
        if rows:
            tb = pa.Table.from_pydict({f.name: [x.get(f.name) for x in rows] for f in SCHEMAS["turns"]},
                                      schema=SCHEMAS["turns"])
            writer = writer or pq.ParquetWriter(tmp, SCHEMAS["turns"], compression="zstd")
            writer.write_table(tb)
        if writer:
            writer.close()
            os.replace(tmp, p)
        acc["v9_meetings_expected"] = len(m2c)
        (p.parent / "summary.json").write_text(json.dumps(dict(acc), ensure_ascii=False, indent=1))
        out[int(term)] = n_mt
    return out


def status(cfg):
    con = open_state(cfg)
    print("built by source/status:", con.execute(
        "SELECT source, status, count(*), sum(n_turns) FROM built GROUP BY 1,2 ORDER BY 1,2").fetchall())
    print("failed attempts (latest run per meeting):", con.execute(
        "SELECT source, status, count(DISTINCT conf_num) FROM attempts GROUP BY 1,2").fetchall())
    print("pending drops:", con.execute("SELECT count(*) FROM pending_drops").fetchone()[0])
    print("runs:", con.execute("SELECT run_id, started, finished FROM runs ORDER BY run_id DESC LIMIT 5").fetchall())


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sources", default="xml,hwp")
    ap.add_argument("--incremental", action="store_true",
                    help="build only meetings not yet built (the default behaviour; kept for clarity)")
    ap.add_argument("--rebuild", action="store_true", help="rebuild the selected meetings even if built")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--conf-nums", help="comma-separated CONFER_NUMs to restrict to")
    ap.add_argument("--out", help="output root (default v10/interim/pipeline)")
    ap.add_argument("--no-meetings", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--xlsx-compare", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--terms", help="with --xlsx-compare: comma-separated terms (default all)")
    a = ap.parse_args(argv)
    cfg = Config(out=Path(a.out)) if a.out else Config()
    _setup_logging(cfg)
    if a.status:
        status(cfg)
        return
    if a.xlsx_compare:
        terms = {int(x) for x in a.terms.split(",")} if a.terms else None
        LOG.info("xlsx_compare: %s", xlsx_compare(cfg, force=a.force, terms=terms))
        return
    only = [int(x) for x in a.conf_nums.split(",")] if a.conf_nums else None
    run(cfg, sources=tuple(s.strip() for s in a.sources.split(",") if s.strip()), workers=a.workers,
        batch_size=a.batch_size, limit=a.limit, only=only, rebuild=a.rebuild,
        build_meetings_table=not a.no_meetings)


if __name__ == "__main__":
    main()
