"""crosswalk.py - v9 -> v10 crosswalk, meeting level and turn level.

    python3 crosswalk.py v9-fp                       # cache v9 sentence fingerprints (once)
    python3 crosswalk.py build [--turns GLOB ...] [--v10-complete]
    python3 crosswalk.py eval  [--turns GLOB ...]    # content evidence vs the v9 audit verdicts

Outputs (interim/pipeline/crosswalk/):
  crosswalk_meetings.parquet   one row per v9 meeting (16,830) plus one row per universe meeting
                               whose transcript no v9 meeting carries (relation 'v10_only')
  crosswalk_turns.parquet      for v9 XLSX meetings whose v10 meeting is built: one row per link
                               v9 speech <-> v10 turn (plus unmatched rows on either side)
  crosswalk_stats.json         counts of every relation / basis / fallback

Meeting level. For a v9 meeting:
  conf_num          the v10 meeting its v9 LABEL refers to (v9_to_api_crosswalk: CONF_ID without the
                    leading zero for XLSX/v7/v8 ids, CONFER_NUM for v6 HTML ids); null when the label
                    matches no meeting (17 v6 HTML meetings).
  content_conf_num  the v10 meeting whose transcript the v9 rows actually carry.
  relation          same             transcript = the labelled meeting
                    v9_wrong_content transcript belongs to another meeting (content_conf_num), or to an
                                     unidentified one (content_conf_num null)
                    duplicate        a second v9 copy of the labelled meeting's transcript that another v9
                                     meeting already carries as 'same' (duplicate_of_v9_meeting_id)
                    v9_only          transcript not found in any built v10 meeting (only with --v10-complete)
                    v10_only         (rows without a v9 meeting) universe or meetings row no v9 row carries
  relation_basis    what decided it: 'content:<...>' (sentence-fingerprint containment in v10 turns),
                    'audit_R_D4:<verdict>' (full-population v8 classifier), 'v6_html_label_mismatch'
                    (D5: v6 ids are viewer CONFER_NUMs, label date/type wrong), 'id_label' (construction).
  content_verified  true only when v10 text was compared.
  content_overlap   from the FORWARD share (the part of the v9 transcript found in the content meeting):
                    'full' (>= CONTAIN_MIN), 'v10_in_v9' (the content meeting's text is contained in the v9
                    transcript, reverse >= CONTAIN_MIN, but most of the v9 transcript is not in it),
                    'partial' (>= PARTIAL_MIN), 'none'; null when the content meeting has no v10 text.
  is_second_copy /  every v9 meeting whose content_conf_num another v9 meeting also carries: one primary
  duplicate_of_v9_  per content meeting (a 'same' row first, then SOURCE_PREF, then meeting_id), every other
  meeting_id        row has is_second_copy=true and duplicate_of_v9_meeting_id = the primary. A second copy
                    keeps relation 'v9_wrong_content' when its label is wrong (the label error is the fact a
                    join on labels needs); 'same' second copies become 'duplicate'.
  in_universe       the target meeting (content_conf_num, else conf_num) is in the meeting universe passed
                    (config paths.universe: meeting_universe_v10 = API meetings + id-gap meetings).
  v10_status        built / built_empty / no_xml / missing (universe_status_sql), 'not_in_meetings' (in the
                    universe, absent from meetings), 'not_in_universe' (neither), 'duplicate_copy' (the meetings
                    row has duplicate_of: an identical copy of another meeting, researcher decision 7; its turns
                    are not in the release turns, v10_duplicate_of = the kept copy).
Content evidence: distinct 64-bit hashes of normalized sentences (parentheticals removed, split on
. ? ! 。 … and newlines, Hangul syllables/digits/Latin kept, >= FP_MIN_CHARS characters). Hashes found
in more than FP_MAX_DF v10 meetings are boilerplate and ignored. containment(m, c) = shared / n_fp(m).

Turn level (XLSX meetings, relation 'same', v10 built):
  v10 turns built from the v9 XLSX rows (source 'xlsx' with source_speech_order; the 18대 builds before researcher
  decision 5): linked by source_speech_order ('source_row').
  otherwise, i.e. HWP turns for 18대 (decision 5, 2026-09-26: HWP for all 4,270 18대 meetings) and XML turns for
  the other terms: sequence alignment on normalized-text keys of v9 speech_text and v10 text_raw (difflib matching blocks; 'exact' when the
  whitespace-free raw texts are equal, 'normalized' when only the normalized texts are); gaps between
  blocks are aligned by a small DP over 1:1, 1:2 ('merge': one v9 speech = two v10 turns) and 2:1
  ('split') moves scored by rapidfuzz similarity (>= SIM_MIN), or by character-offset overlap when the
  gap's concatenated texts are equal; everything else is 'v9_unmatched' / 'v10_unmatched'.
Nothing is dropped: every v9 row and every v10 turn of an aligned meeting appears in crosswalk_turns.
"""
from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Optional, Sequence

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
V10 = HERE.parents[1]
REPO = V10.parent
INTERIM = V10 / "interim"
OUT_DIR = INTERIM / "pipeline" / "crosswalk"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import validate as VA  # noqa: E402

SEED = 8374
V9_SPEECHES = REPO / "data" / "all_speeches_16_22_v9.parquet"
V9_CROSSWALK = INTERIM / "v9_to_api_crosswalk.parquet"
AUDIT_D4 = INTERIM / "audit_R_D4_v8_full.parquet"
AUDIT_FLAGS = INTERIM / "audit_meeting_flags.parquet"
UNIVERSE = INTERIM / "meeting_universe_api.parquet"
CRAWL = INTERIM / "crawl_state.sqlite"

FP_MIN_CHARS = 15
FP_MAX_DF = 50
REV_MIN_FP = 5
CONTAIN_MIN = 0.5
MIN_FP_DECIDE = 10       # fewer v9 fingerprints: never decide 'not the label meeting' from content
PARTIAL_MIN = 0.1        # label is the best candidate but only partly contained (v8 PDF texts missing parts)
SIM_MIN = 0.6
DP_MAX_CELLS = 40_000
SOURCE_PREF = {"xlsx": 0, "v8_flagged": 1, "v8_unflagged": 1, "v7_pdf": 2, "v6_html": 3}

MEETING_SCHEMA = pa.schema([
    ("v9_meeting_id", pa.string()), ("v9_source", pa.string()), ("v9_term", pa.int16()),
    ("v9_hearing_type", pa.string()), ("v9_committee", pa.string()), ("v9_committee_key", pa.string()),
    ("v9_date", pa.string()), ("v9_n_speeches", pa.int64()), ("v9_match_method", pa.string()),
    ("conf_num", pa.int64()), ("conf_id", pa.string()), ("relation", pa.string()), ("relation_basis", pa.string()),
    ("content_verified", pa.bool_()), ("content_overlap", pa.string()), ("content_conf_num", pa.int64()),
    ("content_conf_id", pa.string()), ("content_share_fwd", pa.float64()), ("content_share_rev", pa.float64()),
    ("duplicate_of_v9_meeting_id", pa.string()), ("is_second_copy", pa.bool_()), ("n_v9_carriers", pa.int32()),
    ("in_universe", pa.bool_()),
    ("audit_verdict", pa.string()), ("audit_v8_mislabel_flag", pa.bool_()), ("label_match", pa.bool_()),
    ("audit_agrees", pa.bool_()),
    ("fp_n", pa.int32()), ("fp_share_label", pa.float64()), ("fp_best_conf_num", pa.int64()),
    ("fp_share_best", pa.float64()), ("fp_share_second", pa.float64()), ("fp_share_id_as_confer_num", pa.float64()),
    ("v10_status", pa.string()), ("v10_duplicate_of", pa.int64()), ("v10_source", pa.string()), ("v10_n_turns", pa.int32()),
    ("v10_class_name", pa.string()), ("v10_hearing_type", pa.string()), ("v10_committee_raw", pa.string()),
    ("v10_date", pa.string()), ("turn_alignment", pa.string()), ("n_v9_rows_linked", pa.int32()),
    ("n_v9_rows_unmatched", pa.int32()), ("n_v10_turns_unmatched", pa.int32()),
])
TURN_SCHEMA = pa.schema([
    ("v9_meeting_id", pa.string()), ("v9_speech_order", pa.string()), ("v9_order_num", pa.int32()),
    ("conf_num", pa.int64()), ("turn_seq", pa.int32()), ("match_type", pa.string()),
    ("similarity", pa.float64()), ("block_id", pa.int32()),
])


def _s(x) -> str:
    return "'" + str(x).replace("'", "''") + "'"


def connect(memory_limit: str = "6GB", threads: int = 4) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute("SET enable_progress_bar=false")
    return con


def _src(paths) -> str:
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    return "read_parquet([" + ", ".join(_s(p) for p in paths) + "], union_by_name=true)"


# =============================================================================== fingerprints
def fingerprint_sql(src: str, key: str, text: str) -> str:
    """Distinct (key, h): hashes of normalized sentences with >= FP_MIN_CHARS characters."""
    clean = f"regexp_replace(coalesce({text}, ''), '\\([^()]*\\)', ' ', 'g')"
    return f"""SELECT DISTINCT {key}, hash(ns) AS h FROM (
                 SELECT {key}, {VA.norm_sql('s')} AS ns FROM (
                   SELECT {key}, unnest(regexp_split_to_array({clean}, '{VA.SENT_SPLIT_RE2}')) AS s FROM {src}))
               WHERE length(ns) >= {FP_MIN_CHARS}"""


def build_v9_fingerprints(out: Path = OUT_DIR / "v9_sentence_fp.parquet", speeches=V9_SPEECHES) -> dict:
    t0 = time.time()
    out.parent.mkdir(parents=True, exist_ok=True)
    con = connect()
    src = f"(SELECT meeting_id, speech_text FROM read_parquet({_s(speeches)}) WHERE meeting_id IS NOT NULL)"
    tmp = out.parent / f".{out.name}.tmp"
    con.execute(f"COPY ({fingerprint_sql(src, 'meeting_id', 'speech_text')}) TO {_s(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(tmp, out)
    st = con.execute(f"SELECT count(*) n, count(DISTINCT meeting_id) m FROM read_parquet({_s(out)})").fetchone()
    con.close()
    return {"rows": st[0], "meetings": st[1], "seconds": round(time.time() - t0, 1), "out": str(out)}


def content_evidence(con, v9fp: str, v10_turns_src: str, text_col: str = "text_raw") -> pd.DataFrame:
    """Per v9 meeting: n fingerprints (boilerplate removed) and the top-3 v10 meetings by containment.
    share = max(shared / n_v9, shared / n_v10) where the reverse direction counts only when the v10
    meeting has >= REV_MIN_FP fingerprints: v8/v7 PDF-derived v9 texts carry appendix material that
    v10 keeps in the footer, which dilutes the forward direction (checked on dev plenary pages)."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE _v10fp AS {fingerprint_sql(v10_turns_src, 'conf_num', text_col)}")
    con.execute("CREATE OR REPLACE TEMP TABLE _df AS SELECT h, count(*) df FROM _v10fp GROUP BY 1")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _v9 AS SELECT f.meeting_id, f.h FROM {v9fp} f
                    LEFT JOIN _df USING (h) WHERE coalesce(_df.df, 0) <= {FP_MAX_DF}""")
    return con.execute(f"""
        WITH n AS (SELECT meeting_id, count(*) fp_n FROM _v9 GROUP BY 1),
        n10 AS (SELECT conf_num, count(*) n10 FROM _v10fp JOIN _df USING (h) WHERE _df.df <= {FP_MAX_DF} GROUP BY 1),
        p AS (SELECT a.meeting_id, b.conf_num, count(*) shared FROM _v9 a JOIN _v10fp b USING (h) GROUP BY 1, 2),
        r AS (SELECT p.*, n.fp_n, n10.n10, p.shared / n.fp_n AS share_fwd, p.shared / n10.n10 AS share_rev,
                     greatest(p.shared / n.fp_n, CASE WHEN n10.n10 >= {REV_MIN_FP} THEN p.shared / n10.n10 ELSE 0 END) AS share
              FROM p JOIN n USING (meeting_id) JOIN n10 USING (conf_num)),
        k AS (SELECT r.*, row_number() OVER (PARTITION BY meeting_id ORDER BY share DESC, shared DESC, conf_num) rk FROM r)
        SELECT n.meeting_id, n.fp_n, k.conf_num, k.shared, k.n10, k.share_fwd, k.share_rev, k.share, k.rk
        FROM n LEFT JOIN k ON k.meeting_id = n.meeting_id AND (k.rk <= 3 OR k.share >= 0.01)""").fetchdf()


def v10_fp_meetings(con) -> set:
    return {int(x) for x in con.execute("SELECT DISTINCT conf_num FROM _v10fp").fetchdf()["conf_num"]}


# =============================================================================== meeting level
def load_v9_meetings(con, v9_crosswalk=V9_CROSSWALK) -> pd.DataFrame:
    """v9 meetings (v9_to_api_crosswalk rows; a subset run passes a filtered copy) with the audit verdicts."""
    cw = con.execute(f"""SELECT meeting_id, v9_source, term, hearing_type, committee, committee_key, date, n_speeches,
                                match_method, api_CONFER_NUM, api_CONF_ID FROM read_parquet({_s(v9_crosswalk)})""").fetchdf()
    d4 = con.execute(f"SELECT meeting_id, verdict AS audit_verdict FROM read_parquet({_s(AUDIT_D4)})").fetchdf()
    fl = con.execute(f"SELECT meeting_id, v8_mislabel_flag FROM read_parquet({_s(AUDIT_FLAGS)})").fetchdf()
    cw = cw.merge(d4, on="meeting_id", how="left").merge(fl, on="meeting_id", how="left")
    if cw.meeting_id.duplicated().any():
        raise ValueError("duplicate v9 meeting_id in v9_to_api_crosswalk")
    return cw


def decide_relations(v9: pd.DataFrame, ev: Optional[pd.DataFrame], built_fp: set, universe_ids: set,
                     v10_complete: bool = False) -> pd.DataFrame:
    """Assign conf_num / content_conf_num / relation / basis for every v9 meeting (vectorised over
    small frames; ~17k rows)."""
    out = v9.copy()
    out["conf_num"] = out["api_CONFER_NUM"].astype("Int64")
    out["conf_id"] = out["api_CONF_ID"]
    idnum = pd.to_numeric(out["meeting_id"], errors="coerce").astype("Int64")
    out["id_as_confer_num"] = idnum.where(idnum.isin(list(universe_ids)))
    # content evidence
    best = second = None
    if ev is not None and len(ev):
        e = ev[ev.rk.notna()]
        best = e[e.rk == 1].set_index("meeting_id")
        second = e[e.rk == 2].set_index("meeting_id")
        fpn = ev.drop_duplicates("meeting_id").set_index("meeting_id")["fp_n"]
        out["fp_n"] = out.meeting_id.map(fpn).astype("Int32")
        out["fp_best_conf_num"] = out.meeting_id.map(best["conf_num"]).astype("Int64")
        out["fp_share_best"] = out.meeting_id.map(best["share"])
        out["fp_share_second"] = out.meeting_id.map(second["share"])
        key = e.set_index(["meeting_id", "conf_num"])["share"]
        out["fp_share_label"] = [key.get((m, int(c)), 0.0) if pd.notna(c) and int(c) in built_fp else np.nan
                                 for m, c in zip(out.meeting_id, out.conf_num)]
        out["fp_share_id_as_confer_num"] = [key.get((m, int(c)), 0.0) if pd.notna(c) and int(c) in built_fp else np.nan
                                            for m, c in zip(out.meeting_id, out.id_as_confer_num)]
    else:
        for c in ("fp_n", "fp_best_conf_num", "fp_share_best", "fp_share_second", "fp_share_label",
                  "fp_share_id_as_confer_num"):
            out[c] = pd.NA
    fwd_key = rev_key = None
    if ev is not None and len(ev):
        fwd_key = e.set_index(["meeting_id", "conf_num"])["share_fwd"] if "share_fwd" in e else None
        rev_key = e.set_index(["meeting_id", "conf_num"])["share_rev"] if "share_rev" in e else None
    rel, basis, content, verified = [], [], [], []
    for r in out.itertuples(index=False):
        label = None if pd.isna(r.conf_num) else int(r.conf_num)
        bshare = None if pd.isna(r.fp_share_best) else float(r.fp_share_best)
        bconf = None if pd.isna(r.fp_best_conf_num) else int(r.fp_best_conf_num)
        lshare = None if pd.isna(r.fp_share_label) else float(r.fp_share_label)
        fp_n = 0 if pd.isna(r.fp_n) else int(r.fp_n)
        label_built = label is not None and label in built_fp
        # 1) content evidence
        if fp_n > 0 and label_built and lshare is not None and lshare >= CONTAIN_MIN:
            lf = None if fwd_key is None else fwd_key.get((r.meeting_id, label))
            if lf is not None and not pd.isna(lf) and float(lf) < CONTAIN_MIN:
                # accepted on reverse containment: the label meeting's text is inside the v9 transcript,
                # but most of the v9 transcript is not in the label meeting (appendix, merged sittings)
                b1 = f"content:label_contained_in_v9(rev>={CONTAIN_MIN},fwd<{CONTAIN_MIN})"
            else:
                b1 = f"content:label_share>={CONTAIN_MIN}"
            rel.append("same"); basis.append(b1); content.append(label); verified.append(True)
            continue
        idc = None if pd.isna(r.id_as_confer_num) else int(r.id_as_confer_num)
        if (fp_n > 0 and bshare is not None and bshare >= CONTAIN_MIN and bconf != label
                and (label_built or label is None or bconf == idc or v10_complete)):
            # (while the labelled meeting has no v10 text, only the known id-confusion target counts:
            #  a transcript can legitimately appear under two meetings, e.g. joint sittings)
            rel.append("v9_wrong_content"); basis.append(f"content:other_meeting_share>={CONTAIN_MIN}")
            content.append(bconf); verified.append(True)
            continue
        sshare = 0.0 if pd.isna(r.fp_share_second) else float(r.fp_share_second)
        if (fp_n > 0 and label_built and bconf == label and lshare is not None and lshare >= PARTIAL_MIN
                and lshare >= 2 * sshare):
            rel.append("same"); basis.append(f"content:partial_label_overlap>={PARTIAL_MIN}")
            content.append(label); verified.append(True)
            continue
        if fp_n >= MIN_FP_DECIDE and label_built and (lshare is None or lshare < PARTIAL_MIN):
            # the labelled meeting's v10 text is available and does not contain this transcript
            if v10_complete:
                rel.append("v9_only"); basis.append("content:not_found_in_complete_v10")
                content.append(None); verified.append(True)
            elif (isinstance(r.audit_verdict, str) and r.audit_verdict == "text=viewer(id)"
                  and not pd.isna(r.id_as_confer_num)):
                # not the label meeting (content); the audit names the viewer-id meeting, whose text is not built yet
                rel.append("v9_wrong_content"); basis.append("content:not_in_label_meeting+audit_R_D4:text=viewer(id)")
                content.append(int(r.id_as_confer_num)); verified.append(False)
            else:
                rel.append("v9_wrong_content"); basis.append("content:not_in_label_meeting")
                content.append(None); verified.append(True)
            continue
        # 2) prior evidence (no usable v10 text yet)
        suffix = "" if not (fp_n > 0 and label_built) else "|content_not_found_in_label_meeting"
        if fp_n > 0 and not label_built and bshare is not None and bshare >= CONTAIN_MIN and bconf != label:
            suffix += f"|content_in_other_meeting_{bconf}_label_not_built"
        if r.v9_source == "v6_html" and label is None:
            rel.append("v9_wrong_content"); basis.append("v6_html_label_mismatch" + suffix)
            content.append(None if pd.isna(r.id_as_confer_num) else int(r.id_as_confer_num)); verified.append(False)
        elif isinstance(r.audit_verdict, str) and r.audit_verdict == "text=viewer(id)":
            rel.append("v9_wrong_content"); basis.append("audit_R_D4:text=viewer(id)" + suffix)
            content.append(None if pd.isna(r.id_as_confer_num) else int(r.id_as_confer_num)); verified.append(False)
        elif isinstance(r.audit_verdict, str) and r.audit_verdict == "text=other session":
            rel.append("v9_wrong_content"); basis.append("audit_R_D4:text=other session" + suffix)
            content.append(None); verified.append(False)
        elif label is None:
            rel.append("v9_only"); basis.append("no_label_match" + suffix); content.append(None); verified.append(False)
        else:
            b = f"audit_R_D4:{r.audit_verdict}" if isinstance(r.audit_verdict, str) else "id_label"
            if label not in universe_ids:
                suffix += "|label_not_in_universe"
            rel.append("same"); basis.append(b + suffix); content.append(label); verified.append(False)
    out["relation"], out["relation_basis"] = rel, basis

    def _get(k, m, c):
        if k is None or c is None or int(c) not in built_fp:
            return None
        x = k.get((m, int(c)))
        return 0.0 if x is None or pd.isna(x) else float(x)
    if ev is not None and len(ev):
        cs = [_get(key, m, c) for m, c in zip(out.meeting_id, content)]
        cf = [_get(fwd_key, m, c) for m, c in zip(out.meeting_id, content)]
        cr = [_get(rev_key, m, c) for m, c in zip(out.meeting_id, content)]
    else:
        cs = cf = cr = [None] * len(out)
    out["content_share_fwd"], out["content_share_rev"] = cf, cr

    def _overlap(x, f):
        # x = max(fwd, rev) as used by the relation rules; f = forward share (part of the v9 transcript found)
        if x is None:
            return None
        f = x if f is None else f
        if f >= CONTAIN_MIN:
            return "full"
        if x >= CONTAIN_MIN:
            return "v10_in_v9"
        return "partial" if x >= PARTIAL_MIN else "none"
    # overlap of the v9 transcript with its content meeting (null when that meeting has no v10 text)
    out["content_overlap"] = [_overlap(x, f) for x, f in zip(cs, cf)]
    out["content_conf_num"] = pd.array(content, dtype="Int64")
    out["content_verified"] = verified
    # label_match: content-based answer to "is this v9 transcript the labelled meeting's transcript?"
    lm = []
    for bs in out["relation_basis"]:
        if bs.startswith("content:label_share") or bs.startswith("content:partial_label_overlap") \
                or bs.startswith("content:label_contained_in_v9"):
            lm.append(True)
        elif bs.startswith("content:other_meeting_share") or bs.startswith("content:not_in_label_meeting") \
                or bs.startswith("content:not_found_in_complete_v10"):
            lm.append(False)
        else:
            lm.append(None)
    out["label_match"] = pd.array(lm, dtype="boolean")
    audit_bin = out["audit_verdict"].map({"text=label": True, "text=viewer(id)": False, "text=other session": False})
    out["audit_agrees"] = pd.array([None if (pd.isna(x) or pd.isna(y)) else bool(x == y)
                                    for x, y in zip(out["label_match"], audit_bin)], dtype="boolean")
    # double carriage: several v9 meetings carrying the same content meeting (any relation). One primary per
    # content meeting ('same' first, then source preference, then id); every other row is a second copy.
    out["duplicate_of_v9_meeting_id"] = None
    out["is_second_copy"] = pd.array([None] * len(out), dtype="boolean")
    out["n_v9_carriers"] = pd.array([None] * len(out), dtype="Int32")
    s = out[out.content_conf_num.notna()].copy()
    s["not_same"] = (s.relation != "same").astype(int)
    s["pref"] = s.v9_source.map(SOURCE_PREF).fillna(9)
    s = s.sort_values(["content_conf_num", "not_same", "pref", "meeting_id"])
    first = s.groupby("content_conf_num")["meeting_id"].transform("first")
    out.loc[s.index, "n_v9_carriers"] = s.groupby("content_conf_num")["meeting_id"].transform("size").astype("Int32")
    second = s.meeting_id != first
    out.loc[s.index, "is_second_copy"] = second.to_numpy()
    dup = s.index[second]
    out.loc[dup, "duplicate_of_v9_meeting_id"] = first.loc[dup].to_numpy()
    out.loc[[i for i in dup if out.at[i, "relation"] == "same"], "relation"] = "duplicate"
    return out


# =============================================================================== turn alignment
def _sim(a: str, b: str) -> float:
    from rapidfuzz import fuzz
    if not a and not b:
        return 1.0
    return fuzz.ratio(a, b) / 100.0


def _overlap_links(A_norm: list, B_norm: list) -> list:
    """Equal concatenations: link items whose character spans overlap -> [(i, j)]."""
    ca = np.cumsum([0] + [len(x) for x in A_norm])
    cb = np.cumsum([0] + [len(x) for x in B_norm])
    links = []
    for i in range(len(A_norm)):
        a0, a1 = ca[i], ca[i + 1]
        for j in range(len(B_norm)):
            b0, b1 = cb[j], cb[j + 1]
            if a0 == a1 or b0 == b1:
                continue
            if b0 < a1 and a0 < b1:
                links.append((i, j))
    # zero-length items stay unmatched
    return links


def _dp_gap(A_norm: list, B_norm: list) -> list:
    """Monotone alignment of a gap with moves 1:1, 1:2, 2:1 (score = sim * length) and skips.
    Returns [(list_of_i, list_of_j, sim)]."""
    n, m = len(A_norm), len(B_norm)
    NEG = -1e18
    S = np.full((n + 1, m + 1), NEG)
    back = {}
    S[0, 0] = 0.0
    cache = {}

    def sc(i0, i1, j0, j1):
        k = (i0, i1, j0, j1)
        if k not in cache:
            a = "".join(A_norm[i0:i1])
            b = "".join(B_norm[j0:j1])
            s = _sim(a, b)
            cache[k] = (s * (len(a) + len(b) + 1), s) if s >= SIM_MIN else (None, s)
        return cache[k]
    for i in range(n + 1):
        for j in range(m + 1):
            if i == 0 and j == 0:
                continue
            best, arg = NEG, None
            if i > 0 and S[i - 1, j] > NEG and S[i - 1, j] > best:
                best, arg = S[i - 1, j], (i - 1, j, None)
            if j > 0 and S[i, j - 1] > NEG and S[i, j - 1] > best:
                best, arg = S[i, j - 1], (i, j - 1, None)
            for di, dj in ((1, 1), (1, 2), (2, 1)):
                if i >= di and j >= dj and S[i - di, j - dj] > NEG:
                    w, s = sc(i - di, i, j - dj, j)
                    if w is not None and S[i - di, j - dj] + w > best:
                        best, arg = S[i - di, j - dj] + w, (i - di, j - dj, s)
            S[i, j] = best
            back[(i, j)] = arg
    out = []
    i, j = n, m
    while (i, j) != (0, 0):
        pi, pj, s = back[(i, j)]
        if s is not None:
            out.append((list(range(pi, i)), list(range(pj, j)), s))
        i, j = pi, pj
    return out[::-1]


def align_sequences(a_norm: list, a_raw: list, b_norm: list, b_raw: list, counters: Optional[Counter] = None) -> list:
    """Align v9 rows (a) and v10 turns (b) of one meeting. Returns links
    [(i or None, j or None, match_type, similarity, block_id)], order-preserving, covering every i and j."""
    c = counters if counters is not None else Counter()
    ka = [hash(x) if x else ("", i) for i, x in enumerate(a_norm)]
    kb = [hash(x) if x else ("", -j - 1) for j, x in enumerate(b_norm)]
    sm = difflib.SequenceMatcher(None, ka, kb, autojunk=False)
    links, block = [], 0
    ai = bj = 0
    blocks = sm.get_matching_blocks()

    def gap(i0, i1, j0, j1):
        nonlocal block
        A, B = a_norm[i0:i1], b_norm[j0:j1]
        if not A and not B:
            return
        if not A:
            links.extend((None, j, "v10_unmatched", None, None) for j in range(j0, j1)); return
        if not B:
            links.extend((i, None, "v9_unmatched", None, None) for i in range(i0, i1)); return
        if "".join(A) == "".join(B) and "".join(A):
            block += 1
            ol = _overlap_links(A, B)
            ci = Counter(i for i, _ in ol)
            cj = Counter(j for _, j in ol)
            got_i = {i for i, _ in ol}
            got_j = {j for _, j in ol}
            items = []
            for i, j in ol:
                t = "merge" if ci[i] > 1 else ("split" if cj[j] > 1 else "normalized")
                items.append((i0 + i, j0 + j, t, 1.0, block))
            items += [(i0 + i, None, "v9_unmatched", None, None) for i in range(len(A)) if i not in got_i]
            items += [(None, j0 + j, "v10_unmatched", None, None) for j in range(len(B)) if j not in got_j]
            items.sort(key=lambda x: (x[0] if x[0] is not None else -1, x[1] if x[1] is not None else -1))
            links.extend(_order_links(items))
            c["gap_overlap"] += 1
            return
        if len(A) * len(B) > DP_MAX_CELLS:
            if len(A) == len(B):   # positional pairing, kept only where similar
                c["gap_too_large_positional"] += 1
                items = []
                for k, (x, y) in enumerate(zip(A, B)):
                    s_ = _sim(x, y)
                    if s_ >= SIM_MIN:
                        block += 1
                        items.append((i0 + k, j0 + k, "similar", round(s_, 4), block))
                    else:
                        items += [(i0 + k, None, "v9_unmatched", None, None), (None, j0 + k, "v10_unmatched", None, None)]
                links.extend(_order_links(items))
                return
            c["gap_too_large_unmatched"] += 1
            links.extend((i, None, "v9_unmatched", None, None) for i in range(i0, i1))
            links.extend((None, j, "v10_unmatched", None, None) for j in range(j0, j1))
            return
        c["gap_dp"] += 1
        res = _dp_gap(A, B)
        got_i, got_j, items = set(), set(), []
        for ii, jj, s in res:
            block += 1
            t = "similar" if len(ii) == 1 and len(jj) == 1 else ("merge" if len(ii) == 1 else "split")
            for i in ii:
                for j in jj:
                    items.append((i0 + i, j0 + j, t, round(s, 4), block))
            got_i |= set(ii); got_j |= set(jj)
        items += [(i0 + i, None, "v9_unmatched", None, None) for i in range(len(A)) if i not in got_i]
        items += [(None, j0 + j, "v10_unmatched", None, None) for j in range(len(B)) if j not in got_j]
        links.extend(_order_links(items))

    for bl in blocks:
        gap(ai, bl.a, bj, bl.b)
        for k in range(bl.size):
            i, j = bl.a + k, bl.b + k
            t = "exact" if a_raw[i] == b_raw[j] else "normalized"
            links.append((i, j, t, 1.0, None))
        ai, bj = bl.a + bl.size, bl.b + bl.size
    return links


def _order_links(items: list) -> list:
    """Sort a gap's items by v9 position, then v10 position (unmatched v10 turns last within the gap).
    Row order in the output is cosmetic; the alignment itself is monotone by construction."""
    big = 1 << 60
    return sorted(items, key=lambda x: (x[0] if x[0] is not None else big, x[1] if x[1] is not None else big))


def align_meeting_frames(v9rows: pd.DataFrame, turns: pd.DataFrame, counters: Counter) -> pd.DataFrame:
    """v9rows: meeting_id, speech_order, so_num, norm, rawkey; turns: conf_num, turn_seq, source,
    source_speech_order, norm, rawkey. One meeting each."""
    a = v9rows.sort_values(["so_num", "speech_order"]).reset_index(drop=True)
    b = turns.sort_values("turn_seq").reset_index(drop=True)
    mid, cn = a.meeting_id.iloc[0], int(b.conf_num.iloc[0])
    if (b.source == "xlsx").all() and b.source_speech_order.notna().all():
        m = a.merge(b[["source_speech_order", "turn_seq", "rawkey"]].rename(columns={"source_speech_order": "speech_order", "rawkey": "rk_b"}),
                    on="speech_order", how="outer", indicator="mstate")
        rows = []
        for r in m.itertuples(index=False):
            if r.mstate == "both":
                rows.append((r.speech_order, r.so_num, r.turn_seq, "source_row", 1.0 if r.rawkey == r.rk_b else 0.0, None))
            elif r.mstate == "left_only":
                rows.append((r.speech_order, r.so_num, None, "v9_unmatched", None, None))
            else:
                rows.append((None, None, r.turn_seq, "v10_unmatched", None, None))
        counters["source_row_meetings"] += 1
        counters["source_row_text_differs"] += sum(1 for x in rows if x[3] == "source_row" and x[4] == 0.0)
        src_label = "xlsx_source_row"
    else:
        links = align_sequences(a.norm.tolist(), a.rawkey.tolist(), b.norm.tolist(), b.rawkey.tolist(), counters)
        rows = []
        for i, j, t, s, blk in links:
            rows.append((a.speech_order.iloc[i] if i is not None else None, int(a.so_num.iloc[i]) if i is not None else None,
                         int(b.turn_seq.iloc[j]) if j is not None else None, t, s, blk))
        counters["aligned_meetings"] += 1
        srcs = sorted(set(b.source.dropna()))
        src_label = srcs[0] if len(srcs) == 1 else "mixed"
    counters[f"meetings_by_v10_source:{src_label}"] += 1
    for r in rows:
        counters[f"match_type_by_v10_source:{src_label}:{r[3]}"] += 1
    df = pd.DataFrame(rows, columns=["v9_speech_order", "v9_order_num", "turn_seq", "match_type", "similarity", "block_id"])
    df.insert(0, "v9_meeting_id", mid)
    df.insert(3, "conf_num", cn)
    return df


def align_turns(con, pairs: pd.DataFrame, v10_turns_src: str, speeches=V9_SPEECHES, batch: int = 200,
                writer: Optional[pq.ParquetWriter] = None) -> tuple:
    """pairs: v9_meeting_id, conf_num (XLSX meetings to align). Streams batches; returns (frame or
    None when a writer is given, per-meeting stats, counters)."""
    counters = Counter()
    parts, stats = [], []
    ws = VA.WS_CLASS_RE2
    for k in range(0, len(pairs), batch):
        chunk = pairs.iloc[k:k + batch]
        con.execute("CREATE OR REPLACE TEMP TABLE _ids AS SELECT unnest($1::VARCHAR[]) AS meeting_id, unnest($2::BIGINT[]) AS conf_num",
                    [chunk.v9_meeting_id.tolist(), [int(x) for x in chunk.conf_num]])
        a = con.execute(f"""SELECT meeting_id, CAST(speech_order AS VARCHAR) speech_order, try_cast(speech_order AS INTEGER) so_num,
                                   {VA.norm_sql('speech_text')} norm, md5(regexp_replace(coalesce(speech_text, ''), '{ws}', '', 'g')) rawkey
                            FROM read_parquet({_s(speeches)}) WHERE meeting_id IN (SELECT meeting_id FROM _ids)""").fetchdf()
        b = con.execute(f"""SELECT conf_num, turn_seq, source, CAST(source_speech_order AS VARCHAR) source_speech_order,
                                   {VA.norm_sql('text_raw')} norm, md5(regexp_replace(coalesce(text_raw, ''), '{ws}', '', 'g')) rawkey
                            FROM {v10_turns_src} WHERE conf_num IN (SELECT conf_num FROM _ids)""").fetchdf() \
            if "source_speech_order" in _cols(con, v10_turns_src) else \
            con.execute(f"""SELECT conf_num, turn_seq, source, NULL::VARCHAR source_speech_order,
                                   {VA.norm_sql('text_raw')} norm, md5(regexp_replace(coalesce(text_raw, ''), '{ws}', '', 'g')) rawkey
                            FROM {v10_turns_src} WHERE conf_num IN (SELECT conf_num FROM _ids)""").fetchdf()
        ga, gb = dict(tuple(a.groupby("meeting_id"))), dict(tuple(b.groupby("conf_num")))
        for mid, cn in zip(chunk.v9_meeting_id, chunk.conf_num):
            if mid not in ga or int(cn) not in gb:
                counters["pair_without_rows"] += 1
                continue
            df = align_meeting_frames(ga[mid], gb[int(cn)], counters)
            mt = df.match_type.value_counts()
            stats.append({"v9_meeting_id": mid, "conf_num": int(cn),
                          "n_v9_rows_linked": int(df.loc[df.turn_seq.notna() & df.v9_speech_order.notna(), "v9_speech_order"].nunique()),
                          "n_v9_rows_unmatched": int(mt.get("v9_unmatched", 0)),
                          "n_v10_turns_unmatched": int(mt.get("v10_unmatched", 0))})
            if writer is not None:
                writer.write_table(pa.Table.from_pandas(df, preserve_index=False).cast(TURN_SCHEMA))
            else:
                parts.append(df)
    frame = pd.concat(parts, ignore_index=True) if parts and writer is None else None
    return frame, pd.DataFrame(stats), counters


def _cols(con, src: str) -> set:
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}


# =============================================================================== build
def build(turns_paths: Optional[Sequence[str]] = None, meetings_path: Optional[str] = None, out_dir: Path = OUT_DIR,
          v10_complete: bool = False, v9fp_path: Optional[Path] = None, align: bool = True,
          universe=UNIVERSE, crawl=CRAWL, v9_crosswalk=V9_CROSSWALK) -> dict:
    t0 = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect()
    v9 = load_v9_meetings(con, v9_crosswalk)
    uni = con.execute(f"SELECT CONFER_NUM, CONF_ID, DAE_NUM, CLASS_NAME_unified FROM read_parquet({_s(universe)})").fetchdf()
    universe_ids = set(int(x) for x in uni.CONFER_NUM)
    stats: dict = {"inputs": {"turns": turns_paths, "meetings": meetings_path, "v10_complete": v10_complete,
                              "v9_crosswalk": str(v9_crosswalk), "universe": str(universe)}}
    ev, built_fp, tsrc = None, set(), None
    if turns_paths:
        tsrc = _src(turns_paths)
        v9fp_path = v9fp_path or out_dir / "v9_sentence_fp.parquet"
        if not Path(v9fp_path).exists():
            stats["v9_fp"] = build_v9_fingerprints(Path(v9fp_path))
        ev = content_evidence(con, f"read_parquet({_s(v9fp_path)})", tsrc)
        built_fp = v10_fp_meetings(con)
        stats["v10_meetings_with_fingerprints"] = len(built_fp)
    d = decide_relations(v9, ev, built_fp, universe_ids, v10_complete=v10_complete)
    # v10 status of the content (else label) meeting
    vt = {"universe": str(universe), "crawl": str(crawl) if crawl and Path(crawl).exists() else None}
    if meetings_path:
        vt["meetings"] = meetings_path
    V = VA.Validator(vt, VA.Params(mode="dev"), con=con)
    ms = None
    if V.has("meetings"):
        stat = V.q(VA.universe_status_sql(V))[["conf_num", "status"]]
        b = V.built_expr("m")
        dup_col = "m.duplicate_of" if "duplicate_of" in V.cols("meetings") else "NULL::BIGINT"
        ms = V.q(f"""SELECT conf_num, source, n_turns, class_name, hearing_type, committee_raw, date,
                            {dup_col} AS duplicate_of,
                            CASE WHEN {b} AND coalesce(n_turns, 0) > 0 THEN 'built' WHEN {b} THEN 'built_empty'
                                 ELSE 'missing' END AS status_outside
                     FROM meetings m""")
        ms = ms.merge(stat, on="conf_num", how="left")
        # meetings outside the universe (id-gap scan) get their status from the meetings table
        ms["status"] = ms["status"].where(ms["status"].notna(), ms.pop("status_outside"))
        # an identical copy of another meeting (researcher decision 7): its turns are not in the release turns
        ms.loc[ms.duplicate_of.notna(), "status"] = "duplicate_copy"
        stats["duplicate_copies_in_meetings"] = int(ms.duplicate_of.notna().sum())
    target = d["content_conf_num"].fillna(d["conf_num"])
    if ms is not None:
        mi = ms.set_index("conf_num")
        for c, s in (("v10_status", "status"), ("v10_duplicate_of", "duplicate_of"), ("v10_source", "source"), ("v10_n_turns", "n_turns"),
                     ("v10_class_name", "class_name"), ("v10_hearing_type", "hearing_type"),
                     ("v10_committee_raw", "committee_raw"), ("v10_date", "date")):
            d[c] = target.map(mi[s]) if s in mi else None
        absent = target.notna() & ~target.isin(mi.index)
        d.loc[absent, "v10_status"] = ["not_in_meetings" if int(x) in universe_ids else "not_in_universe"
                                       for x in target[absent]]
    else:
        ui = set(universe_ids)
        d["v10_status"] = [None if pd.isna(x) else ("not_in_meetings" if int(x) in ui else "not_in_universe") for x in target]
        for c in ("v10_source", "v10_n_turns", "v10_class_name", "v10_hearing_type", "v10_committee_raw", "v10_date"):
            d[c] = None
    d["in_universe"] = [None if pd.isna(x) else int(x) in universe_ids for x in target]
    cid = dict(zip(uni.CONFER_NUM.astype("int64"), uni.CONF_ID))
    d["content_conf_id"] = [None if pd.isna(x) else cid.get(int(x)) for x in d.content_conf_num]
    # turn alignment for XLSX meetings
    d["turn_alignment"] = np.where(d.v9_source.eq("xlsx") & d.relation.isin(["same"]), "pending_v10_not_built", "not_applicable")
    d["n_v9_rows_linked"] = pd.array([None] * len(d), dtype="Int32")
    d["n_v9_rows_unmatched"] = pd.array([None] * len(d), dtype="Int32")
    d["n_v10_turns_unmatched"] = pd.array([None] * len(d), dtype="Int32")
    turns_out = out_dir / "crosswalk_turns.parquet"
    if align and tsrc is not None:
        have = {int(x) for x in con.execute(f"SELECT DISTINCT conf_num FROM {tsrc}").fetchdf()["conf_num"]}
        pairs = d.loc[(d.turn_alignment == "pending_v10_not_built") & d.content_conf_num.isin(list(have)),
                      ["meeting_id", "content_conf_num"]].rename(columns={"meeting_id": "v9_meeting_id", "content_conf_num": "conf_num"})
        tmp = out_dir / f".{turns_out.name}.tmp"
        w = pq.ParquetWriter(str(tmp), TURN_SCHEMA, compression="zstd")
        _, st, cnt = align_turns(con, pairs.reset_index(drop=True), tsrc, writer=w)
        w.close()
        os.replace(tmp, turns_out)
        stats["alignment"] = dict(cnt)
        if len(st):
            st = st.set_index("v9_meeting_id")
            m = d.meeting_id.isin(st.index)
            d.loc[m, "turn_alignment"] = "aligned"
            for c in ("n_v9_rows_linked", "n_v9_rows_unmatched", "n_v10_turns_unmatched"):
                d.loc[m, c] = d.loc[m, "meeting_id"].map(st[c]).astype("Int32")
    else:
        pq.write_table(pa.Table.from_pylist([], schema=TURN_SCHEMA), turns_out)
    # v10_only rows: universe meetings and meetings-table rows (e.g. id-gap meetings outside the universe)
    # that no v9 row carries
    carried = set(int(x) for x in d.content_conf_num.dropna())
    only = uni[~uni.CONFER_NUM.isin(list(carried))]
    v10o = pd.DataFrame({"conf_num": only.CONFER_NUM.astype("int64"), "conf_id": only.CONF_ID})
    v10o["in_universe"] = True
    if ms is not None:
        extra = V.q("SELECT conf_num, conf_id FROM meetings")
        extra = extra[~extra.conf_num.isin(list(universe_ids)) & ~extra.conf_num.isin(list(carried))]
        if len(extra):
            ex = pd.DataFrame({"conf_num": extra.conf_num.astype("int64"), "conf_id": extra.conf_id, "in_universe": False})
            v10o = pd.concat([v10o, ex], ignore_index=True)
        stats["v10_only_outside_universe"] = int(len(extra))
    v10o["relation"] = "v10_only"
    v10o["relation_basis"] = "no_v9_row_carries_this_transcript"
    v10o["content_verified"] = False
    if ms is not None:
        v10o = v10o.merge(ms.rename(columns={"status": "v10_status", "duplicate_of": "v10_duplicate_of",
                                             "source": "v10_source", "n_turns": "v10_n_turns",
                                             "class_name": "v10_class_name", "hearing_type": "v10_hearing_type",
                                             "committee_raw": "v10_committee_raw", "date": "v10_date"}),
                          on="conf_num", how="left")
    v10o["turn_alignment"] = "not_applicable"
    res = d.rename(columns={"meeting_id": "v9_meeting_id", "term": "v9_term", "hearing_type": "v9_hearing_type",
                            "committee": "v9_committee", "committee_key": "v9_committee_key", "date": "v9_date",
                            "n_speeches": "v9_n_speeches", "match_method": "v9_match_method",
                            "v8_mislabel_flag": "audit_v8_mislabel_flag"})
    res = pd.concat([res, v10o], ignore_index=True)
    for f in MEETING_SCHEMA:
        if f.name not in res.columns:
            res[f.name] = None
    res = res[[f.name for f in MEETING_SCHEMA]]
    for f in MEETING_SCHEMA:                      # NULL policy: never an empty string (v9 43038 committee '')
        if f.type == pa.string():
            blank = res[f.name].map(lambda x: isinstance(x, str) and not x.strip())
            res.loc[blank, f.name] = None
    tbl = _to_table(res, MEETING_SCHEMA)
    tmp = out_dir / ".crosswalk_meetings.parquet.tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, out_dir / "crosswalk_meetings.parquet")
    v9r = res[res.v9_meeting_id.notna()]
    stats["double_carriage"] = {
        "content_meetings_with_several_v9_carriers": int(v9r.loc[v9r.n_v9_carriers.fillna(0) > 1, "content_conf_num"].nunique()),
        "second_copies": int(v9r.is_second_copy.fillna(False).sum()),
        "second_copies_by_relation": v9r[v9r.is_second_copy.fillna(False).astype(bool)].relation.value_counts().to_dict(),
        "second_copies_by_v9_source": v9r[v9r.is_second_copy.fillna(False).astype(bool)].v9_source.value_counts().to_dict(),
        "second_copies_content_verified": int((v9r.is_second_copy.fillna(False).astype(bool) & v9r.content_verified.fillna(False).astype(bool)).sum()),
    }
    stats["content_overlap"] = {str(k): int(v) for k, v in v9r.content_overlap.value_counts(dropna=False).items()}
    stats["v10_status"] = {str(k): int(v) for k, v in res.v10_status.value_counts(dropna=False).items()}
    if align and tsrc is not None:
        stats["v9_unmatched"] = v9_unmatched_repeats(con, turns_out)
    stats.update({
        "n_rows": len(res), "n_v9_meetings": int(res.v9_meeting_id.notna().sum()),
        "relation": res.relation.value_counts().to_dict(),
        "relation_by_v9_source": res[res.v9_meeting_id.notna()].groupby(["v9_source", "relation"]).size().rename("n").reset_index().to_dict("records"),
        "relation_basis": res.relation_basis.value_counts().to_dict(),
        "content_verified": int(res.content_verified.fillna(False).sum()),
        "audit_agrees": {str(k): int(v) for k, v in res.audit_agrees.value_counts(dropna=False).items()},
        "turn_alignment": res.turn_alignment.value_counts().to_dict(),
        "seconds": round(time.time() - t0, 1),
    })
    (out_dir / "crosswalk_stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=1, default=str))
    con.close()
    return stats


def v9_unmatched_repeats(con, crosswalk_turns_path, speeches=V9_SPEECHES) -> dict:
    """Counts for the v9_unmatched rows of the turn alignment: how many repeat an earlier v9 row of the same
    meeting verbatim (same speaker and speech_text, or same speech_text only), and in how many meetings."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _u AS SELECT v9_meeting_id, v9_speech_order
                    FROM read_parquet({_s(crosswalk_turns_path)}) WHERE match_type = 'v9_unmatched'""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _us AS SELECT meeting_id, CAST(speech_order AS VARCHAR) so,
                           try_cast(speech_order AS INTEGER) son, speaker, md5(coalesce(speech_text, '')) h
                    FROM read_parquet({_s(speeches)}) WHERE meeting_id IN (SELECT DISTINCT v9_meeting_id FROM _u)""")
    q = """SELECT count(*), count(DISTINCT u.v9_meeting_id) FROM _u u JOIN _us a ON a.meeting_id = u.v9_meeting_id AND a.so = u.v9_speech_order
           WHERE EXISTS (SELECT 1 FROM _us b WHERE b.meeting_id = a.meeting_id AND b.son < a.son AND b.h = a.h {x})"""
    n, m = con.execute("SELECT count(*), count(DISTINCT v9_meeting_id) FROM _u").fetchone()
    r1 = con.execute(q.format(x="AND b.speaker IS NOT DISTINCT FROM a.speaker")).fetchone()
    r2 = con.execute(q.format(x="")).fetchone()
    return {"rows": int(n), "meetings": int(m),
            "rows_repeating_earlier_row_same_speaker_and_text": int(r1[0]), "meetings_with_such_rows": int(r1[1]),
            "rows_repeating_earlier_row_same_text": int(r2[0]), "meetings_with_such_rows_text_only": int(r2[1])}


def _to_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    arrays = []
    for f in schema:
        col = df[f.name]
        vals = [None if (x is None or (not isinstance(x, (list, np.ndarray)) and pd.isna(x))) else x for x in col.tolist()]
        if pa.types.is_integer(f.type):
            vals = [None if x is None else int(x) for x in vals]
        elif pa.types.is_floating(f.type):
            vals = [None if x is None else float(x) for x in vals]
        elif pa.types.is_boolean(f.type):
            vals = [None if x is None else bool(x) for x in vals]
        elif pa.types.is_string(f.type):
            vals = [None if x is None else str(x) for x in vals]
        arrays.append(pa.array(vals, type=f.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="v9 -> v10 crosswalk")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("v9-fp")
    b = sub.add_parser("build")
    b.add_argument("--turns", nargs="*", default=None)
    b.add_argument("--meetings", default=None)
    b.add_argument("--out-dir", default=str(OUT_DIR))
    b.add_argument("--v10-complete", action="store_true")
    b.add_argument("--no-align", action="store_true")
    b.add_argument("--v9-crosswalk", default=str(V9_CROSSWALK), help="v9 meetings (a subset run passes a filtered copy)")
    a = ap.parse_args(argv)
    if a.cmd == "v9-fp":
        print(json.dumps(build_v9_fingerprints(), indent=1))
    else:
        st = build(a.turns, a.meetings, Path(a.out_dir), a.v10_complete, align=not a.no_align,
                   v9_crosswalk=a.v9_crosswalk)
        print(json.dumps({k: v for k, v in st.items() if k != "relation_by_v9_source"}, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
