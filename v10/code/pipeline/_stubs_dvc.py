"""TEST-ONLY stand-ins used by test_dyads.py, test_validate.py and test_crosswalk.py.

These are NOT the pipeline's turn builder or enrichment modules (other components own those).
They produce CONTRACT-shaped frames from the saved viewer pages so that dyads / validate /
crosswalk can be exercised before the real turns exist:

    find_view_page(conf_num)        -> Path | None   (raw/viewer/view, raw/samples, raw/samples_random)
    turns_from_view(path, conf_num) -> turns frame (CONTRACT columns, source='xml')
    stub_roles(turns)               -> adds role, role_group (simple position rules)
    stub_enrich(turns)              -> adds small synthetic enrichment columns
    meetings_from_universe(conf_nums, turns) -> meetings frame (CONTRACT columns)
"""
from __future__ import annotations

import gzip
import hashlib
import re
import sys
from pathlib import Path
from typing import Iterable, Optional

import duckdb
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
V10 = HERE.parents[1]
sys.path.insert(0, str(HERE.parent))  # v10/code for parse_viewer
from parse_viewer import parse_view  # noqa: E402

UNIVERSE = V10 / "interim" / "meeting_universe_api.parquet"


def find_view_page(conf_num: int) -> Optional[Path]:
    b = str(int(conf_num)).zfill(6)[:3]
    for p in (V10 / "raw" / "viewer" / "view" / b / f"{conf_num}.html.gz",
              V10 / "raw" / "samples" / f"{conf_num}_view.html.gz",
              V10 / "raw" / "samples_random" / f"{conf_num}_view.html.gz"):
        if p.exists():
            return p
    return None


def turns_from_view(path: Path, conf_num: int) -> pd.DataFrame:
    res = parse_view(gzip.decompress(Path(path).read_bytes()))
    rows = []
    for i, s in enumerate(res["speeches"], start=1):
        name = s.get("name_norm") or ""
        pos = s.get("pos_norm") or ""
        mem = s.get("mem_id")
        rows.append({
            "conf_num": int(conf_num),
            "turn_seq": i,
            "source": "xml",
            "speaker_label_raw": (pos + " " + name).strip(),
            "speaker_pos": pos or None,
            "speaker_name": name or None,
            "speaker_mem_id": int(mem) if mem and str(mem).isdigit() and int(mem) != 0 else None,
            "speaker_area": s.get("area"),
            "text_raw": s["text"],
            "text": s["text_spoken"],
            "has_stage": bool(s["has_stage"]),
            "stage_kinds": list(s["stage_kinds"]),
            "n_fragments": int(s["n_fragments"]),
            "agenda_ordinal": s.get("agenda_ordinal") or None,
            "agenda_text": s.get("agenda_text"),
            "time_hhmm": s.get("time_hhmm"),
            "speech_date": s.get("speech_date"),
            # CONTRACT turn boundary / sitting fields (build_turns 1.7); single sitting in the stub
            "after_end_marker": False,
            "sitting_seq": 1,
            "sitting_how": "end_open_markers",
            "label_how": "in_chk_label",
            "label_confidence": "high",
        })
    t = pd.DataFrame(rows)
    if t.empty:
        return t
    t["conf_num"] = t["conf_num"].astype("int64")
    t["turn_seq"] = t["turn_seq"].astype("int32")
    t["speaker_mem_id"] = t["speaker_mem_id"].astype("Int64")
    t["n_fragments"] = t["n_fragments"].astype("int16")
    t["agenda_ordinal"] = t["agenda_ordinal"].astype("Int32")
    t["has_stage"] = t["has_stage"].astype(bool)
    t["after_end_marker"] = t["after_end_marker"].astype(bool)
    t["sitting_seq"] = t["sitting_seq"].astype("int16")
    return t


_LEG_POS = re.compile(r"^(위원|의원|위원장|소위원장|위원장대리|위원장직무대행|의장|부의장|간사|반장|委員|委員長|議員|議長|副議長)$")
_STAFF_POS = re.compile(r"(?:전문위원|입법조사관|입법심의관|의사국장|사무처|속기|의사과장|의안과장)")


def stub_roles(turns: pd.DataFrame) -> pd.DataFrame:
    pos = turns["speaker_pos"].fillna("")
    leg = pos.str.fullmatch(_LEG_POS.pattern)
    staff = pos.str.contains(_STAFF_POS.pattern) & ~leg
    chair = pos.str.fullmatch(r"(위원장|소위원장|위원장대리|위원장직무대행|의장|부의장|반장|委員長|議長|副議長)")
    out = turns.copy()
    out["role"] = np.where(chair, "chair", np.where(leg, "legislator", np.where(staff, "committee_staff",
                           np.where(pos == "", "other", "other_official"))))
    out["role_group"] = np.where(leg, "legislator", np.where(staff | (pos == ""), "excluded", "nonlegislator"))
    return out


def stub_enrich(turns: pd.DataFrame) -> pd.DataFrame:
    out = turns.copy()
    is_leg = out["role_group"] == "legislator"
    out["naas_cd"] = np.where(is_leg, "X" + out["speaker_name"].fillna("").map(lambda s: hashlib.md5(s.encode()).hexdigest()[:7].upper()), None)
    out["party"] = np.where(is_leg, "정당A", None)
    out["ruling_status"] = np.where(is_leg, "opposition", None)
    out["presidency_state"] = "normal"
    out["ministry_normalized"] = np.where(out["role_group"] == "nonlegislator", "행정부", None)
    return out


def meetings_from_universe(conf_nums: Iterable[int], turns: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE ids AS SELECT unnest($1)::BIGINT AS c", [list(map(int, conf_nums))])
    m = con.execute(f"""SELECT CONFER_NUM AS conf_num, CONF_ID AS conf_id, DAE_NUM AS term,
        CLASS_NAME_unified AS class_name, COMM_NAME AS committee_raw, CONF_DATE AS date, TITLE AS title
        FROM read_parquet('{UNIVERSE}') WHERE CONFER_NUM IN (SELECT c FROM ids) ORDER BY 1""").fetchdf()
    con.close()
    m["conf_num"] = m["conf_num"].astype("int64")
    m["term"] = m["term"].astype("int16")
    m["v9_meeting_id"] = pd.Series([None] * len(m), dtype=object)
    m["hearing_type"] = m["class_name"]
    m["is_subcommittee"] = False
    m["subcommittee"] = None
    m["committee_key"] = None
    m["session_no"] = pd.array([None] * len(m), dtype="Int16")
    m["session_type"] = None
    m["sitting"] = None
    m["date_end"] = m["date"]
    m["audit_year"] = pd.array([None] * len(m), dtype="Int16")
    m["audited_agencies"] = [[] for _ in range(len(m))]
    m["source"] = "xml"
    if turns is not None and len(turns):
        n = turns.groupby("conf_num").size()
        m["n_turns"] = m["conf_num"].map(n).fillna(0).astype("int32")
    else:
        m["n_turns"] = np.int32(0)
    return m
