"""Tests for validate.py: a clean fixture passes every check, and every check FAILs on a
deliberately corrupted fixture (run: python3 -m pytest -q test_validate.py)."""
from __future__ import annotations

import copy
import hashlib
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import _stubs_dvc as S  # noqa: E402
import dyads as DY  # noqa: E402
import legacy_rules as LR  # noqa: E402
import validate as VA  # noqa: E402

CAL = VA.INTERIM / "president_calendar.csv"
LINEAGE = VA.INTERIM / "party_lineage.csv"
IDS = [23949, 29275, 40111, 45443]     # 16대 상임위, 17대 국감, 19대 소위, 21대 상임위 (saved view pages)
UNBUILT = 32224                          # a universe meeting treated as confirmed no_xml
WS_ALL = "".join(chr(c) for c in range(0x110000) if chr(c).isspace())

PA_TYPES = {"BIGINT": pa.int64(), "INTEGER": pa.int32(), "SMALLINT": pa.int16(), "VARCHAR": pa.string(),
            "BOOLEAN": pa.bool_(), "VARCHAR[]": pa.list_(pa.string()), "INT": pa.int32(), "DOUBLE": pa.float64(),
            "BIGINT[]": pa.list_(pa.int64())}
TYPES = {"turns": {**VA.TURN_TYPES, **{c: "VARCHAR" for cols in VA.ENRICH_COLS.values() for c in cols},
                   "seniority": "SMALLINT", "dual_office": "BOOLEAN"},
         "meetings": dict(VA.MEETING_TYPES, is_built="BOOLEAN", in_universe="BOOLEAN", duplicate_of="BIGINT",
                          overlap_with="BIGINT[]"),
         "dyads": dict(VA.SLIM_DYAD_TYPES), "crosswalk": dict(VA.CROSSWALK_TYPES),
         "crosswalk_turns": dict(VA.CROSSWALK_TURN_TYPES)}


def arrow(df: pd.DataFrame, types: dict) -> pa.Table:
    arrays, names = [], []
    for c in df.columns:
        t = PA_TYPES.get(types.get(c, ""), None)
        col = df[c]
        if t is None:
            if col.dtype == object and col.isna().all():
                t = pa.string()
            elif str(col.dtype) == "boolean":
                t = pa.bool_()
        if t is not None:
            vals = [None if (x is None or (not isinstance(x, (list, np.ndarray)) and pd.isna(x))) else
                    (list(x) if isinstance(x, (list, np.ndarray)) else x) for x in col.tolist()]
            if pa.types.is_integer(t):
                vals = [None if x is None else int(x) for x in vals]
            arrays.append(pa.array(vals, type=t))
        else:
            arrays.append(pa.array(col.tolist()) if col.dtype == object else pa.Array.from_pandas(col))
        names.append(c)
    return pa.Table.from_arrays(arrays, names=names)


def _cal_lookup():
    v = VA.Validator({"calendar": str(CAL), "lineage": str(LINEAGE)}, VA.Params(mode="dev"))
    cal = v.q("SELECT * FROM cal")
    pp = pd.read_csv(CAL, dtype=str)[["start", "pres_party_formal"]]
    return cal, pp


def fixture_committee_key(hearing_type, committee_raw):
    k = LR.harmonize_committee(committee_raw, hearing_type)
    if k is None and isinstance(committee_raw, str):
        k = LR.harmonize_committee(committee_raw.split()[0], hearing_type)
    return k or VA.HEARING_TYPE_KEYS.get(hearing_type)


def set_presidency(t: pd.DataFrame, d: pd.Series):
    """Calendar columns and a deterministic party / ruling_status per legislator turn (decision 6 rule: the
    label counting as ruling on the date, cal.ruling_ref)."""
    cal, _ = _cal_lookup()

    def look(x, col):
        r = cal[(cal.start <= x) & (cal["end"] >= x)]
        return r.iloc[0][col] if len(r) else None
    cols = ("presidency_state", "admin", "admin_ideology", "president_party", "president", "president_last_party", "ruling_ref")
    uniq = {x: {c: look(x, c) for c in cols} for x in d.unique()}
    is_leg = t.role_group.eq("legislator")
    for c in cols[:-1]:
        t[c] = d.map(lambda x: uniq[x][c])
    ref = d.map(lambda x: uniq[x]["ruling_ref"])
    # parties by speaker (deterministic): the ruling label, another party, or 무소속
    bucket = t["speaker_name"].fillna("").map(lambda s: int(hashlib.md5(s.encode()).hexdigest(), 16) % 5)
    party = np.where(bucket.isin([0, 1]) & ref.notna(), ref, np.where(bucket.eq(4), "무소속", "정당A"))
    t["party"] = np.where(is_leg, party, None)
    active = is_leg & t.presidency_state.isin(["normal", "suspended", "partyless"])
    t["ruling_status"] = np.where(active & t.party.eq("무소속"), "independent",
                                  np.where(active & ref.notna() & t.party.eq(ref), "ruling",
                                           np.where(active & ref.notna(), "opposition", None)))
    return t


def build_fixture() -> dict:
    parts = [S.stub_roles(S.turns_from_view(S.find_view_page(c), c)) for c in IDS]
    t = pd.concat(parts, ignore_index=True)
    t = S.stub_enrich(t)
    m = S.meetings_from_universe(IDS + [UNBUILT], t)
    dmap = dict(zip(m.conf_num, m.date))
    d = t["speech_date"].fillna(t["conf_num"].map(dmap))
    set_presidency(t, d)
    is_leg = t.role_group.eq("legislator")
    # meetings: a committee key for every built meeting (domain check)
    m["committee_key"] = [fixture_committee_key(h, c) for h, c in zip(m.class_name, m.committee_raw)]
    m["duplicate_of"] = pd.array([None] * len(m), dtype="Int64")
    m["overlap_with"] = [None] * len(m)
    for c in ("role_rule", "role_v9_compat", "affiliation_raw", "person_title", "leg_name_hangul", "leg_name_hanja",
              "gender", "birth_date", "district", "elect_type", "id_method", "id_confidence", "party_lineage",
              "party_method", "minister_panel_id", "link_method"):
        t[c] = None
    t["role_v9_compat"] = t["role"]
    t["seniority"] = pd.array(np.where(is_leg, 1, None), dtype="Int16")
    t["dual_office"] = pd.array([None] * len(t), dtype="boolean")
    # meetings
    built = m.conf_num.isin(IDS)
    m["is_built"] = built
    m["in_universe"] = True
    m["n_turns"] = pd.array([int(x) if b else None for x, b in zip(m.n_turns, built)], dtype="Int32")
    m["source"] = np.where(built, "xml", None)
    m["v9_meeting_id"] = [c.lstrip("0") if b else None for c, b in zip(m.conf_id, built)]
    m["hearing_type"] = m["class_name"]
    m.loc[~built, ["hearing_type"]] = m.loc[~built, "class_name"]
    uni = pd.read_parquet(VA.INTERIM / "meeting_universe_api.parquet",
                          columns=["CONFER_NUM", "CONF_ID", "DAE_NUM", "CLASS_NAME_unified", "CONF_DATE"])
    uni = uni[uni.CONFER_NUM.isin(IDS + [UNBUILT])].reset_index(drop=True)
    crawl = pd.DataFrame([(c, "view", "ok") for c in IDS] + [(UNBUILT, "view", "no_xml"), (UNBUILT, "hwp", "not_found")],
                         columns=["conf_num", "kind", "status"])
    nows = lambda s: len(re.sub(r"\s+", "", s or ""))  # noqa: E731
    cov = t.groupby("conf_num").agg(n_turns=("turn_seq", "size"), turn_text_raw_chars=("text_raw", lambda s: sum(nows(x) for x in s))).reset_index()
    cov["source"] = "xml"
    cov["ok_all"] = True
    ag = t[t.agenda_ordinal.notna()].groupby(["conf_num", "agenda_ordinal"]).agg(
        after_turn_seq=("turn_seq", "min"), text=("agenda_text", "first")).reset_index().rename(columns={"agenda_ordinal": "ordinal"})
    ag["after_turn_seq"] = ag["after_turn_seq"] - 1
    ag["ordinal"] = ag["ordinal"].astype("int32")
    ft = pd.DataFrame([(c, 1, 1, k, f"line {k}") for c in IDS for k in (1, 2)],
                      columns=["conf_num", "section_seq", "group_seq", "item_seq", "line_text"])
    dy = DY.build_dyads(t, m)
    cid = dict(zip(m.conf_num, m.conf_id))
    cw = pd.DataFrame({
        "v9_meeting_id": [cid[c].lstrip("0") for c in IDS] + [str(IDS[0]), None],
        "v9_source": ["xlsx"] * len(IDS) + ["v6_html", None],
        "v9_hearing_type": ["상임위원회"] * (len(IDS) + 1) + [None], "v9_committee": None, "v9_date": None,
        "conf_num": IDS + [IDS[0], UNBUILT], "conf_id": [cid[c] for c in IDS] + [cid[IDS[0]], cid[UNBUILT]],
        "relation": ["same"] * len(IDS) + ["duplicate", "v10_only"], "relation_basis": "fixture",
        "content_verified": True, "content_conf_num": IDS + [IDS[0], None],
        "duplicate_of_v9_meeting_id": [None] * len(IDS) + [cid[IDS[0]].lstrip("0"), None],
        "is_second_copy": [False] * len(IDS) + [True, None]})
    v9m = cw[cw.v9_meeting_id.notna()][["v9_meeting_id", "v9_source"]].rename(columns={"v9_meeting_id": "meeting_id"})
    a = t[t.conf_num == IDS[2]]
    v9s = pd.DataFrame({"meeting_id": cid[IDS[2]].lstrip("0"), "speech_order": a.turn_seq.astype(str).tolist()})
    cwt = pd.DataFrame({"v9_meeting_id": cid[IDS[2]].lstrip("0"), "v9_speech_order": a.turn_seq.astype(str).tolist(),
                        "v9_order_num": a.turn_seq.astype("int32").tolist(), "conf_num": IDS[2],
                        "turn_seq": a.turn_seq.astype("int32").tolist(), "match_type": "exact", "similarity": 1.0})
    return {"turns": t, "meetings": m, "dyads": dy, "agenda": ag, "footer": ft, "coverage": cov, "crosswalk": cw,
            "crosswalk_turns": cwt, "universe": uni, "crawl": crawl, "v9_meetings": v9m, "v9_speeches": v9s,
            "calendar": str(CAL), "lineage": str(LINEAGE)}


@pytest.fixture(scope="module")
def fx():
    return build_fixture()


def run(fx, only=None, params=None, types=None):
    tables = {}
    for k, x in fx.items():
        if isinstance(x, pd.DataFrame):
            tables[k] = arrow(x, (types or {}).get(k, TYPES.get(k, {})))
        else:
            tables[k] = x
    p = params or VA.Params(mode="release")
    rep = VA.Validator(tables, p).run(only=only)
    return {c["id"]: c for c in rep["checks"]}, rep


def test_whitespace_class_equals_python():
    assert re.fullmatch(VA.WS_CLASS_PY + "*", WS_ALL)
    assert all(bool(re.match(VA.WS_CLASS_PY, chr(c))) == chr(c).isspace() for c in range(0x110000))
    import duckdb
    s = "가" + WS_ALL + "나 다​"
    got = duckdb.connect().execute(f"SELECT {VA.nows_len_sql('?')}", [s]).fetchone()[0]
    assert got == len(re.sub(r"\s+", "", s))


def test_clean_fixture_passes_everything(fx):
    res, rep = run(fx)
    bad = {k: (r["status"], r["details"]) for k, r in res.items() if r["status"] != "PASS"}
    assert not bad, json.dumps(bad, ensure_ascii=False, default=str)[:3000]
    assert rep["ok"] and len(res) == len(VA.CHECKS)


def test_dev_mode_skips_missing_inputs(fx):
    f = {"turns": fx["turns"]}
    res, rep = run(f, params=VA.Params(mode="dev"))
    assert res["universe_accounted"]["status"] == "SKIP"
    res2, rep2 = run(f, params=VA.Params(mode="release"))
    assert res2["universe_accounted"]["status"] == "FAIL" and not rep2["ok"]


def test_cli_exit_code(fx, tmp_path):
    for k, x in fx.items():
        if isinstance(x, pd.DataFrame):
            import pyarrow.parquet as pq
            pq.write_table(arrow(x, TYPES.get(k, {})), tmp_path / f"{k}.parquet")
    args = []
    for k, x in fx.items():
        args += [f"--{k.replace('_', '-')}", str(tmp_path / f"{k}.parquet") if isinstance(x, pd.DataFrame) else x]
    rc = VA.main(args + ["--out", str(tmp_path / "r.json"), "--docs-numbers", str(tmp_path / "n.json")])
    assert rc == 0
    nums = json.loads((tmp_path / "n.json").read_text())
    assert nums["dyads.rows"] == len(fx["dyads"]) and nums["validation.FAIL"] == 0
    d2 = fx["dyads"].copy()
    d2.loc[d2.index[0], "direction"] = "question" if d2.loc[d2.index[0], "direction"] == "answer" else "answer"
    import pyarrow.parquet as pq
    pq.write_table(arrow(d2, TYPES["dyads"]), tmp_path / "dyads.parquet")
    rc = VA.main(args + ["--out", str(tmp_path / "r.json"), "--docs-numbers", str(tmp_path / "n.json")])
    assert rc == 1


# ------------------------------------------------------------------ corruptions (one per check)
def _flip(df, col, idx=0):
    df = df.copy()
    df.loc[df.index[idx], col] = not bool(df.loc[df.index[idx], col])
    return df


def c_schema_turns(f):
    f["turns"] = f["turns"].assign(turn_seq=f["turns"].turn_seq.astype(str))


def c_schema_turns_missing_enrichment(f):
    f["turns"] = f["turns"].drop(columns=["ruling_status"])


def c_schema_meetings(f):
    f["meetings"] = f["meetings"].assign(conf_id=f["meetings"].conf_id.astype("int64"))


def c_schema_dyads(f):
    f["dyads"] = f["dyads"].drop(columns=["wit_text"])


def c_schema_dyads_meeting_cols(f):
    """the review's 'dyads.py build <enriched turns>' without meetings: no hearing_type/date/committee"""
    f["dyads"] = f["dyads"].drop(columns=["hearing_type", "date", "class_name"])


def c_dy_meeting_field(f):
    d = f["dyads"].copy()
    d.loc[d.index[d.conf_num == IDS[1]][:5], "hearing_type"] = "상임위원회"   # 17대 국감 meeting mislabeled
    d.loc[d.index[0], "date"] = "1999-01-01"
    f["dyads"] = d


def c_schema_crosswalk(f):
    f["crosswalk"] = f["crosswalk"].assign(conf_num=f["crosswalk"].conf_num.astype(str))


def c_domains(f):
    t = f["turns"].copy()
    t.loc[t.index[3], "role_group"] = "legislatr"
    f["turns"] = t


def c_domains_date(f):
    t = f["turns"].copy()
    t.loc[t.index[5], "speech_date"] = "2020-13-01"
    f["turns"] = t


def c_keys(f):
    f["turns"] = pd.concat([f["turns"], f["turns"].iloc[[10]]], ignore_index=True)


def c_contig(f):
    t = f["turns"]
    f["turns"] = t.drop(index=t.index[(t.conf_num == IDS[1]) & (t.turn_seq == 50)])


def c_turns_meetings(f):
    m = f["meetings"].copy()
    m.loc[m.conf_num == IDS[0], "n_turns"] = m.loc[m.conf_num == IDS[0], "n_turns"] + 1
    f["meetings"] = m


def c_turns_meetings_unbuilt(f):
    m = f["meetings"].copy()
    m.loc[m.conf_num == IDS[3], "is_built"] = False
    f["meetings"] = m


def c_turns_text(f):
    t = f["turns"].copy()
    i = t.index[7]
    t.loc[i, "text"] = t.loc[i, "text_raw"] + " 추가된 글자"
    f["turns"] = t


def c_universe(f):
    u = pd.read_parquet(VA.INTERIM / "meeting_universe_api.parquet", columns=list(f["universe"].columns))
    extra = u[~u.CONFER_NUM.isin(f["universe"].CONFER_NUM)].head(1)
    f["universe"] = pd.concat([f["universe"], extra], ignore_index=True)


def c_universe_missing(f):
    m = f["meetings"].copy()
    u = pd.read_parquet(VA.INTERIM / "meeting_universe_api.parquet", columns=list(f["universe"].columns))
    extra = u[~u.CONFER_NUM.isin(f["universe"].CONFER_NUM)].head(1)
    f["universe"] = pd.concat([f["universe"], extra], ignore_index=True)
    row = m[m.conf_num == UNBUILT].copy()
    row["conf_num"] = int(extra.CONFER_NUM.iloc[0])
    row["conf_id"] = extra.CONF_ID.iloc[0]
    f["meetings"] = pd.concat([m, row], ignore_index=True)   # in meetings, not built, never fetched


def c_cov_builder(f):
    c = f["coverage"].copy()
    c.loc[c.index[0], "ok_all"] = False
    f["coverage"] = c


def c_cov_recompute(f):
    t = f["turns"].copy()
    i = t.index[(t.conf_num == IDS[1])][20]
    t.loc[i, "text_raw"] = t.loc[i, "text_raw"][:-3]
    t.loc[i, "text"] = t.loc[i, "text"][:0]
    f["turns"] = t


def c_cov_truncate_both(f):
    """Review repro: text lost between source and turns, with the recorded count lowered to match."""
    t = f["turns"].copy()
    i = t.index[(t.conf_num == IDS[1])][20]
    cut = 3
    old = t.loc[i, "text_raw"]
    t.loc[i, "text_raw"] = old[:-cut]
    t.loc[i, "text"] = t.loc[i, "text"][:0]
    lost = len(re.sub(r"\s+", "", old)) - len(re.sub(r"\s+", "", old[:-cut]))
    f["turns"] = t
    c = f["coverage"].copy()
    c.loc[c.conf_num == IDS[1], "turn_text_raw_chars"] -= lost
    f["coverage"] = c


def c_cov_null_chars(f):
    c = f["coverage"].copy()
    c["turn_text_raw_chars"] = c["turn_text_raw_chars"].astype("float")
    c.loc[c.index[0], "turn_text_raw_chars"] = None
    f["coverage"] = c


def _string_sorted_dyads(t):
    """Dyads built after sorting turn_seq as strings (the v9 defect D1), mapped back to positions."""
    t = t.copy()
    t["so"] = t.turn_seq.astype(str)
    t = t.sort_values(["conf_num", "so"]).reset_index(drop=True)
    rows = []
    for c, g in t.groupby("conf_num", sort=False):
        g = g.reset_index(drop=True)
        for i in range(len(g) - 1):
            a, b = g.iloc[i], g.iloc[i + 1]
            if a.role_group == "legislator" and b.role_group == "nonlegislator":
                rows.append((c, a.turn_seq, b.turn_seq, "question"))
            elif a.role_group == "nonlegislator" and b.role_group == "legislator":
                rows.append((c, b.turn_seq, a.turn_seq, "answer"))
    k = pd.DataFrame(rows, columns=["conf_num", "leg_turn_seq", "wit_turn_seq", "direction"])
    ts = t.set_index(["conf_num", "turn_seq"])
    out = k.copy()
    for side, col in (("leg", "leg_turn_seq"), ("wit", "wit_turn_seq")):
        idx = pd.MultiIndex.from_arrays([k.conf_num, k[col]])
        for c in ("text", "text_raw", "role", "role_group"):
            out[f"{side}_{c}"] = ts.loc[idx, c].to_numpy()
    out["leg_is_chair"] = out.leg_role.eq("chair")
    out["leg_is_procedural"] = False
    out["wit_is_legislator_title"] = False
    return out


def c_dy_adj(f):
    f["dyads"] = _string_sorted_dyads(f["turns"])


def c_dy_ends(f):
    d = f["dyads"].copy()
    i = d.index[d.conf_num == IDS[0]][5]
    d.loc[i, "conf_num"] = IDS[1]          # a pair attributed to another meeting
    f["dyads"] = d


def c_dy_ends_text(f):
    d = f["dyads"].copy()
    i, j = d.index[3], d.index[4]
    d.loc[i, "wit_text"], d.loc[j, "wit_text"] = d.loc[j, "wit_text"], d.loc[i, "wit_text"]
    f["dyads"] = d


def c_dy_recompute(f):
    f["dyads"] = f["dyads"].drop(index=f["dyads"].index[7])


def c_dy_recompute_string(f):
    f["dyads"] = _string_sorted_dyads(f["turns"])


def c_dy_dir(f):
    d = f["dyads"].copy()
    i = d.index[2]
    d.loc[i, "direction"] = "question" if d.loc[i, "direction"] == "answer" else "answer"
    f["dyads"] = d


def c_dy_flags(f):
    d = f["dyads"]
    i = d.index[~d.leg_is_chair.astype(bool)][0]
    f["dyads"] = _flip(d, "leg_is_chair", list(d.index).index(i))


def c_dy_flags_proc(f):
    d = f["dyads"].copy()
    d["leg_is_procedural"] = ~d["leg_is_procedural"].astype(bool)
    f["dyads"] = d


def c_term_window(f):
    m = f["meetings"].copy()
    m.loc[m.conf_num == IDS[3], "date"] = "2019-10-21"      # mislabeled meeting: 21대 meeting dated in 20대
    f["meetings"] = m


def c_dates_meeting(f):
    m = f["meetings"].copy()
    m.loc[m.conf_num == IDS[2], "date"] = "2015-12-10"
    f["meetings"] = m


def c_dates_meeting_speech(f):
    # 30 days before the meeting date: outside [date - 1 day, date + span] and not an appended record
    # (one day before is allowed since 2026-09-28: overnight sittings opened late on the previous day)
    t = f["turns"].copy()
    t.loc[t.index[(t.conf_num == IDS[0])][3], "speech_date"] = "2000-09-16"
    f["turns"] = t


def _dup_meeting(f, new=999001, merge=False):
    t = f["turns"]
    src = t[t.conf_num == IDS[1]].copy()
    if merge:   # re-segment: merge consecutive pairs of turns (md5 of turns changes, sentences do not)
        src = src.reset_index(drop=True)
        g = src.index // 2
        src = src.groupby(g).agg({**{c: "first" for c in src.columns}, "text": " ".join, "text_raw": " ".join})
        src["turn_seq"] = np.arange(1, len(src) + 1, dtype="int32")
    src["conf_num"] = new
    f["turns"] = pd.concat([t, src], ignore_index=True)
    m = f["meetings"]
    row = m[m.conf_num == IDS[1]].copy()
    row["conf_num"] = new
    row["conf_id"] = "999001"
    row["n_turns"] = len(src)
    f["meetings"] = pd.concat([m, row], ignore_index=True)


def c_dup_long(f):
    _dup_meeting(f)


def c_dup_shingle(f):
    _dup_meeting(f, merge=True)


def _small_meeting_pair(f, a=999100, b=999101):
    """Two 3-turn meetings with identical short text (a verbatim copy of a small meeting)."""
    t, m = f["turns"], f["meetings"]
    base = t[t.conf_num == IDS[0]].head(3).copy()
    texts = ["소규모 회의 첫째 발언입니다.", "둘째 발언은 짧습니다.", "셋째 발언으로 마칩니다."]
    rows = []
    for cn in (a, b):
        x = base.copy()
        x["conf_num"] = cn
        x["turn_seq"] = np.arange(1, 4, dtype="int32")
        x["text"] = texts
        x["text_raw"] = texts
        x["agenda_ordinal"] = pd.array([None] * 3, dtype="Int32")
        rows.append(x)
    f["turns"] = pd.concat([t] + rows, ignore_index=True)
    mrow = m[m.conf_num == IDS[0]].copy()
    new = []
    for cn in (a, b):
        r = mrow.copy()
        r["conf_num"] = cn
        r["conf_id"] = str(cn)
        r["n_turns"] = 3
        new.append(r)
    f["meetings"] = pd.concat([m] + new, ignore_index=True)


def c_dup_small_copy(f):
    _small_meeting_pair(f)


def c_domains_sentinel(f):
    t = f["turns"].copy()
    i = t.index[t.role_group == "legislator"][0]
    t.loc[i, "party"] = "nan"
    f["turns"] = t


def c_universe_outside(f):
    """Review repro: a built meeting outside the API universe with an unverifiable conf_id."""
    t, m = f["turns"], f["meetings"]
    x = t[t.conf_num == IDS[0]].head(3).copy()
    x["conf_num"] = 999999
    x["turn_seq"] = np.arange(1, 4, dtype="int32")
    x["text"] = ["바깥 회의 발언 하나.", "바깥 회의 발언 둘.", "바깥 회의 발언 셋."]
    x["text_raw"] = x["text"]
    x["agenda_ordinal"] = pd.array([None] * 3, dtype="Int32")
    f["turns"] = pd.concat([t, x], ignore_index=True)
    r = m[m.conf_num == IDS[0]].copy()
    r["conf_num"] = 999999
    r["conf_id"] = "999999"
    r["n_turns"] = 3
    f["meetings"] = pd.concat([m, r], ignore_index=True)
    nows = lambda s: len(re.sub(r"\s+", "", s or ""))  # noqa: E731
    cov = f["coverage"]
    f["coverage"] = pd.concat([cov, pd.DataFrame({"conf_num": [999999], "n_turns": [3],
                               "turn_text_raw_chars": [sum(nows(s) for s in x.text_raw)], "source": ["xml"], "ok_all": [True]})],
                              ignore_index=True)
    f["dyads"] = DY.build_dyads(f["turns"], f["meetings"])


def c_cw_second_copy_unmarked(f):
    """A second v9 carrier of IDS[0]'s transcript stored as wrong content without a duplicate marker."""
    cw = f["crosswalk"].copy()
    i = cw.index[cw.relation == "duplicate"][0]
    cw.loc[i, ["relation", "duplicate_of_v9_meeting_id", "is_second_copy"]] = ["v9_wrong_content", None, False]
    cw.loc[i, "conf_num"] = IDS[1]
    f["crosswalk"] = cw


def c_ids(f):
    m = f["meetings"].copy()
    m.loc[m.conf_num == IDS[3], "conf_id"] = m.loc[m.conf_num == IDS[3], "conf_id"].str.lstrip("0")
    f["meetings"] = m


def c_ids_v9_namespace(f):
    cw = f["crosswalk"].copy()
    i = cw.index[cw.v9_source == "xlsx"][0]
    cw.loc[i, "v9_meeting_id"] = str(cw.loc[i, "conf_num"])     # a CONF_ID-namespace id replaced by the CONFER_NUM
    f["crosswalk"] = cw


def c_roles(f):
    t = f["turns"].copy()
    i = t.index[t.role == "committee_staff"]
    if len(i) == 0:
        i = t.index[:1]
        t.loc[i, "speaker_pos"] = "전문위원"
    t.loc[i[:1], "role_group"] = "legislator"
    f["turns"] = t


def c_wit_title(f):
    d = f["dyads"].copy()
    k = max(1, int(0.05 * len(d)))
    d.loc[d.index[:k], "wit_is_legislator_title"] = True
    f["dyads"] = d


def c_links(f):
    t = f["turns"].copy()
    leg = t.index[(t.role_group == "legislator") & (t.conf_num == IDS[1])]
    t.loc[leg[: max(1, len(leg) // 10)], "naas_cd"] = None
    f["turns"] = t


def c_ruling(f):
    t = f["turns"].copy()
    i = t.index[t.role_group == "legislator"][0]
    t.loc[i, "ruling_status"] = None
    f["turns"] = t


def c_ruling_acting(f):
    t = f["turns"].copy()
    i = t.index[t.role_group == "legislator"][0]
    t.loc[i, "presidency_state"] = "acting"
    t.loc[i, "ruling_status"] = "opposition"
    f["turns"] = t


def c_links_blank(f):
    """D10: '' stored instead of null must not count as linked."""
    t = f["turns"].copy()
    leg = t.index[t.role_group == "legislator"]
    t.loc[leg[:96], "naas_cd"] = ""
    f["turns"] = t


def c_party_lost(f):
    """party (and hence ruling_status) lost for half of the legislator turns."""
    t = f["turns"].copy()
    leg = t.index[t.role_group == "legislator"]
    t.loc[leg[: len(leg) // 2], ["party", "ruling_status"]] = None
    f["turns"] = t


def c_party_lost_one(f):
    t = f["turns"].copy()
    i = t.index[(t.role_group == "legislator") & t.naas_cd.notna()][0]
    t.loc[i, ["party", "ruling_status"]] = None
    f["turns"] = t


def c_ruling_constant(f):
    """D7: ruling status a per-(term, party) constant: every legislator in the president's party,
    but ruling_status left 'opposition'."""
    t = f["turns"].copy()
    leg = (t.role_group == "legislator") & t.president_party.notna()
    t.loc[leg, "party"] = t.loc[leg, "president_party"]
    t.loc[leg, "ruling_status"] = "opposition"
    f["turns"] = t


def c_ruling_inverted(f):
    t = f["turns"].copy()
    m = t.conf_num == IDS[0]
    t.loc[m & t.ruling_status.eq("ruling"), "ruling_status"] = "x"
    t.loc[m & t.ruling_status.eq("opposition"), "ruling_status"] = "ruling"
    t.loc[m & t.ruling_status.eq("x"), "ruling_status"] = "opposition"
    f["turns"] = t


def c_ruling_one_wrong(f):
    t = f["turns"].copy()
    i = t.index[t.ruling_status.eq("independent")][0]
    t.loc[i, "ruling_status"] = "opposition"
    f["turns"] = t


def c_president_party(f):
    t = f["turns"].copy()
    t.loc[t.conf_num == IDS[3], "president_party"] = "한나라당"     # 2021 meeting, wrong president's party
    f["turns"] = t


def c_president_name(f):
    t = f["turns"].copy()
    t.loc[t.conf_num == IDS[2], "president"] = "이명박"
    f["turns"] = t


def c_presidency(f):
    t = f["turns"].copy()
    t.loc[t.index[0], "presidency_state"] = "suspended"
    f["turns"] = t


def c_presidency_null_nonleg(f):
    t = f["turns"].copy()
    t.loc[t.role_group == "nonlegislator", "presidency_state"] = None
    f["turns"] = t


def c_admin(f):
    t = f["turns"].copy()
    t.loc[t.conf_num == IDS[3], "admin"] = "김대중"    # D8-type: wrong administration for the date
    f["turns"] = t


def c_agenda(f):
    t = f["turns"].copy()
    t.loc[t.index[4], "agenda_ordinal"] = 99
    f["turns"] = t


def c_footer(f):
    ft = f["footer"].copy()
    ft.loc[ft.index[0], "conf_num"] = 424242
    f["footer"] = ft


def c_cw(f):
    f["crosswalk"] = f["crosswalk"].drop(index=f["crosswalk"].index[1])


def c_cw_dup_target(f):
    cw = f["crosswalk"].copy()
    cw.loc[cw.relation == "duplicate", "duplicate_of_v9_meeting_id"] = "nonexistent"
    f["crosswalk"] = cw


def c_cwt(f):
    x = f["crosswalk_turns"].copy()
    a, b = x.index[3], x.index[4]
    x.loc[a, "turn_seq"], x.loc[b, "turn_seq"] = x.loc[b, "turn_seq"], x.loc[a, "turn_seq"]
    f["crosswalk_turns"] = x


def _two_sittings(f, conf=None):
    """Mark the second half of one meeting as sitting 2 in the turns only (dyads left as built, so a
    dyad at the boundary now spans two sittings)."""
    t = f["turns"].copy()
    conf = conf or IDS[1]
    d = f["dyads"]
    dd = d[d.conf_num == conf]
    cut = int(min(dd.leg_turn_seq.iloc[len(dd) // 2], dd.wit_turn_seq.iloc[len(dd) // 2]))   # a dyad's first turn
    t.loc[(t.conf_num == conf) & (t.turn_seq > cut), "sitting_seq"] = 2
    f["turns"] = t


def c_dy_sitting(f):
    _two_sittings(f)


def c_dy_sitting_carried(f):
    d = f["dyads"].copy()
    d["sitting_seq"] = d["sitting_seq"].astype("int16")
    d.loc[d.index[3], "sitting_seq"] = 2
    f["dyads"] = d


def c_turn_sittings_skip(f):
    t = f["turns"].copy()
    t.loc[(t.conf_num == IDS[0]) & (t.turn_seq > 10), "sitting_seq"] = 3      # 1 -> 3 skips sitting 2
    f["turns"] = t


def c_turn_sittings_reset(f):
    t = f["turns"].copy()
    t.loc[(t.conf_num == IDS[0]) & (t.turn_seq == 5), "after_end_marker"] = True   # true, then false again
    f["turns"] = t


def c_after_end_null(f):
    t = f["turns"].copy()
    t["after_end_marker"] = t["after_end_marker"].astype(object)
    t.loc[t.index[4], "after_end_marker"] = None
    f["turns"] = t


def c_schema_after_end_text(f):
    f["turns"] = f["turns"].assign(after_end_marker=f["turns"].after_end_marker.map(str))


def c_label_how_null(f):
    t = f["turns"].copy()
    t.loc[t.index[6], "label_how"] = None
    f["turns"] = t


def c_docs(f, tmp):
    p = tmp / "README.tmpl.md"
    p.write_text("The data have {{ dyads.rows }} dyads and {{ dyads.no_such_number }} other things.", encoding="utf-8")
    return VA.Params(mode="release", docs_templates=(str(p),))


# ------------------------------------------------------------------ task R3 (2026-09-26): slim dyads, partyless, duplicates, paths
def _dyad_row(d, pred=None, k=0):
    idx = d.index if pred is None else d.index[pred]
    return idx[k]


def c_dy_attr_party(f):
    """audit D planted defect 51895: a dyad's leg_party / leg_ruling_status copy altered (turns untouched)."""
    d = f["dyads"].copy()
    i = _dyad_row(d, d.leg_ruling_status.notna())
    d.loc[i, "leg_party"] = "다른정당"
    d.loc[i, "leg_ruling_status"] = "ruling" if d.loc[i, "leg_ruling_status"] != "ruling" else "opposition"
    f["dyads"] = d


def c_dy_attr_wit(f):
    d = f["dyads"].copy()
    i = _dyad_row(d, None, 5)
    d.loc[i, "wit_role"] = "minister" if d.loc[i, "wit_role"] != "minister" else "witness"
    d.loc[i, "wit_name"] = "가나다"
    f["dyads"] = d


def c_dy_attr_meeting(f):
    d = f["dyads"].copy()
    d.loc[d.index[d.conf_num == IDS[0]][:3], "committee_key"] = "defense"
    f["dyads"] = d


def c_dy_attr_flag(f):
    d = f["dyads"].copy()
    d.loc[d.index[7], "any_after_end_marker"] = True
    f["dyads"] = d


def c_dy_flags_proc_one(f):
    """audit D planted defect 51894: ONE leg_is_procedural flag flipped (a 20,000-row sample missed it)."""
    d = f["dyads"].copy()
    i = d.index[-1]
    d.loc[i, "leg_is_procedural"] = not bool(d.loc[i, "leg_is_procedural"])
    f["dyads"] = d


def c_dy_flags_wit_title_one(f):
    d = f["dyads"].copy()
    i = d.index[~d.wit_is_legislator_title.astype(bool)][-1]
    d.loc[i, "wit_is_legislator_title"] = True
    f["dyads"] = d


def c_domains_committee_key(f):
    """audit D planted defect 51893: an invalid committee_key on a meeting with no dyads."""
    m = f["meetings"].copy()
    m.loc[m.conf_num == UNBUILT, "committee_key"] = "no_such_committee"
    f["meetings"] = m


def c_domains_is_sub_null(f):
    """audit D planted defect 51892: is_subcommittee null on a meeting with no dyads."""
    m = f["meetings"].copy()
    m["is_subcommittee"] = m["is_subcommittee"].astype(object)
    m.loc[m.conf_num == UNBUILT, "is_subcommittee"] = None
    f["meetings"] = m


def c_domains_key_vs_type(f):
    m = f["meetings"].copy()
    m.loc[m.conf_num == IDS[0], "committee_key"] = "plenary"      # a standing committee coded as the plenary key
    f["meetings"] = m


def c_president_last_party(f):
    t = f["turns"].copy()
    t.loc[t.conf_num == IDS[2], "president_last_party"] = "더불어민주당"
    f["turns"] = t


def _to_partyless(f, day="2002-06-11"):
    """Move meeting IDS[0] (16대) into 김대중's partyless window (2002-05-06..2003-02-24) with correct values."""
    t, m = f["turns"].copy(), f["meetings"].copy()
    sel = t.conf_num == IDS[0]
    t.loc[sel, "speech_date"] = day
    m.loc[m.conf_num == IDS[0], ["date", "date_end"]] = day
    d = t["speech_date"].fillna(t["conf_num"].map(dict(zip(m.conf_num, m.date))))
    set_presidency(t, d)
    f["turns"], f["meetings"] = t, m
    f["dyads"] = DY.build_dyads(t, m)
    return sel


def c_partyless_old_rule(f):
    """The pre-decision-6 coding: ruling_status null and presidency_state 'normal' in a partyless window."""
    sel = _to_partyless(f)
    t = f["turns"]
    leg = sel & t.role_group.eq("legislator") & t.party.ne("무소속")
    t.loc[leg, "ruling_status"] = None
    t.loc[sel, "presidency_state"] = "normal"
    f["dyads"] = DY.build_dyads(t, f["meetings"])


def c_partyless_successor(f):
    """열린우리당 in 2007-09 (after its merger into 대통합민주신당): the successor is ruling, 열린우리당 is not."""
    sel = _to_partyless(f, "2007-09-12")
    t = f["turns"]
    leg = sel & t.ruling_status.eq("ruling")
    assert leg.any() and set(t.loc[leg, "party"]) == {"대통합민주신당"}
    t.loc[leg, "party"] = "열린우리당"        # the pre-merger label kept as ruling
    f["dyads"] = DY.build_dyads(t, f["meetings"])


def _duplicate_copy(f, new=999050, of=None):
    """A clean duplicate: meeting `new` repeats IDS[1] verbatim and is marked duplicate_of; its turns / dyads
    are in duplicate_turns / duplicate_dyads, not in the release tables."""
    of = of or IDS[1]
    t, m = f["turns"], f["meetings"]
    x = t[t.conf_num == of].copy()
    x["conf_num"] = new
    r = m[m.conf_num == of].copy()
    r["conf_num"] = new
    r["conf_id"] = str(new)
    r["duplicate_of"] = of
    f["meetings"] = pd.concat([m, r], ignore_index=True)
    f["duplicate_turns"] = x
    f["duplicate_dyads"] = DY.build_dyads(x, f["meetings"])
    cov = f["coverage"]
    c = cov[cov.conf_num == of].copy()
    c["conf_num"] = new
    f["coverage"] = pd.concat([cov, c], ignore_index=True)
    return x


def c_dups_in_release(f):
    """A duplicate copy left in the release turns / dyads (only the meetings marker set)."""
    x = _duplicate_copy(f)
    f["turns"] = pd.concat([f["turns"], x], ignore_index=True)
    f["dyads"] = DY.build_dyads(f["turns"], f["meetings"])
    del f["duplicate_turns"], f["duplicate_dyads"]


def c_dups_not_identical(f):
    """duplicate_of pointing at a meeting whose text differs."""
    _duplicate_copy(f)
    m = f["meetings"].copy()
    m.loc[m.conf_num == 999050, "duplicate_of"] = IDS[0]
    f["meetings"] = m


def c_overlap_dangling(f):
    m = f["meetings"].copy()
    m["overlap_with"] = [([424242] if c == IDS[0] else None) for c in m.conf_num]
    f["meetings"] = m


def c_local_path(f):
    cw = f["crosswalk"].copy()
    cw.loc[cw.index[0], "relation_basis"] = "read from /Users/someone/Desktop/x.parquet"
    f["crosswalk"] = cw


CORRUPTIONS = [
    ("schema_turns", c_schema_turns), ("schema_turns", c_schema_turns_missing_enrichment),
    ("schema_meetings", c_schema_meetings), ("schema_dyads", c_schema_dyads),
    ("schema_dyads", c_schema_dyads_meeting_cols), ("dyads_meeting_fields", c_dy_meeting_field),
    ("schema_crosswalk", c_schema_crosswalk), ("domains", c_domains), ("domains", c_domains_date),
    ("keys_unique", c_keys), ("turns_contiguous", c_contig),
    ("turns_meetings_consistency", c_turns_meetings), ("turns_meetings_consistency", c_turns_meetings_unbuilt),
    ("turns_text", c_turns_text), ("universe_accounted", c_universe), ("universe_accounted", c_universe_missing),
    ("coverage_builder", c_cov_builder), ("coverage_recompute", c_cov_recompute),
    ("coverage_recompute", c_cov_null_chars), ("coverage_source_sample", c_cov_truncate_both),
    ("coverage_source_sample", c_cov_recompute),
    ("dyads_adjacent", c_dy_adj), ("dyads_endpoints", c_dy_ends), ("dyads_endpoints", c_dy_ends_text),
    ("dyads_recompute", c_dy_recompute), ("dyads_recompute", c_dy_recompute_string),
    ("dyads_direction", c_dy_dir), ("dyads_flags", c_dy_flags), ("dyads_flags", c_dy_flags_proc),
    ("dates_term_window", c_term_window), ("dates_meeting", c_dates_meeting),
    ("dates_meeting", c_dates_meeting_speech), ("dup_long_turn", c_dup_long), ("dup_shingle", c_dup_shingle),
    ("dup_meeting_text", c_dup_long), ("dup_meeting_text", c_dup_shingle), ("dup_meeting_text", c_dup_small_copy),
    ("domains", c_domains_sentinel), ("universe_accounted", c_universe_outside),
    ("crosswalk_integrity", c_universe_outside), ("crosswalk_integrity", c_cw_second_copy_unmarked),
    ("ids_namespace", c_ids), ("ids_namespace", c_ids_v9_namespace), ("roles_staff", c_roles),
    ("roles_wit_title_share", c_wit_title), ("legislator_links", c_links), ("legislator_links", c_links_blank),
    ("party_coverage", c_party_lost), ("party_coverage", c_party_lost_one),
    ("ruling_null", c_ruling), ("ruling_null", c_ruling_acting),
    ("ruling_recompute", c_ruling_constant), ("ruling_recompute", c_ruling_inverted),
    ("ruling_recompute", c_ruling_one_wrong), ("presidency_by_date", c_ruling_acting),
    ("president_by_date", c_president_party), ("president_by_date", c_president_name),
    ("presidency_by_date", c_presidency), ("presidency_by_date", c_presidency_null_nonleg), ("admin_by_date", c_admin),
    ("agenda_integrity", c_agenda), ("footer_integrity", c_footer), ("crosswalk_integrity", c_cw),
    ("crosswalk_integrity", c_cw_dup_target), ("crosswalk_turns_integrity", c_cwt),
    ("dyads_sitting", c_dy_sitting), ("dyads_sitting", c_dy_sitting_carried), ("dyads_recompute", c_dy_sitting),
    ("turns_sittings", c_turn_sittings_skip), ("turns_sittings", c_turn_sittings_reset),
    ("turns_sittings", c_after_end_null), ("turns_after_end_marker", c_after_end_null),
    ("label_how_weak_share", c_label_how_null), ("schema_turns", c_schema_after_end_text),
    ("dyads_attributes", c_dy_attr_party), ("dyads_attributes", c_dy_attr_wit), ("dyads_attributes", c_dy_attr_meeting),
    ("dyads_attributes", c_dy_attr_flag), ("dyads_meeting_fields", c_dy_attr_meeting),
    ("dyads_flags", c_dy_flags_proc_one), ("dyads_flags", c_dy_flags_wit_title_one),
    ("domains", c_domains_committee_key), ("domains", c_domains_is_sub_null), ("domains", c_domains_key_vs_type),
    ("president_by_date", c_president_last_party),
    ("partyless_windows", c_partyless_old_rule), ("ruling_recompute", c_partyless_old_rule),
    ("presidency_by_date", c_partyless_old_rule), ("partyless_windows", c_partyless_successor),
    ("ruling_recompute", c_partyless_successor),
    ("duplicates_resolved", c_dups_in_release), ("duplicates_resolved", c_dups_not_identical),
    ("dup_meeting_text", c_dups_in_release), ("duplicates_resolved", c_overlap_dangling),
    ("release_no_local_paths", c_local_path),
]


def test_every_check_has_a_corruption_test():
    covered = {c for c, _ in CORRUPTIONS} | {"docs_numbers"}
    assert covered == {c[0] for c in VA.CHECKS}


@pytest.mark.parametrize("cid,fn", CORRUPTIONS, ids=[f"{c}:{fn.__name__}" for c, fn in CORRUPTIONS])
def test_check_fails_on_corruption(fx, cid, fn):
    clean, _ = run(fx, only=[cid])
    assert clean[cid]["status"] == "PASS", clean[cid]
    f = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
    fn(f)
    types = copy.deepcopy(TYPES)
    if fn in (c_schema_turns,):
        types["turns"]["turn_seq"] = "VARCHAR"
    if fn in (c_schema_meetings,):
        types["meetings"]["conf_id"] = "BIGINT"
    if fn in (c_schema_crosswalk,):
        types["crosswalk"]["conf_num"] = "VARCHAR"
    if fn in (c_schema_after_end_text,):
        types["turns"]["after_end_marker"] = "VARCHAR"      # a boolean stored as text
    res, rep = run(f, only=[cid], types=types)
    assert res[cid]["status"] == "FAIL", res[cid]
    assert not rep["ok"]


def test_docs_numbers_fails_on_unknown_placeholder(fx, tmp_path):
    p = c_docs(fx, tmp_path)
    res, rep = run(fx, only=["docs_numbers"], params=p)
    assert res["docs_numbers"]["status"] == "FAIL"
    assert res["docs_numbers"]["details"]["unresolved"][str(tmp_path / "README.tmpl.md")] == ["dyads.no_such_number"]
    ok = tmp_path / "ok.tmpl.md"
    ok.write_text("{{ dyads.rows }} dyads", encoding="utf-8")
    res, _ = run(fx, only=["docs_numbers"], params=VA.Params(docs_templates=(str(ok),)))
    assert res["docs_numbers"]["status"] == "PASS"
    assert VA.render_template("{{ dyads.rows }}", {"dyads.rows": 12345}) == "12,345"
    with pytest.raises(KeyError):
        VA.render_template("{{ x }}", {})


def test_string_sorted_dyads_fixture_is_really_nonadjacent(fx):
    d = _string_sorted_dyads(fx["turns"])
    assert (abs(d.leg_turn_seq - d.wit_turn_seq) != 1).sum() > 0


# ------------------------------------------------------------------ review round 3 repros (full release run)
def _copy(fx):
    return {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}


def test_repro_ruling_inverted_and_wrong_president_party_blocks_release(fx):
    f = _copy(fx)
    c_ruling_constant(f)
    c_president_party(f)
    res, rep = run(f)
    assert not rep["ok"]
    assert res["ruling_recompute"]["status"] == "FAIL" and res["president_by_date"]["status"] == "FAIL"


def test_repro_lost_party_and_null_presidency_blocks_release(fx):
    f = _copy(fx)
    c_party_lost(f)
    c_presidency_null_nonleg(f)
    res, rep = run(f)
    assert not rep["ok"]
    assert res["party_coverage"]["status"] == "FAIL" and res["presidency_by_date"]["status"] == "FAIL"
    assert res["party_coverage"]["details"]["linked_party_missing"] == 242


def test_satellite_party_counts_as_its_parent(fx):
    """더불어시민당 is a satellite of 더불어민주당 (party_lineage.csv): ruling under 문재인."""
    f = _copy(fx)
    t = f["turns"]
    m = (t.conf_num == IDS[3]) & t.ruling_status.eq("opposition")
    assert m.any()
    t.loc[m, "party"] = "더불어시민당"
    t.loc[m, "ruling_status"] = "ruling"
    res, _ = run(f, only=["ruling_recompute"])
    assert res["ruling_recompute"]["status"] == "PASS", res["ruling_recompute"]["details"]
    t.loc[m, "ruling_status"] = "opposition"
    res, _ = run(f, only=["ruling_recompute"])
    assert res["ruling_recompute"]["status"] == "FAIL"


def test_small_copy_missed_by_near_duplicate_checks_caught_by_whole_meeting_check(fx):
    f = _copy(fx)
    c_dup_small_copy(f)
    res, _ = run(f, only=["dup_long_turn", "dup_shingle", "dup_meeting_text"])
    assert res["dup_long_turn"]["status"] == "PASS" and res["dup_shingle"]["status"] == "PASS"
    assert res["dup_meeting_text"]["status"] == "FAIL"
    assert res["dup_meeting_text"]["examples"][0]["a"] == 999100 and res["dup_meeting_text"]["examples"][0]["b"] == 999101
    assert res["dup_meeting_text"]["details"]["n_meetings_below_near_duplicate_thresholds"] >= 2
    al = VA.Params(mode="release", dup_allowlist=((999100, 999101, "test"),))
    res, _ = run(f, only=["dup_meeting_text"], params=al)
    assert res["dup_meeting_text"]["status"] == "PASS" and res["dup_meeting_text"]["details"]["pairs_allowlisted"] == 1


def test_dup_meeting_text_chunked_fingerprints_equal_single_query(fx, monkeypatch):
    """The whole-meeting fingerprints are built in conf_num chunks (full-build OOM, 2026-09-26): one meeting
    per chunk gives the same pairs and counts as one chunk for everything."""
    f = _copy(fx)
    c_dup_small_copy(f)
    out = {}
    for chunk in (1, 2, 10 ** 9):
        monkeypatch.setattr(VA, "DUP_MEETING_CHUNK", chunk)
        res, _ = run(f, only=["dup_meeting_text"])
        r = res["dup_meeting_text"]
        out[chunk] = (r["status"], r["n_bad"], r["details"]["n_meetings_fingerprinted"],
                      [(e["a"], e["b"], e["n_chars"]) for e in r["examples"]])
    assert out[1] == out[2] == out[10 ** 9]
    assert out[1][0] == "FAIL" and (999100, 999101) in [(a, b) for a, b, _ in out[1][3]]


def test_repro_outside_universe_meeting_blocks_release(fx):
    f = _copy(fx)
    c_universe_outside(f)
    res, rep = run(f)
    assert not rep["ok"]
    u = res["universe_accounted"]
    assert u["status"] == "FAIL" and u["details"]["meetings_outside_universe"] == 1
    assert u["details"]["outside_with_conf_id"] == 1 and u["details"]["outside_built_source_not_fetched"] == 1
    assert res["crosswalk_integrity"]["status"] == "FAIL" and res["crosswalk_integrity"]["details"]["meetings_row_absent"] == 1
    assert any(e.get("check") == "meetings_row_absent" for e in res["crosswalk_integrity"]["examples"])


def test_outside_universe_meeting_with_provenance_passes(fx):
    """An id-gap meeting (not in the Open API): in_universe false, no conf_id, fetched, date = printed date,
    and a v10_only crosswalk row."""
    f = _copy(fx)
    c_universe_outside(f)
    m = f["meetings"]
    m["date_printed"] = m["date"]
    m.loc[m.conf_num == 999999, ["conf_id", "in_universe", "v9_meeting_id"]] = [None, False, None]
    f["crawl"] = pd.concat([f["crawl"], pd.DataFrame({"conf_num": [999999], "kind": ["view"], "status": ["ok"]})], ignore_index=True)
    cw = f["crosswalk"]
    f["crosswalk"] = pd.concat([cw, pd.DataFrame({"conf_num": [999999], "relation": ["v10_only"],
                                                  "relation_basis": ["fixture"], "content_verified": [False]})], ignore_index=True)
    types = copy.deepcopy(TYPES)
    types["meetings"]["date_printed"] = "VARCHAR"
    res, _ = run(f, only=["universe_accounted", "crosswalk_integrity", "ids_namespace", "dates_meeting"], types=types)
    assert all(r["status"] == "PASS" for r in res.values()), {k: r["details"] for k, r in res.items() if r["status"] != "PASS"}
    m.loc[m.conf_num == 999999, "date"] = "2001-01-02"          # date no longer equals the printed date
    res, _ = run(f, only=["universe_accounted"], types=types)
    assert res["universe_accounted"]["status"] == "FAIL" and res["universe_accounted"]["details"]["outside_date_unverified"] == 1


def test_crosswalk_failures_carry_examples(fx):
    f = _copy(fx)
    c_cw_second_copy_unmarked(f)
    res, _ = run(f, only=["crosswalk_integrity"])
    r = res["crosswalk_integrity"]
    assert r["status"] == "FAIL" and r["details"]["multi_carrier_without_single_primary"] == 1
    assert {e["check"] for e in r["examples"]} >= {"multi_carrier_without_single_primary"}


def test_repro_text_lost_with_matching_recorded_count(fx):
    """coverage_recompute cannot see it (by design, it compares turns with the builder's own record);
    the raw re-read does."""
    f = _copy(fx)
    c_cov_truncate_both(f)
    res, rep = run(f, only=["coverage_recompute", "coverage_source_sample"])
    assert res["coverage_recompute"]["status"] == "PASS"
    r = res["coverage_source_sample"]
    assert r["status"] == "FAIL" and r["details"]["xml_sampled"] == 4 and r["details"]["xml_differ"] == 1
    clean, _ = run(fx, only=["coverage_source_sample"])
    assert clean["coverage_source_sample"]["details"]["xml_sampled"] == 4


# ------------------------------------------------------------------ sittings / after_end / labels: WARN and pass cases
def test_after_end_and_weak_labels_warn_not_fail(fx):
    f = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
    t = f["turns"]
    last = t[t.conf_num == IDS[0]].turn_seq.max()
    t.loc[(t.conf_num == IDS[0]) & (t.turn_seq >= last - 1), "after_end_marker"] = True
    t.loc[t.index[:50], ["label_how", "label_confidence"]] = ["single_space_pos_name", "low"]
    f["turns"] = t
    f["dyads"] = DY.build_dyads(t, f["meetings"])
    res, rep = run(f, only=["turns_after_end_marker", "label_how_weak_share", "turns_sittings", "dyads_sitting",
                            "dyads_recompute"])
    assert res["turns_after_end_marker"]["status"] == "WARN"
    assert res["turns_after_end_marker"]["details"]["turns_after_end"] == 2
    assert res["label_how_weak_share"]["status"] == "WARN"
    assert res["label_how_weak_share"]["details"]["sources_over_max_share"] == ["xml"]
    assert all(res[c]["status"] == "PASS" for c in ("turns_sittings", "dyads_sitting", "dyads_recompute"))
    assert rep["ok"]


def test_rebuilt_dyads_with_two_sittings_pass(fx):
    """A meeting with two sittings, dyads rebuilt by dyads.py: the boundary pair is absent and every
    dyad / recompute check passes."""
    f = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
    _two_sittings(f)
    f["dyads"] = DY.build_dyads(f["turns"], f["meetings"])
    res, rep = run(f, only=["dyads_sitting", "dyads_recompute", "dyads_endpoints", "turns_sittings"])
    assert all(r["status"] == "PASS" for r in res.values()), res
    assert res["dyads_sitting"]["details"]["meetings_several_sittings"] == 1
    assert len(f["dyads"]) < len(fx["dyads"])


def test_exclude_after_end_marker_param_is_consistent(fx):
    f = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
    t = f["turns"]
    t.loc[(t.conf_num == IDS[1]) & (t.turn_seq > 100), "after_end_marker"] = True
    f["turns"] = t
    f["dyads"] = DY.build_dyads(t, f["meetings"], exclude_after_end_marker=True)
    p = VA.Params(mode="release", dyads_exclude_after_end_marker=True)
    res, _ = run(f, only=["dyads_recompute"], params=p)
    assert res["dyads_recompute"]["status"] == "PASS"
    res, _ = run(f, only=["dyads_recompute"])            # validator told the wrong parameter: mismatch
    assert res["dyads_recompute"]["status"] == "FAIL"


# ------------------------------------------------------------------ task R3 (2026-09-26) scenario tests
def test_clean_partyless_window_passes_and_is_reported(fx):
    f = _copy(fx)
    sel = _to_partyless(f)
    t = f["turns"]
    assert (t.loc[sel, "presidency_state"] == "partyless").all()
    assert set(t.loc[sel & t.ruling_status.eq("ruling"), "party"]) == {"새천년민주당"}
    ids = ["partyless_windows", "ruling_recompute", "ruling_null", "presidency_by_date", "president_by_date",
           "admin_by_date", "dyads_attributes", "dyads_flags", "domains"]
    res, _ = run(f, only=ids)
    bad = {k: (r["status"], r["details"]) for k, r in res.items() if r["status"] != "PASS"}
    assert not bad, json.dumps(bad, ensure_ascii=False, default=str)[:2000]
    w = [x for x in res["partyless_windows"]["details"]["windows"] if x["start"] == "2002-05-06"][0]
    assert w["legislator_turns"] > 0 and w["ruling"] > 0 and w["ruling_mismatch"] == 0 and w["president_last_party"] == "새천년민주당"
    # the legacy rule ('null') is still available: then the same data fail
    res2, _ = run(f, only=["ruling_recompute"], params=VA.Params(mode="release", partyless_rule="null"))
    assert res2["ruling_recompute"]["status"] == "FAIL"


def test_partyless_lineage_successor_is_ruling(fx):
    f = _copy(fx)
    sel = _to_partyless(f, "2007-09-12")
    t = f["turns"]
    assert set(t.loc[sel & t.ruling_status.eq("ruling"), "party"]) == {"대통합민주신당"}
    assert set(t.loc[sel, "president_last_party"]) == {"열린우리당"}
    res, _ = run(f, only=["partyless_windows", "ruling_recompute", "president_by_date"])
    assert all(r["status"] == "PASS" for r in res.values()), {k: r["details"] for k, r in res.items()}


def test_clean_duplicate_copy_passes_and_is_counted(fx):
    f = _copy(fx)
    _duplicate_copy(f)
    # the copy is a synthetic id outside the universe, so the universe / id / crosswalk checks are not run here
    ids = ["duplicates_resolved", "dup_long_turn", "dup_shingle", "dup_meeting_text", "turns_meetings_consistency",
           "coverage_recompute", "turns_contiguous", "dyads_recompute", "dyads_attributes", "dyads_flags", "dyads_endpoints",
           "keys_unique", "schema_dyads", "domains", "dyads_meeting_fields", "release_no_local_paths"]
    res, rep = run(f, only=ids)
    bad = {k: (r["status"], r["details"]) for k, r in res.items() if r["status"] != "PASS"}
    assert not bad, json.dumps(bad, ensure_ascii=False, default=str)[:3000]
    d = res["duplicates_resolved"]["details"]
    assert d["n_duplicates"] == 1 and d["duplicate_text_not_identical"] == 0 and d["release_turns_of_duplicates"] == 0
    # the copy is visible to the accounting checks through the union view, not to the duplicate-content checks
    assert res["dup_meeting_text"]["details"]["pairs_flagged"] == 0
    assert res["turns_meetings_consistency"]["status"] == "PASS"


def test_recorded_overlap_is_flagged_not_blocking(fx):
    f = _copy(fx)
    _dup_meeting(f)                                   # 999001 repeats IDS[1] (not marked duplicate)
    res, _ = run(f, only=["dup_long_turn"])
    assert res["dup_long_turn"]["status"] == "FAIL"
    m = f["meetings"].copy()
    m["overlap_with"] = [([999001] if c == IDS[1] else [IDS[1]] if c == 999001 else None) for c in m.conf_num]
    f["meetings"] = m
    res, _ = run(f, only=["dup_long_turn", "dup_shingle", "duplicates_resolved"])
    assert res["dup_long_turn"]["status"] == "PASS"
    assert res["dup_long_turn"]["details"]["pairs_recorded_overlap"] == 1
    assert res["duplicates_resolved"]["status"] == "PASS"


def test_release_root_scan_finds_paths_and_user_name(fx, tmp_path):
    import getpass
    import pyarrow.parquet as pq
    rel = tmp_path / "release"
    rel.mkdir()
    pq.write_table(arrow(fx["meetings"], TYPES["meetings"]), rel / "meetings.parquet")
    (rel / "crosswalk_stats.json").write_text(json.dumps({"inputs": {"turns": "v10/build/release/turns"}}))
    p = VA.Params(mode="release", release_root=str(rel))
    res, _ = run(fx, only=["release_no_local_paths"], params=p)
    assert res["release_no_local_paths"]["status"] == "PASS" and res["release_no_local_paths"]["details"]["files_scanned"] == 2
    (rel / "crosswalk_stats.json").write_text(json.dumps({"inputs": {"turns": f"/somewhere/{getpass.getuser()}/turns"}}))
    res, _ = run(fx, only=["release_no_local_paths"], params=p)
    r = res["release_no_local_paths"]
    assert r["status"] == "FAIL" and r["examples"][0]["file"] == "crosswalk_stats.json"
    assert getpass.getuser() not in json.dumps(r, ensure_ascii=False)       # the report never echoes the match
    (rel / "crosswalk_stats.json").write_text("{}")
    m = fx["meetings"].copy()
    m.loc[m.index[0], "title"] = "see /private/var/tmp/x"
    pq.write_table(arrow(m, TYPES["meetings"]), rel / "meetings.parquet")
    res, _ = run(fx, only=["release_no_local_paths"], params=p)
    assert res["release_no_local_paths"]["status"] == "FAIL"
    assert res["release_no_local_paths"]["examples"][0]["hits"][0]["column"] == "title"


def test_relativize_removes_local_paths():
    out = VA.relativize({"a": str(VA.REPO / "v10" / "build" / "x.parquet"), "b": "/private/tmp/q/y.json",
                         "c": [f"read_parquet(['{VA.REPO}/data/z.parquet'])"], "d": 3})
    assert out == {"a": "v10/build/x.parquet", "b": "<external>/y.json", "c": ["read_parquet(['data/z.parquet'])"], "d": 3}




def test_dates_meeting_allows_overnight_and_appended(fx):
    """2026-09-28: one day before the meeting date (an overnight sitting opened late on the previous
    day, 57212) and the printed date of an appended record in a later sitting (30742, 42688) are
    WARN, not FAIL; a turn text_raw printed inside the label is WARN; a stage-only turn may have
    text NULL."""
    f = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
    t = f["turns"]
    m = f["meetings"]
    d0 = m.loc[m.conf_num == IDS[0], "date"].iloc[0]
    day_before = (pd.Timestamp(d0) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    t.loc[t.index[(t.conf_num == IDS[0])][3], "speech_date"] = day_before
    res, _ = run(f, only=["dates_meeting"])
    assert res["dates_meeting"]["status"] == "WARN", res["dates_meeting"]
    if {"sitting_seq", "speech_date_how"} <= set(t.columns):
        f2 = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
        t2 = f2["turns"]
        j = t2.index[(t2.conf_num == IDS[0])][3]
        t2.loc[j, "speech_date"] = "1999-01-01"
        t2.loc[j, "sitting_seq"] = 2
        t2.loc[j, "speech_date_how"] = "sub_cover"
        res, _ = run(f2, only=["dates_meeting"])
        assert res["dates_meeting"]["status"] == "WARN", res["dates_meeting"]
    if "has_stage" in t.columns:
        f3 = {k: (x.copy() if isinstance(x, pd.DataFrame) else x) for k, x in fx.items()}
        t3 = f3["turns"]
        j = t3.index[(t3.conf_num == IDS[0])][2]
        t3.loc[j, "text_raw"] = "(웃음)"
        t3.loc[j, "text"] = None
        t3.loc[j, "has_stage"] = True
        res, _ = run(f3, only=["turns_text"])
        assert res["turns_text"]["status"] == "PASS", res["turns_text"]
        t3.loc[j, "text_raw"] = None
        res, _ = run(f3, only=["turns_text"])
        assert res["turns_text"]["status"] == "WARN", res["turns_text"]
