"""Tests for crosswalk.py (run: python3 -m pytest -q test_crosswalk.py)."""
from __future__ import annotations

import random
import sys
from collections import Counter
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import crosswalk as CW  # noqa: E402
import validate as VA  # noqa: E402

SEED = CW.SEED


def _check_links(links, n, m):
    """every i and every j covered; linked pairs monotone."""
    got_i = {i for i, _, _, _, _ in links if i is not None}
    got_j = {j for _, j, _, _, _ in links if j is not None}
    assert got_i == set(range(n)) and got_j == set(range(m))
    pairs = sorted((i, j) for i, j, *_ in links if i is not None and j is not None)
    js = [j for _, j in pairs]
    assert js == sorted(js), "alignment not monotone"
    for i, j, t, s, b in links:
        if i is None:
            assert t == "v10_unmatched"
        elif j is None:
            assert t == "v9_unmatched"


def test_align_identity():
    a = [f"문장{i}가나다" for i in range(30)]
    links = CW.align_sequences(a, a, a, a)
    assert [(i, j, t) for i, j, t, *_ in links] == [(i, i, "exact") for i in range(30)]


def test_align_normalized_vs_exact():
    a_norm, b_norm = ["가나다", "라마바"], ["가나다", "라마바"]
    links = CW.align_sequences(a_norm, ["x", "y"], b_norm, ["x", "z"])
    assert [l[2] for l in links] == ["exact", "normalized"]


def test_align_merge_split_unmatched():
    a = ["가나다라마", "바사아", "자차카타파하", "거너더", "러머버서어", "저처커", "삭제된발언입니다"]
    b = ["가나다라마", "바사", "아", "자차카타파하", "거너더러머버서어", "새로운턴입니다", "저처커"]
    c = Counter()
    links = CW.align_sequences(a, a, b, b, c)
    _check_links(links, len(a), len(b))
    d = {(i, j): t for i, j, t, *_ in links}
    assert d[(1, 1)] == "merge" and d[(1, 2)] == "merge"           # one v9 row = two v10 turns
    assert d[(3, 4)] == "split" and d[(4, 4)] == "split"           # two v9 rows = one v10 turn
    assert (None, 5) in d and d[(None, 5)] == "v10_unmatched"
    assert (6, None) in d and d[(6, None)] == "v9_unmatched"


def test_align_similar_text():
    a = ["오늘 회의는 예산안 심사입니다 장관께서 설명해 주시기 바랍니다", "네"]
    b = ["오늘 회의는 예산안심사입니다 장관께서 설명해주시기 바랍니다", "네"]
    an = [x.replace(" ", "") + "추가" if k == 0 else x for k, x in enumerate(a)]
    bn = [x.replace(" ", "") for x in b]
    links = CW.align_sequences(an, a, bn, b)
    _check_links(links, 2, 2)
    assert links[0][2] == "similar" and links[0][3] >= CW.SIM_MIN


def test_align_random_edits_property():
    rng = random.Random(SEED)
    syll = [chr(0xAC00 + k) for k in range(0, 11172, 37)]
    for trial in range(40):
        a = ["".join(rng.choice(syll) for _ in range(rng.randint(3, 40))) for _ in range(rng.randint(5, 60))]
        b = []
        for x in a:
            r = rng.random()
            if r < 0.08 and len(x) > 4:          # split
                k = rng.randint(1, len(x) - 1)
                b += [x[:k], x[k:]]
            elif r < 0.12:                        # delete
                continue
            elif r < 0.18:                        # modify one char
                k = rng.randrange(len(x))
                b.append(x[:k] + rng.choice(syll) + x[k + 1:])
            else:
                b.append(x)
            if rng.random() < 0.04:               # insert
                b.append("".join(rng.choice(syll) for _ in range(10)))
        links = CW.align_sequences(a, a, b, b)
        _check_links(links, len(a), len(b))


def test_align_xlsx_source_rows():
    a = pd.DataFrame({"meeting_id": "777", "speech_order": ["1", "2", "10", "3"], "so_num": [1, 2, 10, 3],
                      "norm": ["가", "나", "라", "다"], "rawkey": ["k1", "k2", "k10", "k3"]})
    b = pd.DataFrame({"conf_num": 5, "turn_seq": [1, 2, 3], "source": "xlsx", "source_speech_order": ["1", "2", "3"],
                      "norm": ["가", "나", "다"], "rawkey": ["k1", "k2", "k3"]})
    c = Counter()
    df = CW.align_meeting_frames(a, b, c)
    got = {(r.v9_speech_order, None if pd.isna(r.turn_seq) else int(r.turn_seq), r.match_type) for r in df.itertuples()}
    assert got == {("1", 1, "source_row"), ("2", 2, "source_row"), ("3", 3, "source_row"), ("10", None, "v9_unmatched")}
    assert c["source_row_meetings"] == 1


def test_decide_relations_rules():
    v9 = pd.DataFrame({
        "meeting_id": ["100", "101", "102", "103", "104", "105", "106", "107", "108", "109"],
        "v9_source": ["xlsx", "v8_flagged", "v8_unflagged", "v6_html", "xlsx", "v7_pdf", "v8_flagged", "xlsx",
                      "v8_unflagged", "v8_unflagged"],
        "api_CONFER_NUM": [1000, 1001, 1002, None, 1004, 1004, 1006, 1007, 1008, 1009],
        "api_CONF_ID": ["000100", "000101", "000102", None, "000104", "000105", "000106", "000107", "000108", "000109"],
        "audit_verdict": [None, "text=viewer(id)", "text=label", None, None, None, "text=other session", None,
                          "undetermined", "text=label"],
    })
    universe = {1000, 1001, 1002, 1004, 1006, 1007, 1008, 1009, 101, 103}
    # evidence: 100 found in its label meeting; 107 found in another meeting (2000);
    # 108 not found in its built label meeting; 109 partly found in its label meeting (best candidate)
    ev = pd.DataFrame({"meeting_id": ["100", "107", "108", "109", "109"], "fp_n": [50, 40, 30, 60, 60],
                       "conf_num": [1000, 2000, np.nan, 1009, 3000], "shared": [45, 35, np.nan, 12, 2],
                       "share": [0.9, 0.875, np.nan, 0.2, 0.033], "rk": [1, 1, np.nan, 1, 2]})
    built = {1000, 1007, 2000, 1008, 1009, 3000}
    d = CW.decide_relations(v9, ev, built, universe).set_index("meeting_id")
    assert d.loc["100", "relation"] == "same" and bool(d.loc["100", "content_verified"])
    assert d.loc["100", "content_overlap"] == "full"
    assert d.loc["101", "relation"] == "v9_wrong_content" and d.loc["101", "content_conf_num"] == 101
    assert d.loc["102", "relation"] == "same" and not bool(d.loc["102", "content_verified"])
    assert d.loc["103", "relation"] == "v9_wrong_content" and d.loc["103", "content_conf_num"] == 103
    assert d.loc["106", "relation"] == "v9_wrong_content" and pd.isna(d.loc["106", "content_conf_num"])
    assert d.loc["107", "relation"] == "v9_wrong_content" and d.loc["107", "content_conf_num"] == 2000
    assert d.loc["108", "relation"] == "v9_wrong_content" and pd.isna(d.loc["108", "content_conf_num"])
    assert d.loc["108", "relation_basis"] == "content:not_in_label_meeting" and bool(d.loc["108", "content_verified"])
    assert d.loc["109", "relation"] == "same" and d.loc["109", "content_overlap"] == "partial"
    # 104 (xlsx) and 105 (v7) both carry 1004 by label (no v10 text): the XLSX copy is kept
    assert d.loc["104", "relation"] == "same" and d.loc["105", "relation"] == "duplicate"
    assert d.loc["105", "duplicate_of_v9_meeting_id"] == "104"
    # while the label meeting has no v10 text, content found elsewhere does not decide (legit duplicates)
    d3 = CW.decide_relations(v9, ev, built - {1007}, universe).set_index("meeting_id")
    assert d3.loc["107", "relation"] == "same" and "content_in_other_meeting_2000_label_not_built" in d3.loc["107", "relation_basis"]
    # with a complete v10, content missing from the built label meeting -> v9_only
    d2 = CW.decide_relations(v9, ev, built, universe, v10_complete=True).set_index("meeting_id")
    assert d2.loc["108", "relation"] == "v9_only" and pd.isna(d2.loc["108", "content_conf_num"])


def test_fingerprints_ignore_spacing_punctuation_and_stage_parentheticals():
    con = duckdb.connect()
    df = pd.DataFrame({"k": [1, 2], "t": [
        "국방부장관께서는 이 문제에 대해 답변해 주십시오. 예산 집행률이 낮습니다(웃음)!",
        "국방부 장관께서는 이 문제에 대해 답변해주십시오.\n예산 집행률이 낮습니다 (웃음)?"]})
    con.register("t", df)
    r = con.execute(f"SELECT k, list_sort(list(h)) hs FROM ({CW.fingerprint_sql('t', 'k', 't')}) GROUP BY 1 ORDER BY 1").fetchall()
    assert len(r) == 2 and r[0][1] == r[1][1] and len(r[0][1]) == 1   # short second sentence (<15 chars) dropped


def _small_xlsx_meetings(n=3):
    con = duckdb.connect()
    ids = [45443, 39726, 41767, 44146, 45938, 44371]
    q = f"""SELECT meeting_id, api_CONFER_NUM FROM read_parquet('{CW.V9_CROSSWALK}')
            WHERE v9_source = 'xlsx' AND api_CONFER_NUM IN ({','.join(map(str, ids))}) ORDER BY n_speeches LIMIT {n}"""
    return con.execute(q).fetchdf()


def test_build_end_to_end_on_saved_pages(tmp_path):
    import _stubs_dvc as S
    pick = _small_xlsx_meetings(4)
    parts = [S.turns_from_view(S.find_view_page(int(c)), int(c)) for c in pick.api_CONFER_NUM]
    t = pd.concat(parts, ignore_index=True)
    tp = tmp_path / "turns.parquet"
    t.to_parquet(tp, index=False)
    # v9 fingerprints for just these meetings
    fp = tmp_path / "v9fp.parquet"
    con = CW.connect()
    ids = ", ".join(f"'{m}'" for m in pick.meeting_id)
    src = f"(SELECT meeting_id, speech_text FROM read_parquet('{CW.V9_SPEECHES}') WHERE meeting_id IN ({ids}))"
    con.execute(f"COPY ({CW.fingerprint_sql(src, 'meeting_id', 'speech_text')}) TO '{fp}' (FORMAT PARQUET)")
    meetings = S.meetings_from_universe(list(pick.api_CONFER_NUM.astype(int)), t)
    meetings["is_built"] = True
    mp = tmp_path / "meetings.parquet"
    meetings.to_parquet(mp, index=False)
    st = CW.build([str(tp)], str(mp), out_dir=tmp_path / "cw", v9fp_path=fp)
    cwm = pd.read_parquet(tmp_path / "cw" / "crosswalk_meetings.parquet")
    cwt = pd.read_parquet(tmp_path / "cw" / "crosswalk_turns.parquet")
    n_v9 = duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{CW.V9_CROSSWALK}')").fetchone()[0]
    assert cwm.v9_meeting_id.notna().sum() == n_v9
    sel = cwm.set_index("v9_meeting_id").loc[pick.meeting_id]
    assert (sel.relation == "same").all() and sel.content_verified.all()
    assert (sel.turn_alignment == "aligned").all()
    assert set(cwt.v9_meeting_id) == set(pick.meeting_id)
    # most rows align one to one on these XLSX/XML pairs
    one = cwt.match_type.isin(["exact", "normalized", "similar"]).mean()
    assert one > 0.9, cwt.match_type.value_counts()
    # the validator accepts the crosswalk tables (dev mode; only crosswalk checks)
    v9s = f"(SELECT meeting_id, speech_order FROM read_parquet('{CW.V9_SPEECHES}') WHERE meeting_id IN ({ids}))"
    v9s_df = duckdb.connect().execute(v9s).fetchdf()
    tables = {"crosswalk": str(tmp_path / "cw" / "crosswalk_meetings.parquet"),
              "crosswalk_turns": str(tmp_path / "cw" / "crosswalk_turns.parquet"),
              "turns": str(tp), "v9_speeches": v9s_df, "v9_meetings": str(CW.V9_CROSSWALK),
              "universe": str(CW.UNIVERSE)}
    rep = VA.Validator(tables, VA.Params(mode="dev")).run(only=["schema_crosswalk", "crosswalk_turns_integrity", "domains"])
    res = {c["id"]: c for c in rep["checks"]}
    assert res["schema_crosswalk"]["status"] == "PASS", res["schema_crosswalk"]
    assert res["crosswalk_turns_integrity"]["status"] == "PASS", res["crosswalk_turns_integrity"]
    assert res["domains"]["status"] == "PASS", res["domains"]


def test_second_copies_and_reverse_containment_and_label_outside_universe():
    """review round 3: every carrier of an already-carried transcript is marked (also v9_wrong_content rows);
    a label accepted only by reverse containment is not 'full'; a label outside the universe is flagged."""
    v9 = pd.DataFrame({
        "meeting_id": ["200", "201", "202", "203", "204", "205"],
        "v9_source": ["xlsx", "v8_flagged", "v6_html", "v8_flagged", "v7_pdf", "xlsx"],
        "api_CONFER_NUM": [2000, 2100, None, 2300, 2400, 41344],
        "api_CONF_ID": ["000200", "000201", None, "000203", "000204", None],
        "audit_verdict": [None, "text=viewer(id)", None, None, None, None],
    })
    universe = {2000, 2100, 2300, 2400, 201, 202}
    ev = pd.DataFrame({
        "meeting_id": ["200", "201", "202", "203", "204"],
        "fp_n": [100, 80, 60, 90, 448],
        "conf_num": [2000, 2000, 2000, 2300, 2400], "shared": [95, 70, 55, 88, 43],
        "share_fwd": [0.95, 0.875, 0.917, 0.978, 0.096], "share_rev": [0.95, 0.74, 0.58, 0.9, 1.0],
        "share": [0.95, 0.875, 0.917, 0.978, 1.0], "rk": [1, 1, 1, 1, 1]})
    built = {2000, 2100, 2300, 2400}
    d = CW.decide_relations(v9, ev, built, universe).set_index("meeting_id")
    # 200 carries 2000 as its own label; 201 (v8, label 2100) and 202 (v6, no label) carry 2000 too
    assert d.loc["200", "relation"] == "same" and not d.loc["200", "is_second_copy"]
    assert d.loc["201", "relation"] == "v9_wrong_content" and d.loc["201", "content_conf_num"] == 2000
    assert bool(d.loc["201", "is_second_copy"]) and d.loc["201", "duplicate_of_v9_meeting_id"] == "200"
    assert d.loc["202", "relation"] == "v9_wrong_content" and d.loc["202", "duplicate_of_v9_meeting_id"] == "200"
    assert int(d.loc["200", "n_v9_carriers"]) == 3
    assert not bool(d.loc["203", "is_second_copy"]) and d.loc["203", "content_overlap"] == "full"
    # 204: label meeting fully inside a much larger v9 transcript
    assert d.loc["204", "relation"] == "same" and d.loc["204", "content_overlap"] == "v10_in_v9"
    assert d.loc["204", "relation_basis"].startswith("content:label_contained_in_v9")
    assert bool(d.loc["204", "label_match"])
    assert abs(d.loc["204", "content_share_fwd"] - 0.096) < 1e-9 and d.loc["204", "content_share_rev"] == 1.0
    # 205: label 41344 is not in the universe
    assert d.loc["205", "relation"] == "same" and d.loc["205", "relation_basis"].endswith("|label_not_in_universe")


def test_build_v10_only_rows_include_meetings_outside_universe(tmp_path):
    import _stubs_dvc as S
    pick = _small_xlsx_meetings(1)
    c = int(pick.api_CONFER_NUM.iloc[0])
    t = S.turns_from_view(S.find_view_page(c), c)
    tp = tmp_path / "turns.parquet"
    t.to_parquet(tp, index=False)
    meetings = S.meetings_from_universe([c], t)
    extra = meetings.copy()
    extra["conf_num"] = 999_999_001          # an id-gap meeting (not in the Open API universe)
    extra["conf_id"] = None
    meetings = pd.concat([meetings, extra], ignore_index=True)
    meetings["is_built"] = True
    mp = tmp_path / "meetings.parquet"
    meetings.to_parquet(mp, index=False)
    fp = tmp_path / "v9fp.parquet"
    con = CW.connect()
    src = f"(SELECT meeting_id, speech_text FROM read_parquet('{CW.V9_SPEECHES}') WHERE meeting_id = '{pick.meeting_id.iloc[0]}')"
    con.execute(f"COPY ({CW.fingerprint_sql(src, 'meeting_id', 'speech_text')}) TO '{fp}' (FORMAT PARQUET)")
    st = CW.build([str(tp)], str(mp), out_dir=tmp_path / "cw", v9fp_path=fp, align=False)
    cwm = pd.read_parquet(tmp_path / "cw" / "crosswalk_meetings.parquet")
    r = cwm[cwm.conf_num == 999_999_001]
    assert len(r) == 1 and r.relation.iloc[0] == "v10_only" and r.in_universe.iloc[0] == False  # noqa: E712
    assert r.v10_status.iloc[0] == "built"
    assert st["v10_only_outside_universe"] == 1
    uni = set(pd.read_parquet(CW.UNIVERSE, columns=["CONFER_NUM"]).CONFER_NUM.astype(int))
    seen = set(cwm.conf_num.dropna().astype(int)) | set(cwm.content_conf_num.dropna().astype(int))
    assert uni <= seen


# ------------------------------------------------------------------ task R3 (2026-09-26)

def test_v9_xlsx_rows_align_to_hwp_turns_for_18dae():
    """Researcher decision 5: 18대 is built from HWP for all 4,270 meetings, so the v9 XLSX rows of an 18대
    meeting are aligned to the HWP turns by sequence alignment (no source_row link)."""
    import build_turns as bt
    cw = pd.read_parquet(CW.V9_CROSSWALK)
    x = cw[(cw.v9_source == "xlsx") & (cw.term == 18) & (cw.api_CONFER_NUM == 31920)]
    assert len(x) == 1
    mid, cn = x.meeting_id.iloc[0], 31920
    raw = CW.V10 / "raw" / "hwp" / f"{cn // 1000:03d}" / f"{cn}.hwp"
    if not raw.exists():
        pytest.skip("raw HWP not saved")
    res = bt.hwp_extract(cn, raw.read_bytes(), term=18)
    t = pd.DataFrame([{"conf_num": cn, "turn_seq": int(r["turn_seq"]), "source": "hwp", "source_speech_order": None,
                       "text_raw": r["text_raw"]} for r in res["tables"]["turns"]])
    con = CW.connect()
    con.register("_t", t)
    fr, st, cnt = CW.align_turns(con, pd.DataFrame({"v9_meeting_id": [mid], "conf_num": [cn]}), "_t")
    assert "source_row" not in set(fr.match_type)
    n_v9 = con.execute(f"SELECT count(*) FROM read_parquet('{CW.V9_SPEECHES}') WHERE meeting_id = '{mid}'").fetchone()[0]
    linked = fr[fr.match_type.isin(["exact", "normalized", "similar", "merge", "split"])]
    assert linked.v9_speech_order.nunique() == n_v9 and len(t) == n_v9
    assert cnt["meetings_by_v10_source:hwp"] == 1 and cnt["aligned_meetings"] == 1
    assert cnt["match_type_by_v10_source:hwp:exact"] > 0.8 * n_v9


def test_xlsx_built_turns_still_link_by_source_row():
    a = pd.DataFrame({"meeting_id": "778", "speech_order": ["1", "2"], "so_num": [1, 2],
                      "norm": ["가", "나"], "rawkey": ["k1", "k2"]})
    b = pd.DataFrame({"conf_num": 6, "turn_seq": [1, 2], "source": "xlsx", "source_speech_order": ["1", "2"],
                      "norm": ["가", "나"], "rawkey": ["k1", "k2"]})
    c = Counter()
    CW.align_meeting_frames(a, b, c)
    assert c["meetings_by_v10_source:xlsx_source_row"] == 1 and c["match_type_by_v10_source:xlsx_source_row:source_row"] == 2
    b2 = b.assign(source="hwp", source_speech_order=None)
    c2 = Counter()
    df = CW.align_meeting_frames(a, b2, c2)
    assert set(df.match_type) == {"exact"} and c2["meetings_by_v10_source:hwp"] == 1


def test_build_marks_duplicate_copy(tmp_path):
    import _stubs_dvc as S
    pick = _small_xlsx_meetings(1)
    c = int(pick.api_CONFER_NUM.iloc[0])
    t = S.turns_from_view(S.find_view_page(c), c)
    tp = tmp_path / "turns.parquet"
    t.to_parquet(tp, index=False)
    meetings = S.meetings_from_universe([c], t)
    copy = meetings.copy()
    copy["conf_num"] = 999_999_002
    copy["conf_id"] = None
    meetings = pd.concat([meetings, copy], ignore_index=True)
    meetings["is_built"] = True
    meetings["duplicate_of"] = pd.array([None, c], dtype="Int64")
    mp = tmp_path / "meetings.parquet"
    meetings.to_parquet(mp, index=False)
    fp = tmp_path / "v9fp.parquet"
    con = CW.connect()
    src = f"(SELECT meeting_id, speech_text FROM read_parquet('{CW.V9_SPEECHES}') WHERE meeting_id = '{pick.meeting_id.iloc[0]}')"
    con.execute(f"COPY ({CW.fingerprint_sql(src, 'meeting_id', 'speech_text')}) TO '{fp}' (FORMAT PARQUET)")
    st = CW.build([str(tp)], str(mp), out_dir=tmp_path / "cw", v9fp_path=fp, align=False)
    cwm = pd.read_parquet(tmp_path / "cw" / "crosswalk_meetings.parquet")
    r = cwm[cwm.conf_num == 999_999_002]
    assert len(r) == 1 and r.v10_status.iloc[0] == "duplicate_copy" and int(r.v10_duplicate_of.iloc[0]) == c
    assert cwm.loc[cwm.conf_num == c, "v10_status"].eq("built").all()
    assert st["duplicate_copies_in_meetings"] == 1
