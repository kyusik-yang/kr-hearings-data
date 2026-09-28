"""Tests for dyads.py (run: python3 -m pytest -q test_dyads.py)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import dyads  # noqa: E402
import legacy_rules as LR  # noqa: E402

OUT = dyads.OUT_DIR


def _t(conf, groups, roles=None, texts=None, pos=None, seqs=None):
    n = len(groups)
    seqs = list(range(1, n + 1)) if seqs is None else seqs
    roles = roles or [{"legislator": "legislator", "nonlegislator": "minister", "excluded": "committee_staff",
                       None: None}[g] for g in groups]
    texts = texts or [f"text {conf}-{s}" for s in seqs]
    pos = pos or [{"legislator": "위원", "nonlegislator": "장관", "excluded": "전문위원", None: None}[g] for g in groups]
    return pd.DataFrame({
        "conf_num": np.int64(conf), "turn_seq": np.array(seqs, dtype="int32"), "role_group": groups,
        "role": roles, "text": texts, "text_raw": [t + " (stage)" for t in texts], "speaker_pos": pos,
        "speaker_label_raw": [f"{p} 홍길동" if p else None for p in pos],
        "party": ["정당A" if g == "legislator" else None for g in groups],
        "term": np.int16(21), "hearing_type": "상임위원회",
    })


def _keys(d):
    return sorted(zip(d.conf_num.astype(int), d.leg_turn_seq.astype(int), d.wit_turn_seq.astype(int), d.direction))


def test_adjacency_excluded_breaks():
    t = _t(1, ["legislator", "nonlegislator", "nonlegislator", "legislator", "excluded", "nonlegislator", "legislator"])
    d = dyads.build_dyads(t)
    assert _keys(d) == [(1, 1, 2, "question"), (1, 4, 3, "answer"), (1, 7, 6, "answer")]


def test_null_role_group_breaks_adjacency_and_is_counted():
    t = _t(1, ["legislator", None, "nonlegislator", "legislator"])
    d, st = dyads.build_dyads(t, return_stats=True)
    assert _keys(d) == [(1, 4, 3, "answer")]
    assert st["n_role_group_null"] == 1


def test_no_cross_meeting_pairs():
    a = _t(1, ["nonlegislator", "legislator"])
    b = _t(2, ["nonlegislator", "legislator"])
    d = dyads.build_dyads(pd.concat([a, b], ignore_index=True))
    assert _keys(d) == [(1, 2, 1, "answer"), (2, 2, 1, "answer")]


def test_numeric_not_string_order_and_row_order_irrelevant():
    groups = ["legislator", "nonlegislator"] * 6  # seq 1..12; string sort would put 10,11,12 after 1
    t = _t(5, groups)
    shuffled = t.sample(frac=1.0, random_state=dyads.SEED).reset_index(drop=True)
    d1, d2 = dyads.build_dyads(t), dyads.build_dyads(shuffled)
    assert _keys(d1) == _keys(d2)
    assert len(d1) == 11
    assert all(abs(a - b) == 1 for a, b in zip(d1.leg_turn_seq, d1.wit_turn_seq))
    assert (9, 10) in {(min(a, b), max(a, b)) for a, b in zip(d1.leg_turn_seq, d1.wit_turn_seq)}


def test_gap_in_turn_seq_is_not_bridged():
    t = _t(3, ["legislator", "nonlegislator"], seqs=[1, 3])
    d, st = dyads.build_dyads(t, return_stats=True)
    assert len(d) == 0 and st["n_meetings_noncontiguous"] == 1


def test_duplicate_positions_raise():
    t = _t(1, ["legislator", "nonlegislator"], seqs=[1, 1])
    with pytest.raises(ValueError):
        dyads.build_dyads(t)


def test_columns_prefixes_texts_and_direction():
    t = _t(1, ["legislator", "nonlegislator", "legislator"])
    d = dyads.build_dyads(t)
    # slim release layout (decision 8): exactly these columns, in this order, no double prefixes
    assert list(d.columns) == list(dyads.SLIM_COLUMNS) and len(d.columns) == 39
    assert not [c for c in d.columns if c.startswith(("leg_leg_", "wit_leg_", "leg_wit_", "wit_wit_"))]
    for c in ("leg_text_raw", "wit_text_raw", "wit_party", "leg_role_group", "leg_term"):
        assert c not in d.columns, c
    assert (d.direction.eq("question") == (d.leg_turn_seq < d.wit_turn_seq)).all()
    r = d.set_index("leg_turn_seq")
    assert r.loc[1, "leg_text"] == "text 1-1" and r.loc[1, "wit_text"] == "text 1-2"
    assert r.loc[3, "wit_role"] == "minister" and r.loc[3, "leg_party"] == "정당A"
    assert d.conf_num.dtype == "int64" and d.leg_turn_seq.dtype == "int32"
    assert d.term.dtype == "int16"


def test_chair_and_procedural_flags():
    texts = ["다음은 존경하는 김철수 위원님 질의해 주시기 바랍니다.", "답변드리겠습니다. 그 사안은 검토 중입니다.",
             "예.", "답변 중입니다.", "장관님, 이 예산이 왜 두 배로 늘었는지 설명해 보십시오. 작년 결산과 맞지 않습니다.",
             "네, 그렇습니다."]
    roles = ["chair", "minister", "chair", "minister", "legislator", "minister"]
    groups = ["legislator", "nonlegislator"] * 3
    t = _t(1, groups, roles=roles, texts=texts, pos=["위원장", "장관", "위원장", "장관", "위원", "장관"])
    d = dyads.build_dyads(t).set_index(["leg_turn_seq", "wit_turn_seq"])
    assert bool(d.loc[(1, 2), "leg_is_chair"]) and bool(d.loc[(1, 2), "leg_is_procedural"])
    assert bool(d.loc[(3, 2), "leg_is_procedural"])         # chair filler '예.' counts
    assert bool(d.loc[(3, 4), "leg_is_chair"])
    assert not bool(d.loc[(5, 4), "leg_is_procedural"])     # substantive question
    assert not bool(d.loc[(5, 4), "leg_is_chair"])


def test_procedural_patterns_examples():
    pos = ["다음은 김철수 위원님 질의해 주십시오.", "홍길동 위원님 질의하십시오.", "수고하셨습니다.",
           "이상으로 오늘 회의를 종료하겠습니다.", "산회를 선포합니다.", "장관님 답변해 주십시오.",
           "시간이 다 됐습니다."]
    neg = ["이 사업의 예산 집행률이 30%밖에 안 되는데 그 이유가 무엇입니까?",
           "위원님 말씀에 동의하기 어렵습니다. 법 개정이 필요합니다.", "", None]
    assert all(dyads.procedural_flags(pos, [True] * len(pos)))
    assert not any(dyads.procedural_flags(neg, [True] * len(neg)))
    # filler counts only for the chair
    assert dyads.procedural_flags(["예."], [True]) == [True]
    assert dyads.procedural_flags(["예."], [False]) == [False]
    # known recall gap of the frozen patterns (documented, not tuned after measurement)
    assert dyads.procedural_flags(["이상으로 오늘 회의를 마치겠습니다."], [True]) == [False]
    # long text is never procedural
    assert dyads.procedural_flags(["수고하셨습니다. " * 60], [True]) == [False]


def test_fast_procedural_equals_reference_on_real_texts():
    s = pd.concat([pd.read_csv(OUT / f"procedural_precision_sample_round{r}.csv") for r in (1, 2, 3)])
    texts = s["leg_text"].tolist()
    ch = s["leg_is_chair"].astype(bool).tolist()
    # add non-flagged real-ish variants: witness texts are never procedural questions
    texts += s["leg_text"].str.slice(0, 20).tolist()
    ch += [False] * len(s)
    assert dyads.procedural_flags(texts, ch) == dyads.procedural_flags(texts, ch, reference=True)


def test_frozen_regex_reproduces_round3_sample():
    """The precision figure was measured on round 3 with the frozen patterns: every sampled dyad
    must still be flagged (guards against editing the patterns after measurement)."""
    s = pd.read_csv(OUT / "procedural_precision_sample_round3.csv")
    flags = dyads.procedural_flags(s["leg_text"].tolist(), s["leg_is_chair"].astype(bool).tolist())
    assert all(flags)
    summ = json.loads((OUT / "procedural_precision_summary.json").read_text())
    assert summ["round3"]["n"] == 200 and summ["round3"]["true_procedural"] == int((s.hand_label == "procedural").sum())
    # the headline is the strict figure: borderline notes count as not procedural (review round 3)
    border = s.hand_note.fillna("").astype(str).str.lower().str.startswith("borderline") & (s.hand_label == "procedural")
    strict = (int((s.hand_label == "procedural").sum()) - int(border.sum())) / len(s)
    assert summ["headline"]["precision_strict"] == round(strict, 4) and summ["headline"]["round"] == 3
    assert any("Recall is not measured" in c for c in summ["caveats"])


def test_wit_legislator_title_flag():
    t = _t(1, ["legislator", "nonlegislator", "legislator", "nonlegislator", "legislator"],
           pos=["위원", "위원", "위원장", "국방부장관", "위원"])
    t.loc[3, "speaker_pos"] = None
    t.loc[3, "speaker_label_raw"] = "소위원장 홍길동"
    d = dyads.build_dyads(t).set_index(["leg_turn_seq", "wit_turn_seq"])
    assert bool(d.loc[(1, 2), "wit_is_legislator_title"])
    assert bool(d.loc[(5, 4), "wit_is_legislator_title"])   # label fallback
    t2 = _t(2, ["legislator", "nonlegislator"], pos=["위원", "국방부장관"])
    assert not dyads.build_dyads(t2)["wit_is_legislator_title"].any()


def test_file_builder_equals_in_memory(tmp_path):
    t = pd.concat([_t(1, ["legislator", "nonlegislator"] * 4), _t(2, ["nonlegislator", "legislator", "excluded"])],
                  ignore_index=True)
    p = tmp_path / "turns.parquet"
    t.to_parquet(p, index=False)
    st = dyads.build_dyads_file(str(p), tmp_path / "d.parquet")
    a = pd.read_parquet(tmp_path / "d.parquet")
    b = dyads.build_dyads(t)
    assert _keys(a) == _keys(b) and st["n_dyads"] == len(b)
    assert list(a.columns) == list(b.columns)


def test_independent_recomputation_on_real_pages():
    import _stubs_dvc as S
    ids = [45443, 46213, 52419, 23949, 29275]
    parts = []
    for c in ids:
        p = S.find_view_page(c)
        if p is None:
            continue
        parts.append(S.stub_roles(S.turns_from_view(p, c)))
    if not parts:
        pytest.skip("no saved view pages")
    t = pd.concat(parts, ignore_index=True)
    d = dyads.build_dyads(t)
    # independent: shift within meeting
    t = t.sort_values(["conf_num", "turn_seq"])
    nxt = t.groupby("conf_num").shift(-1)
    g0, g1 = t.role_group, nxt.role_group
    m = ((g0 == "legislator") & (g1 == "nonlegislator")) | ((g0 == "nonlegislator") & (g1 == "legislator"))
    exp = set()
    for (cn, s0, s1, a) in zip(t.conf_num[m], t.turn_seq[m], nxt.turn_seq[m], g0[m]):
        exp.add((int(cn), int(s0), int(s1), "question") if a == "legislator" else (int(cn), int(s1), int(s0), "answer"))
    assert set(_keys(d)) == exp and len(d) == len(exp) > 100
    # texts carried verbatim
    tt = t.set_index(["conf_num", "turn_seq"])
    sample = d.sample(n=min(50, len(d)), random_state=dyads.SEED)
    for r in sample.itertuples():
        assert r.leg_text == tt.loc[(r.conf_num, r.leg_turn_seq), "text"]
        assert r.wit_text == tt.loc[(r.conf_num, r.wit_turn_seq), "text"]


# ------------------------------------------------------------------ legacy

def test_legacy_role_sets_match_legacy_rules():
    assert set(dyads.LEG_ROLES_V9) == set(LR.LEG_ROLES)
    assert set(dyads.NONLEG_ROLES_V9) == set(LR.NONLEG_ROLES)


def _v9_frame(mid, roles, orders):
    n = len(roles)
    df = pd.DataFrame({c: [None] * n for c in dyads.V9_SPEECH_COLS})
    df["meeting_id"] = mid
    df["role"] = roles
    df["speech_order"] = [str(o) for o in orders]
    df["speech_text"] = [f"{mid}:{o}" for o in orders]
    df["person_name"] = [f"p{o}" for o in orders]
    df["term"] = 20
    df["seniority"] = 1
    df["dual_office"] = None
    return df


@pytest.mark.parametrize("order", ["lexicographic", "numeric"])
def test_legacy_matches_legacy_rules_loop(order):
    rng = np.random.default_rng(dyads.SEED)
    roles_pool = ["legislator", "chair", "minister", "witness", "committee_staff", "other"]
    frames = []
    for k, mid in enumerate(["100", "20", "3"]):
        n = 25
        frames.append(_v9_frame(mid, list(rng.choice(roles_pool, n)), list(range(1, n + 1))))
    sp = pd.concat(frames, ignore_index=True).sample(frac=1.0, random_state=dyads.SEED)
    got = dyads.legacy_v9_dyads(sp, order=order)
    exp = []
    for mid in sorted(sp.meeting_id.unique()):
        rows = sp[sp.meeting_id == mid].to_dict("records")
        for li, wi, dr in LR.build_dyad_index_pairs(rows, order=order):
            exp.append((mid, rows[li]["speech_text"], rows[wi]["speech_text"], dr))
    assert list(zip(got.meeting_id, got.leg_speech, got.witness_speech, got.direction)) == exp


def test_legacy_string_sort_pairs_nonadjacent():
    sp = _v9_frame("7", ["legislator", "minister"] * 6, list(range(1, 13)))
    lex = dyads.legacy_v9_dyads(sp, "lexicographic")
    num = dyads.legacy_v9_dyads(sp, "numeric")
    assert len(num) == 11
    pairs = {(a, b) for a, b in zip(lex.leg_speech, lex.witness_speech)}
    assert ("7:1", "7:10") in pairs  # string order 1,10,11,12,2,...


def test_legacy_bit_for_bit_on_published_v9_small():
    r = dyads.verify_legacy_v9(n_meetings=8)
    assert r["all_rows_equal_in_file_order"] and r["meetings_exact"] == 8


def test_recorded_legacy_verification_100_meetings():
    r = json.loads((OUT / "verify_legacy_v9.json").read_text())
    assert r["n_meetings"] == 100 and r["meetings_exact"] == 100 and r["all_rows_equal_in_file_order"]
    assert r["n_legacy_dyads"] == r["n_published_dyads"]


# ------------------------------------------------------------------ meeting-level columns, chunking (review round 3)
def _meetings(confs):
    return pd.DataFrame({"conf_num": np.array(confs, dtype="int64"), "conf_id": [f"{c:06d}" for c in confs],
                         "v9_meeting_id": [str(c) for c in confs], "term": np.int16(21), "class_name": "상임위원회",
                         "hearing_type": "상임위원회", "is_subcommittee": False, "committee_raw": "국방위원회",
                         "subcommittee": None, "committee_key": "국방", "date": "2021-03-02", "source": "xml",
                         "title": "x", "audited_agencies": [[] for _ in confs]})


def test_meeting_level_columns_joined_from_meetings(tmp_path):
    t = pd.concat([_t(1, ["legislator", "nonlegislator"] * 3), _t(2, ["nonlegislator", "legislator"])], ignore_index=True)
    t = t.drop(columns=["term", "hearing_type"])          # enrichment output carries no meeting columns
    m = _meetings([1])                                     # meeting 2 has no meetings row
    d, st = dyads.build_dyads(t, m, return_stats=True)
    assert dyads.MEETING_LEVEL_COLS == ("term", "date", "hearing_type", "class_name", "committee_key", "is_subcommittee")
    for c in dyads.MEETING_LEVEL_COLS:
        assert c in d.columns, c
        assert "leg_" + c not in d.columns and "wit_" + c not in d.columns
    assert st["meeting_cols_missing"] == [] and st["n_dyads_meeting_missing"] == 1
    r = d[d.conf_num == 1]
    assert (r.hearing_type == "상임위원회").all() and (r.date == "2021-03-02").all() and (r.committee_key == "국방").all()
    assert d.loc[d.conf_num == 2, "hearing_type"].isna().all()   # kept, not dropped
    assert d.term.dtype == "Int16" or str(d.term.dtype) in ("int16", "Int16")
    # without meetings: the missing columns are reported and null
    d0, st0 = dyads.build_dyads(t, return_stats=True)
    assert "hearing_type" in st0["meeting_cols_missing"] and d0["hearing_type"].isna().all()
    assert list(d0.columns) == list(dyads.SLIM_COLUMNS)
    # file builder: same rows and columns
    tp, mp = tmp_path / "t.parquet", tmp_path / "m.parquet"
    t.to_parquet(tp, index=False)
    m.to_parquet(mp, index=False)
    stf = dyads.build_dyads_file(str(tp), tmp_path / "d.parquet", meetings_path=str(mp))
    f = pd.read_parquet(tmp_path / "d.parquet")
    assert list(f.columns) == list(d.columns) and _keys(f) == _keys(d)
    assert stf["n_dyads_meeting_missing"] == 1 and stf["meeting_cols_missing"] == []
    assert f.loc[f.conf_num == 1, "committee_key"].eq("국방").all()


def test_chunked_file_builder_is_chunk_invariant(tmp_path):
    rng = np.random.default_rng(dyads.SEED)
    parts = []
    for c in range(1, 13):
        n = int(rng.integers(2, 30))
        parts.append(_t(c, list(rng.choice(["legislator", "nonlegislator", "excluded"], n))))
    t = pd.concat(parts, ignore_index=True).sample(frac=1.0, random_state=dyads.SEED)
    p = tmp_path / "t.parquet"
    t.to_parquet(p, index=False)
    outs = []
    for k, ch in enumerate((10**9, 25, 1)):
        st = dyads.build_dyads_file(str(p), tmp_path / f"d{k}.parquet", chunk_turns=ch)
        outs.append((st, pd.read_parquet(tmp_path / f"d{k}.parquet")))
    assert outs[0][0]["n_chunks"] == 1 and outs[2][0]["n_chunks"] == 12
    for st, df in outs[1:]:
        pd.testing.assert_frame_equal(df, outs[0][1])
    # rows are ordered by (conf_num, first turn of the pair)
    df = outs[2][1]
    key = list(zip(df.conf_num, np.minimum(df.leg_turn_seq, df.wit_turn_seq)))
    assert key == sorted(key)
    assert _keys(df) == _keys(dyads.build_dyads(t))


# ------------------------------------------------------------------ sittings and after_end_marker (2026-09-26)

def test_never_pairs_across_a_sitting_change():
    t = _t(1, ["legislator", "nonlegislator", "legislator", "nonlegislator", "legislator", "nonlegislator"])
    t["sitting_seq"] = np.array([1, 1, 1, 2, 2, 2], dtype="int16")
    d, st = dyads.build_dyads(t, return_stats=True)
    # 3-4 would pair across the boundary between sitting 1 and sitting 2
    assert _keys(d) == [(1, 1, 2, "question"), (1, 3, 2, "answer"), (1, 5, 4, "answer"), (1, 5, 6, "question")]
    assert st["n_pairs_blocked_by_sitting"] == 1 and st["n_meetings_several_sittings"] == 1
    assert not st["sitting_seq_missing"]
    assert list(d.sitting_seq) == [1, 1, 2, 2]


def test_null_sitting_breaks_adjacency_and_is_counted():
    t = _t(1, ["legislator", "nonlegislator", "legislator"])
    t["sitting_seq"] = pd.array([1, None, 1], dtype="Int16")
    d, st = dyads.build_dyads(t, return_stats=True)
    assert len(d) == 0 and st["n_sitting_seq_null"] == 1


def test_missing_sitting_column_is_one_sitting_and_reported():
    t = _t(1, ["legislator", "nonlegislator"])
    d, st = dyads.build_dyads(t, return_stats=True)
    assert len(d) == 1 and st["sitting_seq_missing"] and st["n_pairs_blocked_by_sitting"] == 0


def test_after_end_marker_carried_and_optionally_excluded(tmp_path):
    t = _t(1, ["legislator", "nonlegislator", "legislator", "nonlegislator"])
    t["sitting_seq"] = np.int16(1)
    t["after_end_marker"] = [False, False, True, True]
    d, st = dyads.build_dyads(t, return_stats=True)
    assert _keys(d) == [(1, 1, 2, "question"), (1, 3, 2, "answer"), (1, 3, 4, "question")]
    assert list(d.any_after_end_marker) == [False, True, True]
    assert st["n_turns_after_end_marker"] == 2 and st["n_pairs_blocked_by_after_end"] == 0
    d2, st2 = dyads.build_dyads(t, return_stats=True, exclude_after_end_marker=True)
    assert _keys(d2) == [(1, 1, 2, "question")]
    assert st2["n_pairs_blocked_by_after_end"] == 2 and st2["exclude_after_end_marker"]
    # the file builder gives the same result
    p = tmp_path / "t.parquet"
    t.to_parquet(p, index=False)
    s3 = dyads.build_dyads_file(str(p), tmp_path / "d.parquet", exclude_after_end_marker=True)
    assert _keys(pd.read_parquet(tmp_path / "d.parquet")) == _keys(d2) and s3["n_pairs_blocked_by_after_end"] == 2


def test_file_builder_sitting_guard_equals_in_memory(tmp_path):
    t = pd.concat([_t(1, ["legislator", "nonlegislator"] * 3), _t(2, ["nonlegislator", "legislator"] * 2)],
                  ignore_index=True)
    t["sitting_seq"] = np.array([1, 1, 1, 2, 2, 2, 1, 1, 1, 1], dtype="int16")
    p = tmp_path / "turns.parquet"
    t.to_parquet(p, index=False)
    st = dyads.build_dyads_file(str(p), tmp_path / "d.parquet", chunk_turns=4)
    a = pd.read_parquet(tmp_path / "d.parquet")
    b = dyads.build_dyads(t)
    assert _keys(a) == _keys(b) and st["n_pairs_blocked_by_sitting"] == 1
    assert (1, 4, 3, "answer") not in _keys(a)


# ------------------------------------------------------------------ slim layout (researcher decision 8, 2026-09-26)

def test_slim_any_flags_either_side_and_null_when_source_absent():
    t = _t(1, ["legislator", "nonlegislator", "legislator", "nonlegislator"])
    t["label_confidence"] = ["high", "low", "high", "high"]
    t["time_regress"] = pd.array([None, None, True, None], dtype="boolean")
    t["after_end_marker"] = [False, False, False, True]
    d, st = dyads.build_dyads(t, return_stats=True)
    d = d.set_index(["leg_turn_seq", "wit_turn_seq"])
    assert list(d.any_low_label_confidence) == [True, True, False]        # (1,2) (3,2) (3,4)
    assert list(d.any_time_regress) == [False, True, True]                # null counts as false
    assert list(d.any_after_end_marker) == [False, False, True]
    assert d.any_label_inconsistent.isna().all()                          # no source column: null, reported
    assert "any_label_inconsistent" in st["slim_sources_missing"] and st["n_any_label_inconsistent_null"] == 3
    t["label_inconsistent"] = [False, False, False, True]
    d2, st2 = dyads.build_dyads(t, return_stats=True)
    assert list(d2.any_label_inconsistent) == [False, False, True]
    assert "any_label_inconsistent" not in st2["slim_sources_missing"]
    assert st2["slim_sources"]["any_label_inconsistent"] == "either_turn.label_inconsistent"


def test_slim_names_dates_and_government_columns_come_from_the_right_side():
    t = _t(1, ["legislator", "nonlegislator"])
    t["speaker_name"] = ["宋榮珍", "이종섭"]
    t["leg_name_hangul"] = ["송영진", None]
    t["speech_date"] = ["2021-03-02", "2021-03-03"]
    t["admin"] = ["문재인", "X"]
    t["presidency_state"] = ["normal", "acting"]
    t["title_raw"] = [None, "국방부장관"]
    t["ministry_normalized"] = [None, "국방부"]
    t["dual_office"] = pd.array([None, True], dtype="boolean")
    t["sitting_seq"] = np.int16(1)
    r = dyads.build_dyads(t).iloc[0]
    assert r.leg_name == "송영진" and r.wit_name == "이종섭"
    assert r.speech_date == "2021-03-02" and r.admin == "문재인" and r.presidency_state == "normal"
    assert r.wit_title_raw == "국방부장관" and r.wit_ministry_normalized == "국방부" and bool(r.wit_dual_office)
    t.loc[0, "leg_name_hangul"] = None
    assert dyads.build_dyads(t).iloc[0].leg_name == "宋榮珍"                 # unlinked: printed name


def test_file_builder_exclude_and_only_conf_nums(tmp_path):
    t = pd.concat([_t(1, ["legislator", "nonlegislator"] * 2), _t(2, ["nonlegislator", "legislator"]),
                   _t(3, ["legislator", "nonlegislator"])], ignore_index=True)
    p = tmp_path / "t.parquet"
    t.to_parquet(p, index=False)
    st = dyads.build_dyads_file(str(p), tmp_path / "a.parquet", exclude_conf_nums=[2])
    a = pd.read_parquet(tmp_path / "a.parquet")
    assert sorted(a.conf_num.unique()) == [1, 3] and st["n_turns_excluded"] == 2 and st["n_meetings_excluded"] == 1
    st2 = dyads.build_dyads_file(str(p), tmp_path / "b.parquet", only_conf_nums=[2])
    b = pd.read_parquet(tmp_path / "b.parquet")
    assert sorted(b.conf_num.unique()) == [2] and st2["n_dyads"] == 1
    assert len(a) + len(b) == len(dyads.build_dyads(t))
    assert list(a.columns) == list(b.columns) == list(dyads.SLIM_COLUMNS)


def test_extra_columns_only_on_request():
    t = _t(1, ["legislator", "nonlegislator"])
    d = dyads.build_dyads(t, extra_turn_cols=("speaker_label_raw",))
    assert list(d.columns) == list(dyads.SLIM_COLUMNS) + ["leg_speaker_label_raw", "wit_speaker_label_raw"]
    assert "leg_speaker_label_raw" not in dyads.build_dyads(t).columns
