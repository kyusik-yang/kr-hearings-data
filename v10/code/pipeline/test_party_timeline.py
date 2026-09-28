"""Tests for party_timeline.py (run: python -m pytest -q test_party_timeline.py).

Unit tests use the calendar / lineage CSVs in v10/interim and synthetic events. Tests marked
`needs_build` read the written spells (v10/interim/pipeline/party_timeline/party_spells.parquet)
and are skipped when the build has not been run."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import party_timeline as pt  # noqa: E402

BUILT = (pt.OUT / "party_spells.parquet").exists()
needs_build = pytest.mark.skipif(not BUILT, reason="party_timeline build outputs not written")


@pytest.fixture(scope="module")
def cal():
    return pt.load_calendar()


@pytest.fixture(scope="module")
def lin():
    return pt.Lineage()


@pytest.fixture(scope="module")
def rs(cal, lin):
    """Resolver on an empty spell table (label-level rules only)."""
    empty = pd.DataFrame(columns=["naas_cd", "term", "stint", "party", "start", "end", "spell_id", "basis",
                                  "is_speaker_nonpartisan", "party_before_speaker"])
    spans = pd.DataFrame(columns=["naas_cd", "term", "stint", "seat_start", "seat_end", "party_at_entry"])
    return pt.Resolver(spells=empty, spans=spans, ntr={}, lin=lin, cal=cal,
                       windows=pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"]))


# ----------------------------------------------------------------------------- calendar

TRANSITIONS = [
    # date, state, president, president_party, acting
    ("2003-02-24", "partyless", "김대중", None, None),       # decision 6: partyless state (was 'normal')
    ("2003-02-25", "normal", "노무현", "새천년민주당", None),
    ("2004-04-01", "suspended", "노무현", None, "고건"),       # suspension wins over partyless
    ("2007-02-27", "normal", "노무현", "열린우리당", None),     # formal exit 2007-02-28 (KTV), not 02-22
    ("2007-02-28", "partyless", "노무현", None, None),
    ("2008-02-24", "partyless", "노무현", None, None),       # decision 6: partyless state (was 'normal')
    ("2008-02-25", "normal", "이명박", "한나라당", None),
    ("2013-02-24", "normal", "이명박", "새누리당", None),
    ("2013-02-25", "normal", "박근혜", "새누리당", None),
    ("2016-12-08", "normal", "박근혜", "새누리당", None),
    ("2016-12-09", "suspended", "박근혜", "새누리당", "황교안"),
    ("2017-02-13", "suspended", "박근혜", "자유한국당", "황교안"),
    ("2017-03-09", "suspended", "박근혜", "자유한국당", "황교안"),
    ("2017-03-10", "acting", None, None, "황교안"),
    ("2017-05-09", "acting", None, None, "황교안"),
    ("2017-05-10", "normal", "문재인", "더불어민주당", None),
    ("2022-05-09", "normal", "문재인", "더불어민주당", None),
    ("2022-05-10", "normal", "윤석열", "국민의힘", None),
    ("2024-12-13", "normal", "윤석열", "국민의힘", None),
    ("2024-12-14", "suspended", "윤석열", "국민의힘", "한덕수"),
    ("2024-12-27", "suspended", "윤석열", "국민의힘", "최상목"),
    ("2025-04-03", "suspended", "윤석열", "국민의힘", "한덕수"),
    ("2025-04-04", "acting", None, None, "한덕수"),
    ("2025-05-02", "acting", None, None, "이주호"),
    ("2025-06-03", "acting", None, None, "이주호"),
    ("2025-06-04", "normal", "이재명", "더불어민주당", None),
]


@pytest.mark.parametrize("date,state,president,party,acting", TRANSITIONS)
def test_presidency_transitions(cal, date, state, president, party, acting):
    p = pt.presidency_on(date, cal)
    assert p["presidency_state"] == state
    assert p["president"] == president
    assert p["president_party"] == party
    assert p["acting_president"] == acting


def test_calendar_contiguous_and_states(cal):
    assert set(cal.presidency_state) == {"normal", "partyless", "suspended", "acting"}
    assert cal.start.iloc[0] == "1998-02-25"
    assert cal.end.iloc[-1] == "9999-12-31"


def test_calendar_sources_verified():
    v = pt.verify_calendar()
    starts = v[v.check == "start"]
    assert starts.found.all(), starts[~starts.found]
    # every in-office start of a new president is attested on an official government page
    for d in ("2003-02-25", "2008-02-25", "2013-02-25", "2017-03-10", "2017-05-10", "2022-05-10",
              "2025-04-04", "2025-06-04"):
        r = starts[starts.value == d].iloc[0]
        assert r.official_source, d


# ----------------------------------------------------------------------------- ruling rule

@pytest.mark.parametrize("party,date,expected,reason", [
    ("새천년민주당", "2003-02-25", "ruling", None),
    ("한나라당", "2003-02-25", "opposition", None),
    ("새천년민주당", "2003-09-29", "ruling", None),   # decision 6: last party ruling (was NULL president_partyless)
    ("한나라당", "2008-02-25", "ruling", None),
    ("통합민주당", "2008-02-25", "opposition", None),
    ("새누리당", "2013-02-25", "ruling", None),
    ("새누리당", "2016-12-09", "ruling", None),                       # suspension: party stays ruling
    ("자유한국당", "2017-02-20", "ruling", None),
    ("더불어민주당", "2017-02-20", "opposition", None),
    ("자유한국당", "2017-03-10", None, "acting"),                      # removal: acting president
    ("더불어민주당", "2017-05-10", "ruling", None),
    ("더불어민주당", "2022-05-09", "ruling", None),
    ("더불어민주당", "2022-05-10", "opposition", None),
    ("국민의힘", "2022-05-10", "ruling", None),
    ("국민의힘", "2024-12-14", "ruling", None),                        # suspension
    ("국민의힘", "2025-04-04", None, "acting"),
    ("더불어민주당", "2025-06-03", None, "acting"),
    ("더불어민주당", "2025-06-04", "ruling", None),
    ("무소속", "2021-01-01", "independent", None),
    ("무소속", "2025-04-10", None, "acting"),
])
def test_ruling_rule(rs, party, date, expected, reason):
    st, why, _ = rs.ruling(party, date)
    assert st == expected
    assert why == reason


@pytest.mark.parametrize("party,date,camp,status", [
    ("더불어시민당", "2020-04-01", "더불어민주당", "ruling"),     # satellite counts with its main party
    ("미래한국당", "2020-04-01", "미래통합당", "opposition"),
    ("국민의미래", "2024-04-01", "국민의힘", "ruling"),
    ("더불어민주연합", "2024-04-01", "더불어민주당", "opposition"),
])
def test_satellite_camp(rs, party, date, camp, status):
    assert rs.satellite_parent(party, date)
    assert rs.camp(party, date) == camp
    assert rs.ruling(party, date)[0] == status


@pytest.mark.parametrize("label,since,date,expected", [
    ("더불어시민당", "2020-03-08", "2020-05-12", "더불어시민당"),
    ("더불어시민당", "2020-03-08", "2020-05-13", "더불어민주당"),
    ("미래한국당", "2020-02-05", "2020-05-28", "미래한국당"),
    ("미래한국당", "2020-02-05", "2020-05-29", "미래통합당"),
    ("미래한국당", "2020-02-05", "2020-09-02", "국민의힘"),
    ("국민의미래", "2024-02-27", "2024-04-22", "국민의미래"),
    ("국민의미래", "2024-02-27", "2024-04-23", "국민의힘"),
    ("더불어민주연합", "2024-03-03", "2024-05-07", "더불어민주연합"),
    ("더불어민주연합", "2024-03-03", "2024-05-08", "더불어민주당"),
    ("새누리당", "2016-12-01", "2017-02-13", "자유한국당"),
    ("한나라당", "2008-01-01", "2012-02-13", "새누리당"),
])
def test_satellite_mergers_and_renames(rs, label, since, date, expected):
    assert rs.formal_on(label, since, date) == expected


def test_notice_transitions_win_when_earlier(lin):
    ntr = {"정의당": [("2024-01-30", "녹색정의당", "party_rename", 48024)],
           "녹색정의당": [("2024-05-20", "정의당", "party_rename", 48064)],
           "국민의미래": [("2024-04-26", "국민의힘", "party_merge", 48063)]}
    t = pt.next_transition("정의당", "2023-01-01", "2024-12-31", lin, ntr)
    assert t[:2] == ("2024-01-30", "녹색정의당")
    # lineage registration date (2024-04-23) precedes the Assembly notice (2024-04-26)
    t = pt.next_transition("국민의미래", "2024-03-15", "2024-05-29", lin, ntr)
    assert t[:2] == ("2024-04-23", "국민의힘") and t[3] == "2024-04-26"


# ----------------------------------------------------------------------------- parsing helpers

@pytest.mark.parametrize("s,expected", [
    ("2024. 3. 17.", "2024-03-17"), ("2024\n5. 8.", "2024-05-08"), ("2000년12월28일자", "2000-12-28"),
    ("2024.3. 8.", "2024-03-08"), ("2020.\n5. 30.", "2020-05-30"), ("'05. 9. 29", "2005-09-29"),
])
def test_parse_date(s, expected):
    assert pt.iso(pt.parse_date(s)) == expected


def test_parse_date_month_day_uses_reference_year():
    import datetime as dt
    assert pt.iso(pt.parse_date("(12월 30일)", dt.date(2003, 1, 5))) == "2002-12-30"
    assert pt.iso(pt.parse_date("(10월 13일)", dt.date(2003, 10, 15))) == "2003-10-13"


def test_split_names_spread_and_duplicates():
    names, _, _ = pt.split_names("강기정 신 명 신국환 김  현 김현미 설  훈 박  정")
    assert names == ["강기정", "신명", "신국환", "김현", "김현미", "설훈", "박정"]
    names, _, declared = pt.split_names("이수진 이수진(비) 이용빈\n(이상 3인)")
    assert names == ["이수진", "이수진", "이용빈"] and declared == 3


@pytest.mark.parametrize("raw,expected", [
    ("미래통합당\n(약칭:통합당)", "미래통합당"), ("더불어민주당\n(민주당, 더민주)", "더불어민주당"),
    ("새누리당(7인)", "새누리당"), ("어느 교섭단체에도 속하지 아니하는 의원", pt.NON_GROUP),
    ("비교섭단체(민주당)", pt.NON_GROUP), ("어느交涉團體에도속하지아니하는議員", pt.NON_GROUP),
    ("더불어\n민주당\n\n더불어\n민주당", "더불어민주당"), ("미  래\n통합당", "미래통합당"),
])
def test_norm_party(raw, expected):
    assert pt.norm_party(raw) == expected


@pytest.mark.parametrize("cap,kind", [
    ("◯交涉團體所屬議員名簿提出", "roster"), ("◯交涉團體加入", "join"), ("◯交涉團體所屬議員除籍", "leave"),
    ("◯의원 당적 변경", "switch"), ("【報告事項】議員退職", "exit"), ("◯의석 승계", "enter"),
    ("◯교섭단체 명칭 변경", "group_rename"), ("◯통지", "notice"), ("◯상임위원 개선", "committee"),
])
def test_item_kind(cap, kind):
    assert pt.item_kind(cap) == kind


def test_caucus_names():
    assert pt.is_caucus_name("국민참여통합신당\n주비위원회")
    assert pt.is_caucus_name("평화와 정의의 의원 모임")
    assert not pt.is_caucus_name("더불어민주당")
    assert pt.FORMING_RE.search(pt.norm_party("열린우리당주비위원회"))
    assert not pt.FORMING_RE.search(pt.norm_party("선진과 창조의 모임"))


# ----------------------------------------------------------------------------- TermBuilder

def _ev(kind, date, cands, **kw):
    base = {"kind": kind, "date": date, "cands": tuple(cands), "cands_all": tuple(cands), "conf_num": 1,
            "item_seq": 0, "table_idx": 0, "row_idx": 0, "name_raw": kw.pop("name_raw", "X"), "group": None,
            "reason": None, "from_party": None, "to_party": None, "party": None, "district": None,
            "old_party": None, "new_party": None, "reps": None, "raw": "", "role_in_row": None, "term": 21}
    base.update(kw)
    return base


def _spans(rows):
    return pd.DataFrame([{"naas_cd": n, "term": 21, "stint": 1, "name": n, "name_hanja": None, "seat_start": a,
                          "seat_end": b, "seat_end_raw": b, "party_at_entry": p, "party_elected": p,
                          "district": "d", "district_hist": dh, "source": "t"} for n, a, b, p, dh in rows])


def test_termbuilder_satellite_dance_and_seat_exit(lin):
    spans = _spans([("A", "2020-05-30", "2024-05-29", "더불어시민당", "비례대표"),   # expelled, moves to satellite
                    ("B", "2020-05-30", "2024-01-05", "미래한국당", "비례대표"),     # 탈당 = seat loss
                    ("C", "2020-05-30", "2024-05-29", "무소속", "지역구")])            # independent, joins later
    ev = pd.DataFrame([
        _ev("roster", "2020-05-30", ["A"], group="더불어민주당"),
        _ev("roster", "2020-05-30", ["B"], group="미래통합당"),
        _ev("group_form", "2020-05-30", [], group="더불어민주당"),
        _ev("group_rename", "2020-09-02", [], from_party="미래통합당", to_party="국민의힘"),
        _ev("leave", "2024-03-17", ["A"], group="더불어민주당", reason="제명"),
        _ev("switch", "2024-03-18", ["A"], from_party="더불어민주당", to_party="더불어민주연합"),
        _ev("party_merge", "2024-05-08", [], old_party="더불어민주연합", new_party="더불어민주당"),
        _ev("leave", "2024-01-05", ["B"], group="국민의힘", reason="탈당"),
        _ev("exit", "2024-01-05", ["B"], group="국민의힘", reason="탈당"),
        _ev("join", "2021-08-05", ["C"], group="국민의힘"),
    ])
    tb = pt.TermBuilder(21, spans, ev, lin, idx=None)
    sp = tb.run()
    sp, _ = pt.lineage_relabel(sp, lin, pt.notice_transitions(ev))
    a = sp[sp.naas_cd == "A"].sort_values("start")[["party", "start", "end"]].values.tolist()
    assert a == [["더불어민주당", "2020-05-30", "2024-03-16"], ["무소속", "2024-03-17", "2024-03-17"],
                 ["더불어민주연합", "2024-03-18", "2024-05-07"], ["더불어민주당", "2024-05-08", "2024-05-29"]]
    b = sp[sp.naas_cd == "B"].sort_values("start")[["party", "start", "end"]].values.tolist()
    assert b == [["미래통합당", "2020-05-30", "2020-09-01"], ["국민의힘", "2020-09-02", "2024-01-05"]]
    c = sp[sp.naas_cd == "C"].sort_values("start")[["party", "start", "end"]].values.tolist()
    assert c == [["무소속", "2020-05-30", "2021-08-04"], ["국민의힘", "2021-08-05", "2024-05-29"]]
    log = pd.DataFrame(tb.log)
    assert (log.status == "leave_at_seat_end").sum() == 1
    assert (log.status == "applied_from_is_party_before_recent_exit").sum() == 1


def test_termbuilder_undated_exit_flag(lin):
    spans = _spans([("S", "2020-05-30", "2024-05-29", "더불어민주당", "지역구")])
    ev = pd.DataFrame([_ev("roster", "2020-05-30", ["S"], group="더불어민주당"),
                       _ev("switch", "2024-03-17", ["S"], from_party="무소속", to_party="새로운미래")])
    tb = pt.TermBuilder(21, spans, ev, lin, idx=None)
    sp = tb.run().sort_values("start")
    assert sp.party.tolist() == ["더불어민주당", "새로운미래"]
    assert sp.end_undated_exit.tolist() == [True, False]


def test_speaker_rule_imputes_by_law():
    sp = pd.DataFrame([{"naas_cd": "K", "term": 20, "stint": 1, "party": "더불어민주당", "start": "2016-05-30",
                        "end": "2020-05-29", "basis": "start_roster", "party_before": None, "speaker_exit": False,
                        "is_speaker_nonpartisan": False, "party_before_speaker": None}])
    ten = pd.DataFrame([{"term": 20, "naas_cd": "K", "name_raw": "K", "first_date": "2018-07-16",
                         "last_date": "2020-05-20", "n_meetings": 70}])
    out, log = pt.apply_speaker_rule(sp, ten)
    assert out.party.tolist() == ["더불어민주당", "무소속"]
    assert out.start.tolist() == ["2016-05-30", "2018-07-17"]
    assert out.is_speaker_nonpartisan.tolist() == [False, True]
    assert out.party_before_speaker.iloc[1] == "더불어민주당"
    assert log.status.tolist() == ["imputed_by_law"]


def test_lineage_relabel_splits_and_merges(lin):
    sp = pd.DataFrame([{"naas_cd": "X", "term": 21, "stint": 1, "party": "미래통합당", "start": "2020-05-30",
                        "end": "2024-05-29", "basis": "start_roster"}])
    out, n = pt.lineage_relabel(sp, lin, {"미래통합당": [("2020-09-02", "국민의힘", "group_rename", 45551)]})
    assert out[["party", "start", "end"]].values.tolist() == [["미래통합당", "2020-05-30", "2020-09-01"],
                                                             ["국민의힘", "2020-09-02", "2024-05-29"]]


# ----------------------------------------------------------------------------- enrich

def test_enrich_keeps_order_and_scopes(rs):
    t = pd.DataFrame({"conf_num": [1, 1, 1], "turn_seq": [1, 2, 3],
                      "speech_date": ["2022-05-10", "2025-04-10", None],
                      "naas_cd": [None, None, None], "role_group": ["nonlegislator", "legislator", "legislator"]},
                     index=[7, 3, 5])
    m = pd.DataFrame({"conf_num": [1], "term": [21], "date": ["2022-05-10"]})
    e = pt.enrich(t, m, resolver=rs)
    assert list(e.index) == [7, 3, 5]
    assert e.presidency_state.tolist() == ["normal", "acting", "normal"]     # null speech_date -> meeting date
    assert e.party.isna().all()
    # CONTRACT: party_method is 'person_spell', 'label_lineage' or null; the reason is in ruling_null_reason
    assert e.party_method.tolist() == [None, None, None]
    assert e.ruling_null_reason.tolist() == [None, "no_naas_cd", "no_naas_cd"]
    for c in pt.ENRICH_COLS:
        assert c in e.columns


@needs_build
def test_enrich_on_built_spells_known_cases():
    sp = pd.read_parquet(pt.OUT / "party_spells.parquet")

    def nid(name, term):
        return sp[(sp.name == name) & (sp.term == term)].naas_cd.iloc[0]
    rows = [("김진표", 21, "2022-07-10", "무소속", "independent", True),
            ("김진표", 21, "2022-06-10", "더불어민주당", "opposition", False),
            ("윤상현", 21, "2021-01-01", "무소속", "independent", False),
            ("윤상현", 21, "2022-06-01", "국민의힘", "ruling", False),
            ("김근태", 21, "2024-03-20", "국민의미래", "ruling", False),
            ("용혜인", 21, "2024-02-20", "새진보연합", "opposition", False),
            ("양향자", 21, "2023-10-01", "한국의희망", "opposition", False)]
    t = pd.DataFrame([{"conf_num": 1, "turn_seq": i + 1, "speech_date": d, "naas_cd": nid(n, tm),
                       "role_group": "legislator"} for i, (n, tm, d, *_r) in enumerate(rows)])
    e = pt.enrich(t, pd.DataFrame({"conf_num": [1], "term": [21], "date": ["2021-01-01"]}))
    assert e.party.tolist() == [r[3] for r in rows]
    assert e.ruling_status.tolist() == [r[4] for r in rows]
    assert e.is_speaker_nonpartisan.tolist() == [r[5] for r in rows]
    assert (e.party_method == "person_spell").all()


@needs_build
def test_built_spells_integrity():
    sp = pd.read_parquet(pt.OUT / "party_spells.parquet")
    assert sp.party.notna().all()
    for (n, t), g in sp.groupby(["naas_cd", "term"]):
        g = g.sort_values("start")
        assert (g.start <= g.end).all()
        assert (g.start.values[1:] > g.end.values[:-1]).all(), (n, t)
    for t in range(16, 23):
        assert sp[sp.term == t].naas_cd.nunique() >= pt.TERM_SEATS[t]


def test_alias_is_date_bounded(lin):
    # '국민회의' (천정배, 2016) must not resolve to 새정치국민회의 (1995-2000) and thus to 더불어민주당
    assert lin.key("국민회의", "2016-02-24") is None
    assert not lin.same_party("더불어민주당", "국민회의", "2016-02-23")
    assert lin.key("국민회의", "1999-01-01") == "새정치국민회의"


def test_rename_notice_does_not_move_other_parties(lin):
    spans = _spans([("D", "2012-05-30", "2016-05-29", "민주통합당", "지역구")])
    spans["term"] = 19
    ev = pd.DataFrame([_ev("roster", "2012-05-30", ["D"], group="민주통합당", term=19),
                       _ev("party_merge", "2016-02-24", [], old_party="국민회의", new_party="국민의당", term=19)])
    sp = pt.TermBuilder(19, spans, ev, lin, idx=None).run()
    sp, _ = pt.lineage_relabel(sp, lin, pt.notice_transitions(ev))
    assert sp.sort_values("start").party.tolist() == ["민주통합당", "민주당", "새정치민주연합", "더불어민주당"]


# ----------------------------------------------------------------------------- review round 1 (2026-09-26)

def _spans_t(rows, term):
    sp = _spans(rows)
    sp["term"] = term
    return sp


def _evt(term, *evs):
    return pd.DataFrame([{**e, "term": term} for e in evs])


def test_f1_join_goes_to_newly_seated_namesake(lin):
    # 18대 김선동: a namesake seated by by-election joins his party's 교섭단체 on the seat day
    spans = _spans_t([("OLD", "2008-05-30", "2012-05-29", "한나라당", "서울 도봉구을"),
                      ("NEW", "2011-04-27", "2012-05-29", "민주노동당", "전남 순천시")], 18)
    ev = _evt(18, _ev("roster", "2008-05-30", ["OLD"], group="한나라당"),
              _ev("join", "2011-04-27", ["OLD", "NEW"], group="민주노동당"))
    tb = pt.TermBuilder(18, spans, ev, lin, idx=None)
    sp = tb.run()
    assert sp[sp.naas_cd == "OLD"].party.tolist() == ["한나라당"]
    assert sp[sp.naas_cd == "NEW"].party.tolist() == ["민주노동당"]
    log = pd.DataFrame(tb.log)
    assert log[log.kind == "join"].how.tolist() == ["newly_seated"]


def test_f1_not_yet_member_still_used_for_sitting_namesakes(lin):
    # 20대 김성태 x2 (2017-05-06): the one outside the group joins it
    spans = _spans_t([("A", "2016-05-30", "2020-05-29", "새누리당", "서울 강서구을"),
                      ("B", "2016-05-30", "2020-05-29", "새누리당", "비례대표")], 20)
    ev = _evt(20, _ev("roster", "2016-05-30", ["A"], group="새누리당"),
              _ev("roster", "2016-05-30", ["B"], group="새누리당"),
              _ev("switch", "2017-01-24", ["A"], from_party="새누리당", to_party="바른정당"),
              _ev("join", "2017-05-06", ["A", "B"], group="자유한국당"))
    tb = pt.TermBuilder(20, spans, ev, lin, idx=None)
    tb.run()
    log = pd.DataFrame(tb.log)
    j = log[log.kind == "join"].iloc[0]
    assert (j.naas_cd, j.how) == ("A", "not_yet_member")


def test_f2_nongroup_row_is_no_exit_evidence(lin):
    # 17대 류근찬: a '비교섭' committee row is the normal listing of a small party's member
    spans = _spans_t([("R", "2004-05-30", "2008-05-29", "자유민주연합", "충남 보령시서천군")], 17)
    ev = _evt(17, _ev("observation", "2004-07-05", ["R"], group="어느 교섭단체에도 속하지 아니하는 의원",
                      role_in_row="name"),
              _ev("switch", "2006-01-27", ["R"], from_party="무소속", to_party="국민중심당"))
    tb = pt.TermBuilder(17, spans, ev, lin, idx=None)
    sp = tb.run().sort_values("start")
    assert sp[["party", "start", "end"]].values.tolist() == [["자유민주연합", "2004-05-30", "2006-01-26"],
                                                             ["국민중심당", "2006-01-27", "2008-05-29"]]
    assert sp.end_undated_exit.tolist() == [True, False]
    assert [w["reason"] for w in tb.windows] == ["undated_exit_before_switch"]
    assert (tb.windows[0]["lo"], tb.windows[0]["hi"]) == ("2004-05-31", "2006-01-26")


def test_f2_ended_party_is_independent_for_switch(lin):
    # 17대 정몽준: 국민통합21 ended 2004-09-13; '무소속 -> 한나라당' in 2007 is consistent
    spans = _spans_t([("J", "2004-05-30", "2008-05-29", "국민통합21", "서울 동작구을")], 17)
    ev = _evt(17, _ev("switch", "2007-12-03", ["J"], from_party="무소속", to_party="한나라당"))
    tb = pt.TermBuilder(17, spans, ev, lin, idx=None)
    sp = tb.run()
    sp, _ = pt.lineage_relabel(sp, lin, {})
    assert sp.sort_values("start")[["party", "start"]].values.tolist() == [
        ["국민통합21", "2004-05-30"], ["무소속", "2004-09-13"], ["한나라당", "2007-12-03"]]
    assert pd.DataFrame(tb.log).status.tolist() == ["applied"]
    assert tb.windows == []


def test_f3_switch_from_party_backfills_and_flags(lin):
    # 20대 민주평화당: leave 국민의당 2018-02-05, then '민주평화당 -> 무소속' 2019-08-16
    spans = _spans_t([("P", "2016-05-30", "2020-05-29", "국민의당", "광주 북구을")], 20)
    ev = _evt(20, _ev("roster", "2016-05-30", ["P"], group="국민의당"),
              _ev("leave", "2018-02-05", ["P"], group="국민의당", reason="탈당"),
              _ev("switch", "2019-08-16", ["P"], from_party="민주평화당", to_party="무소속"))
    tb = pt.TermBuilder(20, spans, ev, lin, idx=None)
    sp = tb.run().sort_values("start")
    assert sp[["party", "start", "end"]].values.tolist() == [["국민의당", "2016-05-30", "2018-02-04"],
                                                             ["무소속", "2018-02-05", "2018-02-05"],
                                                             ["민주평화당", "2018-02-06", "2019-08-15"],
                                                             ["무소속", "2019-08-16", "2020-05-29"]]
    b = sp[sp.party == "민주평화당"].iloc[0]
    assert (b.basis, b.inferred_rule) == ("inferred_from_switch_from_party", "party_founded")
    assert [(w["reason"], w["lo"], w["hi"]) for w in tb.windows] == [
        ("switch_from_party_unrecorded", "2018-02-06", "2019-08-15")]
    assert pd.DataFrame(tb.log).status.tolist()[-1] == "applied_backfilled_from_party"


def test_f4_postdated_election_label_backdated(lin, cal):
    assert lin.label_at("자유한국당", "2016-05-30") == ("새누리당", "backdated_rename")
    assert lin.label_at("국민의힘", "2020-05-30") == ("미래통합당", "backdated_rename")
    assert lin.label_at("민주통합당", "2008-05-30") == ("민주통합당", "postdated_unresolved")
    spans = _spans_t([("F", "2016-05-30", "2019-07-11", "자유한국당", "경북 경산시")], 20)
    tb = pt.TermBuilder(20, spans, _evt(20, _ev("group_form", "2016-05-30", [], group="더불어민주당")), lin, idx=None)
    sp, _ = pt.lineage_relabel(tb.run(), lin, {})
    assert sp.sort_values("start")[["party", "start", "basis"]].values.tolist() == [
        ["새누리당", "2016-05-30", "election_party_backdated"], ["자유한국당", "2017-02-13", "lineage_relabel"]]
    sp = sp.assign(spell_id=["s1", "s2"], is_speaker_nonpartisan=False, party_before_speaker=None)
    rs = pt.Resolver(spells=sp, spans=spans, ntr={}, lin=lin, cal=cal,
                     windows=pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"]))
    t = pd.DataFrame({"conf_num": 1, "turn_seq": [1, 2], "speech_date": ["2016-07-01", "2016-12-20"],
                      "naas_cd": "F", "role_group": "legislator"})
    e = pt.enrich(t, pd.DataFrame({"conf_num": [1], "term": [20], "date": ["2016-07-01"]}), resolver=rs)
    assert e.party.tolist() == ["새누리당", "새누리당"]
    assert e.ruling_status.tolist() == ["ruling", "ruling"]


def test_f5_structural_windows(lin):
    # 17대: 무소속 after leaving, next record the 통합민주당 roster (merger of 대통합민주신당 founded
    # 2007-08-05 without a member roster in the minutes)
    spans = _spans_t([("U", "2004-05-30", "2008-05-29", "열린우리당", "서울"),
                      ("V", "2004-05-30", "2008-05-29", "열린우리당", "경기")], 17)
    ev = _evt(17, _ev("roster", "2004-05-30", ["U"], group="열린우리당"),
              _ev("roster", "2004-05-30", ["V"], group="열린우리당"),
              _ev("leave", "2007-07-20", ["U"], group="열린우리당", reason="탈당"),
              _ev("roster", "2008-02-18", ["U"], group="통합민주당"))
    tb = pt.TermBuilder(17, spans, ev, lin, idx=None)
    sp, _ = pt.lineage_relabel(tb.run(), lin, {})
    sp = pt.speaker_flags(sp.assign(speaker_exit=sp.speaker_exit.fillna(False)))
    sp_next = spans.assign(term=18, seat_start="2008-05-30", seat_end="2012-05-29", party_at_entry="한나라당",
                           party_elected="한나라당")   # V re-elected from another party: contradicted
    w = pt.structural_windows(sp, pd.concat([spans, sp_next]), {17: tb}, lin)
    got = sorted((x["naas_cd"], x["reason"], x["lo"], x["hi"]) for x in w)
    assert ("U", "possible_unrecorded_membership", "2007-07-20", "2008-02-17") in got
    assert ("V", "contradicted_by_next_term_election_party", "2004-05-31", "2008-05-29") in got
    marked = pt.mark_uncertain(sp, pd.DataFrame(w))
    u = marked[(marked.naas_cd == "U") & (marked.party == "무소속")].iloc[0]
    assert bool(u.uncertain) and u.uncertain_reason == "possible_unrecorded_membership"


def test_f6_month_day_after_sitting():
    import datetime as dt
    assert pt.iso(pt.parse_date("(5월10일)", dt.date(2004, 3, 12), dt.date(2004, 5, 29))) == "2004-05-10"
    assert pt.iso(pt.parse_date("(5월27일)", dt.date(2004, 3, 12), dt.date(2004, 5, 29))) == "2004-05-27"
    assert pt.iso(pt.parse_date("(12월 30일)", dt.date(2003, 1, 5), dt.date(2003, 1, 23))) == "2002-12-30"
    pm = pd.DataFrame({"conf_num": [1, 2, 3], "term": [16, 16, 16], "date": ["2004-03-02", "2004-03-12", "2004-03-12"]})
    lim = pt.forward_limits(pm)
    assert lim[1] == "2004-03-15" and lim[2] == "2004-05-29" and lim[3] == "2004-05-29"


def test_f7_enter_notice_label_carried_forward(lin):
    # 19대 황인자: the 의석승계 notice prints the list party '자유선진당' (merged into 새누리당 2012-11-16)
    spans = _spans_t([("H", "2013-12-16", "2016-05-29", "자유선진당", "비례대표")], 19)
    ev = _evt(19, _ev("enter", "2013-12-16", ["H"], party="자유선진당"))
    sp = pt.TermBuilder(19, spans, ev, lin, idx=None).run()
    assert sp.party.tolist() == ["새누리당"] and sp.basis.tolist() == ["enter_notice"]


def test_f8_labels_are_date_bounded(lin, rs):
    assert lin.key("미래한국당", "2008-04-10") is None
    assert lin.key("미래한국당", "2020-04-01") == "미래한국당"
    assert not lin.same_party("새누리당", "자유한국당", "2017-06-01")      # 2017 새누리당 (조원진) is another party
    assert lin.same_party("새누리당", "자유한국당", "2017-02-20")          # stale label shortly after the rename
    assert lin.key("민주당", "2016-03-17") is None
    assert lin.key("바른정당", "2017-01-09") == "바른정당"                 # 교섭단체 renamed before registration
    assert lin.key("자유선진당", "2013-12-16", historic=True) == "자유선진당"
    assert rs.satellite_parent("미래한국당", "2008-04-10") is None
    assert rs.camp("미래한국당", "2008-04-10") == "미래한국당"


def test_notice_alias_for_rename_outside_lineage():
    lin2 = pt.Lineage()
    added = lin2.register_notice_aliases({"중도통합민주당": [("2007-08-20", "민주당", "party_rename", 31040)]})
    assert added == [("민주당", "2007-08-20", "2008-03-17", "중도통합민주당")]
    assert lin2.key("민주당", "2007-11-23") == "중도통합민주당"
    assert lin2.formal_on("민주당", "2008-03-01")[0] == "통합민주당"
    assert lin2.key("민주당", "2006-01-01") == "민주당(2005)"


def test_notice_text_records():
    base = {"kind": "notice_text", "date": "2007-10-02",
            "raw": "2007. 10. 2 중앙선거관리위원장으로부터 참주인연합의 중앙당 등록 통지가 있었으며 김선미 의원으로부터 "
                   "동 정당에 입당하였다는 보고가 있었음"}
    out = pt.notice_text_records(base)
    assert [(r["kind"], r.get("new_party"), r.get("name_raw"), r.get("group")) for r in out] == [
        ("party_register", "참주인연합", None, None), ("join", None, "김선미", "참주인연합")]
    out = pt.notice_text_records({**base, "date": "2008-02-18",
                                  "raw": "2008. 2. 18 중앙선거관리위원장으로부터 대통합민주신당과 민주당이 통합민주당으로 신설합당하였다는 통지가 있었음"})
    assert [(r["old_party"], r["new_party"]) for r in out] == [("대통합민주신당", "통합민주당"), ("민주당", "통합민주당")]
    out = pt.notice_text_records({**base, "date": None, "raw": "중도통합민주당 중앙당 등록"})
    assert [r["kind"] for r in out] == ["notice_text"]


def test_f9_inferred_change_window_recorded(lin):
    spans = _spans_t([("Z", "2016-05-30", "2020-05-29", "국민의당", "서울")], 20)
    ev = _evt(20, _ev("roster", "2016-05-30", ["Z"], group="국민의당"),
              _ev("observation", "2017-03-01", ["Z"], group="바른정당", role_in_row="name", conf_num=5),
              _ev("observation", "2017-04-01", ["Z"], group="바른정당", role_in_row="name", conf_num=6))
    tb = pt.TermBuilder(20, spans, ev, lin, idx=None)
    sp = tb.run().sort_values("start")
    assert sp.party.tolist() == ["국민의당", "바른정당"]
    assert sp.start_window.tolist()[1] == "(2016-05-30, 2017-03-01]"
    assert [(w["reason"], w["lo"], w["hi"]) for w in tb.windows] == [("inferred_change_window", "2016-05-31", "2017-02-28")]
    lg = pd.DataFrame(tb.log)
    assert lg[lg.status == "obs_inferred_change"].window.tolist() == ["(2016-05-30, 2017-03-01]"]
    m = pt.mark_uncertain(sp.assign(term=20), pd.DataFrame(tb.windows))
    assert m.uncertain.tolist() == [True, False] and m.uncertain_days.tolist()[0] == 274


def test_f12_null_dates_logged_and_merger_date_header(lin):
    assert pt.header_fields(["존속하는 정당", "흡수되는 정당", "합당 연월일"]) == ["survivor", "absorbed", "date"]
    spans = _spans_t([("N", "2020-05-30", "2024-05-29", "더불어민주당", "서울")], 21)
    ev = _evt(21, _ev("roster", "2020-05-30", ["N"], group="더불어민주당"), _ev("exit", None, ["N"]))
    tb = pt.TermBuilder(21, spans, ev, lin, idx=None)
    tb.run()
    assert "no_date" in pd.DataFrame(tb.log).status.tolist() and tb.stats["no_date_exit"] == 1


def test_f13_enrich_dates_do_not_depend_on_batch(cal, lin):
    sp = pd.DataFrame([{"naas_cd": "K", "term": 21, "stint": 1, "party": "무소속", "start": "2022-06-11",
                        "end": "2024-05-29", "spell_id": "s", "basis": "leave", "is_speaker_nonpartisan": True,
                        "party_before_speaker": "더불어민주당"}])
    spans = pd.DataFrame([{"naas_cd": "K", "term": 21, "stint": 1, "seat_start": "2020-05-30",
                           "seat_end": "2024-05-29", "party_at_entry": "더불어민주당"}])
    rs = pt.Resolver(spells=sp, spans=spans, ntr={}, lin=lin, cal=cal,
                     windows=pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"]))
    m = pd.DataFrame({"conf_num": [1], "term": [21], "date": ["2022-07-10"]})
    res = []
    for first in ("1997-12-31", "2022-02-30", "2021-01-01"):
        t = pd.DataFrame({"conf_num": 1, "turn_seq": [1, 2], "speech_date": [first, "2022/07/10"],
                          "naas_cd": "K", "role_group": "legislator"})
        e = pt.enrich(t, m, resolver=rs)
        res.append(tuple(e.iloc[1][["party", "party_method", "ruling_status"]]))
    assert res == [("무소속", "person_spell", "independent")] * 3
    t = pd.DataFrame({"conf_num": 1, "turn_seq": [1, 2], "speech_date": ["1997-12-31", "2022-02-30"],
                      "naas_cd": "K", "role_group": "legislator"})
    e = pt.enrich(t, pd.DataFrame({"conf_num": [1], "term": [21], "date": [None]}), resolver=rs)
    assert e.presidency_state.isna().tolist() == [True, True]
    assert e.ruling_null_reason.tolist() == ["outside_calendar", "no_date"]


def test_f14_f15_nearest_spell_fallback(cal, lin):
    sp = pd.DataFrame([{"naas_cd": "K", "term": 21, "stint": 1, "party": "더불어민주당", "start": "2020-05-30",
                        "end": "2022-06-10", "spell_id": "s1", "basis": "start_roster", "is_speaker_nonpartisan": False,
                        "party_before_speaker": None},
                       {"naas_cd": "K", "term": 21, "stint": 1, "party": "무소속", "start": "2022-06-11",
                        "end": "2024-05-29", "spell_id": "s2", "basis": "leave", "is_speaker_nonpartisan": True,
                        "party_before_speaker": "더불어민주당"}])
    spans = pd.DataFrame([{"naas_cd": "K", "term": 21, "stint": 1, "seat_start": "2020-05-30",
                           "seat_end": "2024-05-29", "party_at_entry": "더불어민주당"},
                          {"naas_cd": "L", "term": 20, "stint": 1, "seat_start": "2016-05-30",
                           "seat_end": "2020-05-29", "party_at_entry": "자유한국당"}])
    rs = pt.Resolver(spells=sp, spans=spans, ntr={}, lin=lin, cal=cal,
                     windows=pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"]))
    t = pd.DataFrame({"conf_num": 1, "turn_seq": [1, 2, 3], "speech_date": ["2024-05-30", "2016-07-01", None],
                      "naas_cd": ["K", "L", "K"], "role_group": "legislator"})
    e = pt.enrich(t, pd.DataFrame({"conf_num": [1], "term": [21], "date": [None]}), resolver=rs)
    assert e.party.tolist()[:2] == ["무소속", "새누리당"]
    assert e.party_basis.tolist()[0] == "nearest_spell_before:leave"
    assert e.party_method.tolist() == ["person_spell", "label_lineage", None]
    assert set(e.party_method.dropna()) <= {"person_spell", "label_lineage"}
    assert e.is_speaker_nonpartisan.tolist()[0] is True


def test_f17_item_heads_in_lines_labels_and_bracketless_captions():
    assert pt.item_kind("報告事項】常任委員辭任및補任") == "committee"
    assert pt.item_kind("報告事項】議員辭職") == "exit"
    toks = [("head", "【報告事項】"), ("line", "  ○常任委員辭任및補任"),
            ("table", "【報告事項】", ["委員名", "辭任委員會", "補任委員會", "交涉團體"], [["權哲賢", "敎育", "環境勞動", "한나라당"]]),
            ("label", "◯特別委員長選任"), ("line", "대법관임명동의에관한인사청문특별위원회"),
            ("label", "◯常任委員長職務代理指定"), ("line", "위원장직무대리  간사  진 수 희")]
    items = pt.segment_items(toks)
    assert [(i["kind"], len(i["tokens"])) for i in items] == [("committee", 1), ("committee", 1)]


def test_f18_duplicate_report_applied_once(lin):
    spans = _spans_t([("Q", "2012-05-30", "2016-05-29", "통합진보당", "비례대표")], 19)
    ev = _evt(19, _ev("roster", "2012-05-30", ["Q"], group="통합진보당"),
              _ev("leave", "2012-09-10", ["Q"], group="통합진보당", reason="탈당"),
              _ev("switch", "2012-10-31", ["Q"], from_party="어느 교섭단체에도 속하지 아니하는 의원",
                  to_party="진보\n정의당", conf_num=36516),
              _ev("switch", "2012-10-31", ["Q"], from_party="무소속", to_party="진보정의당", conf_num=36598))
    tb = pt.TermBuilder(19, spans, ev, lin, idx=None)
    tb.run()
    st = pd.DataFrame(tb.log)
    st = st[st.kind == "switch"].status.tolist()
    assert st == ["duplicate_report", "applied"] or st == ["applied", "duplicate_report"]
    assert tb.stats["duplicate_report"] == 1


def test_f16_cache_key_parts():
    assert pt._groups_digest(frozenset({"a", "b"})) != pt._groups_digest(frozenset({"a", "c"}))
    assert len(pt._hwp_parser_digest()) == 12


def test_f8_notice_about_same_named_party_of_another_era(lin, cal):
    ntr = {"미래한국당": [("2020-05-29", "미래통합당", "party_merge", 45300)]}
    assert pt.next_transition("미래한국당", "2008-03-19", "9999-12-31", lin, ntr) is None
    assert pt.next_transition("미래한국당", "2020-03-01", "2020-06-30", lin, ntr)[:2] == ("2020-05-29", "미래통합당")
    rs = pt.Resolver(spells=pd.DataFrame(columns=["naas_cd", "term", "stint", "party", "start", "end", "spell_id", "basis",
                                                  "is_speaker_nonpartisan", "party_before_speaker"]),
                     spans=pd.DataFrame(columns=["naas_cd", "term", "stint", "seat_start", "seat_end", "party_at_entry"]),
                     ntr=ntr, lin=lin, cal=cal, windows=pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"]))
    assert rs.family("미래한국당", "2008-04-10") == "미래한국당"
    assert rs.ruling("자유한국당", "2016-07-01")[0] == "ruling"          # a label adopted later by rename


def test_f5_no_record_after_unreadable_report(lin):
    inv = pd.DataFrame({"conf_num": [1, 2, 3], "term": [16, 16, 16], "date": ["2003-09-01", "2003-09-26", "2003-10-10"],
                        "source": ["xml", "xml", "xml"], "n_empty_party_items": [0, 4, 0]})
    per = pt.gap_periods_from_inventory(inv)
    assert per[16] == [("2003-09-02", "2003-09-26", 2, "empty_party_items")]
    spans = _spans_t([("A", "2000-05-30", "2004-05-29", "새천년민주당", "서울"),
                      ("B", "2000-05-30", "2004-05-29", "새천년민주당", "경기")], 16)
    ev = _evt(16, _ev("roster", "2000-05-30", ["A"], group="새천년민주당"),
              _ev("roster", "2000-05-30", ["B"], group="새천년민주당"),
              _ev("observation", "2003-12-01", ["B"], group="새천년민주당", role_in_row="name"))
    tb = pt.TermBuilder(16, spans, ev, lin, idx=None, gap_dates=["2003-09-26"])
    sp = tb.run()
    w = pt.unreadable_report_windows(sp, {16: tb}, per)
    assert [(x["naas_cd"], x["lo"], x["hi"]) for x in w] == [("A", "2003-09-02", "2004-05-29")]
    m = pt.mark_uncertain(sp, pd.DataFrame(w))
    assert m.unconfirmed_after_gap.tolist() == [True, False] and m.uncertain.tolist() == [False, False]


# ----------------------------------------------------------------------------- R2: decision 6 (partyless president)

@pytest.mark.parametrize("party,date,expected", [
    ("새천년민주당", "2002-05-06", "ruling"),        # 김대중 left 새천년민주당: his last party stays ruling
    ("한나라당", "2002-06-01", "opposition"),
    ("새천년민주당", "2003-10-01", "ruling"),        # 노무현 left 새천년민주당 2003-09-29
    ("열린우리당", "2003-12-01", "opposition"),      # not a lineage successor of 새천년민주당
    ("새천년민주당", "2004-04-01", "ruling"),        # suspension inside the partyless window
    ("열린우리당", "2004-05-20", "ruling"),          # joined 열린우리당
    ("열린우리당", "2007-02-27", "ruling"),
    ("열린우리당", "2007-02-28", "ruling"),          # formal exit: last party
    ("대통합민주신당", "2007-08-19", "opposition"),  # founded 2007-08-05, not yet the successor
    ("대통합민주신당", "2007-08-20", "ruling"),      # 열린우리당 merged into it
    ("중도통합민주당", "2007-09-01", "opposition"),
    ("통합민주당", "2008-02-18", "ruling"),          # 대통합민주신당 -> 통합민주당 2008-02-17
    ("한나라당", "2008-02-24", "opposition"),
    ("한나라당", "2008-02-25", "ruling"),
])
def test_r2_partyless_last_party_and_successors(rs, party, date, expected):
    st, why, _ = rs.ruling(party, date)
    assert (st, why) == (expected, None)


@pytest.mark.parametrize("date,state", [("2002-05-06", "partyless"), ("2007-02-28", "partyless"),
                                        ("2017-04-01", "acting"), ("2025-05-01", "acting")])
def test_r2_independent_only_null_in_acting(rs, date, state):
    st, why, pr = rs.ruling("무소속", date)
    assert pr["presidency_state"] == state
    if state == "acting":
        assert (st, why) == (None, "acting")
    else:
        assert (st, why) == ("independent", None)


def test_r2_presidency_columns(cal):
    p = pt.presidency_on("2007-09-01", cal)
    assert (p["presidency_state"], p["president_party"], p["president_last_party"], p["last_party_since"]) == \
        ("partyless", None, "열린우리당", "2007-02-27")
    p = pt.presidency_on("2004-04-01", cal)
    assert (p["presidency_state"], p["president_last_party"], p["acting_president"]) == ("suspended", "새천년민주당", "고건")
    p = pt.presidency_on("2013-03-01", cal)
    assert (p["presidency_state"], p["president_party"], p["president_last_party"]) == ("normal", "새누리당", "새누리당")
    p = pt.presidency_on("2017-04-01", cal)
    assert (p["presidency_state"], p["president"], p["president_last_party"]) == ("acting", None, None)
    assert pt.presidency_on("1990-01-01", cal)["presidency_state"] is None      # outside: NULL, not 'vacant'
    assert set(cal.presidency_state) <= set(pt.PRESIDENCY_STATES) and "vacant" not in pt.PRESIDENCY_STATES
    # the partyless windows of decision 6 (the suspension is 'suspended' with president_last_party set)
    pl = cal[cal.president_party.isna() & cal.president.notna()]
    assert pl[["start", "end"]].values.tolist() == [["2002-05-06", "2003-02-24"], ["2003-09-29", "2004-03-11"],
                                                   ["2004-03-12", "2004-05-14"], ["2004-05-15", "2004-05-19"],
                                                   ["2007-02-28", "2008-02-24"]]


def _cal_csv(tmp_path, rows):
    cols = ["start", "end", "president", "status", "acting_president", "pres_party_formal", "pres_party_elected_from",
            "camp", "source", "note", "pres_last_party"]
    p = tmp_path / "cal.csv"
    pd.DataFrame([dict(zip(cols, r)) for r in rows], columns=cols).to_csv(p, index=False)
    return p


def test_r2_load_calendar_guards(tmp_path):
    ok = [("2000-01-01", "2000-12-31", "A", "in_office", "", "P", "P", "c", "s", "", ""),
          ("2001-01-01", "2001-12-31", "A", "in_office", "", "", "P", "c", "s", "", "P"),
          ("2002-01-01", "2002-06-30", "", "vacant_after_removal", "B", "", "P", "c", "s", "", ""),
          ("2002-07-01", "", "C", "in_office", "", "Q", "Q", "c", "s", "", "")]
    c = pt.load_calendar(_cal_csv(tmp_path, ok))
    assert c.presidency_state.tolist() == ["normal", "partyless", "acting", "normal"]
    assert c.president_last_party.tolist() == ["P", "P", None, "Q"]
    assert c.last_party_since.iloc[1] == "2000-12-31"
    bad_last = [list(r) for r in ok]
    bad_last[1][10] = "X"                                  # pres_last_party contradicts the earlier rows
    with pytest.raises(ValueError, match="pres_last_party"):
        pt.load_calendar(_cal_csv(tmp_path, bad_last))
    no_acting = [list(r) for r in ok]
    no_acting[2][4] = ""                                   # removal without an acting president ('vacant')
    with pytest.raises(ValueError, match="acting president"):
        pt.load_calendar(_cal_csv(tmp_path, no_acting))
    no_prev = [list(r) for r in ok][1:]
    with pytest.raises(ValueError, match="no earlier party"):
        pt.load_calendar(_cal_csv(tmp_path, no_prev))
    legacy = pd.read_csv(_cal_csv(tmp_path, ok)).drop(columns=["pres_last_party"])   # column absent: derived
    legacy.to_csv(tmp_path / "legacy.csv", index=False)
    assert pt.load_calendar(tmp_path / "legacy.csv").president_last_party.tolist() == ["P", "P", None, "Q"]


def test_r2_exit_date_verified_on_official_source():
    v = pt.verify_calendar()
    r = v[(v.row_start == "2007-02-28") & (v.check == "start")].iloc[0]
    assert r.found and r.official_source and r.source_file == "ktv_77096.html"
    lp = v[v.check == "last_party"]
    assert len(lp) == 5 and lp.found.all()


def test_r2_enrich_partyless_columns(cal, lin):
    sp = pd.DataFrame([
        {"naas_cd": "U", "term": 17, "stint": 1, "party": "열린우리당", "start": "2004-05-30", "end": "2007-08-19",
         "spell_id": "u1", "basis": "start_roster", "is_speaker_nonpartisan": False, "party_before_speaker": None},
        {"naas_cd": "U", "term": 17, "stint": 1, "party": "대통합민주신당", "start": "2007-08-20", "end": "2008-02-16",
         "spell_id": "u2", "basis": "lineage_relabel", "is_speaker_nonpartisan": False, "party_before_speaker": None},
        {"naas_cd": "H", "term": 17, "stint": 1, "party": "한나라당", "start": "2004-05-30", "end": "2008-05-29",
         "spell_id": "h1", "basis": "start_roster", "is_speaker_nonpartisan": False, "party_before_speaker": None},
        {"naas_cd": "I", "term": 17, "stint": 1, "party": "무소속", "start": "2004-05-30", "end": "2008-05-29",
         "spell_id": "i1", "basis": "start_roster", "is_speaker_nonpartisan": False, "party_before_speaker": None}])
    spans = pd.DataFrame([{"naas_cd": k, "term": 17, "stint": 1, "seat_start": "2004-05-30", "seat_end": "2008-05-29",
                           "party_at_entry": p} for k, p in (("U", "열린우리당"), ("H", "한나라당"), ("I", "무소속"))])
    rs = pt.Resolver(spells=sp, spans=spans, ntr={}, lin=lin, cal=cal,
                     windows=pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"]))
    t = pd.DataFrame({"conf_num": 1, "turn_seq": range(1, 7),
                      "speech_date": ["2007-02-27", "2007-02-28", "2007-09-01", "2007-09-01", "2007-09-01", "2007-02-28"],
                      "naas_cd": ["U", "U", "U", "H", "I", None],
                      "role_group": ["legislator"] * 5 + ["nonlegislator"]})
    e = pt.enrich(t, pd.DataFrame({"conf_num": [1], "term": [17], "date": ["2007-02-27"]}), resolver=rs)
    assert e.presidency_state.tolist() == ["normal", "partyless", "partyless", "partyless", "partyless", "partyless"]
    assert e.president_party.tolist() == ["열린우리당", None, None, None, None, None]
    assert e.president_last_party.tolist() == ["열린우리당"] * 6
    assert e.ruling_status.tolist() == ["ruling", "ruling", "ruling", "opposition", "independent", None]
    assert e.ruling_null_reason.tolist() == [None] * 6
    assert "president_last_party" in pt.ENRICH_COLS
