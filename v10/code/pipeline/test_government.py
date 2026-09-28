"""Tests for government.py.  Run:  python -m pytest -q test_government.py"""
import datetime as dt
import os
import sys

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))

import government as G  # noqa: E402
import legacy_rules as L  # noqa: E402

HAVE_CAL = os.path.exists(G.CALENDAR_PATH)
HAVE_PANEL = os.path.exists(G.LEGACY_PANEL_PATH)        # legacy_296 panel (minister-data working tree)
needs_cal = pytest.mark.skipif(not HAVE_CAL, reason="president_calendar.csv not available")
needs_panel = pytest.mark.skipif(not (HAVE_CAL and HAVE_PANEL), reason="panel CSV not available")


# ------------------------------------------------------------------ Hanja
def test_hanja_reading_table_well_formed():
    s = G._HANJA_READINGS_STR
    assert len(s) % 2 == 0
    keys = [s[i] for i in range(0, len(s), 2)]
    assert len(keys) == len(set(keys))
    assert all("가" <= s[i + 1] <= "힣" for i in range(0, len(s), 2))
    assert all(G.HANJA_RE.match(k) for k in keys)


@pytest.mark.parametrize("syl,exp", [("리", "이"), ("로", "노"), ("녀", "여"), ("류", "유"), ("림", "임"),
                                     ("량", "양"), ("력", "역"), ("라", "나"), ("니", "이"), ("김", "김")])
def test_initial_sound_rule(syl, exp):
    assert G.initial_sound_rule(syl) == exp


@pytest.mark.parametrize("src,exp", [
    ("副總理兼財政經濟部長官", "부총리겸재정경제부장관"),
    ("\uf92f動部長官", "노동부장관"),          # compatibility ideograph 勞 (U+F92F), seen in 16대 pages
    ("女性部長官", "여성부장관"),
    ("國務總理", "국무총리"),
    ("大統領秘書室長", "대통령비서실장"),
    ("서울地方檢察廳檢事長", "서울지방검찰청검사장"),
    ("國民年金管理公團理事長", "국민연금관리공단이사장"),
])
def test_hanja_to_hangul(src, exp):
    out, left = G.hanja_to_hangul(src)
    assert out == exp and left == 0


# ------------------------------------------------------------------ ministry normalisation
@pytest.mark.parametrize("pos,exp,rule_part", [
    ("국방부장관", "국방부", "lexicon"),
    ("부총리겸기획재정부장관", "기획재정부", "deputy_pm+lexicon"),
    ("부총리겸교육부장관후보자", "교육부", "deputy_pm"),
    ("법무부장관직무대행", "법무부", "lexicon"),
    ("기획재정부제1차관", "기획재정부", "lexicon"),
    ("국토교통부제2차관", "국토교통부", "lexicon"),
    ("외교통상부차관보", "외교통상부", "lexicon"),
    ("보건복지가족부장관", "보건복지가족부", "lexicon"),
    ("여성부장관", "여성부", "lexicon"),
    ("국무총리", "국무총리", "lexicon"),
    ("국무총리실장", "국무총리실", "lexicon"),
    ("국무총리비서실장", "국무총리비서실", "lexicon"),
    ("국무조정실장", "국무조정실", "lexicon"),
    ("대통령권한대행국무총리", "국무총리", "acting_for_president"),
    ("대통령권한대행 국무총리", "국무총리", "acting_for_president"),
    ("국무총리직무대행부총리겸기획재정부장관", "기획재정부", "acting_for_pm"),
    ("국무총리직무대행", "국무총리", "lexicon"),
    ("서울지방국세청장", "국세청", "regional_office"),
    ("부산광역시지방경찰청장", "경찰청", "regional_office"),
    ("서울특별시경찰청장", "경찰청", "regional_office"),
    ("중부지방해양경찰청장", "해양경찰청", "regional_office"),
    ("수원지방검찰청검사장", "검찰청", "regional_office"),
    ("서울지방고용노동청장", "지방고용노동청", "regional_office"),
    ("한강유역환경청장", "유역환경청", "regional_office"),
    ("공정거래위원장", "공정거래위원회", "lexicon"),
    ("방송통신위원장", "방송통신위원회", "lexicon"),
    ("국세청장", "국세청", "lexicon"),
    ("법제처장", "법제처", "lexicon"),
    ("식품의약품안전처장", "식품의약품안전처", "lexicon"),
    ("검찰총장", "검찰청", "lexicon"),
    ("법부무장관", "법무부", "typo_map"),
    ("국방장관후보자", "국방부", "typo_map"),
    ("특임장관", "특임장관", "lexicon"),
    ("특임장관실제3조정관", "특임장관", "lexicon"),
    ("산업통상부장관", "산업통상부", "lexicon"),
    ("副總理兼財政經濟部長官", "재정경제부", "hanja+deputy_pm+lexicon"),
    ("\uf92f動部長官", "노동부", "hanja+lexicon"),
    ("光州地方勞動廳長", "지방노동청", "hanja+regional_office"),
    ("주미합중국대한민국대사", "재외공관", "overseas_mission"),
    ("대구\u318d경북지방병무청장", "병무청", "regional_office"),     # U+318D -> U+119E under NFKC
    ("대구\u00b7경북지방병무청장", "병무청", "regional_office"),
    ("원주지방환경관리청장", "지방환경청", "regional_office"),
    ("특임차관", "특임장관", "typo_map"),
    ("해양수산차관보", "해양수산부", "typo_map"),
    ("산업자원통상부에너지산업정책관", "산업통상자원부", "typo_map"),
    ("경제사회발전노사정위원장", "경제사회발전노사정위원회", "lexicon"),
    ("영화진흥위원장", "영화진흥위원회", "generic_commission"),
    ("사행산업통합감독위원장직무대행", "사행산업통합감독위원회", "generic_commission"),
    ("4\u318d16세월호참사특별조사위원장", "416세월호참사특별조사위원회", "generic_commission"),
    ("영화진흥위원회부위원장", "영화진흥위원회", "generic_suffix"),
    ("대도시권광역교통위원회광역환승과장", "대도시권광역교통위원회", "generic_suffix"),
    ("주OECD대한민국대표부대사", "재외공관", "overseas_mission"),
    ("서울구치소장", "교정기관", "regional_office"),
    ("대전지방공정거래사무소장", "공정거래위원회", "regional_office"),
    ("특별감찰관후보자", "특별감찰관", "lexicon"),
    ("대통령정책실장", "대통령비서실", "lexicon"),
    ("국무총식", "국무총리", "typo_map"),
    ("친일반민족행위자재산조사위원회사무처장", "친일반민족행위자재산조사위원회", "generic_suffix"),   # not '회사'
    # circle markers and bullets are separators
    ("○環境部水質保全局長", "환경부", "hanja+lexicon"), ("◯關稅廳長", "관세청", "hanja+lexicon"),
    ("대구\u2219경북지방병무청장", "병무청", "regional_office"), ("대구\u2022경북지방병무청장", "병무청", "regional_office"),
    ("駐토론토大韓民國總領事", "재외공관", "hanja+overseas_mission"),
    ("주미합중국대한민국대사관공사", "재외공관", "overseas_mission"),       # diplomatic 公使, not a public corp
    # deputy head of a commission printed without 회
    ("금융부위원장", "금융위원회", "lexicon"),
])
def test_normalize_ministry(pos, exp, rule_part):
    m, rule = G.normalize_ministry(pos)
    assert m == exp
    assert rule_part in rule


@pytest.mark.parametrize("pos,rule", [
    (None, "empty"), ("", "empty"), ("증인", "no_org"), ("진술인", "no_org"),
    ("서울특별시장", "local_government"), ("경기도교육감", "local_government"),
    ("한국도로공사사장", "non_government"), ("구리농수산물도매시장관리공사전무이사", "non_government"),
    ("국방과학연구소장", "non_government"), ("공군참모총장", "military"),
    ("서울고등법원장", "judiciary"),
    ("국회사무총장", "assembly_body"),
    # NA committee chairs are never government commissions
    ("재정경제위원장", "no_org"), ("예산결산특별위원장", "no_org"), ("법안심사소위원장", "no_org"),
    ("법제사법위원회수석전문위원", "no_org"), ("예산결산특별위원회위원", "no_org"), ("법안심사소위원회위원장", "no_org"),
    # generic '...부/처' of schools and memorial halls are not government bodies
    ("국립국악고등학교서무부장", "no_org"), ("독립기념관사무처장", "no_org"),
    ("NH투자증권주식회사대표이사", "non_government"), ("삼성전자회사원", "non_government"),
    # company markers (㈜ U+321C, ㈱ U+3231, (주), (株)) are kept as '주식회사'
    ("삼성물산㈜대표이사", "non_government"), ("코레일테크㈜경영관리본부장", "non_government"),
    ("(주)강원랜드사장", "non_government"), ("㈱대한항공전무", "non_government"),
    ("(株)漢陽木材및(株)漢陽工營社長", "non_government"),
    # not Korean missions abroad: KOTRA office, foreign mission in Korea
    ("주폴란드공화국대한민국무역진흥공사무역관장", "non_government"), ("駐폴란드共和國大韓民國貿易振興公社貿易館長", "non_government"),
    ("주한미국대사", "no_org"),
    # the 부 of a deputy title is not a generic org suffix
    ("세월호인양추진단부단장", "no_org"), ("혁신도시발전추진단부단장", "no_org"),
])
def test_normalize_ministry_none(pos, rule):
    m, r = G.normalize_ministry(pos)
    assert m is None and r == rule


def test_typo_keys_shadowed_by_lexicon():
    # v9 typo keys that only extend a correct lexicon key are labelled 'lexicon' (same ministry)
    assert G.normalize_ministry("행정자치부장관") == ("행정자치부", "lexicon")
    assert G.normalize_ministry("행정자치부장관후보자") == ("행정자치부", "lexicon")
    assert G.normalize_ministry("농림수산식품부제1차관") == ("농림수산식품부", "lexicon")
    assert G.normalize_ministry("行政自治部長官") == ("행정자치부", "hanja+lexicon")
    assert G.normalize_ministry("여성부가족부장관") == ("여성가족부", "typo_map")   # target differs: kept
    # dropping a shadowed key never changes the ministry: its longest remaining prefix key
    # (always a lexicon key) maps to the same target
    for k in G._TYPO_SHADOWED:
        best = next(pk for pk in G._PREFIX_KEYS if k.startswith(pk))
        assert best in G.ORG_LEXICON and G.ORG_LEXICON[best] == G._TYPO[k], k


def test_typo_key_needs_title():
    # v9 typo keys '국방'/'과학기술'/'재정경제' must not fire inside longer names
    assert G.normalize_ministry("재정경제위원장대리")[0] != "재정경제부"
    assert G.normalize_ministry("과학기술정책연구원장")[0] != "과학기술부"


def test_lineage():
    assert G.ministry_family("여성부") == G.ministry_family("여성가족부") == "gender_family"
    assert set(G.ministry_lineage("국토해양부")) == {"land_transport", "oceans"}
    assert G.ministry_family("국세청") is None
    assert G.ministry_family(None) is None


def test_org_family_renames_and_units():
    assert G.ministry_family("식품의약품안전청") == G.ministry_family("식품의약품안전처") == "food_drug_safety"
    assert G.ministry_family("문화재청") == G.ministry_family("국가유산청")
    assert G.ministry_family("지방고용노동청") == G.ministry_family("고용노동부") == "labor"
    assert G.ministry_family("재외공관") == G.ministry_family("외교부") == "foreign_affairs"
    # ORG_FAMILY never widens panel matching
    assert G.ministry_lineage("중소기업청") == () and G.ministry_lineage("국가보훈처") == ()
    assert not set(G.ORG_FAMILY) & set(G.MINISTRY_LINEAGE)


@needs_panel
def test_lineage_covers_panel_ministries():
    p = pd.read_csv(G.PANEL_PATH)
    assert set(p["ministry"]) <= set(G.MINISTRY_LINEAGE)


# ------------------------------------------------------------------ calendar / admin
@needs_cal
def test_calendar_contiguous_and_ideology_matches_camp():
    cal = G.load_calendar()
    for r in cal.itertuples(index=False):
        if r.status == "in_office":
            assert G.ADMIN_IDEOLOGY[r.president].lower() == r.camp


@needs_cal
@pytest.mark.parametrize("d,exp", [
    ("2001-06-01", ("김대중", "Progressive", "normal")),       # v9 coded 김대중 Conservative
    ("2004-03-20", ("노무현", "Progressive", "suspended")),
    ("2016-12-20", ("박근혜", "Conservative", "suspended")),
    ("2017-03-09", ("박근혜", "Conservative", "suspended")),
    ("2017-03-10", ("권한대행(황교안)", None, "acting")),
    ("2017-05-09", ("권한대행(황교안)", None, "acting")),
    ("2017-05-10", ("문재인", "Progressive", "normal")),
    ("2022-05-09", ("문재인", "Progressive", "normal")),
    ("2022-05-10", ("윤석열", "Conservative", "normal")),
    ("2024-12-14", ("윤석열", "Conservative", "suspended")),
    ("2025-01-10", ("윤석열", "Conservative", "suspended")),
    ("2025-04-04", ("권한대행(한덕수)", None, "acting")),
    ("2025-05-02", ("권한대행(이주호)", None, "acting")),
    ("2025-06-04", ("이재명", "Progressive", "normal")),
    ("2026-09-01", ("이재명", "Progressive", "normal")),
    (None, (None, None, None)),
    ("1990-01-01", (None, None, None)),
    ("not a date", (None, None, None)),
])
def test_admin_for_date(d, exp):
    assert G.admin_for_date(d) == exp


@needs_cal
def test_suspended_admin_parameter():
    assert G.admin_for_date("2016-12-20", suspended_admin="acting") == ("권한대행(황교안)", None, "suspended")
    assert G.admin_for_date("2025-01-10", suspended_admin="acting") == ("권한대행(최상목)", None, "suspended")


# ------------------------------------------------------------------ panel audit (real file)
@needs_panel
def test_panel_known_issues():
    p = G.load_panel()
    assert len(p) == 296
    kim = p[(p["name"] == "김진표") & (p["ministry"] == "교육인적자원부")].iloc[0]
    assert "end_before_start" in kim["issues"] and not kim["valid_dates"]
    choi = p[(p["name"] == "최상목") & (p["ministry"] == "기획재정부")].iloc[0]
    assert choi["end_imputed"] and choi["end_eff"] == dt.date(2025, 6, 3)
    for nm in ("강선우", "이진숙"):
        r = p[p["name"] == nm].iloc[0]
        assert "missing_start" in r["issues"] and not r["valid_dates"]
    assert int(p["valid_dates"].sum()) == 293
    assert p["minister_panel_id"].is_unique
    a = G.audit_panel(p)
    assert {"end_before_start", "missing_end", "missing_start"} <= set(a["issue"])


# ------------------------------------------------------------------ linkage (stub panel)
STUB = [
    # ministry, name, start, end, dual_office
    ("외교통상부", "유명환", "2008-02-29", "2010-09-17", False),
    ("산업자원부", "김영주", "2007-01-29", "2008-02-24", False),
    ("고용노동부", "김영주", "2017-08-14", "2018-11-08", True),
    ("여성가족부", "백희영", "2009-09-24", "2011-09-15", False),
    ("교육인적자원부", "김진표", "2005-01-28", "2005-01-03", True),     # end before start
    ("기획재정부", "최상목", "2023-12-28", None, False),                 # missing end
    ("교육부", "이진숙", None, None, False),                             # missing start
    ("국무총리", "이한동", "2000-05-22", "2002-07-11", True),
    ("재정경제부", "김진표", "2003-02-27", "2004-01-09", False),
    ("국방부", "김관진", "2010-11-26", "2013-02-24", False),
    ("국방부", "김관진", "2013-02-25", "2014-07-03", False),
    ("특임장관", "주호영", "2009-09-30", "2010-08-10", True),
    ("법무부", "정성호", "2025-07-18", None, False),                     # open: end_eff = date.max
]


@pytest.fixture(scope="module")
def stub_index(tmp_path_factory):
    d = tmp_path_factory.mktemp("panel")
    rows = [dict(ministry=m, name=n, name_en="", start=s, end=e, admin="", admin_ideology="",
                 dual_office=do, notes="") for m, n, s, e, do in STUB]
    path = d / "panel.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    if not HAVE_CAL:
        pytest.skip("calendar needed for end imputation")
    return G.PanelIndex(G.load_panel(str(path)), buffer_days=7, nominee_pre_days=60, nominee_post_days=60)


def _link(idx, name, ministry, d, role="minister"):
    c, lab = idx.link(name, ministry, d, role)
    return (c.minister_panel_id if c is not None else None), lab


def test_link_exact_and_lineage(stub_index):
    assert _link(stub_index, "유명환", "외교통상부", "2009-01-05") == ("유명환|외교통상부|2008-02-29", "tenure:exact")
    # transcript says 여성부 (2008-2010 name), panel says 여성가족부
    assert _link(stub_index, "백희영", "여성부", "2010-02-01") == ("백희영|여성가족부|2009-09-24", "tenure:lineage")


def test_link_buffer(stub_index):
    assert _link(stub_index, "유명환", "외교통상부", "2010-09-20")[1] == "buffer:exact"
    assert _link(stub_index, "유명환", "외교통상부", "2010-09-30") == (None, "unmatched:outside_window")


def test_no_date_free_fallbacks(stub_index):
    # v9 fallback_2: acting minister 2006 linked to his 2008 appointment (admin 이명박 in 2006)
    assert _link(stub_index, "유명환", "외교통상부", "2006-10-27", "minister_acting") == \
        (None, "unmatched:acting_no_acting_record")
    assert _link(stub_index, "유명환", "외교통상부", "2006-10-27", "minister") == (None, "unmatched:outside_window")
    cand = [{"ministry": "외교통상부", "start_dt": pd.Timestamp("2008-02-29"), "end_dt": pd.Timestamp("2010-09-17")}]
    assert L.link_minister_panel_v9("유명환", "외교통상부", pd.Timestamp("2006-10-27"), cand)[1] == "fallback_2_name_ministry_anydate"
    # v9 fallback_3: single entry, any ministry
    assert _link(stub_index, "주호영", "국방부", "2010-01-01") == (None, "unmatched:ministry_mismatch")


def test_link_acting_label_precedes_ministry_mismatch(stub_index):
    # an acting PM whose own panel rows are for a ministry: still the acting label
    assert _link(stub_index, "김관진", "국무총리", "2012-01-01", "minister_acting") == \
        (None, "unmatched:acting_no_acting_record")


def test_link_acting_never_linked(stub_index):
    # the panel has no acting periods: an acting minister is never linked
    assert _link(stub_index, "유명환", "외교통상부", "2009-01-05", "minister_acting") == \
        (None, "unmatched:acting_inside_own_tenure")
    assert _link(stub_index, "유명환", "외교통상부", "2010-09-20", "minister_acting") == \
        (None, "unmatched:acting_no_acting_record")
    assert _link(stub_index, "홍길동", "외교통상부", "2009-01-05", "minister_acting")[1] == "unmatched:name_not_in_panel"


def test_link_nominee_window(stub_index):
    assert _link(stub_index, "김영주", "고용노동부", "2017-07-28", "minister_nominee") == \
        ("김영주|고용노동부|2017-08-14", "nominee:exact")
    assert _link(stub_index, "김영주", "고용노동부", "2017-04-01", "minister_nominee")[1] == "unmatched:outside_window"
    # panel start earlier than the hearing (nomination / approximate dates): up to 60 days after
    assert _link(stub_index, "김영주", "고용노동부", "2017-09-20", "minister_nominee")[1] == "nominee:exact"
    # beyond the post-window but inside the panel tenure: linked with a conflict label
    assert _link(stub_index, "김영주", "고용노동부", "2017-11-01", "minister_nominee") == \
        ("김영주|고용노동부|2017-08-14", "nominee_in_tenure:exact")
    assert _link(stub_index, "김영주", "고용노동부", "2019-01-10", "minister_nominee")[1] == "unmatched:outside_window"
    # a following appointment's nominee window beats the current tenure
    assert _link(stub_index, "김관진", "국방부", "2013-02-20", "minister_nominee") == \
        ("김관진|국방부|2013-02-25", "nominee:exact")


def test_same_name_two_people(stub_index):
    assert _link(stub_index, "김영주", "산업자원부", "2007-06-01")[0] == "김영주|산업자원부|2007-01-29"
    assert _link(stub_index, "김영주", "고용노동부", "2018-01-10")[0] == "김영주|고용노동부|2017-08-14"
    assert _link(stub_index, "김영주", "고용노동부", "2007-06-01")[1] == "unmatched:outside_window"


def test_consecutive_appointments_pick_containing_one(stub_index):
    assert _link(stub_index, "김관진", "국방부", "2013-02-25")[0] == "김관진|국방부|2013-02-25"
    assert _link(stub_index, "김관진", "국방부", "2013-02-24")[0] == "김관진|국방부|2010-11-26"


def test_invalid_panel_rows(stub_index):
    assert _link(stub_index, "김진표", "교육인적자원부", "2005-06-01") == (None, "unmatched:panel_dates_invalid")
    assert _link(stub_index, "이진숙", "교육부", "2025-07-16", "minister_nominee") == (None, "unmatched:panel_dates_invalid")
    # the same person's other (valid) appointment still links
    assert _link(stub_index, "김진표", "재정경제부", "2003-06-01")[1] == "tenure:exact"


def test_end_imputed(stub_index):
    pid, lab = _link(stub_index, "최상목", "기획재정부", "2024-10-10")
    assert pid == "최상목|기획재정부|2023-12-28" and lab == "tenure:exact:end_imputed"
    assert _link(stub_index, "최상목", "기획재정부", "2025-07-01")[1] == "unmatched:outside_window"


def test_open_ended_row_windows(stub_index):
    # end_eff = date.max: the buffer / nominee windows saturate instead of overflowing
    r = stub_index.panel.set_index("name").loc["정성호"]
    assert r["end_eff"] == dt.date.max and r["end_imputed"]
    assert _link(stub_index, "정성호", "법무부", "2025-07-15") == ("정성호|법무부|2025-07-18", "buffer:exact:end_imputed")
    assert _link(stub_index, "정성호", "법무부", "2025-07-01")[1] == "unmatched:outside_window"
    assert _link(stub_index, "정성호", "법무부", "2030-01-01")[1] == "tenure:exact:end_imputed"
    assert _link(stub_index, "정성호", "법무부", "2025-06-20", "minister_nominee")[1] == "nominee:exact:end_imputed"
    assert _link(stub_index, "정성호", "법무부", "2025-07-15", "minister_acting")[1] == "unmatched:acting_no_acting_record"
    assert _link(stub_index, "정성호", "법무부", "2025-08-15", "minister_acting")[1] == "unmatched:acting_inside_own_tenure"


def test_shift_saturates():
    assert G._shift(dt.date.max, dt.timedelta(days=7)) == dt.date.max
    assert G._shift(dt.date.min, dt.timedelta(days=-7)) == dt.date.min
    assert G._shift(dt.date(2025, 1, 1), dt.timedelta(days=7)) == dt.date(2025, 1, 8)


@needs_panel
@pytest.mark.parametrize("name,ministry,d,role", [
    ("구윤철", "기획재정부", "2025-12-30", "minister"), ("송미령", "농림축산식품부", "2025-05-30", "minister"),
    ("정성호", "법무부", "2025-07-15", "minister"), ("김민석", "국무총리", "2025-06-30", "prime_minister"),
])
def test_real_panel_open_rows_do_not_overflow(name, ministry, d, role):
    # legacy_296 panel (moved from the default panel on 2026-09-28)
    c, lab = G._default_index(panel="legacy_296").link(name, ministry, d, role)
    assert c is not None and lab.startswith("buffer:") and lab.endswith(":end_imputed")


@needs_panel
def test_real_panel_enrich_open_row():
    t = pd.DataFrame({"speaker_pos": ["기획재정부장관"], "speaker_name": ["구윤철"],
                      "speech_date": ["2025-12-30"], "role": ["minister"]})
    out = G.enrich(t, None, panel_index=G._default_index(panel="legacy_296"))      # legacy_296 panel
    assert out.loc[0, "minister_panel_id"] == "구윤철|기획재정부|2026-01-02"
    assert out.loc[0, "link_method"].startswith("buffer:") and out[list(G.V2_LINK_COLUMNS)].isna().all().all()


def test_unmatched_reasons(stub_index):
    assert _link(stub_index, "홍길동", "국방부", "2010-01-01")[1] == "unmatched:name_not_in_panel"
    assert _link(stub_index, "유명환", None, "2009-01-01")[1] == "unmatched:no_ministry"
    assert _link(stub_index, "유명환", "외교통상부", None)[1] == "unmatched:no_date"
    assert _link(stub_index, None, "외교통상부", "2009-01-01")[1] == "unmatched:no_name"


def test_resolve_hanja_name(stub_index):
    assert stub_index.resolve_name("李漢東") == ("이한동", "hanja")
    assert stub_index.resolve_name("金振杓") == ("김진표", "hanja")
    assert stub_index.resolve_name("백희영")[1] == "hangul"
    assert stub_index.resolve_name("백영희") == ("백희영", "name_fix")
    assert stub_index.resolve_name("\u9fa6\u9fa7")[1] == "hanja_unreadable"   # no reading in any table
    assert stub_index.resolve_name(None)[1] == "empty"


def test_hangul_name_candidates():
    assert G.hangul_name_candidates("홍길동") == ["홍길동"]
    # surname reading with the initial-sound rule comes first (林 -> 임, not 림)
    assert G.hangul_name_candidates("林東源")[0] == "임동원"
    assert G.hangul_name_candidates("李漢東")[0] == "이한동"
    assert G.hangul_name_candidates("柳明桓")[0] == "유명환"      # 柳, 桓 in the built-in table


# ------------------------------------------------------------------ enrich contract
def _turns():
    return pd.DataFrame({
        "conf_num": [9, 9, 9, 3, 3, 3, 3, 5],
        "turn_seq": [1, 2, 3, 1, 2, 3, 4, 1],
        "speaker_pos": ["위원장", "외교통상부장관", "외교통상부장관직무대리", "위원", "國務總理", "서울지방국세청장",
                        "여성부장관", None],
        "speaker_name": ["홍길동", "유명환", "유명환", "김철수", "李漢東", "갑을병", "백희영", "무명"],
        "speech_date": ["2009-01-05", "2009-01-05", "2006-10-27", "2001-06-01", "2001-06-01", "2001-06-01",
                        None, "2017-04-01"],
        "role": ["chair", "minister", "minister_acting", "legislator", "prime_minister", "agency_head",
                 "minister", "witness"],
    }, index=[7, 3, 5, 1, 0, 2, 6, 4])


@needs_cal
def test_enrich_contract(stub_index):
    t = _turns()
    meetings = pd.DataFrame({"conf_num": [3, 9, 5], "date": ["2001-06-01", "2009-01-05", "2017-04-01"]})
    meetings.loc[0, "date"] = "2010-02-01"   # meeting 3's date used only for the null speech_date row
    out = G.enrich(t, meetings, panel_index=stub_index)
    assert list(out.index) == list(t.index) and len(out) == len(t)
    assert (out[t.columns] .fillna("<NA>") == t.fillna("<NA>")).all().all()
    for c in (*G.ADDED_COLUMNS, "presidency_state"):
        assert c in out.columns
    leg = out["role"].isin(["chair", "legislator"])
    assert out.loc[leg, "ministry_normalized"].isna().all()
    assert out.loc[leg, "link_method"].isna().all()
    assert (out.loc[leg, "ministry_rule"] == "legislator").all()
    r = out.loc[3]
    assert r["minister_panel_id"] == "유명환|외교통상부|2008-02-29" and r["dual_office"] == False  # noqa: E712
    assert r["admin"] == "이명박" and r["admin_ideology"] == "Conservative"
    assert out.loc[5, "link_method"] == "unmatched:acting_no_acting_record" and pd.isna(out.loc[5, "minister_panel_id"])
    assert out.loc[5, "admin"] == "노무현"                         # admin from the date, not the panel
    assert out.loc[0, "gov_link_name"] == "이한동" and out.loc[0, "link_method"] == "tenure:exact"
    assert out.loc[0, "admin"] == "김대중" and out.loc[0, "admin_ideology"] == "Progressive"
    assert out.loc[2, "ministry_normalized"] == "국세청" and pd.isna(out.loc[2, "link_method"])
    assert out.loc[6, "gov_date_source"] == "meeting_date" and out.loc[6, "link_method"] == "tenure:lineage"
    assert out.loc[4, "admin"] == "권한대행(황교안)" and pd.isna(out.loc[4, "admin_ideology"])
    assert out.loc[4, "presidency_state"] == "acting"
    assert out.attrs["government"]["n_turns"] == len(t)


@needs_cal
def test_enrich_keeps_existing_presidency_state(stub_index):
    t = _turns()
    t["presidency_state"] = "normal"
    out = G.enrich(t, None, panel_index=stub_index)
    assert (out["presidency_state"] == "normal").all()
    assert out.attrs["government"]["presidency_state_disagreements"] >= 1


@needs_cal
def test_enrich_subset_and_empty(stub_index):
    t = _turns()
    full = G.enrich(t, None, panel_index=stub_index)
    sub = G.enrich(t[t["conf_num"] == 9], None, panel_index=stub_index)
    cols = ["ministry_normalized", "minister_panel_id", "link_method", "admin"]
    assert sub[cols].fillna("<NA>").equals(full.loc[sub.index, cols].fillna("<NA>"))
    empty = G.enrich(t.iloc[0:0], None, panel_index=stub_index)
    assert len(empty) == 0 and "admin" in empty.columns


@pytest.mark.parametrize("pos,exp", [("국무총리직무대행", True), ("國務總理職務代行", True), ("국무총리 권한대행", True),
                                     ("국무총리", False), ("국무총리서리", False), ("대통령권한대행국무총리", False),
                                     ("국무총리직무대행부총리겸기획재정부장관", False), (None, False)])
def test_is_bare_acting_pm(pos, exp):
    assert G.is_bare_acting_pm(pos) is exp


@needs_cal
def test_enrich_bare_acting_pm_not_linked(stub_index):
    t = pd.DataFrame({"conf_num": [1, 1, 1], "turn_seq": [1, 2, 3],
                      "speaker_pos": ["국무총리직무대행", "국무총리", "대통령권한대행국무총리"],
                      "speaker_name": ["이한동", "이한동", "이한동"],
                      "speech_date": ["2001-06-01", "2001-06-01", "2001-06-01"],
                      "role": ["prime_minister"] * 3})
    out = G.enrich(t, None, panel_index=stub_index)
    assert pd.isna(out.loc[0, "minister_panel_id"]) and out.loc[0, "link_method"] == "unmatched:acting_inside_own_tenure"
    assert out.loc[0, "role"] == "prime_minister"          # role column untouched
    assert out.loc[1, "minister_panel_id"] == "이한동|국무총리|2000-05-22"
    assert out.loc[2, "minister_panel_id"] == "이한동|국무총리|2000-05-22"
    assert out.attrs["government"]["prime_minister_bare_acting_linked_as_acting"] == 1


@needs_cal
def test_enrich_date_choice_matches_party_timeline_rule(stub_index):
    t = pd.DataFrame({"conf_num": [1, 1, 2, 3, 4],
                      "speaker_pos": ["국방부장관"] * 5, "speaker_name": ["김관진"] * 5,
                      "speech_date": [None, "2011.06.01", "2011-06-01T10:00", "2011-06-01", None],
                      "date": ["2011-06-01", "2011-06-01", "2011-06-02", "2011-06-03", None],
                      "role": ["minister"] * 5})
    meetings = pd.DataFrame({"conf_num": [4], "date": ["2011-06-04"]})
    out = G.enrich(t, meetings, panel_index=stub_index)
    # null speech_date -> the turns' own meeting date column
    assert out.loc[0, "gov_date_source"] == "meeting_date" and out.loc[0, "admin"] == "이명박"
    assert out.loc[0, "link_method"] == "tenure:exact"
    # 10 characters but not an ISO date: chosen (as party_timeline does), labelled, null admin
    assert out.loc[1, "gov_date_source"] == "unparseable_speech_date" and pd.isna(out.loc[1, "admin"])
    assert out.loc[1, "link_method"] == "unmatched:no_date"
    # longer than 10 characters: not a speech_date (party_timeline uses == 10), meeting date used
    assert out.loc[2, "gov_date_source"] == "meeting_date"
    assert out.loc[3, "gov_date_source"] == "speech_date"
    # no turns date -> meetings argument
    assert out.loc[4, "gov_date_source"] == "meeting_date" and out.loc[4, "admin"] == "이명박"
    dg = out.attrs["government"]
    assert dg["date_fallback"] == {"turns.date": 2, "meetings.date": 1}
    assert dg["admin_null"]["unparseable_date"] == 1 and dg["admin_null"]["no_date"] == 0


@needs_cal
def test_enrich_na_role_group_is_not_legislator(stub_index):
    t = pd.DataFrame({"speaker_pos": ["증인", "국방부장관"], "speaker_name": ["갑", "김관진"],
                      "speech_date": ["2011-06-01", "2011-06-01"], "role": [None, "minister"],
                      "role_group": pd.array([pd.NA, "nonlegislator"], dtype="string")})
    out = G.enrich(t, None, panel_index=stub_index)
    assert out.loc[0, "ministry_rule"] == "no_org"
    assert out.loc[1, "ministry_normalized"] == "국방부"


@needs_cal
def test_enrich_does_not_modify_input(stub_index):
    t = _turns()
    t["text"] = "x" * 10
    before = t.copy(deep=True)
    out = G.enrich(t, None, panel_index=stub_index)
    assert list(t.columns) == list(before.columns)
    assert t.fillna("<NA>").equals(before.fillna("<NA>"))
    again = G.enrich(out, None, panel_index=stub_index)        # re-enrich an enriched frame
    assert out["admin"].fillna("<NA>").equals(again["admin"].fillna("<NA>"))
    assert list(t.columns) == list(before.columns)


def test_enrich_requires_role():
    with pytest.raises(KeyError):
        G.enrich(_turns().drop(columns=["role"]), None)


# ------------------------------------------------------------------ consistency with party_timeline
@needs_cal
def test_presidency_state_matches_party_timeline():
    """government.admin_for_date and party_timeline.presidency_on give the same presidency_state
    (and the same president / acting president) on every calendar boundary day, the days next
    to it, and the 1st and 15th of every month 1998-03 .. 2026-12."""
    try:
        import party_timeline as PT
        pcal = PT.load_calendar()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"party_timeline not importable: {e!r}")
    cal = G.load_calendar()
    days = set()
    for r in cal.itertuples(index=False):
        for b in (r.start_d, r.end_d):
            if b is not None and not pd.isna(b):
                days.update(b + dt.timedelta(days=k) for k in (-1, 0, 1))
    for y in range(1998, 2027):
        for m in range(1, 13):
            days.update({dt.date(y, m, 1), dt.date(y, m, 15)})
    days = sorted(d for d in days if dt.date(1998, 2, 25) <= d <= dt.date(2026, 12, 31))
    for d in days:
        k = d.isoformat()
        adm, ide, st = G.admin_for_date(k, cal)
        pt = PT.presidency_on(k, pcal)
        assert st == pt["presidency_state"], k
        if st in ("normal", "partyless", "suspended"):          # 'partyless': decision 6 (R2)
            assert adm == pt["president"] and ide is not None, k
        else:
            assert adm == f"권한대행({pt['acting_president']})" and ide is None, k
    assert len(days) > 600


# ------------------------------------------------------------------ transcript evidence on panel dates
def test_panel_date_evidence(stub_index):
    import government_eval as E
    obs = pd.DataFrame({
        "speaker_name": ["유명환", "유명환", "유명환", "홍길동", "김영주"],
        "ministry_normalized": ["외교통상부", "외교통상부", "외교통상부", "국방부", "고용노동부"],
        "speech_date": ["2009-01-05", "2008-01-10", "2010-09-20", "2009-01-01", "2017-10-01"],
        "n": [10, 3, 2, 5, 4], "source": "xlsx"})
    per, miss = E.panel_date_evidence(obs, stub_index)
    r = per.set_index("minister_panel_id").loc["유명환|외교통상부|2008-02-29"]
    assert r["obs_rows"] == 15 and r["rows_before_start_beyond_buffer"] == 3
    assert r["max_days_before_start"] == 50 and r["rows_after_end_beyond_buffer"] == 0   # 3 days: inside buffer
    assert per.set_index("minister_panel_id").loc["김영주|고용노동부|2017-08-14", "obs_rows"] == 4
    assert list(miss["name"]) == ["홍길동"] and miss["rows"].sum() == 5


# ------------------------------------------------------------------ R2: link gates, repaired titles, partyless state
@needs_cal
def test_r2_no_link_on_inconsistent_or_low_confidence_labels(stub_index):
    t = pd.DataFrame({"conf_num": [9, 9, 9, 9], "turn_seq": [1, 2, 3, 4],
                      "speaker_pos": ["외교통상부장관"] * 4, "speaker_name": ["유명환"] * 4,
                      "speech_date": ["2009-01-05"] * 4, "role": ["minister"] * 4,
                      "label_inconsistent_in_meeting": pd.array([False, True, pd.NA, True], dtype="boolean"),
                      "label_confidence": ["high", "high", "low", "low"]})
    out = G.enrich(t, None, panel_index=stub_index)
    assert out.loc[0, "minister_panel_id"] == "유명환|외교통상부|2008-02-29"
    assert out.link_method.tolist()[1:] == ["unlinked:label_inconsistent_in_meeting", "unlinked:label_confidence_low",
                                            "unlinked:label_confidence_low"]
    assert out.minister_panel_id.iloc[1:].isna().all() and out.dual_office.iloc[1:].isna().all()
    assert out.role.tolist() == ["minister"] * 4                 # role untouched
    assert out.attrs["government"]["link_blocked"] == {"label_confidence_low": 2, "label_inconsistent_in_meeting": 1,
                                                       "former_title": 0}
    # without the columns nothing is blocked
    base = G.enrich(t.drop(columns=["label_inconsistent_in_meeting", "label_confidence"]), None, panel_index=stub_index)
    assert base.minister_panel_id.notna().all()


@needs_cal
def test_r2_repaired_title_normalised_from_majority(stub_index):
    t = pd.DataFrame({"conf_num": [1, 1], "turn_seq": [1, 2], "speaker_pos": ["국세", "국세"],
                      "speaker_name": ["갑을병"] * 2, "speech_date": ["2001-06-01"] * 2, "role": ["agency_head"] * 2,
                      "label_repaired": pd.array([True, False], dtype="boolean"),
                      "label_meeting_majority": pd.array(["국세청장", None], dtype="string")})
    out = G.enrich(t, None, panel_index=stub_index)
    assert out.loc[0, "ministry_normalized"] == "국세청"
    assert pd.isna(out.loc[1, "ministry_normalized"]) and out.loc[1, "ministry_rule"] == "no_org"
    assert out.attrs["government"]["ministry_from_repaired_label"] == 1


@needs_cal
@pytest.mark.parametrize("d,state,admin", [("2002-05-06", "partyless", "김대중"), ("2007-02-27", "normal", "노무현"),
                                           ("2007-02-28", "partyless", "노무현"), ("2004-04-01", "suspended", "노무현"),
                                           ("2017-04-01", "acting", "권한대행(황교안)")])
def test_r2_admin_states(d, state, admin):
    adm, _, st = G.admin_for_date(d)
    assert (adm, st) == (admin, state)


def test_r2_calendar_without_acting_president_refused(tmp_path):
    p = tmp_path / "cal.csv"
    pd.DataFrame({"start": ["2000-01-01", "2001-01-01"], "end": ["2000-12-31", ""], "president": ["A", ""],
                  "status": ["in_office", "vacant_after_removal"], "acting_president": ["", ""],
                  "pres_party_formal": ["P", ""]}).to_csv(p, index=False)
    with pytest.raises(ValueError, match="acting president"):
        G.load_calendar.__wrapped__(str(p))


@needs_cal
def test_former_title_never_linked(stub_index):
    """2026-09-28: '(전)환경부장관' is a former minister appearing as a witness; it is never linked to a
    panel spell (link_method 'unlinked:former_title')."""
    t = pd.DataFrame({"conf_num": [9, 9], "turn_seq": [1, 2],
                      "speaker_pos": ["(전)외교통상부장관", "외교통상부장관"],
                      "speaker_name": ["유명환", "유명환"],
                      "speech_date": ["2009-01-05", "2009-01-05"],
                      "role": ["minister", "minister"]})
    meetings = pd.DataFrame({"conf_num": [9], "date": ["2009-01-05"]})
    out = G.enrich(t, meetings, panel_index=stub_index)
    assert out["link_method"].iloc[0] == "unlinked:former_title" and pd.isna(out["minister_panel_id"].iloc[0])
    assert out["minister_panel_id"].iloc[1] == "유명환|외교통상부|2008-02-29"
    assert out.attrs["government"]["link_blocked"]["former_title"] == 1


# ================================================================== panel v2 (minister-data v2.0.0)
# Fixtures are small release directories built from rows of the v2 snapshot named in config.yaml
# (switchable.government.minister_release); the MANIFEST of a fixture is recomputed from its files.
def _v2_release_dir():
    try:
        st = G.government_settings()
    except Exception:  # noqa: BLE001
        return None
    d = st.get("minister_release")
    return d if d and os.path.exists(os.path.join(d, "MANIFEST.json")) else None


V2_RELEASE = _v2_release_dir()
needs_v2_release = pytest.mark.skipif(V2_RELEASE is None, reason="minister-data v2.0.0 snapshot not available")

FX_SPELLS = ("P762c28d6-foreign-1", "P0daf481c-pm-1", "P42af3e2e-interior-1", "P42af3e2e-interior-2",
             "P2b9edfac-defense-1", "Pc550ca21-budget-1", "P5901c7db-industry-1", "Pa00908b5-labor-1",
             "P56eccc43-finance-1", "P56eccc43-pm-2", "P2bb77d23-finance-1", "Pb7c145aa-health-1")
FX_ALIASES = ("외교통상부장관", "외교통상부", "외교통상부차관", "국무총리", "國務總理", "국무총리직무대행", "행정안전부장관",
              "행정안전부", "안전행정부장관", "안전행정부", "행정자치부장관", "국방부장관", "국방부", "국가안보실장겸국방부장관", "기획예산처장관",
              "기획예산처", "법무부장관직무대행", "법무부장관", "법무부", "여성가족부장관후보자", "여성가족부장관", "여성가족부",
              "국가보훈처장", "산업자원부장관", "고용노동부장관", "부총리겸재정경제부장관", "재정경제부", "보건복지부장관",
              "보건복지부")
FX_NOMS = ("N20b674a7", "Na5cc0d87", "N9ba2e231", "N585dce57")
FX_ACTING = ("H03914c1e", "Hba9ce0b1", "Hc11b4406", "H5c770c41")
FX_VARIANTS = ("진임",)


def _write_release(src, dst, pick, version="v2.0.0"):
    """Rows of the v2 release files selected by `pick` {file: (column, values)} written to dst with a MANIFEST."""
    import hashlib
    import json
    man = json.load(open(os.path.join(src, "MANIFEST.json"), encoding="utf-8"))
    files = {}
    for f in G.V2_FILES:
        df = pd.read_csv(os.path.join(src, f), dtype=str, keep_default_na=False, na_values=[""])
        col, vals = pick[f]
        key = df[col].map(G._nk) if col == "alias_string" else df[col]
        df[key.isin([G._nk(v) for v in vals] if col == "alias_string" else vals)].to_csv(os.path.join(dst, f), index=False)
        b = open(os.path.join(dst, f), "rb").read()
        files[f] = {"sha256": hashlib.sha256(b).hexdigest(), "rows": 0, "bytes": len(b)}
    json.dump({"version": version, "window": man["window"], "files": files},
              open(os.path.join(dst, "MANIFEST.json"), "w", encoding="utf-8"))
    return str(dst)


FX_PICK = {"spells.csv": ("spell_id", FX_SPELLS), "ministry_alias.csv": ("alias_string", FX_ALIASES),
           "person_name_variants.csv": ("variant_string", FX_VARIANTS), "nominations.csv": ("nomination_id", FX_NOMS),
           "acting_heads.csv": ("acting_id", FX_ACTING)}


@pytest.fixture(scope="module")
def v2_dir(tmp_path_factory):
    if V2_RELEASE is None:
        pytest.skip("minister-data v2.0.0 snapshot not available")
    return _write_release(V2_RELEASE, tmp_path_factory.mktemp("v2fx"), FX_PICK)


@pytest.fixture(scope="module")
def v2(v2_dir):
    return G.SpellIndex(v2_dir, spell_buffer_days=1, expect_version="v2.0.0")


def _l(idx, name, title, ministry, d, role="minister"):
    r = idx.link(name, title, ministry, d, role)
    return r.method, (r.spell_id or r.nomination_id or r.acting_id)


# ------------------------------------------------------------------ snapshot and configuration
@needs_v2_release
def test_v2_config_default_panel_and_snapshot():
    st = G.government_settings()
    assert st["panel"] == "v2" and st["spell_buffer_days"] == 1
    assert os.path.relpath(st["minister_release"], G.V10) == os.path.join("interim", "external", "minister_data_v2.0.0")
    idx = G._default_index(buffer_days=7, nominee_pre_days=60, nominee_post_days=60)   # as run_all.py calls it
    assert isinstance(idx, G.SpellIndex) and idx.version == "v2.0.0" and idx.cutoff == dt.date(2026, 9, 24)
    assert len(idx.spells) == 687
    assert isinstance(G._default_index(panel="legacy_296"), G.PanelIndex) if HAVE_PANEL else True


@needs_v2_release
def test_v2_snapshot_matches_its_manifest():
    import hashlib
    import json
    man = json.load(open(os.path.join(V2_RELEASE, "MANIFEST.json"), encoding="utf-8"))
    assert man["version"] == "v2.0.0"
    for f, m in man["files"].items():
        assert hashlib.sha256(open(os.path.join(V2_RELEASE, f), "rb").read()).hexdigest() == m["sha256"], f


def test_v2_settings_refuse_paths_outside_the_snapshot_root(tmp_path):
    def cfg(**g):
        p = tmp_path / f"c{len(list(tmp_path.iterdir()))}.yaml"
        import yaml
        p.write_text(yaml.safe_dump({"switchable": {"government": g}}), encoding="utf-8")
        return str(p)
    good = G.government_settings.__wrapped__(cfg(panel="v2", minister_release="interim/external/x", spell_buffer_days=1))
    assert good["minister_release"] == os.path.join(G.V10, "interim", "external", "x")
    assert G.government_settings.__wrapped__(cfg(panel="legacy_296")) == {"panel": "legacy_296"}
    for bad in (dict(panel="v2_rc3", minister_release="interim/external/x", spell_buffer_days=1),
                dict(panel="v2", minister_release="../../minister-data/_rebuild/release/v2.0.0", spell_buffer_days=1),
                dict(panel="v2", minister_release="/abs/interim/external/x", spell_buffer_days=1),
                dict(panel="v2", minister_release="interim/external/x", spell_buffer_days=-1),
                dict(panel="v2", minister_release="interim/external/x")):
        with pytest.raises(ValueError):
            G.government_settings.__wrapped__(cfg(**bad))


@needs_v2_release
def test_v2_release_sha_and_version_checked(v2_dir, tmp_path):
    import shutil
    d = tmp_path / "tampered"
    shutil.copytree(v2_dir, d)
    with open(d / "spells.csv", "a", encoding="utf-8") as fh:
        fh.write("\n")
    with pytest.raises(ValueError, match="sha256"):
        G.SpellIndex(str(d))
    assert G.SpellIndex(str(d), verify=False).version == "v2.0.0"
    with pytest.raises(ValueError, match="version"):
        G.SpellIndex(v2_dir, expect_version="v2.0.0-rc2")


# ------------------------------------------------------------------ person
@needs_v2_release
def test_v2_person_hangul_hanja_and_whitespace(v2):
    assert _l(v2, "유명환", "외교통상부장관", "외교통상부", "2009-01-05") == ("spell:exact", "P762c28d6-foreign-1")
    assert _l(v2, "유 명환", "외교통상부 장관", None, "2009-01-05") == ("spell:exact", "P762c28d6-foreign-1")
    assert _l(v2, "柳明桓", "外交通商部長官", "외교통상부", "2009-01-05") == ("spell:exact", "P762c28d6-foreign-1")
    r = v2.link("李漢東", "國務總理", "국무총리", "2001-06-01", "prime_minister")
    assert (r.method, r.spell_id, r.name, r.person_id) == ("spell:exact", "P0daf481c-pm-1", "이한동", "P0daf481c")
    assert _l(v2, "홍길동", "외교통상부장관", "외교통상부", "2009-01-05") == ("unlinked:name_not_in_panel", None)


@needs_v2_release
def test_v2_person_name_variant_valid_for_lineage_and_date(v2):
    # 진임 = 진념 (typo variant, budget 1999-05-24..2000-08-07)
    assert _l(v2, "진임", "기획예산처장관", "기획예산처", "2000-07-01") == ("spell:exact", "Pc550ca21-budget-1")
    assert _l(v2, "진임", "기획예산처장관", "기획예산처", "2000-09-01")[0] == "unlinked:name_not_in_panel"   # variant expired
    assert _l(v2, "진임", "외교통상부장관", "외교통상부", "2000-07-01")[0] == "unlinked:person_in_other_lineage"


@needs_v2_release
def test_v2_same_name_two_people(v2):
    assert _l(v2, "김영주", "산업자원부장관", "산업자원부", "2007-06-01") == ("spell:exact", "P5901c7db-industry-1")
    assert _l(v2, "김영주", "고용노동부장관", "고용노동부", "2018-01-10") == ("spell:exact", "Pa00908b5-labor-1")
    assert _l(v2, "김영주", "고용노동부장관", "고용노동부", "2019-06-01")[0] == "unlinked:outside_spell"


# ------------------------------------------------------------------ lineage
@needs_v2_release
def test_v2_lineage_from_alias_suffix_and_ministry_fallback(v2):
    lin, how = v2.resolve_lineage("외교통상부장관", None, "2009-01-05")
    assert lin == "foreign" and how.startswith("alias:")
    assert v2.resolve_lineage("여성가족부장관후보자", None, "2025-07-14")[0] == "gender"
    assert v2.resolve_lineage("법무부장관직무대행", None, "2025-01-10")[0] == "justice"
    # unknown printed title: ministry_normalized decides
    assert v2.resolve_lineage("외교통상부장관님", "외교통상부", "2009-01-05")[0] == "foreign"
    # alias not valid on the date (외교통상부 ends 2013-03-22)
    assert v2.resolve_lineage("외교통상부장관", "외교통상부", "2015-01-01") == (None, "unresolved")
    assert v2.resolve_lineage("국무총리", None, "2001-01-01", "minister") == ("pm", "pm_title")
    assert v2.resolve_lineage("아무개", None, "2001-01-01", "prime_minister") == ("pm", "pm_title")


@needs_v2_release
def test_v2_person_scoped_alias_only_for_that_person(v2):
    # '행정안전부장관' printed for 정종섭 on 2014-07-24 (안전행정부 period): person- and date-scoped row
    assert _l(v2, "정종섭", "행정안전부장관", "행정안전부", "2014-07-24") == ("spell:exact", "P42af3e2e-interior-1")
    assert _l(v2, "정종섭", "행정안전부장관", "행정안전부", "2014-07-25")[0] == "unlinked:lineage_unresolved"
    assert v2.resolve_lineage("행정안전부장관", "행정안전부", "2014-07-24", "minister", frozenset({"Pxxxx"})) == \
        (None, "unresolved")
    assert _l(v2, "신원식", "국가안보실장겸국방부장관", None, "2024-09-05") == ("spell:exact", "P2b9edfac-defense-1")


@needs_v2_release
def test_v2_vice_minister_title_and_out_of_scope_never_link(v2):
    # the printed vice-minister title decides even though ministry_normalized would resolve
    assert _l(v2, "유명환", "외교통상부차관", "외교통상부", "2009-01-05") == ("unlinked:vice_minister_title", None)
    assert v2.resolve_lineage("국가보훈처장", "국가보훈처", "2010-01-01") == (None, "out_of_scope")
    assert _l(v2, "유명환", "국가보훈처장", "국가보훈처", "2009-01-05")[0] == "unlinked:lineage_out_of_scope"


@needs_v2_release
def test_v2_person_in_other_lineage(v2):
    r = v2.link("유명환", "국방부장관", "국방부", "2009-01-05", "minister")
    assert r.method == "unlinked:person_in_other_lineage" and r.lineage == "defense" and "P762c28d6-foreign-1" in r.detail


# ------------------------------------------------------------------ date
@needs_v2_release
def test_v2_date_window_one_day_buffer(v2):
    f = lambda d: _l(v2, "유명환", "외교통상부장관", "외교통상부", d)     # noqa: E731  spell 2008-02-29..2010-09-08
    assert f("2008-02-29") == ("spell:exact", "P762c28d6-foreign-1") and f("2010-09-08")[0] == "spell:exact"
    assert f("2008-02-28") == ("spell:buffer", "P762c28d6-foreign-1") and f("2010-09-09")[0] == "spell:buffer"
    assert f("2008-02-27")[0] == "unlinked:outside_spell" and f("2010-09-10")[0] == "unlinked:outside_spell"
    r = v2.link("유명환", "외교통상부장관", "외교통상부", "2010-09-18", "minister")
    assert r.detail.endswith(" 10d")


@needs_v2_release
def test_v2_buffer_configurable(v2_dir):
    b0, b3 = G.SpellIndex(v2_dir, spell_buffer_days=0), G.SpellIndex(v2_dir, spell_buffer_days=3)
    assert _l(b0, "유명환", "외교통상부장관", "외교통상부", "2010-09-09")[0] == "unlinked:outside_spell"
    assert _l(b3, "유명환", "외교통상부장관", "외교통상부", "2010-09-11")[0] == "spell:buffer"


@needs_v2_release
def test_v2_open_spell_ends_at_release_cutoff(v2):
    s = v2.spells["Pb7c145aa-health-1"]
    assert s.end is None and s.end_eff == dt.date(2026, 9, 24)
    assert _l(v2, "정은경", "보건복지부장관", "보건복지부", "2026-09-20")[0] == "spell:exact"


@needs_v2_release
def test_v2_consecutive_spells_pick_the_containing_one(v2):
    # 정종섭 interior-1 2014-07-16..2014-11-18 (안전행정부), interior-2 from 2014-11-19 (행정자치부): on each boundary
    # day the other spell is inside its 1-day buffer, the containing spell wins
    assert _l(v2, "정종섭", "안전행정부장관", "안전행정부", "2014-11-18") == ("spell:exact", "P42af3e2e-interior-1")
    assert _l(v2, "정종섭", "행정자치부장관", "행정자치부", "2014-11-19") == ("spell:exact", "P42af3e2e-interior-2")
    assert _l(v2, "정종섭", "안전행정부장관", "안전행정부", "2014-11-19")[0] == "unlinked:lineage_unresolved"   # name out of force


# ------------------------------------------------------------------ nominees
@needs_v2_release
def test_v2_nominee_links_to_nomination_by_hearing_date(v2):
    # N20b674a7 유명환 외교통상부장관, hearing 2008-02-27
    r = v2.link("유명환", "외교통상부장관후보자", "외교통상부", "2008-02-26", "minister_nominee")
    assert (r.method, r.nomination_id, r.spell_id, r.person_id) == ("nomination:hearing", "N20b674a7",
                                                                    "P762c28d6-foreign-1", "P762c28d6")
    assert r.dual_office is False
    assert _l(v2, "유명환", "외교통상부장관후보자", "외교통상부", "2008-02-25", "minister_nominee")[0] == "unlinked:outside_hearing"
    assert _l(v2, "홍길동", "외교통상부장관후보자", "외교통상부", "2008-02-27", "minister_nominee")[0] == \
        "unlinked:name_not_in_panel"
    assert _l(v2, "유명환", "법무부장관후보자", "법무부", "2008-02-27", "minister_nominee")[0] == "unlinked:person_in_other_lineage"


@needs_v2_release
def test_v2_withdrawn_nomination_has_no_spell(v2):
    r = v2.link("강선우", "여성가족부장관후보자", "여성가족부", "2025-07-14", "minister_nominee")
    assert (r.method, r.nomination_id, r.spell_id, r.person_id, r.dual_office) == \
        ("nomination:hearing", "N9ba2e231", None, None, None)


@needs_cal
@needs_v2_release
def test_v2_pm_nominee_title_of_role_nominee_is_linked(v2):
    t = pd.DataFrame({"conf_num": [1, 1, 1], "turn_seq": [1, 2, 3],
                      "speaker_pos": ["국무총리후보자", "대법관후보자", "국무총리후보자"],
                      "speaker_name": ["한덕수", "홍길동", "한덕수"],
                      "speech_date": ["2022-05-03", "2022-05-03", "2022-05-06"], "role": ["nominee"] * 3})
    out = G.enrich(t, None, panel_index=v2)
    assert out.loc[0, "link_method"] == "nomination:hearing" and out.loc[0, "minister_nomination_id"] == "Na5cc0d87"
    assert out.loc[0, "minister_panel_id"] == out.loc[0, "minister_spell_id"] == "P56eccc43-pm-2"
    assert pd.isna(out.loc[1, "link_method"])                  # not a cabinet nominee: not in link scope
    assert out.loc[2, "link_method"] == "unlinked:outside_hearing"
    assert out.attrs["government"]["nominee_cabinet_title_turns"] == 2


# ------------------------------------------------------------------ acting heads
@needs_v2_release
def test_v2_acting_pm_links_to_acting_heads(v2):
    # 한덕수 acting PM 2006-03-16..04-19 (Hc11b4406) while finance minister (finance-1)
    r = v2.link("한덕수", "국무총리직무대행", "국무총리", "2006-04-01", "prime_minister")
    assert (r.method, r.acting_id, r.spell_id, r.person_id, r.lineage, r.dual_office) == \
        ("acting_head:pm", "Hc11b4406", None, "P56eccc43", "pm", None)
    assert _l(v2, "한덕수", "국무총리직무대행", "국무총리", "2006-05-01", "prime_minister")[0] == "unlinked:outside_acting_period"
    assert _l(v2, "한덕수", "부총리겸재정경제부장관", "재정경제부", "2006-04-01") == ("spell:exact", "P56eccc43-finance-1")
    assert _l(v2, "한덕수", "국무총리", "국무총리", "2023-01-01", "prime_minister") == ("spell:exact", "P56eccc43-pm-2")
    assert _l(v2, "최경환", "국무총리직무대행", "국무총리", "2015-05-01", "prime_minister") == ("acting_head:pm", "H5c770c41")


@needs_v2_release
def test_v2_minister_acting_links_to_acting_heads_of_its_lineage(v2):
    r = v2.link("김석우", "법무부장관직무대행", "법무부", "2025-01-10", "minister_acting")
    assert (r.method, r.acting_id, r.person_id, r.lineage) == ("acting_head:lineage", "H03914c1e", None, "justice")
    assert r.detail == "incidental, not exhaustive"
    assert _l(v2, "김석우", "법무부장관직무대행", "법무부", "2025-06-10", "minister_acting") == ("acting_head:lineage", "Hba9ce0b1")
    assert _l(v2, "김석우", "법무부장관직무대행", "법무부", "2025-05-10", "minister_acting")[0] == \
        "unlinked:outside_acting_period"
    assert _l(v2, "김석우", "국방부장관직무대행", "국방부", "2025-01-10", "minister_acting")[0] == "unlinked:not_in_acting_heads"
    assert _l(v2, "홍길동", "법무부장관직무대행", "법무부", "2025-01-10", "minister_acting")[0] == "unlinked:not_in_acting_heads"


# ------------------------------------------------------------------ dual office at the speech date
@needs_v2_release
def test_v2_dual_office_at_speech_date(v2):
    # 신원식 defense 2023-10-07..2024-09-06, National Assembly seat until 2023-10-31
    assert v2.link("신원식", "국방부장관", "국방부", "2023-10-20", "minister").dual_office is True
    assert v2.link("신원식", "국방부장관", "국방부", "2023-11-20", "minister").dual_office is False
    assert v2.link("유명환", "외교통상부장관", "외교통상부", "2009-01-05", "minister").dual_office is False


# ------------------------------------------------------------------ enrich (v2) contract, gates, admin
@needs_cal
@needs_v2_release
def test_v2_enrich_columns_gates_and_admin(v2, stub_index):
    t = pd.DataFrame({"conf_num": [9] * 7, "turn_seq": list(range(1, 8)),
                      "speaker_pos": ["외교통상부장관", "외교통상부장관", "외교통상부장관", "(전)외교통상부장관",
                                      "법무부장관직무대행", "국무총리직무대행", "위원"],
                      "speaker_name": ["유명환", "유명환", "유명환", "유명환", "김석우", "한덕수", "홍길동"],
                      "speech_date": ["2009-01-05", "2009-01-05", "2009-01-05", "2009-01-05", "2025-01-10", "2006-04-01",
                                      "2009-01-05"],
                      "role": ["minister", "minister", "minister", "minister", "minister_acting", "prime_minister",
                               "legislator"],
                      "label_inconsistent_in_meeting": pd.array([False, True, False, False, False, False, False],
                                                                dtype="boolean"),
                      "label_confidence": ["high", "high", "low", "high", "high", "high", "high"]},
                     index=[5, 3, 1, 0, 2, 4, 6])
    out = G.enrich(t, None, panel_index=v2)
    assert list(out.index) == list(t.index)
    for c in G.ADDED_COLUMNS:
        assert c in out.columns
    r = out.loc[5]
    assert (r.minister_panel_id, r.minister_spell_id, r.minister_person_id, r.minister_lineage, r.link_method) == \
        ("P762c28d6-foreign-1", "P762c28d6-foreign-1", "P762c28d6", "foreign", "spell:exact")
    assert r.dual_office == False and r.gov_link_name == "유명환"  # noqa: E712
    assert out.loc[[3, 1, 0], "link_method"].tolist() == ["unlinked:label_inconsistent_in_meeting",
                                                          "unlinked:label_confidence_low", "unlinked:former_title"]
    assert out.loc[[3, 1, 0], list(G.V2_LINK_COLUMNS) + ["minister_panel_id", "dual_office"]].isna().all().all()
    assert out.loc[2, "minister_panel_id"] == out.loc[2, "minister_acting_id"] == "H03914c1e"
    assert out.loc[4, "minister_panel_id"] == "Hc11b4406" and out.loc[4, "link_method"] == "acting_head:pm"
    assert pd.isna(out.loc[4, "minister_spell_id"]) and pd.isna(out.loc[4, "dual_office"])
    assert pd.isna(out.loc[6, "link_method"]) and pd.isna(out.loc[6, "minister_lineage"])
    dg = out.attrs["government"]
    assert dg["panel"] == "v2" and dg["panel_release"] == "v2.0.0"
    assert dg["link_blocked"] == {"label_confidence_low": 1, "label_inconsistent_in_meeting": 1, "former_title": 1}
    # admin / ministry columns do not depend on the panel
    old = G.enrich(t, None, panel_index=stub_index)
    for c in ("admin", "admin_ideology", "presidency_state", "ministry_normalized", "ministry_family", "gov_date_source"):
        assert out[c].fillna("<NA>").equals(old[c].fillna("<NA>")), c
    assert out.loc[5, "admin"] == "이명박" and out.loc[4, "admin"] == "노무현"
    # legacy_296: the v2 columns exist and are null
    assert old[list(G.V2_LINK_COLUMNS)].isna().all().all() and old.attrs["government"]["panel"] == "legacy_296"


@needs_cal
@needs_v2_release
def test_v2_enrich_subset_equals_full_and_input_untouched(v2):
    t = pd.DataFrame({"conf_num": [1, 1, 2, 2], "turn_seq": [1, 2, 1, 2],
                      "speaker_pos": ["외교통상부장관", "국방부장관", "國務總理", "법무부장관직무대행"],
                      "speaker_name": ["유명환", "신원식", "李漢東", "김석우"],
                      "speech_date": ["2009-01-05", "2023-10-20", "2001-06-01", "2025-01-10"],
                      "role": ["minister", "minister", "prime_minister", "minister_acting"]})
    before = t.copy(deep=True)
    full = G.enrich(t, None, panel_index=v2)
    sub = G.enrich(t[t.conf_num == 2], None, panel_index=v2)
    cols = ["minister_panel_id", "link_method", *G.V2_LINK_COLUMNS]
    assert sub[cols].fillna("<NA>").equals(full.loc[sub.index, cols].fillna("<NA>"))
    assert t.fillna("<NA>").equals(before.fillna("<NA>"))
    assert full.loc[1, "dual_office"] == True  # noqa: E712
