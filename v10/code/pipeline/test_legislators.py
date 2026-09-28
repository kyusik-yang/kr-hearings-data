"""Tests for legislators.py. Run:  python -m pytest -q test_legislators.py

Most tests use the reference tables built from the Open API downloads
(interim/pipeline/legislators/*.parquet), so the expected NAAS_CDs are real ones. Seat and committee
dates used below were read from person_terms.parquet / committee_spells.parquet (의원이력, 위원회경력).
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import legislators as L  # noqa: E402


@pytest.fixture(scope="module")
def ref():
    return L.load_reference()


@pytest.fixture(scope="module")
def R(ref):
    return ref["resolver"]


def res(R, term, date, pos, name, mem_id=None, area=None, committee=None, leg_side=True, tclass="legislator",
        label=None):
    return R.resolve(term, date, pos, name, mem_id, area, label, committee, leg_side, tclass)


# ----------------------------------------------------------------------------- pure helpers

@pytest.mark.parametrize("label,pos,name", [
    ("이수진(비) 위원", "위원", "이수진(비)"),
    ("최경환 위원(국)", "위원(국)", "최경환"),
    ("위원장 김영주", "위원장", "김영주"),
    ("국토교통위원장대리 김병욱", "국토교통위원장대리", "김병욱"),
    ("김선동 위원", "위원", "김선동"),
])
def test_split_label(label, pos, name):
    assert L.split_label(label) == (pos, name)


def test_clean_name_markers_and_glued_title():
    assert L.clean_name("이수진(비)") == ("이수진", ["비"])
    assert L.clean_name("김성곤위원") == ("김성곤", [])
    assert L.clean_name("李李") == ("李李", [])  # NFKC folds the compatibility ideograph


def test_parent_committee():
    assert L.parent_committee("안전행정위원회-제2반") == "안전행정위원회"
    assert L.parent_committee("외교통일위원회-미주반") == "외교통일위원회"
    assert L.parent_committee("교육위원회 예산안심사소위원회") == "교육위원회"
    assert L.committee_norm("제19대 정치쇄신 특별위원회") == "정치쇄신특별위원회"


def test_title_class():
    assert L.title_class("위원") == "legislator"
    assert L.title_class("委員長代理") == "legislator"
    assert L.title_class("국방부장관") == "cabinet"
    assert L.title_class("국가정보원장후보자") == "nominee"
    assert L.title_class("") == "none"


def test_party_marker_match():
    assert L._party_marker_match("한", {"자유한국당"}) == 2
    assert L._party_marker_match("새", {"새누리당"}) == 2
    assert L._party_marker_match("평", {"민주평화당"}) == 2
    assert L._party_marker_match("국", {"자유한국당"}) == 1
    assert L._party_marker_match("평", {"자유한국당", "새누리당"}) == 0


def test_lineage_chain(R):
    ch = R.lineage.chain("자유한국당")
    assert {"새누리당", "한나라당", "미래통합당", "국민의힘"} <= ch
    assert "더불어민주당" not in ch and "바른정당" not in ch  # no sibling crossing
    assert "민주평화당" in R.lineage.chain("민생당")


# ----------------------------------------------------------------------------- reference tables

def test_reference_tables(ref):
    p, pt, cw = ref["persons"], ref["person_terms"], ref["memid_crosswalk"]
    assert p.naas_cd.is_unique and len(p) == 3296
    assert pt.groupby(["naas_cd", "term", "stint"]).size().max() == 1
    per_term = pt.groupby("term").naas_cd.nunique().to_dict()
    assert all(300 <= n <= 340 for n in per_term.values())
    assert set(pt.elect_type.unique()) <= {"지역구", "비례"}
    assert cw.naas_cd.notna().all() and cw.mem_id.is_unique


# ----------------------------------------------------------------------------- homonyms

def test_kim_youngju_across_terms(R):
    # 19대: 0W194007 (영등포갑, whole term) and E6S73230 (비례, left 2013-12-12)
    r = res(R, 19, "2014-06-30", "위원장", "김영주", committee="환경노동위원회")
    assert (r["naas_cd"], r["method"]) == ("0W194007", "homonym_seat_dates")
    r = res(R, 19, "2013-01-15", "위원", "김영주", committee="행정안전위원회")
    assert (r["naas_cd"], r["method"]) == ("E6S73230", "homonym_committee")
    r = res(R, 19, "2013-01-15", "위원", "김영주", committee="정무위원회")
    assert (r["naas_cd"], r["method"]) == ("0W194007", "homonym_committee")
    r = res(R, 19, "2013-01-15", "위원", "김영주", committee="국토해양위원회")
    assert r["naas_cd"] is None and r["method"] == "unresolved_ambiguous"
    # 20대 and 21대: one 김영주
    for t, d in ((20, "2017-01-10"), (21, "2021-01-10")):
        r = res(R, t, d, "위원", "김영주")
        assert (r["naas_cd"], r["method"], r["confidence"]) == ("0W194007", "name_term", "high")
    # 2017 minister (dual office) keeps a link through the minister panel
    r = res(R, 20, "2017-12-01", "고용노동부장관", "김영주", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("0W194007", "nonleg_dual_office")


def test_choi_kyunghwan_markers(R):
    # 20대: FJ03481D (경산, 새누리→자유한국당, seat ended 2019-07-11) and KA04352K (광주 북구을, 국민의당→민주평화당)
    assert res(R, 20, "2016-10-05", "위원", "최경환(새)")["naas_cd"] == "FJ03481D"
    assert res(R, 20, "2017-03-01", "위원", "최경환(한)")["naas_cd"] == "FJ03481D"
    assert res(R, 20, "2016-07-01", "위원", "최경환(국)")["naas_cd"] == "KA04352K"
    r = res(R, 20, "2018-10-01", "의원", "최경환(평)")
    assert (r["naas_cd"], r["method"]) == ("KA04352K", "homonym_marker_party")
    r = res(R, 20, "2019-10-01", "위원", "최경환")
    assert (r["naas_cd"], r["method"]) == ("KA04352K", "homonym_seat_dates")
    # 19대: one 최경환
    assert res(R, 19, "2013-05-01", "위원", "최경환")["naas_cd"] == "FJ03481D"


def test_kim_sungtae(R):
    # 20대: BQS2021C (강서을) and 9UW75767 (비례)
    r = res(R, 20, "2018-02-21", "위원장", "김성태", committee="국회운영위원회")
    assert (r["naas_cd"], r["method"]) == ("BQS2021C", "homonym_committee")
    r = res(R, 20, "2017-09-01", "위원", "김성태", committee="과학기술정보방송통신위원회")
    assert (r["naas_cd"], r["method"]) == ("9UW75767", "homonym_committee")
    r = res(R, 20, "2016-12-29", "위원장", "김성태", mem_id=6077)
    assert (r["naas_cd"], r["method"]) == ("BQS2021C", "mem_id")
    # 19대: one 김성태
    assert res(R, 19, "2013-05-01", "위원", "김성태")["naas_cd"] == "BQS2021C"


def test_kim_sundong(R):
    # 18대: DTG4846A (도봉을) and ZC87486D (순천, by-election 2011-04-27)
    r = res(R, 18, "2009-01-01", "위원", "김선동", committee="교육과학기술위원회")
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("DTG4846A", "homonym_seat_dates", "high")
    r = res(R, 18, "2011-06-14", "위원", "김선동", committee="외교통상통일위원회")
    assert (r["naas_cd"], r["method"]) == ("ZC87486D", "homonym_committee")
    r = res(R, 18, "2011-06-14", "위원", "김선동", committee="교육과학기술위원회")
    assert r["naas_cd"] == "DTG4846A"
    # 20대: one 김선동 (도봉을)
    assert res(R, 20, "2017-01-01", "위원", "김선동")["naas_cd"] == "DTG4846A"


def test_lee_sujin_21(R):
    # 21대: 0R68099X (비례) and D4L60530 (동작을); the minutes mark the list member '이수진(비)'
    r = res(R, 21, "2021-01-01", "위원", "이수진(비)")
    assert (r["naas_cd"], r["method"]) == ("0R68099X", "homonym_marker_elect_type")
    r = res(R, 21, "2021-06-01", "위원", "이수진", committee="법제사법위원회")
    assert (r["naas_cd"], r["method"]) == ("D4L60530", "homonym_committee")
    r = res(R, 21, "2021-01-01", "위원", "이수진", area="서울 동작구을")
    assert (r["naas_cd"], r["method"]) == ("D4L60530", "homonym_area")
    # 22대: one 이수진 (성남중원, the 21대 list member)
    assert res(R, 22, "2024-07-01", "위원", "이수진")["naas_cd"] == "0R68099X"


def test_kim_byungwook_21_same_hanja(R):
    # both 金炳旭: committee or area decides
    assert res(R, 21, "2021-01-10", "위원", "김병욱", committee="정무위원회")["naas_cd"] == "GFF1986K"
    assert res(R, 21, "2021-01-10", "위원", "김병욱", committee="교육위원회")["naas_cd"] == "KB04377U"
    assert res(R, 21, "2021-01-10", "위원", "김병욱", area="경북 포항시남구울릉군")["naas_cd"] == "KB04377U"


def test_park_jiwon_22_seat_dates(R):
    # 22대: 8BF5855P (해남완도진도) and H7X3372O (군산김제부안을, from 2026-06-04)
    r = res(R, 22, "2024-10-07", "위원", "박지원")
    assert (r["naas_cd"], r["method"]) == ("8BF5855P", "homonym_seat_dates")


# ----------------------------------------------------------------------------- mem_id rules

def test_memid_seat_override(R):
    # the viewer links 이재영 to the 평택을 member-term id 7206 after that member lost his seat (2014-01-16)
    r = res(R, 19, "2015-07-08", "위원", "이재영", mem_id=7206, area="경기 평택시을")
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("URQ4401W", "mem_id_seat_override", "medium")
    r = res(R, 19, "2013-06-01", "위원", "이재영", mem_id=7206)
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("1XO42697", "mem_id", "high")


def test_memid_swapped_pos_name(R, ref):
    cw = ref["memid_crosswalk"]
    mid = int(cw[(cw.name_rec == "유승민") & (cw.term == 19)].mem_id.iloc[0])
    r = res(R, 19, "2015-01-01", "劉承旼", "國防委員長", mem_id=mid)
    assert (r["naas_cd"], r["method"]) == ("PWU27609", "mem_id_pos_name_swapped")


def test_memid_unknown_falls_back_to_name(R):
    r = res(R, 21, "2021-01-10", "위원", "김영주", mem_id=999999)
    assert r["naas_cd"] == "0W194007" and "not in record crosswalk" in r["note"]


# ----------------------------------------------------------------------------- spelling fallbacks

def test_dueum_initial_sound(R):
    r = res(R, 17, "2006-01-01", "위원", "류시민")
    assert (r["naas_cd"], r["method"]) == ("XFH9918T", "name_term_dueum")
    assert res(R, 18, "2009-01-01", "위원장", "류선호")["naas_cd"] == "SKV1971K"


def test_hanja_variants(R):
    assert res(R, 16, "2001-01-01", "議員", "劉承旼")["naas_cd"] is None  # not a 16대 member
    # glyph variants with the same reading (裴/裵 배; API '설松雄' has hangul where the label has 偰)
    r = res(R, 16, "2001-01-01", "議員", "裴基善")
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("Y009633J", "hanja_term_variant", "medium")
    r = res(R, 16, "2001-01-01", "議員", "偰松雄")
    assert (r["naas_cd"], r["confidence"]) == ("K1A87893", "medium")
    # hangul reading of the printed hanja (李嬿叔 -> 이연숙; API hanja is '李연淑')
    r = res(R, 16, "2001-01-01", "委員", "李嬿叔")
    assert (r["naas_cd"], r["method"]) == ("S8266690", "hanja_reading_name")
    assert L.hanja_to_hangul("李嬿叔") == "리연숙" and L.same_reading("李", "이", initial=True)
    r = res(R, 16, "2001-01-01", "建設交通委員長代理", "松雄")
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("K1A87893", "hanja_term_partial", "low")


def test_unresolved_is_counted_not_guessed(R):
    r = res(R, 21, "2021-01-01", "위원", "홍길동")
    assert r["naas_cd"] is None and r["method"] == "unresolved_no_member_in_term"
    r = res(R, 21, "2021-01-01", "위원", "김해영")  # 20대 member only
    assert r["naas_cd"] is None and "20" in r["note"]


# ----------------------------------------------------------------------------- non-legislator titles

def test_nonleg_links(R):
    r = res(R, 19, "2014-10-01", "부총리겸기획재정부장관", "최경환", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("FJ03481D", "nonleg_dual_office")
    r = res(R, 21, "2022-06-01", "국토교통부장관", "원희룡", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("1EL35837", "nonleg_former_member_panel")
    # a unique former member with the same name is not evidence: 국무총리 김황식 is not the 16대 member
    r = res(R, 18, "2011-01-01", "국무총리", "김황식", leg_side=False, tclass="cabinet")
    assert r["naas_cd"] is None and r["method"] == "nonleg_former_unverified"
    r = res(R, 20, "2017-06-01", "국가정보원장후보자", "서훈", leg_side=False, tclass="nominee")
    assert r["naas_cd"] is None
    # a person who becomes a member only later is not linked
    r = res(R, 22, "2024-07-24", "방송통신위원장후보자", "이진숙", leg_side=False, tclass="nominee")
    assert r["naas_cd"] is None and r["method"] == "nonleg_future_member"
    # a sitting member with the same name but no dual-office record
    r = res(R, 21, "2021-01-10", "국장", "김영주", leg_side=False, tclass="other")
    assert r["naas_cd"] is None and r["method"] == "nonleg_name_collision"


# ----------------------------------------------------------------------------- enrich

def _turns():
    return pd.DataFrame({
        "conf_num": [10, 10, 10, 11, 11, 12],
        "turn_seq": [1, 2, 3, 1, 2, 1],
        "source": ["xlsx"] * 6,
        "speaker_label_raw": ["위원장 이수진(비)", "이수진 위원", "환경부장관 한정애", "최경환(평) 위원", "김영주 위원",
                              "류시민 위원"],
        "speaker_pos": ["위원장", "위원", "환경부장관", "위원", "위원", "위원"],
        "speaker_name": ["이수진(비)", "이수진", "한정애", "최경환(평)", "김영주", "류시민"],
        "speaker_mem_id": [None] * 6,
        "speaker_area": [None] * 6,
        "speech_date": ["2021-03-02", "2021-03-02", "2021-03-02", "2018-10-10", "2018-10-10", "2006-02-01"],
    }, index=[5, 3, 9, 1, 0, 7])


def _meetings():
    return pd.DataFrame({"conf_num": [10, 11, 12], "term": [21, 20, 17], "class_name": ["상임위원회"] * 3,
                         "hearing_type": ["상임위원회"] * 3,
                         "committee_raw": ["국토교통위원회", "행정안전위원회", "보건복지위원회"],
                         "subcommittee": [None] * 3, "date": ["2021-03-02", "2018-10-10", "2006-02-01"]})


def test_enrich_contract_and_order(ref):
    t = _turns()
    out = L.enrich(t, _meetings(), ref)
    assert list(out.index) == list(t.index)
    assert list(out.columns[: len(t.columns)]) == list(t.columns)
    for c in ["naas_cd", "leg_name_hangul", "leg_name_hanja", "gender", "birth_date", "district", "elect_type",
              "seniority", "id_method", "id_confidence"]:
        assert c in out.columns
    assert "term" not in out.columns  # helper join columns are dropped
    o = out.set_index("turn_seq", append=True)
    assert out.loc[5, "naas_cd"] == "0R68099X"
    # unmarked label in the same meeting as the marked one -> the other 이수진
    assert out.loc[3, "naas_cd"] == "D4L60530" and out.loc[3, "id_method"] == "homonym_meeting_complement"
    # 환경부장관 한정애 (2021) is a sitting 21대 member: linked through the dual-office panel, not legislator side
    assert out.loc[9, "naas_cd"] == "XPP2564T" and out.loc[9, "id_method"] == "nonleg_dual_office"
    assert not out.loc[9, "leg_side"]
    assert out.loc[1, "naas_cd"] == "KA04352K"
    assert out.loc[0, "naas_cd"] == "0W194007"
    assert out.loc[7, "naas_cd"] == "XFH9918T"
    assert int(out.loc[0, "seniority"]) == 3  # 김영주: 17, 19, 20대
    assert out.loc[7, "elect_type"] == "지역구" and out.loc[7, "district"].endswith("덕양구갑")
    assert o is not None


def test_enrich_uses_role_group(ref):
    t = _turns()
    t["role_group"] = ["legislator", "legislator", "nonlegislator", "legislator", "excluded", "legislator"]
    out = L.enrich(t, _meetings(), ref)
    assert out.loc[0, "leg_side_basis"] == "role_group" and not out.loc[0, "leg_side"]
    assert pd.isna(out.loc[0, "naas_cd"])


def test_enrich_empty(ref):
    t = _turns().iloc[:0]
    out = L.enrich(t, _meetings(), ref)
    assert len(out) == 0 and "naas_cd" in out.columns


def test_linking_methods_cover_outputs(ref):
    out = L.enrich(_turns(), _meetings(), ref)
    linked = out[out.naas_cd.notna()]
    assert set(linked.id_method) <= set(L.LINKING_METHODS)
    assert not set(out[out.naas_cd.isna()].id_method) & set(L.LINKING_METHODS)


# ----------------------------------------------------------------------------- label repair

@pytest.mark.parametrize("pos,name,label,cd", [
    ("김형오", "의장", "김형오 의장", "4MX3134T"),          # HWP: name in the position slot
    ("권경석", None, "권경석", "1MB8686B"),                 # HWP: bare name label
    ("노철래", "위원님", "노철래 위원님", "CKH5381Y"),       # honorific
    ("金成會", "義員", "金成會 義員", "BEJ7022K"),           # hanja, typo glyph in the title
])
def test_label_repair(R, pos, name, label, cd):
    r = res(R, 18, "2008-12-09", pos, name, label=label)
    assert (r["naas_cd"], r["confidence"], r["label_repair"]) == (cd, "medium", True)


def test_label_repair_no_name_left(R):
    r = res(R, 18, "2009-10-23", "委員長", None, label="委員長")
    assert r["naas_cd"] is None and r["method"] == "unresolved_no_name"
    r = res(R, 18, "2008-09-02", "김낙연", "위원장", label="김낙연 위원장")  # not a member
    assert r["naas_cd"] is None


def test_hanja_variant_needs_same_reading(R):
    # '金鶴訟' is one glyph from 金鶴松 (訟/松 both 송) and from 金鶴在 (在 재): only the same reading counts
    r = res(R, 18, "2009-02-10", "委員長", "金鶴訟")
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("KDL4367Q", "hanja_term_variant", "medium")
    # a glyph with another reading is not linked, even with committee evidence ('鄭夢憲' is not 鄭夢準)
    assert res(R, 18, "2009-02-10", "委員長", "金鶴甲")["naas_cd"] is None
    assert res(R, 18, "2009-02-10", "委員長", "金鶴甲", committee="국방위원회")["naas_cd"] is None


def test_nonleg_panel_ministry_rows(R):
    # the panel row for 김진표 교육인적자원부 (dual office, 17대) has an end date before its start date:
    # the ministry named in the title still finds it
    r = res(R, 17, "2005-06-01", "부총리겸교육인적자원부장관", "김진표", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("Q9H5708M", "nonleg_dual_office")
    # 이달곤: 18대 list member heard as nominee before resigning; the panel note names 18대
    r = res(R, 18, "2009-02-02", "행정안전부장관후보자", "이달곤", leg_side=False, tclass="nominee")
    assert (r["naas_cd"], r["method"]) == ("QUM8288K", "nonleg_sitting_member_panel_note")
    # 산업자원부장관 김영주 (2007) is not the 17대 member: the panel note flags the homonym
    r = res(R, 17, "2007-06-01", "산업자원부장관", "김영주", leg_side=False, tclass="cabinet")
    assert r["naas_cd"] is None and r["method"] == "nonleg_name_collision"


def test_hangul_typo_with_committee(R):
    # HWP '우체창' (우제창) in the 예결위 subcommittee; '우' names at distance 1: 우제창 only on the committee
    r = res(R, 18, "2008-09-11", "위원", "우체창", committee="예산결산특별위원회")
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("GFO6700V", "name_fuzzy_committee", "low")
    assert not L._HANJA_NAME_RE.match("우체창") and L._HANJA_NAME_RE.match("金鶴訟")


def test_fused_and_multi_token_labels(R):
    r = res(R, 16, "2002-01-01", None, None, label="農林海洋水産委員長代理崔善榮")
    assert (r["naas_cd"], r["label_repair"]) == ("BNT6492X", True)
    assert L.repair_name("소위원장", "소위원장 김부겸", "소위원장 김부겸 위원") == "김부겸"
    assert L.repair_name("위원", "이수진(비)", "이수진(비) 위원") is None  # a real name is left alone


def test_split_and_forced_repair(R):
    r = res(R, 16, "2002-01-01", "李", "協委員", label="李 協委員")  # 李協 printed with a space
    assert (r["naas_cd"], r["label_repair"]) == ("BJG5829N", True)
    r = res(R, 16, "2002-01-01", "金孝錫議員", "간략하게", label="金孝錫議員 간략하게")  # speech text in the name slot
    assert (r["naas_cd"], r["confidence"]) == ("UIN2835Q", "medium")
    assert res(R, 16, "2002-01-01", "委員長", "千容宅;")["naas_cd"] == "O8Z3184P"  # stray punctuation
    assert res(R, 16, "2002-01-01", "委員長代理", None, label="委員長代理")["naas_cd"] is None


def test_forced_repair_is_narrow(R):
    # the printed name is a (non-member) hanja name: the fused name in the position slot is not taken
    assert L.repair_name("張泰玩委員", "李南基", "張泰玩委員 李南基", force=True) is None
    assert L.repair_name("金孝錫議員", "간략하게", "金孝錫議員 간략하게", force=True) == "金孝錫"


def test_truncated_glued_title(R):
    assert L.clean_name("李漢久委")[0] == "李漢久"
    r = res(R, 16, "2002-01-01", None, None, label="李漢久委")
    assert r["naas_cd"] is not None and r["label_repair"]


def test_one_jamo_apart():
    for a, b in [("우제창", "우체창"), ("전병헌", "전병현"), ("서갑원", "서갑워"), ("박준선", "박준석"), ("윤호중", "윤호증")]:
        assert L.one_jamo_apart(a, b)
    assert not L.one_jamo_apart("정몽준", "정몽헌") and not L.one_jamo_apart("김영주", "김영주")


# ----------------------------------------------------------------------------- review fixes (2026-09-26)

@pytest.mark.parametrize("note,terms", [
    ("비겸직 (15·16대 의원이었으나 22대 아님; 경기도지사 출신); date approx", {15, 16}),
    ("비겸직 (18대 의원이었으나 21대/22대 아님; 교수 출신); date approx", {18}),
    ("비겸직 (이화여자대학교 교수 출신; 21대 비례의원 아님; 의원 경력 없음)", set()),
    ("비겸직 (초대 이명박 농림수산식품부 장관; 18대 MP 아님; 최초 의원 당선은 20대 2016년)", {20}),
    ("비겸직 (임명 당시 제주특별자치도지사 재직 중; 21대 의원 아님; 20대 총선 인천 남동구 갑 낙선; "
     "이전 의원직은 16~18대 서울 양천구 갑/관악구 을)", {16, 17, 18}),
    ("비겸직 (17대 비례의원 2004~2008; 의원직 사퇴 후 이명박 수석→차관→장관; 임명 당시 현직 의원 아님; "
     "start/end corrected; 18대 서울 용산구는 오류)", {17}),
    ("비겸직 (17대 비례의원이었으나 18대에는 의원직 없음; 기획재정부 차관 출신 관료 신분으로 임명)", {17}),
    ("비겸직 (19대 의원 임기 만료 후 20대 미출마; 농림부 장관 임명 2017-07-03 당시 현직 의원 아님)", {19}),
    ("비겸직 (18대 의원직 2009-02-03 사퇴 후 다음날 장관 임명; date approx)", {18}),
    ("겸직 장관? (20대 의원 임기 2020-05까지) - 임명 시점 요확인", set()),  # flagged note
    ("비겸직 (관료 출신 김영주 1950년생; 17대 비례의원 김영주 1955년생과 동명이인 혼동; date corrected)", set()),
])
def test_panel_note_terms_negation(note, terms):
    assert L.panel_note_terms(note) == terms


def test_panel_note_negated_term_is_not_linked(R):
    # 고용노동부장관 김문수 (2024-25) is not the sitting 22대 member 김문수 (86R9476S, 순천갑)
    for d, pos, tc in (("2024-09-12", "고용노동부장관", "cabinet"), ("2024-08-26", "고용노동부장관후보자", "nominee")):
        r = res(R, 22, d, pos, "김문수", leg_side=False, tclass=tc)
        assert r["naas_cd"] is None and r["method"] == "nonleg_name_collision"
    # the 18대 nominee 이달곤 (note names 18대, the only term he served) is still linked
    r = res(R, 18, "2009-02-02", "행정안전부장관후보자", "이달곤", leg_side=False, tclass="nominee")
    assert r["naas_cd"] == "QUM8288K"


def test_title_office_and_ministry_match():
    assert L.title_office("고용노동부장관직무대행") == ("고용노동부", True)
    assert L.title_office("부총리겸기획재정부장관후보자") == ("기획재정부", False)
    assert L.title_office("國務總理") == ("국무총리", False)
    assert L.title_office("公職候補者") == (None, False)
    assert L.title_office("국가정보원장후보자") == (L.NONCABINET, False)
    assert L.title_office("農林部長官") == ("農林部", False) and L.office_key("農林部") == "농림부"
    assert L.ministry_compatible("노동부", "고용노동부") and L.ministry_compatible("행정자치부", "안전행정부")
    assert L.ministry_compatible("보건복지가족부", "보건복지부") and L.ministry_compatible("환경부", "기후에너지환경부")
    assert L.ministry_compatible("행정안정부", "행정안전부")      # unknown (typo) office: not used to reject
    assert not L.ministry_compatible("고용노동부", "국무총리")
    assert not L.ministry_compatible(L.NONCABINET, "교육부")
    assert L.ministry_compatible(None, "교육부")


def test_panel_ministries_are_known(R):
    ms = {r["ministry"] for rows in R.panel.values() for r in rows}
    assert ms <= set(L.CABINET_LINEAGE), ms - set(L.CABINET_LINEAGE)


def test_dual_office_checks_office_and_window(R):
    # acting labor minister 김민석 (2025-04-16) is not the member who becomes 국무총리 in 2025-07
    r = res(R, 22, "2025-04-16", "고용노동부장관직무대행", "김민석", leg_side=False, tclass="cabinet")
    assert r["naas_cd"] is None
    # the 국무총리 row: nominee hearing and incumbent are linked (a row of the office named in the title
    # counts at any date, because panel end dates are partly wrong)
    assert res(R, 22, "2025-06-24", "국무총리후보자", "김민석", leg_side=False, tclass="nominee")["naas_cd"] == "MLH1404S"
    assert res(R, 22, "2025-09-10", "국무총리", "김민석", leg_side=False, tclass="cabinet")["naas_cd"] == "MLH1404S"
    # titles that name no office rely on the window: 45 days before the confirmation date is inside
    # the nominee window (120 days) and outside the cabinet window (30 days); 170 days is outside both
    assert res(R, 22, "2025-05-10", "公職候補者", "김민석", leg_side=False, tclass="nominee")["naas_cd"] == "MLH1404S"
    assert res(R, 22, "2025-05-10", "부총리", "김민석", leg_side=False, tclass="cabinet")["naas_cd"] is None
    assert res(R, 22, "2025-01-05", "公職候補者", "김민석", leg_side=False, tclass="nominee")["naas_cd"] is None
    # 김희정 여성가족부: the panel row ends 2015-03-12, she speaks as minister until 2015-11
    r = res(R, 19, "2015-10-12", "여성가족부장관", "김희정", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("HRG9655C", "nonleg_dual_office")
    # a title naming another ministry does not take the 고용노동부 row of 김영주 (2017)
    r = res(R, 20, "2017-12-01", "환경부장관", "김영주", leg_side=False, tclass="cabinet")
    assert r["naas_cd"] is None
    # an acting title matches the official's own other office: 부총리 최경환 as 국무총리직무대행 (2015)
    r = res(R, 19, "2015-05-12", "국무총리직무대행", "최경환", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("FJ03481D", "nonleg_dual_office") and "기획재정부" in r["note"]
    # renamed ministries still match (노동부 title, 고용노동부 row)
    r = res(R, 18, "2010-05-01", "노동부장관", "임태희", leg_side=False, tclass="cabinet")
    assert (r["naas_cd"], r["method"]) == ("PJ76653N", "nonleg_dual_office")


def test_former_dual_office_minister_across_terms(R):
    # 20대 dual-office ministers still in office in 21대: linked in both terms
    for pos, nm, cd in (("행정안전부장관", "진영", "IDV9108J"), ("부총리겸교육부장관", "유은혜", "5CO4531H"),
                        ("중소벤처기업부장관", "박영선", "S8431719"), ("국토교통부장관", "김현미", "9XB6464U"),
                        ("법무부장관", "추미애", "URV1689Q")):
        r20 = res(R, 20, "2020-02-10", pos, nm, leg_side=False, tclass="cabinet")
        r21 = res(R, 21, "2020-10-07", pos, nm, leg_side=False, tclass="cabinet")
        assert r20["naas_cd"] == cd and r21["naas_cd"] == cd, (nm, r20["method"], r21["method"])
        assert r21["method"] == "nonleg_former_member_panel"


def test_memid_not_used_on_nonlegislator_side(R):
    r = res(R, 22, "2024-09-26", "방송통신위원장직무대행", "김태규", mem_id=7216, leg_side=False, tclass="other")
    assert r["naas_cd"] is None and r["memid_status"] == "not_used_nonlegislator"
    r = res(R, 21, "2021-01-10", "참고인", "김영주", mem_id=6291, leg_side=False, tclass="other")
    assert r["naas_cd"] is None
    r = res(R, 21, "2021-01-10", "위원", "김영주", mem_id=6291)
    assert (r["naas_cd"], r["method"], r["memid_status"]) == ("0W194007", "mem_id", "used")


def test_memid_needs_the_printed_name(R):
    # a different printed name: the id is not used, the printed name is resolved instead
    for t, d in ((20, "2019-01-10"), (21, "2021-01-10")):
        r = res(R, t, d, "위원", "홍길동", mem_id=6291)
        assert r["naas_cd"] is None and r["memid_status"] == "not_used_name"
    # one glyph from the record name (typo): linked through the id at medium
    r = res(R, 19, "2013-05-01", "위원", "은수민", mem_id=6983)
    assert (r["naas_cd"], r["method"], r["confidence"]) == ("G8Y72423", "mem_id_name_mismatch", "medium")


def test_initial_sound_only_word_initial(R):
    assert not L.same_reading("龍", "용") and L.same_reading("龍", "용", initial=True)
    assert res(R, 16, "2001-01-01", "委員", "金德容")["naas_cd"] is None  # 김덕용 is not 金德龍 김덕룡
    assert L.dueum_form("로") == "노" and L.dueum_form("녀") == "여" and L.dueum_form("룡") == "용"


def test_title_class_hanja_commission_heads(R):
    cn = R.committee_names
    for p in ("中央勞動委員長", "金融監督委員長", "公正去來委員長", "中央選擧管理委員長", "放送委員長",
              "女性特別委員長", "中小企業特別委員長"):
        assert L.title_class(p, cn) == "other", p
    for p in ("財政經濟委員長代理", "環境勞動委員長代理", "國會運營委員長代理", "政治改革特別委員長代理",
              "豫算決算特別委員長", "委員長", "女性委員長"):
        assert L.title_class(p, cn) == "legislator", p


def _hom_turns():
    # two meetings; 19대 김영주 homonyms and a marked/unmarked 이수진 pair
    return pd.DataFrame({
        "conf_num": [10, 10, 10, 20, 20],
        "turn_seq": [1, 2, 3, 1, 2],
        "speaker_label_raw": ["위원장 이수진(비)", "이수진 위원", "김영주 위원", "김영주 위원", "최경환 위원"],
        "speaker_pos": ["위원장", "위원", "위원", "위원", "위원"],
        "speaker_name": ["이수진(비)", "이수진", "김영주", "김영주", "최경환"],
        "speaker_mem_id": [None] * 5, "speaker_area": [None] * 5,
        "speech_date": ["2021-03-02", "2021-03-02", "2021-03-02", "2013-01-15", "2013-01-15"],
    })


def _hom_meetings():
    return pd.DataFrame({"conf_num": [10, 20], "term": [21, 19], "class_name": ["상임위원회"] * 2,
                         "hearing_type": ["상임위원회"] * 2, "committee_raw": ["국토교통위원회", "행정안전위원회"],
                         "subcommittee": [None] * 2, "date": ["2021-03-02", "2013-01-15"]})


def test_enrich_is_batch_independent(ref):
    t, m = _hom_turns(), _hom_meetings()
    full = L.enrich(t, m, ref)
    parts = pd.concat([L.enrich(t[t.conf_num == c], m, ref) for c in (10, 20)])
    cols = ["naas_cd", "id_method", "id_confidence", "id_note"]
    assert full[cols].equals(parts.loc[full.index, cols])
    assert full.loc[3, "naas_cd"] == "E6S73230"  # 19대 김영주 on 행정안전위원회 (committee cue)
    assert full.loc[1, "id_method"] == "homonym_meeting_complement"


def test_enrich_dates_and_keys(ref):
    m = pd.DataFrame({"conf_num": [1, 2], "term": [20, 20], "class_name": ["상임위원회"] * 2,
                      "hearing_type": ["상임위원회"] * 2, "committee_raw": ["문화체육관광위원회"] * 2,
                      "subcommittee": [None] * 2, "date": ["2019-10-01", "2019-10-01"]})
    base = dict(source="xlsx", speaker_label_raw="최경환 위원", speaker_pos="위원", speaker_name="최경환",
                speaker_mem_id=None, speaker_area=None)
    t = pd.DataFrame([dict(conf_num=1, turn_seq=1, speech_date=None, **base),
                      dict(conf_num=2, turn_seq=1, speech_date="", **base)])
    o = L.enrich(t, m, ref)
    assert list(o.naas_cd) == ["KA04352K", "KA04352K"] and list(o.leg_date_basis) == ["meeting_date"] * 2
    assert L.iso_date("2019.10.01") == "2019-10-01" and L.iso_date("20191001") == "2019-10-01"
    assert L.iso_date("2019-02-30") is None and L.iso_date("") is None and L.iso_date(pd.NaT) is None
    t2 = t.assign(speech_date=["2019.10.01", "bad"])
    with pytest.warns(UserWarning, match="do not parse"):
        o2 = L.enrich(t2, m, ref)
    assert list(o2.leg_date_basis) == ["speech_date", "meeting_date"] and list(o2.naas_cd) == ["KA04352K"] * 2
    # conf_num as str in meetings, int in turns
    o3 = L.enrich(t.assign(speech_date="2019-10-01"), m.assign(conf_num=m.conf_num.astype(str)), ref)
    assert list(o3.naas_cd) == ["KA04352K"] * 2
    # a null term in turns is filled from meetings (with a warning); the input column is unchanged
    t4 = t.assign(speech_date="2019-10-01", term=[float("nan"), 20.0])
    with pytest.warns(UserWarning, match="filled from meetings"):
        o4 = L.enrich(t4, m, ref)
    assert list(o4.naas_cd) == ["KA04352K"] * 2 and pd.isna(o4.loc[0, "term"])
    # turns without a meetings row are reported
    with pytest.warns(UserWarning, match="no meetings row"):
        o5 = L.enrich(t.assign(conf_num=[1, 99]), m, ref)
    assert o5.loc[1, "id_method"] == "unresolved_no_term"


# R2: a label the parser rates label_confidence 'low' is never linked
def test_r2_no_link_on_low_confidence_label(ref):
    t = _turns()
    t["label_confidence"] = ["high", "high", "low", "low", "medium", None]
    out = L.enrich(t, _meetings(), ref)
    base = L.enrich(t.drop(columns=["label_confidence"]), _meetings(), ref)
    assert base.loc[9, "naas_cd"] == "XPP2564T" and base.loc[1, "naas_cd"] == "KA04352K"
    for i in (9, 1):                                     # both would have linked
        assert pd.isna(out.loc[i, "naas_cd"]) and out.loc[i, "id_method"] == "unlinked:label_confidence_low"
        assert pd.isna(out.loc[i, "leg_name_hangul"]) and pd.isna(out.loc[i, "id_confidence"])
    keep = [5, 3, 0, 7]
    assert out.loc[keep, "naas_cd"].equals(base.loc[keep, "naas_cd"])
    assert out.attrs["legislators"] == {"rows": 6, "label_confidence_low_rows": 2, "label_confidence_low_links_blocked": 2}
    assert "unlinked:label_confidence_low" not in L.LINKING_METHODS


def test_mem_id_correction_for_a_same_name_pair():
    res = {"naas_cd": "BQS2021C", "method": "mem_id", "confidence": "high", "memid_status": "used", "note": None}
    out = L._apply_mem_id_correction(42927, "김성태 위원", dict(res))
    assert (out["naas_cd"], out["method"], out["memid_status"]) == ("9UW75767", "mem_id_corrected", "corrected")
    assert "BQS2021C" in out["note"] and "mem_id_corrected" in L.LINKING_METHODS
    assert L._apply_mem_id_correction(42927, "金成泰 위원", dict(res))["naas_cd"] == "BQS2021C"   # other label
    assert L._apply_mem_id_correction(42928, "김성태 위원", dict(res))["naas_cd"] == "BQS2021C"   # other meeting
    assert L._apply_mem_id_correction("42927", "김성태 위원", dict(res))["naas_cd"] == "9UW75767"  # enrich string key
    assert L._apply_mem_id_correction(None, "김성태 위원", dict(res))["naas_cd"] == "BQS2021C"


def test_mem_id_correction_through_enrich():
    ref = L.load_reference()
    R = ref["resolver"]
    mem = {code: k for k, (code, term, *_rest) in R.memid.items() if term == 20 and code in ("BQS2021C", "9UW75767")}
    if set(mem) != {"BQS2021C", "9UW75767"}:
        pytest.skip("record member-term crosswalk without the two 20th-term 김성태")
    t = pd.DataFrame({"conf_num": [42927, 42927, 42927], "turn_seq": [65, 67, 99], "term": [20, 20, 20],
                      "speech_date": ["2018-03-12"] * 3, "speaker_pos": ["위원"] * 3,
                      "speaker_name": ["金成泰", "김성태", "김성태"],
                      "speaker_label_raw": ["金成泰 위원", "김성태 위원", "김성태 의원"],
                      "speaker_mem_id": [mem["9UW75767"], mem["BQS2021C"], mem["BQS2021C"]],
                      "role_group": ["legislator"] * 3})
    m = pd.DataFrame({"conf_num": [42927], "term": [20], "date": ["2018-03-12"],
                      "committee_raw": ["헌법개정및정치개혁특별위원회"], "subcommittee": [None]})
    out = L.enrich(t, m, ref=ref)
    assert (out.loc[0, "naas_cd"], out.loc[0, "id_method"]) == ("9UW75767", "mem_id")
    assert (out.loc[1, "naas_cd"], out.loc[1, "id_method"], out.loc[1, "id_memid_status"]) == \
        ("9UW75767", "mem_id_corrected", "corrected")
    assert (out.loc[2, "naas_cd"], out.loc[2, "id_method"]) == ("BQS2021C", "mem_id")   # other label: unchanged
