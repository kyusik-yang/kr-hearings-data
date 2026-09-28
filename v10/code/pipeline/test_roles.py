"""Tests for roles.py. Run: python -m pytest -q test_roles.py

Most cases use a small stub roster and a stub list of National Assembly committee names so
the expected outcome depends only on the rules. A few cases at the end use the default
lazily loaded data files (skipped when the files are absent).
"""
import os
import sys

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))

import roles  # noqa: E402

# --------------------------------------------------------------------------- stubs

NA_COMM = frozenset(roles._norm_key(x) for x in (
    "국회운영위원회", "법제사법위원회", "정무위원회", "기획재정위원회", "재정경제위원회", "교육위원회",
    "과학기술정보방송통신위원회", "문화체육관광방송통신위원회", "국방위원회", "행정안전위원회", "행정자치위원회",
    "보건복지위원회", "환경노동위원회", "국토교통위원회", "정보위원회", "여성가족위원회", "여성위원회",
    "농림해양수산위원회", "예산결산특별위원회", "윤리특별위원회", "방송통신특별위원회", "여성특별위원회",
    "정치개혁특별위원회", "안건조정위원회", "건설교통위원회",
))
NA_BY_TERM = {
    16: frozenset(roles._norm_key(x) for x in ("과거사진상규명에관한특별위원회", "여성특별위원회", "법제사법위원회")),
    18: frozenset(roles._norm_key(x) for x in ("중소기업경쟁력강화특별위원회", "예산결산특별위원회")),
}
ROSTER = {
    16: frozenset({"이연숙", "李姸淑", "김철수", "金哲洙", "이한동", "李漢東", "송영진", "宋榮珍", "이재오", "李在五", "원희룡", "元喜龍",
                   "이협", "李協", "여성의원"}),
    17: frozenset({"홍길동", "이한구"}),
    18: frozenset({"홍길동", "이한구", "조순형", "趙舜衡"}),
    19: frozenset({"홍길동", "박영수"}),
    20: frozenset({"홍길동"}),
    21: frozenset({"홍길동"}),
}


def C(term=19, cls="상임위원회", sub=False, comm="법제사법위원회", subc=None):
    return dict(term=term, class_name=cls, hearing_type=cls, is_subcommittee=sub, committee_raw=comm,
                subcommittee=subc)


STAND = C()
STAND16 = C(term=16)
OPS = C(comm="국회운영위원회")
SUB = C(term=20, sub=True, subc="법안심사제1소위원회")
PLEN = C(cls="국회본회의", comm="국회본회의")
PLEN16 = C(term=16, cls="국회본회의", comm="국회본회의")
PLEN18 = C(term=18, cls="국회본회의", comm="국회본회의")
AUDIT = C(term=18, cls="국정감사", comm="행정안전위원회")
BUDGET = C(term=18, cls="예산결산특별위원회", comm="예산결산특별위원회")
BUDGET_SUB = C(term=18, cls="예산결산특별위원회", sub=True, comm="예산결산특별위원회", subc="계수조정소위원회")
ADJ = C(term=21, sub=True, comm="법제사법위원회", subc="안건조정위원회")
CONFIRM = C(term=20, cls="특별위원회", comm="대법관(노정희)임명동의에관한인사청문특별위원회")
NOCTX = dict(term=None, class_name=None, hearing_type=None, is_subcommittee=None, committee_raw=None, subcommittee=None)


def run(pos, name=None, ctx=STAND, mem_id=None, label_raw=None):
    return roles.classify(pos, name, label_raw=label_raw, mem_id=mem_id, roster=ROSTER, na_committees=NA_COMM,
                          na_committees_by_term=NA_BY_TERM, **ctx)


# (id, pos, name, ctx, mem_id, expected role, expected rule prefix or None)
CASES = [
    # ---------------- legislator
    ("member_wiwon", "위원", "홍길동", STAND, None, "legislator", "leg.member"),
    ("member_uiwon_plenary", "의원", "홍길동", PLEN, None, "legislator", "leg.member"),
    ("hanja_member_委員", "委員", "金哲洙", STAND16, None, "legislator", "leg.member"),
    ("hanja_member_議員", "議員", "金哲洙", PLEN16, None, "legislator", "leg.member"),
    ("hanja_member_typo_義員", "義員", "李在五", PLEN16, None, "legislator", "leg.member"),
    ("hanja_fused_name_first", "宋榮珍議員", None, PLEN16, None, "legislator", "leg.member"),
    ("hanja_name_cut", "李", "協委員", STAND16, None, "legislator", "leg.member"),
    ("hanja_fused_over_junk_name", "元喜龍委員", "금년", STAND16, None, "legislator", "leg.member"),
    ("hangul_fused_name_first", "김성곤위원", None, STAND, None, "legislator", "leg.member"),
    ("title_party_tag", "위원(국)", "최경환", STAND, None, "legislator", "leg.member"),
    ("title_typo_wiwonnim", "위원님", "홍길동", STAND, None, "legislator", "leg.member"),
    ("member_memid", "위원", "홍길동", C(term=21), 1234, "legislator", "leg.member.memid"),
    ("memid_no_title", None, "홍길동", C(term=21), 1234, "legislator", "memid.no_title"),
    ("bare_name_in_roster", None, "박영수", STAND, None, "legislator", "bare_name.roster"),
    ("subchair_reporting_full", "소위원장", "홍길동", STAND, None, "legislator", "leg.chair.subcommittee.reporting"),
    ("subchair_reporting_budget_D6", "소위원장", "이한구", BUDGET, None, "legislator", "leg.chair.subcommittee.reporting"),
    ("subchair_named_reporting", "법안심사소위원장", "홍길동", STAND, None, "legislator", "leg.chair.subcommittee.reporting"),
    ("subchair_typo_reporting", "소위원잔", "홍길동", STAND, None, "legislator", "leg.chair.subcommittee.reporting"),
    ("other_comm_chair_deputy_in_judiciary", "보건복지위원장대리", "홍길동", STAND, None, "legislator",
     "leg.chair.named.na_committee.reporting_other_committee"),
    ("comm_chair_reporting_plenary", "법제사법위원장", "홍길동", PLEN, None, "legislator",
     "leg.chair.named.na_committee.reporting_plenary"),
    ("special_chair_reporting_plenary", "정치개혁특별위원장", "홍길동", PLEN, None, "legislator",
     "leg.chair.named.na_committee.reporting_plenary"),
    ("speaker_title_outside_plenary", "의장", "홍길동", STAND, None, "legislator", "leg.chair.speaker.nonplenary"),
    ("adj_chair_reporting", "조정위원장", "홍길동", STAND, None, "legislator", "leg.chair.adjustment.reporting"),
    ("memid_chair_named_plenary", "보건복지위원장", "홍길동", C(term=21, cls="국회본회의", comm="국회본회의"), 99,
     "legislator", "leg.chair.named.na_committee.reporting_plenary.memid"),
    ("memid_decisive_odd_title", "간사", "홍길동", C(term=21), 99, "legislator", "memid.decisive"),
    # ---------------- chair
    ("chair_bare", "위원장", "홍길동", STAND, None, "chair", "leg.chair.committee"),
    ("chair_deputy", "위원장대리", "홍길동", STAND, None, "chair", "leg.chair.committee.acting"),
    ("chair_acting", "위원장직무대행", "홍길동", STAND, None, "chair", "leg.chair.committee.acting"),
    ("chair_typo", "위윈장대리", "홍길동", STAND, None, "chair", "leg.chair.committee.acting"),
    ("chair_hanja_委員長", "委員長", "金哲洙", STAND16, None, "chair", "leg.chair.committee"),
    ("chair_memid", "위원장", "홍길동", C(term=21), 77, "chair", "leg.chair.committee.memid"),
    ("subchair_presiding", "소위원장", "홍길동", SUB, None, "chair", "leg.chair.subcommittee.presiding"),
    ("subchair_presiding_budget", "소위원장", "이한구", BUDGET_SUB, None, "chair", "leg.chair.subcommittee.presiding"),
    ("subchair_named_presiding", "법안심사소위원장", "홍길동", SUB, None, "chair", "leg.chair.subcommittee.presiding"),
    ("subchair_deputy_presiding", "소위원장대리", "홍길동", SUB, None, "chair", "leg.chair.subcommittee.presiding"),
    ("subchair_typo_presiding", "소위원자", "홍길동", SUB, None, "chair", "leg.chair.subcommittee.presiding"),
    ("subchair_memid", "소위원장", "홍길동", C(term=21, sub=True, subc="예산결산기금심사소위원회"), 5, "chair",
     "leg.chair.subcommittee.presiding.memid"),
    ("subchair_no_ctx", "소위원장", "홍길동", NOCTX, None, "chair", "leg.chair.subcommittee.presiding_unknown_ctx"),
    ("adj_chair_presiding", "조정위원장", "홍길동", ADJ, None, "chair", "leg.chair.adjustment.presiding"),
    ("speaker_plenary", "의장", "홍길동", PLEN, None, "chair", "leg.chair.speaker.presiding"),
    ("deputy_speaker_plenary", "부의장", "홍길동", PLEN, None, "chair", "leg.chair.speaker.presiding"),
    ("speaker_hanja_議長", "議長", "李漢東", PLEN16, None, "chair", "leg.chair.speaker.presiding"),
    ("deputy_speaker_hanja_副議長", "副議長", "金哲洙", PLEN16, None, "chair", "leg.chair.speaker.presiding"),
    ("speaker_acting_hanja", "議長職務代行", "趙舜衡", PLEN18, None, "chair", "leg.chair.speaker.presiding"),
    ("speaker_gukhoe", "국회의장", "홍길동", PLEN, None, "chair", "leg.chair.speaker.presiding"),
    ("speaker_whole_house", "의장", "홍길동", C(cls="전원위원회", comm="전원위원회"), None, "chair",
     "leg.chair.speaker.presiding"),
    ("audit_team_leader", "반장", "홍길동", AUDIT, None, "chair", "leg.chair.audit_team"),
    ("audit_team_leader_deputy", "반장대리", "홍길동", AUDIT, None, "chair", "leg.chair.audit_team"),
    ("named_chair_own_committee", "법제사법위원장", "홍길동", STAND, None, "chair",
     "leg.chair.named.na_committee.presiding"),
    ("budget_chair_own", "예산결산특별위원장", "이한구", BUDGET, None, "chair", "leg.chair.named.na_committee.presiding"),
    ("named_chair_abbrev_prefix", "농림해양위원장", "김철수", C(term=16, comm="농림해양수산위원회"), None, "chair",
     "leg.chair.named.na_committee_prefix.presiding"),
    # ---------------- government commission heads vs committee chairs
    ("gov_comm_broadcasting", "방송통신위원장", "이효성", C(comm="과학기술정보방송통신위원회"), None,
     "independent_official", "gov.commission_head"),
    ("gov_comm_ftc", "공정거래위원장", "홍길동", C(comm="정무위원회"), None, "independent_official", "gov.commission_head"),
    ("gov_comm_human_rights_head", "국가인권위원장", "홍길동", OPS, None, "independent_official", "gov.commission_head"),
    ("gov_comm_fsc", "금융위원장", "홍길동", C(comm="정무위원회"), None, "independent_official", "gov.commission_head"),
    ("gov_comm_acrc", "국민권익위원장", "홍길동", C(comm="정무위원회"), None, "independent_official", "gov.commission_head"),
    ("gov_comm_nssc", "원자력안전위원장", "홍길동", C(comm="과학기술정보방송통신위원회"), None,
     "independent_official", "gov.commission_head"),
    ("gov_comm_deputy", "규제개혁위원회부위원장", "홍길동", C(comm="정무위원회"), None, "independent_official",
     "gov.commission_head"),
    ("gov_comm_hanja", "放送委員長", "金哲洙", STAND16, None, "independent_official", "gov.commission_head"),
    ("gov_comm_refuted_by_roster", "여성특별위원장", "강기원", C(term=16, comm="여성특별위원회"), None,
     "independent_official", "leg.chair.named.na_committee.name_not_in_roster>gov.commission_head"),
    ("na_comm_name_in_roster", "여성특별위원장", "김철수", C(term=16, comm="여성특별위원회"), None, "chair",
     "leg.chair.named.na_committee.presiding"),
    ("na_special_variant_same_term", "과거사진상조사특별위원장대리", "김철수", C(term=16, comm="법제사법위원회"), None,
     "legislator", "leg.chair.named.na_special_same_term.reporting_other_committee"),
    ("gov_special_commission", "중소기업특별위원장", "한준호", C(term=16, comm="국회운영위원회"), None,
     "independent_official", "gov.commission_head"),
    ("gov_special_commission_dual_office_memid", "중소기업특별위원장", "김덕배", C(term=16, cls="국정감사", comm="국회운영위원회"),
     11, "independent_official", "memid.title_conflict.gov.commission_head"),
    ("viewer_homonym_memid", "방송통신위원장직무대행", "김태규", C(term=22, cls="국회본회의", comm="국회본회의"), 7216,
     "independent_official", "memid.title_conflict.gov.commission_head"),
    ("special_18_prefix_not_other_term", "중소기업특별위원장", "홍길동", C(term=16, comm="국회운영위원회"), None,
     "independent_official", "gov.commission_head"),
    ("plenary_chair_report_never_refuted", "建設交通委員長代理", "松雄", PLEN16, None, "legislator",
     "leg.chair.named.na_committee.reporting_plenary"),
    ("hanja_variant_name_reading_in_roster", "女性特別委員長", "李嬿淑", C(term=16, comm="여성특별위원회"), None, "chair",
     "leg.chair.named.na_committee.presiding"),
    ("gov_special_inspector", "특별감찰관", "홍길동", OPS, None, "independent_official", "gov.special_inspector"),
    ("gov_human_rights_staff", "국가인권위원회사무총장", "홍길동", OPS, None, "independent_official", "gov.human_rights"),
    # ---------------- committee staff
    ("staff_expert", "전문위원", "홍길동", STAND, None, "committee_staff", "staff.committee"),
    ("staff_chief_expert", "수석전문위원", "홍길동", STAND, None, "committee_staff", "staff.committee"),
    ("staff_leg_researcher", "입법조사관", "홍길동", STAND, None, "committee_staff", "staff.committee"),
    ("staff_leg_researcher_asst", "입법조사관보", "홍길동", STAND, None, "committee_staff", "staff.committee"),
    ("staff_leg_review", "입법심의관", "홍길동", STAND, None, "committee_staff", "staff.committee"),
    ("staff_named_na", "법제사법위원회수석전문위원", "홍길동", STAND, None, "committee_staff", "staff.committee_named"),
    ("staff_hanja", "專門委員", "金哲洙", STAND16, None, "committee_staff", "staff.committee"),
    ("staff_hanja_chief", "首席專門委員", "金哲洙", STAND16, None, "committee_staff", "staff.committee"),
    ("staff_memid_never_legislator_wo_id", "전문위원", "홍길동", PLEN, None, "committee_staff", "staff.committee"),
    ("expert_member_non_na", "노사정위원회전문위원", "홍길동", STAND, None, "other_official", "gov.expert_member"),
    # ---------------- prime minister
    ("pm_sitting", "국무총리", "한덕수", PLEN, None, "prime_minister", "exec.prime_minister"),
    ("pm_acting", "국무총리직무대행", "최상목", PLEN, None, "prime_minister", "exec.prime_minister"),
    ("pm_acting_with_ministry", "국무총리직무대행기획재정부장관", "최경환", PLEN, None, "prime_minister",
     "exec.prime_minister"),
    ("pm_as_acting_president", "대통령권한대행국무총리", "한덕수", PLEN, None, "prime_minister", "exec.prime_minister"),
    ("pm_hanja", "國務總理", "李漢東", PLEN16, None, "prime_minister", "exec.prime_minister"),
    ("pm_designate_seori", "국무총리서리", "장상", PLEN16, None, "prime_minister", "exec.prime_minister"),
    ("pm_dual_office_memid", "국무총리", "홍길동", C(term=20, cls="국회본회의", comm="국회본회의"), 55,
     "prime_minister", "memid.dual_office.exec.prime_minister"),
    ("pm_office_chief_not_pm", "국무총리실장", "홍길동", OPS, None, "senior_bureaucrat", "senior"),
    ("pm_secretary_chief_not_pm", "국무총리비서실장", "홍길동", OPS, None, "senior_bureaucrat", "senior"),
    ("pm_office_bureau_not_pm", "국무총리실국정운영실장", "홍길동", OPS, None, "senior_bureaucrat", "senior"),
    ("pm_office_policy_officer", "국무총리실공직복무관리관", "홍길동", OPS, None, "mid_bureaucrat", "mid"),
    ("pm_office_secretary", "국무총리비서실정무수석비서관", "홍길동", OPS, None, "other_official", "other_official.kw"),
    ("pm_nominee", "국무총리후보자", "홍길동", C(cls="특별위원회"), None, "nominee", "nom.other"),
    ("foreign_deputy_pm", "우즈베키스탄부총리", "X", STAND, None, "other", "fallback.other"),
    # ---------------- ministers
    ("minister", "국방부장관", "이종섭", C(comm="국방위원회"), None, "minister", "exec.minister"),
    ("minister_hanja", "國防部長官", "金東信", STAND16, None, "minister", "exec.minister"),
    ("minister_hanja_fused_name", "環境部長官金明子", None, STAND16, None, "minister", "exec.minister"),
    ("minister_dual_office_memid", "행정안전부장관", "홍길동", C(term=21), 12, "minister",
     "memid.dual_office.exec.minister"),
    ("deputy_pm_minister", "부총리겸기획재정부장관", "홍길동", STAND, None, "minister", "exec.minister"),
    ("deputy_pm_bare", "부총리", "홍길동", STAND, None, "minister", "exec.minister"),
    ("minister_without_portfolio", "특임장관", "홍길동", STAND, None, "minister", "exec.minister"),
    ("minister_aide_not_minister", "과학기술정보통신부장관정책보좌관", "홍길동", STAND, None, "other_official",
     "other_official.kw"),
    ("market_corp_not_minister", "구리농수산물도매시장관리공사전무이사", "홍길동", STAND, None, "org_head", "v4."),
    ("minister_acting_jikmudaehaeng", "국방부장관직무대행", "홍길동", STAND, None, "minister_acting",
     "exec.minister.acting"),
    ("minister_acting_jikmudaeri", "국방부장관직무대리", "홍길동", STAND, None, "minister_acting", "exec.minister.acting"),
    ("minister_acting_gwonhan", "행정안전부장관권한대행", "홍길동", STAND, None, "minister_acting", "exec.minister.acting"),
    ("minister_nominee", "국방부장관후보자", "김용현", CONFIRM, None, "minister_nominee", "nom.minister"),
    ("minister_nominee_naejeongja", "통일부장관내정자", "홍길동", CONFIRM, None, "minister_nominee", "nom.minister"),
    ("minister_nominee_memid", "국토교통부장관후보자", "홍길동", C(term=21, cls="특별위원회"), 3, "minister_nominee",
     "memid.dual_office.nom.minister"),
    # ---------------- nominees
    ("nominee_justice", "대법관후보자", "노정희", CONFIRM, None, "nominee", "nom.other"),
    ("nominee_const_court_head", "헌법재판소장후보자", "홍길동", CONFIRM, None, "nominee", "nom.other"),
    ("nominee_audit_head", "감사원장후보자", "홍길동", CONFIRM, None, "nominee", "nom.other"),
    ("nominee_prep_team_not_nominee", "한국방송공사사장후보자(박장범)인사청문준비단장", "홍길동", STAND, None,
     "broadcasting", "broadcast"),
    # ---------------- vice ministers
    ("vice_minister", "국방부차관", "홍길동", STAND, None, "vice_minister", "exec.vice_minister"),
    ("vice_minister_first", "기획재정부제1차관", "홍길동", STAND, None, "vice_minister", "exec.vice_minister"),
    ("assistant_minister", "해양수산부차관보", "홍길동", STAND, None, "senior_bureaucrat", "exec.assistant_minister"),
    # ---------------- hearing roles
    ("witness", "증인", "홍길동", STAND, None, "witness", "hear.witness"),
    ("witness_deputy", "증인(홍길동)대리", "김철수", STAND, None, "witness", "hear.witness"),
    ("witness_hanja", "證人", "金哲洙", STAND16, None, "witness", "hear.witness"),
    ("testifier", "진술인", "홍길동", STAND, None, "testifier", "hear.testifier"),
    ("expert_witness", "참고인", "홍길동", STAND, None, "expert_witness", "hear.expert"),
    ("expert_witness_hanja", "參考人", "金哲洙", STAND16, None, "expert_witness", "hear.expert"),
    ("appraiser", "감정인", "홍길동", STAND, None, "expert_witness", "hear.expert"),
    # ---------------- agency heads, prosecutors, courts
    ("agency_head", "국세청장", "홍길동", STAND, None, "agency_head", "agency.head"),
    ("police_agency_head", "경찰청장", "홍길동", STAND, None, "agency_head", "agency.head"),
    ("regional_police_head", "서울지방경찰청장", "홍길동", AUDIT, None, "agency_head", "agency.head"),
    ("prosecutor_office_head", "서울중앙지방검찰청검사장", "홍길동", AUDIT, None, "agency_head",
     "judicial.prosecution_office"),
    ("prosecutor_high_office_head", "서울고등검찰청검사장", "홍길동", AUDIT, None, "agency_head",
     "judicial.prosecution_office"),
    ("court_president", "서울중앙지방법원장", "홍길동", AUDIT, None, "other_official", "judicial.court"),
    ("high_court_president", "서울고등법원장", "홍길동", AUDIT, None, "other_official", "judicial.court"),
    ("prosecutor", "법무부검찰국공공형사과검사", "홍길동", STAND, None, "other_official", "tail.gov_officer"),
    # ---------------- bureaucrats
    ("senior_budget_office", "기획재정부예산실장", "홍길동", STAND, None, "senior_bureaucrat", "senior"),
    ("senior_bureau_director", "행정안전부지방재정국장", "홍길동", STAND, None, "senior_bureaucrat", "senior"),
    ("senior_bok_governor", "한국은행총재", "홍길동", STAND, None, "senior_bureaucrat", "senior"),
    ("senior_ambassador", "주미국대사", "홍길동", STAND, None, "senior_bureaucrat", "senior"),
    ("senior_customs", "인천세관장", "홍길동", AUDIT, None, "senior_bureaucrat", "senior.customs_prison"),
    ("senior_prison", "서울구치소장", "홍길동", AUDIT, None, "senior_bureaucrat", "senior.customs_prison"),
    ("mid_policy_officer", "국방부획득정책관", "홍길동", STAND, None, "mid_bureaucrat", "mid"),
    ("mid_legal_officer", "국방부법무관리관", "홍길동", STAND, None, "mid_bureaucrat", "mid"),
    ("mid_labor_coop_v5", "고용노동부노사협력관", "홍길동", STAND, None, "mid_bureaucrat", "v5."),
    ("other_official_gov_officer", "문화체육관광부콘텐츠미디어산업관", "홍길동", STAND, None, "other_official",
     "tail.gov_officer"),
    ("other_official_division", "기획재정부국제조세제도과", "홍길동", STAND, None, "other_official", "tail.gov_officer"),
    ("other_official_hanja_officer", "外交通商部企劃豫算擔當官", "韓秉吉", STAND16, None, "other_official",
     "tail.gov_officer"),
    ("other_official_spokesman", "청와대대변인", "홍길동", OPS, None, "other_official", "other_official.kw"),
    ("president", "대통령", "홍길동", PLEN, None, "other_official", "exec.president"),
    ("transition_committee", "제18대대통령직인수위원회위원", "홍길동", OPS, None, "other_official",
     "inst.transition_committee"),
    # ---------------- local government
    ("mayor", "서울특별시장", "홍길동", AUDIT, None, "local_gov_head", "local.head"),
    ("governor", "경기도지사", "홍길동", AUDIT, None, "local_gov_head", "local.head"),
    ("vice_governor", "강원도정무부지사", "홍길동", AUDIT, None, "local_gov_head", "local.head"),
    ("district_head", "서울특별시중구청장", "홍길동", AUDIT, None, "local_gov_head", "local.head"),
    ("county_head", "양평군수", "홍길동", AUDIT, None, "local_gov_head", "local.head"),
    ("superintendent", "서울특별시교육감", "홍길동", AUDIT, None, "local_gov_head", "local.education"),
    ("superintendent_2", "경기도교육감", "홍길동", AUDIT, None, "local_gov_head", "local.education"),
    ("education_office_head", "경기도광명교육청교육장직무대리", "홍길동", AUDIT, None, "local_gov_head", "local.education"),
    # ---------------- military, police
    ("jcs_chair", "합동참모의장", "홍길동", C(comm="국방위원회"), None, "military", "military"),
    ("army_chief", "육군참모총장", "홍길동", C(comm="국방위원회"), None, "military", "military"),
    ("army_legal_office", "육군본부법무실장", "홍길동", C(comm="국방위원회"), None, "military", "military"),
    ("security_command", "국군기무사령관", "홍길동", C(comm="국방위원회"), None, "military", "military"),
    ("academy_head", "공군사관학교장", "홍길동", C(comm="국방위원회"), None, "military", "military"),
    ("army_unit_head", "국군복지단장", "홍길동", C(comm="국방위원회"), None, "military", "military"),
    ("police_bureau", "경찰청수사국장", "홍길동", STAND, None, "police", "police"),
    ("police_station", "종로경찰서장", "홍길동", AUDIT, None, "police", "police"),
    ("fire_station", "강남소방서장", "홍길동", AUDIT, None, "police", "police"),
    # ---------------- financial, audit, election, court, assembly
    ("fss_governor", "금융감독원장", "홍길동", C(comm="정무위원회"), None, "financial_regulator", "financial"),
    ("mpc_member", "금융통화위원", "홍길동", C(comm="기획재정위원회"), None, "financial_regulator", "financial"),
    ("fsc_staff", "금융위원회사무처장", "홍길동", C(comm="정무위원회"), None, "financial_regulator", "financial"),
    ("audit_head", "감사원장", "홍길동", C(comm="법제사법위원회"), None, "audit_official", "inst.audit"),
    ("audit_secretary_general", "감사원사무총장", "홍길동", STAND, None, "audit_official", "inst.audit"),
    ("audit_commissioner", "감사원감사위원", "홍길동", STAND, None, "audit_official", "inst.audit"),
    ("corp_internal_auditor", "한국토지주택공사상임감사위원", "홍길동", AUDIT, None, "org_head", "org.internal_auditor"),
    ("nec_secretary_general", "중앙선거관리위원회사무총장", "홍길동", OPS, None, "election_official", "inst.election"),
    ("nec_chair", "중앙선거관리위원장", "홍길동", OPS, None, "election_official", "inst.election"),
    ("ccourt_secretariat", "헌법재판소사무처장", "홍길동", STAND, None, "constitutional_court", "inst.constitutional_court"),
    ("ccourt_head", "헌법재판소장", "홍길동", STAND, None, "constitutional_court", "inst.constitutional_court"),
    ("assembly_sg", "국회사무총장", "홍길동", OPS, None, "assembly_official", "inst.assembly"),
    ("assembly_sg_bare_plenary", "사무총장", "홍길동", PLEN, None, "assembly_official", "inst.assembly_bare"),
    ("assembly_sg_bare_ops", "사무총장", "홍길동", OPS, None, "assembly_official", "inst.assembly_bare"),
    ("sg_bare_other_committee", "사무총장", "홍길동", STAND, None, "other_official", "other_official.kw"),
    ("assembly_library", "국회도서관장", "홍길동", OPS, None, "assembly_official", "inst.assembly"),
    ("assembly_nabo_bare_ops", "예산정책처장", "홍길동", OPS, None, "assembly_official", "inst.assembly_bare"),
    ("assembly_futures", "국회미래연구원장", "홍길동", OPS, None, "assembly_official", "inst.assembly"),
    ("assembly_clerk_plenary", "의사국장", "홍길동", PLEN, None, "assembly_official", "inst.assembly_bare"),
    # ---------------- corporations and organisations
    ("public_corp_president", "한국전력공사사장", "홍길동", AUDIT, None, "public_corp_head", "corp.head"),
    ("state_bank", "중소기업은행장", "홍길동", AUDIT, None, "public_corp_head", "corp.head"),
    ("pension_service_chair", "국민연금공단이사장", "홍길동", AUDIT, None, "public_corp_head", "corp.board_chair.public"),
    ("guarantee_fund_chair", "신용보증기금이사장", "홍길동", AUDIT, None, "public_corp_head", "corp.board_chair.public"),
    ("foundation_chair", "한국연구재단이사장", "홍길동", AUDIT, None, "org_head", "corp.board_chair.other"),
    ("mutual_aid_chair", "군인공제회이사장", "홍길동", AUDIT, None, "org_head", "corp.board_chair.other"),
    ("mbc_owner_chair", "방송문화진흥회이사장", "홍길동", AUDIT, None, "org_head", "corp.board_chair.other"),
    ("red_cross", "대한적십자사회장", "홍길동", AUDIT, None, "org_head", "org.head"),
    ("consumer_agency", "한국소비자원장", "홍길동", AUDIT, None, "org_head", "org.head"),
    ("corp_executive_sangmu", "건강보험심사평가원심사상무", "홍길동", AUDIT, None, "org_head", "tail.corp_executive"),
    ("corp_director_v4", "한국도로공사기획이사", "홍길동", AUDIT, None, "org_head", "v4."),
    ("research_head_kdi", "한국개발연구원장", "홍길동", AUDIT, None, "research_head", "research.head"),
    ("research_head_science", "국립환경과학원장", "홍길동", AUDIT, None, "research_head", "research.head"),
    ("research_head_lab", "농촌경제연구소장", "홍길동", AUDIT, None, "research_head", "research.head"),
    ("museum_head", "국립중앙박물관장", "홍길동", AUDIT, None, "cultural_institution_head", "culture.head"),
    ("gallery_head", "국립현대미술관장", "홍길동", AUDIT, None, "cultural_institution_head", "culture.head"),
    ("library_head", "국립중앙도서관장", "홍길동", AUDIT, None, "cultural_institution_head", "culture.head"),
    ("culture_center_acting", "국립아시아문화전당장직무대리", "홍길동", AUDIT, None, "cultural_institution_head",
     "culture.head"),
    ("theatre_hanja", "國立中央劇場長", "金明坤", STAND16, None, "cultural_institution_head", "culture.head"),
    ("broadcast_commission_member", "방송위원회상임위원", "홍길동", AUDIT, None, "broadcasting", "broadcast"),
    ("broadcast_commission_sg", "방송위원회사무총장", "홍길동", AUDIT, None, "broadcasting", "broadcast"),
    ("kbs_president", "한국방송공사사장", "홍길동", AUDIT, None, "public_corp_head", "corp.head"),
    ("coop_ceo", "농업협동조합중앙회대표이사", "홍길동", AUDIT, None, "cooperative_head", "coop"),
    ("coop_executive", "수산업협동조합중앙회상무", "홍길동", AUDIT, None, "cooperative_head", "coop"),
    ("coop_chair_org_head", "농협중앙회장", "홍길동", AUDIT, None, "org_head", "org.head"),
    ("coop_senior_post", "농업협동조합중앙회기획실장", "홍길동", AUDIT, None, "senior_bureaucrat", "senior"),
    ("private_company", "㈜강원랜드대표이사", "홍길동", AUDIT, None, "private_sector", "private.company"),
    ("private_company_2", "주식회사강원랜드사장직무권한대행", "홍길동", AUDIT, None, "private_sector", "private.company"),
    ("foundation_director_v4_first", "(재)한국에너지정보문화재단상임이사", "홍길동", AUDIT, None, "org_head", "v4."),
    ("private_association_v5", "(사)한국장애인단체총연맹", "홍길동", AUDIT, None, "private_sector", "v5."),
    ("public_corp_deputy_chair", "한국증권거래소부이사장보", "홍길동", AUDIT, None, "public_corp_head",
     "corp.board_chair.public"),
    ("pension_corp_auditor_not_official", "공무원연금공단감사", "홍길동", AUDIT, None, "org_head", "v4."),
    ("university_named_broadcast", "한국방송통신대학교총장", "홍길동", AUDIT, None, "other_official", "other_official.kw"),
    ("ministry_division_with_broadcast", "정보통신부전파방송관리국전파감리과장", "홍길동", STAND, None, "other_official",
     "other_official.kw"),
    ("broadcast_body_staff", "방송통신위원회기획조정관", "홍길동", STAND, None, "broadcasting", "broadcast"),
    ("art_director", "예술감독", "홍길동", AUDIT, None, "private_sector", "private.v5"),
    # ---------------- printed typos and unit titles (HWP / XML)
    ("typo_member_name_first", "김동성", "위윈", C(term=18), None, "legislator", "leg.member"),
    ("typo_pm", "국모총리", "김황식", PLEN18, None, "prime_minister", "exec.prime_minister"),
    ("typo_testifier", "진실인", "우희종", STAND, None, "testifier", "hear.testifier"),
    ("typo_agency_head_suffix", "조달청정", "장수만", AUDIT, None, "agency_head", "agency.head"),
    ("typo_office_head_suffix", "기획재정부예산실잘", "이용걸", AUDIT, None, "senior_bureaucrat", "senior"),
    ("tax_office_head", "예산세무서장", "김진현", AUDIT, None, "senior_bureaucrat", "senior.customs_prison"),
    ("unit_officer_damdang", "기획재정부국고과공공자금관리기금담당", "채현호", AUDIT, None, "other_official",
     "tail.gov_officer"),
    ("unit_team", "농림수산식품부동물방역팀", "김정주", AUDIT, None, "other_official", "tail.gov_officer"),
    ("unit_clerk_head", "국무총리실정보화계장", "이해정", AUDIT, None, "other_official", "tail.gov_officer"),
    ("vice_consul_hanja", "副領事", "金某", C(term=16, cls="국정감사", comm="통일외교통상위원회"), None,
     "other_official", "diplomatic_staff"),
    ("director_grade_hanja", "韓國輸出入銀行理事待遇", "金某", C(term=16, cls="국정감사", comm="재정경제위원회"), None,
     "org_head", "tail.corp_executive"),
    ("typo_agency_head_jeongchang", "새만금ㆍ군산경제자유구역정창", "홍길동", AUDIT, None, "agency_head", "agency.head"),
    ("rda_research_institute", "농촌진흥청농업과학기술원장", "홍길동", AUDIT, None, "research_head", "research.head"),
    ("research_admin_grade", "국가보안기술연구소책임행정원", "홍길동", AUDIT, None, "other_official",
     "tail.research_staff_grade"),
    ("bok_branch_head", "韓國銀行大田支店長", "金某", C(term=16, cls="국정감사", comm="재정경제위원회"), None,
     "other_official", "tail.bok_branch"),
    ("typo_deputy_secretary_general", "사무차창", "홍길동", OPS, None, "assembly_official", "inst.assembly_bare"),
    # ---------------- fixes found in the 300-string hand audit
    ("na_special_name_with_gyeom_verb", "헌법재판소재판관후보자를겸하는헌법재판소장(이강국)임명동의에관한인사청문특별위원장대리",
     "홍길동", PLEN, None, "legislator", "leg.chair.named."),
    ("na_confirmation_committee_pattern", "대법관(이기택)임명동의에관한인사청문특별위원장", "강기원", PLEN, None, "legislator",
     "leg.chair.named.na_pattern.reporting_plenary"),
    ("union_chair", "전국언론노동조합위원장", "홍길동", C(comm="과학기술정보방송통신위원회"), None, "org_head", "org.union"),
    ("union_committee_chair", "전국교직원노동조합유치원위원장", "홍길동", C(comm="교육위원회"), None, "org_head", "org.union"),
    ("ministry_union_division_not_union", "노동부노사정책국노동조합과장", "홍길동", C(comm="환경노동위원회"), None,
     "other_official", "other_official.kw"),
    ("science_museum", "국립광주과학관장직무대행", "홍길동", AUDIT, None, "cultural_institution_head", "culture.head"),
    ("typo_reference_witness", "참조인", "홍길동", STAND, None, "expert_witness", "hear.expert"),
    ("embassy_junior", "주영국대한민국대사관3등서기관", "홍길동", AUDIT, None, "other_official", "diplomatic_staff"),
    ("embassy_clerk", "주모로코왕국대한민국대사관실무관", "홍길동", AUDIT, None, "other_official", "diplomatic_staff"),
    ("consulate_consul", "주로스앤젤레스대한민국총영사관영사", "홍길동", AUDIT, None, "other_official", "diplomatic_staff"),
    ("embassy_minister_stays_senior", "주일본국대한민국대사관공사", "홍길동", AUDIT, None, "senior_bureaucrat", "senior"),
    ("ambassador", "주짐바브웨공화국대한민국대사", "홍길동", AUDIT, None, "senior_bureaucrat", "senior"),
    ("consul_general", "주뉴욕대한민국총영사관총영사", "홍길동", AUDIT, None, "senior_bureaucrat", "senior"),
    ("defence_attache", "주아르헨티나공화국대한민국대사관국방무관", "홍길동", AUDIT, None, "military", "military"),
    ("hwp_label_text_fused_chair", "위원장 김영선 발언권 드리기 전에 분위기를……", None, STAND, None, "chair",
     "leg.chair.committee"),
    ("hwp_label_text_fused_corp", "한국토지공사사장 이종상", "……", AUDIT, None, "public_corp_head", "corp.head"),
    # ---------------- other / unknown
    ("defense_counsel_hanja", "辯護人", "金永晩", STAND16, None, "other", "fallback.other"),
    ("legislator_aide", "최민희의원비서", "이정아", STAND, None, "other", "fallback.other"),
    ("bare_name_not_in_roster", None, "강기원", STAND, None, "other", "bare_name.unmatched"),
    ("bare_name_no_term", None, "강기원", NOCTX, None, "other", "bare_name.unchecked"),
    ("empty_label", None, None, STAND, None, "unknown", "empty_label"),
]

assert len(CASES) >= 150, len(CASES)


@pytest.mark.parametrize("cid,pos,name,ctx,mem_id,role,rule", CASES, ids=[c[0] for c in CASES])
def test_case(cid, pos, name, ctx, mem_id, role, rule):
    r = run(pos, name, ctx, mem_id)
    assert r.role == role, (cid, r)
    assert r.role_group == roles.role_group(role)
    if rule:
        assert r.role_rule.startswith(rule), (cid, r.role_rule)


def test_every_role_covered():
    covered = {c[5] for c in CASES}
    missing = set(roles.ALL_ROLES) - covered
    assert not missing, missing
    assert len(roles.ALL_ROLES) == 34  # 33 v9 roles + 'unknown'


def test_role_groups():
    assert roles.role_group("chair") == "legislator"
    assert roles.role_group("legislator") == "legislator"
    assert roles.role_group("minister") == "nonlegislator"
    for r in ("committee_staff", "other", "unknown"):
        assert roles.role_group(r) == "excluded"


# --------------------------------------------------------------------------- person_title

@pytest.mark.parametrize("pos,ctx,expected", [
    ("위원장대리", STAND, "대리"),
    ("위원장직무대행", STAND, "직무대행"),
    ("위원장직무대리", STAND, "직무대리"),
    ("반장", AUDIT, "반장"),
    ("반장대리", AUDIT, "반장대리"),
    ("반장직무대행", AUDIT, "반장직무대행"),
    ("국방부장관직무대행", STAND, "직무대행"),
    ("위원장", STAND, None),
    ("위원", STAND, None),
])
def test_person_title(pos, ctx, expected):
    assert run(pos, "홍길동", ctx).person_title == expected


def test_title_raw_keeps_printed_title_and_affiliation_is_institution():
    r = run("國防部長官", "金東信", STAND16)
    assert r.title_raw == "國防部長官" and r.pos_hangul == "국방부장관"
    assert r.affiliation_raw == "國防部"


# review finding F3: affiliation_raw = institution printed in the title (v9 semantics for the
# executive roles: '국방부장관 김관진' -> '국방부'); the printed title is title_raw
@pytest.mark.parametrize("title,aff", [
    ("국방부장관", "국방부"), ("국방부장관직무대행", "국방부"), ("국방부장관후보자", "국방부"),
    ("기획재정부제1차관", "기획재정부"), ("해양수산부차관보", "해양수산부"), ("부총리겸기획재정부장관", "기획재정부"),
    ("경찰청장", "경찰청"), ("국정홍보처장", "국정홍보처"), ("감사원장", "감사원"), ("금융감독원부원장", "금융감독원"),
    ("한국전력공사사장", "한국전력공사"), ("국민연금공단이사장", "국민연금공단"), ("한국은행총재", "한국은행"),
    ("한국마사회장", "한국마사회"), ("대한적십자사회장", "대한적십자사"), ("중소기업은행장", "중소기업은행"),
    ("서울특별시장", "서울특별시"), ("경기도지사", "경기도"), ("강원도정무부지사", "강원도"), ("양평군수", "양평군"),
    ("서울특별시교육감", "서울특별시"), ("서울중앙지방검찰청검사장", "서울중앙지방검찰청"),
    ("주일본국대한민국대사관공사", "주일본국대한민국대사관"), ("주미국대사", "주미국"),
    ("주영국대한민국대사관3등서기관", "주영국대한민국대사관"), ("공군사관학교장", "공군사관학교"),
    ("육군참모총장", "육군"), ("합동참모의장", "합동참모"), ("국회사무총장", "국회"),
    ("방송위원회상임위원", "방송위원회"), ("법제사법위원회수석전문위원", "법제사법위원회"),
    ("공정거래위원회위원장", "공정거래위원회"), ("국방부획득정책관", "국방부획득"),
    ("기획재정부국제조세제도과", "기획재정부국제조세제도과"),
    ("위원", None), ("위원장", None), ("위원장대리", None), ("소위원장", None), ("증인", None), ("참고인", None),
    ("증인(홍길동)대리", None), ("국무총리", None), ("전문위원", None), ("의장", None), (None, None),
])
def test_affiliation_from_title(title, aff):
    assert roles.affiliation_from_title(title) == aff


# review finding F1: same-term special-committee prefix match is refuted by the roster
def test_same_term_special_refuted_by_roster():
    by_term = {20: frozenset({roles._norm_key("가습기살균제사고진상규명과피해구제및재발방지대책마련을위한국정조사특별위원회")}),
               21: frozenset()}
    pos = "가습기살균제사건과4ㆍ16세월호참사특별조사위원장"
    for t in (20, 21):
        r = roles.classify(pos, "장완익", term=t, class_name="국정감사", hearing_type="국정감사", is_subcommittee=False,
                           committee_raw="정무위원회", roster=ROSTER, na_committees=NA_COMM, na_committees_by_term=by_term)
        assert r.role == "independent_official", (t, r)
    r = roles.classify(pos, "장완익", term=20, class_name="국정감사", hearing_type="국정감사", is_subcommittee=False,
                       committee_raw="정무위원회", roster=ROSTER, na_committees=NA_COMM, na_committees_by_term=by_term)
    assert r.role_rule.startswith("leg.chair.named.na_special_same_term.name_not_in_roster>")
    # a member of the term keeps the NA reading
    r = run("과거사진상조사특별위원장대리", "김철수", C(term=16, comm="법제사법위원회"))
    assert r.role == "legislator"


# review finding F4: label_raw completes a data-pos that holds only part of the label
@pytest.mark.parametrize("pos,label,name", [
    ("薛", "薛 勳議員", "薛勳"), ("薛", "薛 勳委員", "薛勳"), ("南宮", "南宮 晳議員", "南宮晳"),
    ("尹景湜", "尹景湜 議員", "尹景湜"),
])
def test_label_raw_completes_split_hanja_name(pos, label, name):
    roster = {16: frozenset({"薛勳", "설훈", "南宮晳", "남궁석", "尹景湜"})}
    r = roles.classify(pos, None, label_raw=label, term=16, class_name="국회본회의", hearing_type="국회본회의",
                       committee_raw="국회본회의", is_subcommittee=False, roster=roster, na_committees=NA_COMM,
                       na_committees_by_term=NA_BY_TERM)
    assert r.role == "legislator" and r.name == name, r
    assert "label_raw_extra" in r.pos_fix


def test_label_raw_adds_missing_name_to_title():
    r = roles.classify("韓國輸出保險公社社長", None, label_raw="韓國輸出保險公社社長 李英雨", term=16, roster=ROSTER,
                       na_committees=NA_COMM, na_committees_by_term=NA_BY_TERM)
    assert r.role == "public_corp_head" and r.name == "李英雨"


# review finding F5: title typos and truncations
@pytest.mark.parametrize("pos,name,label,ctx,role,name_out", [
    ("李海鳳委員;", None, None, STAND16, "legislator", "李海鳳"),
    ("安商守委員;", None, None, STAND16, "legislator", "安商守"),
    ("李在禎委原", None, None, STAND16, "legislator", "李在禎"),
    ("金晟祚委員李忠馥", None, None, STAND16, "legislator", "金晟祚"),
    ("財政經濟委員會長代理", "安澤秀", None, PLEN16, "legislator", "安澤秀"),
    ("産業資源委員長代理", "李", None, C(term=16, comm="국방위원회"), "legislator", "李"),
    ("위원원장대리", "류근찬", None, C(term=18), "chair", "류근찬"),
    ("소위원쟝", "박기춘", None, C(term=18, sub=True, subc="법안심사소위원회"), "chair", "박기춘"),
    ("소위원님", "이혜훈", None, C(term=18, sub=True, subc="법안심사소위원회"), "chair", "이혜훈"),
    ("소위원회", "정해걸", None, C(term=18, sub=True, subc="법안심사소위원회"), "chair", "정해걸"),
    ("우제창·위원", None, None, C(term=18), "legislator", "우제창"),
    ("강기정", "議위원", None, C(term=18), "legislator", "강기정"),
])
def test_title_typos(pos, name, label, ctx, role, name_out):
    na = NA_COMM | {roles._norm_key("재정경제위원회"), roles._norm_key("산업자원위원회")}
    r = roles.classify(pos, name, label_raw=label, roster=ROSTER, na_committees=na, na_committees_by_term=NA_BY_TERM,
                       **ctx)
    assert r.role == role and r.name == name_out, r


def test_truncated_member_title_needs_roster():
    roster = {16: frozenset({"李漢久", "이한구"})}
    r = roles.classify("李漢久委", None, term=16, roster=roster, na_committees=NA_COMM, na_committees_by_term=NA_BY_TERM)
    assert r.role == "legislator" and r.name == "李漢久" and "title_truncated" in r.pos_fix
    r = roles.classify("金某某委", None, term=16, roster=roster, na_committees=NA_COMM, na_committees_by_term=NA_BY_TERM)
    assert (r.role, r.role_rule) == ("other", "title_truncated.name_not_in_roster")


def test_one_character_name_is_not_refuted():
    assert roles.in_roster("李", 16, ROSTER) is None
    assert roles.in_roster("", 16, ROSTER) is None


# review finding F6: a viewer mem_id does not turn a printed non-legislator title into a legislator
@pytest.mark.parametrize("pos,role", [
    ("증인", "witness"), ("참고인", "expert_witness"), ("경찰청장", "agency_head"), ("전문위원", "committee_staff"),
    ("진술인", "testifier"),
])
def test_memid_keeps_printed_nonlegislator_title(pos, role):
    r = run(pos, "홍길동", C(term=19, comm="기획재정위원회"), mem_id=12345)
    assert r.role == role and r.role_rule.startswith("memid.title_conflict."), r


# review finding F7: government committee member titles and unit names are not split into names
@pytest.mark.parametrize("pos", ["민간위원", "공익위원", "근로자위원", "사용자위원", "비상근위원", "정부위원", "명예위원",
                                 "외부위원"])
def test_government_committee_member_titles_not_legislators(pos):
    r = run(pos, None, C(term=19))
    assert r.role != "legislator" and "fused_name_first" not in r.pos_fix, r


@pytest.mark.parametrize("pos", ["대통령비서실", "감사원사무처"])
def test_unit_name_not_split_into_title_and_name(pos):
    r = run(pos, None, C(term=19))
    assert r.name is None and "fused_name" not in r.pos_fix, r


def test_fused_title_name_still_split():
    r = run("環境部長官金明子", None, STAND16)
    assert r.role == "minister" and r.name == "金明子"
    r = run("수석전문위원姜長錫", None, STAND16)
    assert r.role == "committee_staff" and r.name == "姜長錫"


# review finding F8: word-internal initial-sound rule and surname readings
@pytest.mark.parametrize("raw,hangul", [
    ("瀋陽總領事", "심양총영사"), ("副領事", "부영사"), ("韓國輸出入銀行理事待遇", "한국수출입은행이사대우"),
    ("私立學校敎職員年金管理公團理事長", "사립학교교직원연금관리공단이사장"), ("勞動部勞使協力官室", "노동부노사협력관실"),
    ("管理事務所長", "관리사무소장"), ("國務總理", "국무총리"),
])
def test_normalize_title_word_internal(raw, hangul):
    assert roles.normalize_title(raw)[0] == hangul


def test_hanja_name_readings_surnames():
    assert "김원길" in roles.hanja_name_readings("金元吉")
    assert {"유근찬", "류근찬"} <= roles.hanja_name_readings("柳根燦")
    assert roles.in_roster("金元吉", 16, {16: frozenset({"김원길"})}) is True
    assert roles.in_roster("柳根燦", 18, {18: frozenset({"류근찬"})}) is True


# review finding F10: reference-file failures are recorded and warned
def test_loader_failure_is_recorded(monkeypatch):
    monkeypatch.setattr(roles, "ROSTER_PATH", "/nonexistent/roster.parquet")
    monkeypatch.setattr(roles, "PERSON_TERMS_PATH", "/nonexistent/pt.parquet")
    monkeypatch.setattr(roles, "UNIVERSE_PATH", "/nonexistent/universe.parquet")
    for f in (roles.default_roster, roles.default_na_committees, roles.default_na_committees_by_term):
        f.cache_clear()
    try:
        with pytest.warns(RuntimeWarning):
            assert roles.default_roster() == {}
        with pytest.warns(RuntimeWarning):
            roles.default_na_committees()
        with pytest.warns(RuntimeWarning):
            assert roles.default_na_committees_by_term() == {}
        st = roles.load_status()
        for k in ("roster_members_term", "roster_person_terms", "na_committees_universe", "na_committees_by_term"):
            assert st[k]["ok"] is False and st[k]["error"]
    finally:
        for f in (roles.default_roster, roles.default_na_committees, roles.default_na_committees_by_term):
            f.cache_clear()


# review finding F12: public classify() input normalisation
def test_classify_input_normalisation():
    import numpy as np
    assert run("증인", "홍길동", C(term=19), mem_id=0.0).role == "witness"
    assert run("증인", "홍길동", C(term=19), mem_id="0.0").role == "witness"
    assert run("위원", "홍길동", C(term=21), mem_id="1234.0").role_rule == "leg.member.memid"
    assert run("소위원장", "홍길동", C(term=19, sub="False")).role == "legislator"
    assert run("소위원장", "홍길동", C(term=19, sub="True")).role == "chair"
    assert run("소위원장", "홍길동", C(term=19, sub=np.bool_(True))).role == "chair"
    assert run("소위원장", "홍길동", C(term=19, sub="maybe")).role_rule.endswith("presiding_unknown_ctx")
    r = run(pd.NA, "홍길동", C(term=19))
    assert r.pos_hangul == "" and r.role_rule.startswith("bare_name")
    assert run(float("nan"), pd.NA, C(term=19)).role == "unknown"


# review finding F13: '겸' split
@pytest.mark.parametrize("pos,role,person_title", [
    ("국방과학연구소민군겸용기술센터장", "other_official", None),
    ("영화진흥위원회부위원장겸직무대행", "independent_official", "직무대행"),
    ("부총리겸교육인적자원부차관", "vice_minister", None),
    ("부총리겸재정경제부장관", "minister", None),
    ("부총리겸교육인적자원부", "minister", None),
])
def test_gyeom_split(pos, role, person_title):
    r = run(pos, "홍길동", STAND)
    assert r.role == role and r.person_title == person_title, r


def test_trailing_gyeom_same_as_without():
    a = run("주브라질연방공화국대한민국대사관2등서기관겸", "홍길동", AUDIT)
    b = run("주브라질연방공화국대한민국대사관2등서기관", "홍길동", AUDIT)
    assert a.role == b.role == "other_official"


# review finding F14: fused two-speaker labels take title and name from the same label
@pytest.mark.parametrize("pos,name,label,role,name_out", [
    ("박형준", "의원◯국무총리", "박형준 의원◯국무총리 이해찬", "prime_minister", "이해찬"),
    ("임해규", "의원◯행정자치부장관", "임해규 의원◯행정자치부장관 박명재", "minister", "박명재"),
    ("申溪輪委員○陳述人", "김영배", "申溪輪委員○陳述人 김영배", "testifier", "김영배"),
    ("國家報勳處長", "李在達○徐相燮委員", "國家報勳處長 李在達○徐相燮委員", "legislator", "徐相燮"),
])
def test_two_speaker_labels(pos, name, label, role, name_out):
    r = run(pos, name, C(term=17), label_raw=label)
    assert r.role == role and r.name == name_out and "two_speakers" in r.pos_fix, r


def test_upstream_fused_label_split_is_redone():
    r = run("副總理兼財政經濟部長", "官陳稔", PLEN16, label_raw="副總理兼財政經濟部長官陳稔")
    assert r.role == "minister" and r.name == "陳稔" and "resplit_fused_label" in r.pos_fix, r
    r = run("環境部長官", "金明子", STAND16, label_raw="環境部長官金明子")
    assert r.role == "minister" and r.name == "金明子" and "resplit" not in r.pos_fix
    r = run("保健福祉部次官", "張錫準", STAND16, label_raw="保健福祉部次官張錫準")
    assert r.role == "vice_minister" and r.name == "張錫準" and "resplit" not in r.pos_fix


def test_title_with_space_in_label():
    r = run("서울", "高等檢察廳檢事長鄭鎭圭", C(term=17, cls="국정감사", comm="법제사법위원회"),
            label_raw="서울 高等檢察廳檢事長 鄭鎭圭")
    assert r.role == "agency_head" and r.name == "鄭鎭圭" and "label_title_with_space" in r.pos_fix, r
    r = run("위원장", "홍길동", STAND, label_raw="위원장 홍길동")
    assert r.role == "chair" and r.pos_fix == "none"


def test_time_stamp_before_marker_is_junk():
    r = run("10시40분)◯議長", "朴寬用", PLEN16, label_raw="10시40분)◯議長 朴寬用")
    assert r.role == "chair" and "two_speakers" not in r.pos_fix and "lead_junk" in r.pos_fix


# review finding F15: bare 위원장 in a subcommittee meeting is marked
def test_bare_chair_in_subcommittee_marked():
    r = run("위원장", "최영희", SUB)
    assert r.role == "chair" and r.role_rule == "leg.chair.committee.in_subcommittee"


# --------------------------------------------------------------------------- label parsing

@pytest.mark.parametrize("label,pos,name", [
    ("홍길동 위원", "위원", "홍길동"),
    ("위원장 홍길동", "위원장", "홍길동"),
    ("국방부장관 이종섭", "국방부장관", "이종섭"),
    ("이수진(비) 위원", "위원", "이수진(비)"),
    ("소위원장 이한구", "소위원장", "이한구"),
    ("국무총리", "국무총리", None),
    ("홍길동", None, "홍길동"),
    ("◯위원장 홍길동", "위원장", "홍길동"),
    ("", None, None),
])
def test_split_label(label, pos, name):
    assert roles.split_label(label) == (pos, name)


@pytest.mark.parametrize("raw,hangul", [
    ("委員", "위원"), ("委員長", "위원장"), ("議長", "의장"), ("副議長", "부의장"),
    ("國務總理", "국무총리"), ("專門委員", "전문위원"), ("理事長", "이사장"), ("勞動部長官", "노동부장관"),
    ("國防部 長官", "국방부장관"), ("행정·자치", "행정ㆍ자치"),
])
def test_normalize_title(raw, hangul):
    assert roles.normalize_title(raw)[0] == hangul


def test_label_raw_fallback():
    r = roles.classify(None, None, label_raw="홍길동 위원", term=19, roster=ROSTER, na_committees=NA_COMM,
                       na_committees_by_term=NA_BY_TERM)
    assert r.role == "legislator" and r.name == "홍길동" and "from_label_raw" in r.pos_fix


# --------------------------------------------------------------------------- v9 compat

@pytest.mark.parametrize("pos_h,name,has_mid,expected", [
    ("위원", "홍길동", False, "legislator"),
    ("위원장", "홍길동", False, "chair"),
    ("소위원장", "홍길동", False, "chair"),
    ("국무총리실장", "홍길동", False, "prime_minister"),
    ("서울특별시교육감", "홍길동", False, "independent_official"),
    ("서울중앙지방검찰청검사장", "홍길동", False, "public_corp_head"),
    ("한국연구재단이사장", "홍길동", False, "public_corp_head"),
    ("한국개발연구원장", "홍길동", False, "org_head"),
    ("입법조사관", "홍길동", False, "independent_official"),
    ("방송통신위원장", "홍길동", False, "independent_official"),
    ("반장", "홍길동", True, "legislator"),
])
def test_v9_compat_chain(pos_h, name, has_mid, expected):
    role, src = roles.v9_compat(pos_h, name, has_mid, lookup={})
    assert (role, src) == (expected, "chain")


def test_v9_compat_lookup_wins():
    role, src = roles.v9_compat("위원", "홍길동", False, lookup={("홍길동 위원", False): "chair"})
    assert (role, src) == ("chair", "lookup")


# --------------------------------------------------------------------------- rule table

def test_rule_table_is_data():
    t = roles.rule_table()
    assert {"stage", "order", "rule_id", "role", "pattern", "note", "changes_v9"} <= set(t.columns)
    assert t["rule_id"].is_unique
    nl = t[t["stage"] == "nonlegislator"]
    assert list(nl["order"]) == list(range(1, len(nl) + 1))
    targets = set(t["role"]) - {"@committee_chair", "@subcommittee_chair", "@adjustment_chair", "@speaker",
                                "@named_chair", "@minister", "@assembly_bare", "@coop"}
    assert targets <= set(roles.ALL_ROLES)


# --------------------------------------------------------------------------- enrich

def _turns_meetings():
    turns = pd.DataFrame({
        "conf_num": [2, 1, 1, 2, 1, 3],
        "turn_seq": [1, 1, 2, 2, 3, 1],
        "speaker_pos": ["소위원장", "위원장", "국방부장관", "소위원장", "위원", None],
        "speaker_name": ["이한구", "홍길동", "이종섭", "이한구", "홍길동", None],
        "speaker_mem_id": pd.array([None, None, None, None, 1234, None], dtype="Int64"),
        "speaker_label_raw": ["소위원장 이한구", "위원장 홍길동", "국방부장관 이종섭", "소위원장 이한구", "홍길동 위원", None],
    }, index=[10, 11, 12, 13, 14, 15])
    meetings = pd.DataFrame({
        "conf_num": [1, 2, 3],
        "term": [21, 18, 18],
        "class_name": ["상임위원회", "예산결산특별위원회", "예산결산특별위원회"],
        "hearing_type": ["상임위원회", "예산결산특별위원회", "예산결산특별위원회"],
        "is_subcommittee": [False, False, True],
        "committee_raw": ["국방위원회", "예산결산특별위원회", "예산결산특별위원회"],
        "subcommittee": [None, None, "계수조정소위원회"],
    })
    return turns, meetings


def test_enrich_preserves_rows_order_index():
    turns, meetings = _turns_meetings()
    out = roles.enrich(turns, meetings, roster=ROSTER, na_committees=NA_COMM, v9_lookup={}, na_committees_by_term=NA_BY_TERM)
    assert list(out.index) == list(turns.index)
    assert len(out) == len(turns)
    for c in roles.CONTRACT_COLS:
        assert c in out.columns
    for c in turns.columns:
        pd.testing.assert_series_equal(out[c], turns[c])
    assert list(out["role"]) == ["legislator", "chair", "minister", "legislator", "legislator", "unknown"]
    assert list(out["role_group"]) == ["legislator", "legislator", "nonlegislator", "legislator", "legislator",
                                       "excluded"]
    assert out.loc[14, "role_rule"] == "leg.member.memid"
    assert out["role_v9_compat"].notna().all()


def test_enrich_subcommittee_context():
    turns, meetings = _turns_meetings()
    turns.loc[15, ["speaker_pos", "speaker_name"]] = ["소위원장", "이한구"]
    out = roles.enrich(turns, meetings, roster=ROSTER, na_committees=NA_COMM, v9_lookup={}, na_committees_by_term=NA_BY_TERM)
    assert out.loc[15, "role"] == "chair"
    assert out.loc[10, "role"] == "legislator"


def test_enrich_conflicting_duplicate_meetings_raise():
    turns, meetings = _turns_meetings()
    dup = meetings[meetings.conf_num == 1].assign(is_subcommittee=True)
    with pytest.raises(ValueError):
        roles.enrich(turns, pd.concat([meetings, dup]), roster=ROSTER, na_committees=NA_COMM, v9_lookup={},
                     na_committees_by_term=NA_BY_TERM)
    out = roles.enrich(turns, pd.concat([meetings, meetings[meetings.conf_num == 1]]), roster=ROSTER,
                       na_committees=NA_COMM, v9_lookup={}, na_committees_by_term=NA_BY_TERM)
    assert out.attrs["roles_enrich"]["meetings_exact_duplicate_rows_dropped"] == 1


def test_enrich_does_not_modify_input_and_matches_classify():
    turns, meetings = _turns_meetings()
    before = turns.copy()
    out = roles.enrich(turns, meetings, roster=ROSTER, na_committees=NA_COMM, v9_lookup={}, na_committees_by_term=NA_BY_TERM)
    pd.testing.assert_frame_equal(turns, before)
    m = meetings.set_index("conf_num")
    for idx, t in turns.iterrows():
        c = m.loc[t.conf_num]
        r = roles.classify(t.speaker_pos, t.speaker_name, label_raw=t.speaker_label_raw,
                           mem_id=t.speaker_mem_id if pd.notna(t.speaker_mem_id) else None, term=c.term,
                           class_name=c.class_name, hearing_type=c.hearing_type, is_subcommittee=c.is_subcommittee,
                           committee_raw=c.committee_raw, subcommittee=c.subcommittee, roster=ROSTER,
                           na_committees=NA_COMM, na_committees_by_term=NA_BY_TERM)
        got = tuple(None if pd.isna(v) else v for v in out.loc[idx, ["role", "role_rule", "title_raw", "affiliation_raw"]])
        assert got == (r.role, r.role_rule, r.title_raw, r.affiliation_raw)


# review finding F2: v9_compat for XLSX turns uses the XLSX label and v9's member_id
def test_enrich_v9_compat_uses_source_member_id_and_xlsx_label():
    turns = pd.DataFrame({
        "conf_num": [1, 1, 1],
        "turn_seq": [1, 2, 3],
        "source": ["xlsx", "xlsx", "xml"],
        "speaker_pos": ["반장", "반장", "반장"],
        "speaker_name": ["홍길동", "홍길동", "홍길동"],
        "speaker_mem_id": pd.array([None, None, None], dtype="Int64"),
        "source_member_id": ["123", None, None],
        "speaker_label_raw": ["반장 홍길동", "반장 홍길동", "반장 홍길동"],
    })
    meetings = pd.DataFrame({"conf_num": [1], "term": [18], "class_name": ["국정감사"], "hearing_type": ["국정감사"],
                             "is_subcommittee": [False], "committee_raw": ["행정안전위원회"], "subcommittee": [None]})
    out = roles.enrich(turns, meetings, roster=ROSTER, na_committees=NA_COMM, v9_lookup={}, na_committees_by_term=NA_BY_TERM)
    assert list(out["role_v9_compat"]) == ["legislator", "other", "other"]
    lk = {("반장 홍길동", True): "chair"}
    out = roles.enrich(turns, meetings, roster=ROSTER, na_committees=NA_COMM, v9_lookup=lk, na_committees_by_term=NA_BY_TERM)
    assert list(out["role_v9_compat"]) == ["chair", "other", "other"]
    assert list(out["role_v9_compat_src"]) == ["lookup", "chain", "chain"]


def test_enrich_empty():
    turns, meetings = _turns_meetings()
    out = roles.enrich(turns.iloc[:0], meetings)
    assert len(out) == 0 and set(roles.CONTRACT_COLS) <= set(out.columns)


def test_enrich_meeting_missing_is_not_dropped():
    turns, meetings = _turns_meetings()
    out = roles.enrich(turns, meetings[meetings.conf_num != 2], roster=ROSTER, na_committees=NA_COMM, v9_lookup={},
                       na_committees_by_term=NA_BY_TERM)
    assert len(out) == len(turns)
    # without context 소위원장 falls back to presiding (counted by its rule id)
    assert out.loc[10, "role_rule"].endswith("presiding_unknown_ctx")


# --------------------------------------------------------------------------- default data files

_HAVE_UNIVERSE = os.path.exists(roles.UNIVERSE_PATH)


@pytest.mark.skipif(not _HAVE_UNIVERSE, reason="meeting universe parquet not available")
@pytest.mark.parametrize("pos,comm,expected", [
    ("방송통신위원장", "과학기술정보방송통신위원회", "independent_official"),
    ("공정거래위원장", "정무위원회", "independent_official"),
    ("국가인권위원장", "국회운영위원회", "independent_official"),
    ("금융위원장", "정무위원회", "independent_official"),
    ("정무위원장", "정무위원회", "chair"),
    ("국방위원장", "국방위원회", "chair"),
    ("정보위원장", "정보위원회", "chair"),
    ("과학기술정보방송통신위원장", "과학기술정보방송통신위원회", "chair"),
])
def test_default_committee_list(pos, comm, expected):
    r = roles.classify(pos, "홍길동", term=21, class_name="상임위원회", hearing_type="상임위원회",
                       is_subcommittee=False, committee_raw=comm, roster={})
    assert r.role == expected, r


# --------------------------------------------------------------------------- R2: within-meeting label consistency

def _mk(rows, conf_meta=None):
    """rows: (conf_num, speaker_pos, speaker_name[, label_fused])."""
    t = pd.DataFrame({
        "conf_num": [r[0] for r in rows],
        "turn_seq": list(range(1, len(rows) + 1)),
        "source": "xml",
        "speaker_pos": [r[1] for r in rows],
        "speaker_name": [r[2] for r in rows],
        "speaker_label_raw": [f"{r[1]} {r[2]}" for r in rows],
        "speaker_mem_id": pd.array([None] * len(rows), dtype="Int64"),
        "label_fused": pd.array([r[3] if len(r) > 3 else False for r in rows], dtype="boolean"),
    })
    confs = sorted({r[0] for r in rows})
    m = pd.DataFrame({"conf_num": confs, "term": 17, "class_name": "상임위원회", "hearing_type": "상임위원회",
                      "is_subcommittee": False, "committee_raw": "재정경제위원회", "subcommittee": None})
    if conf_meta:
        for c, v in conf_meta.items():
            m[c] = v
    return t, m


def _enr(t, m):
    return roles.enrich(t, m, roster=ROSTER, na_committees=NA_COMM, v9_lookup={}, na_committees_by_term=NA_BY_TERM)


def test_r2_title_typo_helpers():
    assert roles._within_one_edit("법무부장관", "법무부차관")
    assert roles._within_one_edit("문화체육관광부제1차", "문화체육관광부제1차관")
    assert not roles._within_one_edit("국토해양부장관", "국토해양부제1차관")
    assert roles.is_title_typo_of("문화체육관광부제1차", "문화체육관광부제1차관")      # strict prefix
    assert roles.is_title_typo_of("국토해양부제1치관", "국토해양부제1차관")           # one substitution
    assert not roles.is_title_typo_of("", "위원")                                  # a missing title is no typo
    assert not roles.is_title_typo_of("위원", "위원")
    assert not roles.is_title_typo_of("국토해양부장관", "국토해양부제1차관")


def test_r2_minority_recognised_title_flagged_role_kept():
    rows = [(1, "국토해양부제1차관", "권도엽")] * 3 + [(1, "국토해양부장관", "권도엽"), (1, "위원", "김철수")]
    out = _enr(*_mk(rows))
    assert out.role.tolist() == ["vice_minister"] * 3 + ["minister", "legislator"]      # printed role kept
    assert out.label_inconsistent_in_meeting.tolist() == [False, False, False, True, False]
    assert out.label_repaired.tolist() == [False] * 5
    assert out.label_meeting_majority.iloc[3] == "국토해양부제1차관"
    assert out.label_meeting_majority.iloc[:3].isna().all()


def test_r2_one_edit_between_recognised_titles_is_flagged_not_repaired():
    rows = [(1, "법무부차관", "이귀남")] * 4 + [(1, "법무부장관", "이귀남")]
    out = _enr(*_mk(rows))
    assert out.role.tolist() == ["vice_minister"] * 4 + ["minister"]
    assert out.label_inconsistent_in_meeting.tolist() == [False] * 4 + [True]
    assert out.attrs["roles_enrich"]["label_typo_like_but_recognised_keys"] == 1


def test_r2_member_printed_once_with_minister_title():
    rows = [(1, "위원", "홍길동")] * 5 + [(1, "농림부장관", "홍길동")]
    out = _enr(*_mk(rows))
    assert out.role.iloc[5] == "minister" and bool(out.label_inconsistent_in_meeting.iloc[5])
    assert not out.label_inconsistent_in_meeting.iloc[:5].any()


def test_r2_unrecognised_typo_repaired_to_majority():
    rows = [(1, "문화체육관광부제1차관", "모철민")] * 3 + [(1, "문화체육관광부제1차", "모철민")]
    t, m = _mk(rows)
    before = _enr(t.iloc[3:], m).role.iloc[0]
    out = _enr(t, m)
    assert before == "other"                                    # alone, the typo title is not recognised
    assert out.role.iloc[3] == "vice_minister" and out.role_group.iloc[3] == "nonlegislator"
    assert out.role_rule.iloc[3].startswith("meeting_majority_repair>")
    assert bool(out.label_repaired.iloc[3]) and not bool(out.label_inconsistent_in_meeting.iloc[3])
    assert out.label_meeting_majority.iloc[3] == "문화체육관광부제1차관"
    assert out.title_raw.iloc[3] == "문화체육관광부제1차"            # printed title kept
    assert "meeting_majority_repair" in out.pos_fix.iloc[3]
    assert out.attrs["roles_enrich"]["label_repaired_rows"] == 1


def test_r2_chair_and_member_titles_are_one_class():
    rows = [(1, "위원장", "홍길동")] * 3 + [(1, "위원", "홍길동")] * 2
    out = _enr(*_mk(rows))
    assert set(out.role) == {"chair", "legislator"}
    assert not out.label_inconsistent_in_meeting.any() and not out.label_repaired.any()


def test_r2_tie_between_classes_flags_all_and_other_meeting_is_separate():
    rows = [(1, "법무부장관", "김철수"), (1, "법무부차관", "김철수"), (2, "법무부장관", "김철수")]
    out = _enr(*_mk(rows))
    assert out.label_inconsistent_in_meeting.tolist() == [True, True, False]
    assert out.label_meeting_majority.isna().all()
    assert out.attrs["roles_enrich"]["label_groups_no_majority"] == 1


def test_r2_fused_labels_excluded():
    rows = [(1, "國家報勳處長", "李在達○徐相燮委員", True), (1, "위원", "홍길동"), (1, "委員長", "劉容泰○金容甲", True)]
    t, m = _mk(rows)
    t.loc[1, "label_fused"] = pd.NA                             # NA counts as not fused
    out = _enr(t, m)
    assert out.role.tolist()[::2] == ["other", "other"]
    assert out.role_group.tolist() == ["excluded", "legislator", "excluded"]
    assert out.role_rule.iloc[0].startswith(roles.FUSED_RULE_PREFIX + "[legislator]>")
    assert not out.label_inconsistent_in_meeting.any()
    assert out.attrs["roles_enrich"]["label_fused_rows"] == 2
    # the group matches the role (validate roles_staff: role_group consistent with role)
    assert all(roles.role_group(r) == g for r, g in zip(out.role, out.role_group))


def test_r2_batch_independent_by_meeting():
    rows = ([(1, "국토해양부제1차관", "권도엽")] * 3 + [(1, "국토해양부장관", "권도엽")] +
            [(2, "문화체육관광부제1차관", "모철민")] * 2 + [(2, "문화체육관광부제1차", "모철민")] +
            [(3, "법무부장관", "이귀남"), (3, "법무부차관", "이귀남")])
    t, m = _mk(rows)
    cols = list(roles.CONTRACT_COLS + roles.EXTRA_COLS + roles.CONSISTENCY_COLS)
    whole = _enr(t, m)[cols]
    parts = pd.concat([_enr(t[t.conf_num == c], m)[cols] for c in (3, 1, 2)]).loc[whole.index]
    pd.testing.assert_frame_equal(whole, parts)


def test_r2_incomplete_meeting_is_counted_and_warned():
    rows = [(1, "위원", "홍길동")] * 3
    t, m = _mk(rows, {"n_turns": 5})
    with pytest.warns(RuntimeWarning, match="fewer turns"):
        out = _enr(t, m)
    assert out.attrs["roles_enrich"]["label_consistency_meetings_incomplete"] == 1
    t, m = _mk(rows, {"n_turns": 3})
    assert _enr(t, m).attrs["roles_enrich"]["label_consistency_meetings_incomplete"] == 0


def test_r2_empty_input_has_consistency_columns():
    t, m = _mk([(1, "위원", "홍길동")])
    out = _enr(t.iloc[:0], m)
    assert set(roles.CONSISTENCY_COLS) <= set(out.columns)


def test_r2_prefix_of_majority_repaired_even_if_recognised():
    rows = [(1, "財政經濟部長官", "李憲宰")] * 3 + [(1, "財政經濟部長", "李憲宰")]
    t, m = _mk(rows)
    alone = _enr(t.iloc[3:], m).role.iloc[0]
    out = _enr(t, m)
    assert alone != "minister"                                   # '...部長' alone is a department head title
    assert out.role.iloc[3] == "minister" and bool(out.label_repaired.iloc[3])
    assert roles.title_typo_kind("재정경제부장", "재정경제부장관") == "prefix"
    assert roles.title_typo_kind("법무부장관", "법무부차관") == "one_edit"
    assert roles.title_typo_kind("국토해양부장관", "국토해양부제1차관") is None


def test_r2_member_title_prefix_is_not_repaired():
    rows = [(1, "위원회사무총장", "홍길동")] * 3 + [(1, "위원", "홍길동")]
    out = _enr(*_mk(rows))
    assert out.role.iloc[3] == "legislator"
    assert bool(out.label_inconsistent_in_meeting.iloc[3]) and not bool(out.label_repaired.iloc[3])


def test_is_former_title_flag():
    """2026-09-28: titles printed as a former office ('(전)…', '(前)…', '前國防部長官') carry
    is_former_title; the role still follows the office in the title."""
    t, m = _mk([(1, "(전)육군참모총장", "김철수"), (1, "육군참모총장", "이영희"), (1, "前國防部長官", "朴三洙"),
                (1, "전문위원", "최민수")])
    out = _enr(t, m)
    assert out["is_former_title"].tolist() == [True, False, True, False]
    assert out["role"].iloc[0] == out["role"].iloc[1]
