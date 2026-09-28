"""
legacy_rules.py - rules extracted from the kr-hearings-data v3-v9 build scripts.

Purpose
-------
Single importable home (no file or network I/O) for every classification,
cleanup, harmonization and linkage rule that the v3-v9 pipeline applied, so
that v10 can (a) reproduce v9 behaviour for regression tests and (b) replace
rules deliberately rather than by accident.

Provenance conventions
----------------------
Every rule carries a SOURCE tag of the form ``file:line`` relative to the v9
build scripts in ``validation/`` (commit aef984d, read 2026-09-24 and re-checked
2026-09-25). Those scripts are not distributed since v10.

* VERBATIM      - logic copied from the repo; behaviour identical to the latest
                  version that used it.
* RECONSTRUCTED - the original v9 build code is not public. Rebuilt from
                  docs/v9/PIPELINE_v9.md Stage 2 plus the observed v9 labels,
                  and checked for agreement with the v9 labels. Do not treat it
                  as the original.
* KNOWN-BUG     - kept verbatim for reproduction, with a corrected sibling.

Pipeline lineage (which step last touched each rule)
----------------------------------------------------
v1/v2  01_build_speech_dataset.py (not in repo): XLSX parse, speaker cascade,
       committee map, legislator metadata (party, ruling_status, seniority,
       gender, naas_cd), dyads (likely a string sort, see DYADS section).
v2     unknown post-processing that moved chairs without legislator status to
       non-legislator roles (over-correction, see investigate_failures.py).
v3     fix_and_rebuild.py: member_id => chair/legislator, dedup on
       (meeting_id, speech_order), numeric-sorted dyads.
v4     build_v4.py: person_title, member-name canonicalisation, 'other'
       reclass (v4 rules), text + date normalisation, numeric-sorted dyads.
v5     build_v5.py: member_id nulls, person_title decontamination, empty
       names, member_uid, 장관직무대리 => minister_acting, 'other' reclass
       (v5 rules), non-legislator name/affiliation split, numeric dyads.
v6     build_v6.py: 42 인사청문특별위원회 meetings (HTML), own classifier
       (classify_speaker_build_v6), mp_metadata join on (name, term),
       string-sorted dyads for new meetings.
v7     build_v7.py: 228 인사청문 meetings (PDF, parsed elsewhere), no dyads.
v8     build_v8.py: 2,081 meetings of 국정조사/예결/본회의 parsed in
       assembly_hearing_pipeline (not in repo, classifier unknown),
       string-sorted dyads for new meetings.
v9     build_v9.py: ministry normalisation, minister panel linkage,
       ruling_status '' => null, FULL dyad rebuild with string sort.

meeting_id namespaces in v9 (measured 2026-09-25, 04_meeting_id_namespace.py)
----------------------------------------------------------------------------
XLSX (상임위원회, 국정감사) and v7 PDF 인사청문 (228) and v8 (국정조사, 예결,
본회의): the id is a CONF_ID (5 digits, or 6 digits zero-padded for 21-22대);
the v9 date agrees with the Open API row of that CONF_ID. v6 HTML 인사청문 (42):
the id is a viewer CONFER_NUM. For v8 the TRANSCRIPT was in many meetings
fetched with the same number used as a CONFER_NUM (see report section 4.2).
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Iterable, Mapping, Optional, Sequence

try:  # pandas is only used for NA detection, to mirror pd.isna() exactly
    import pandas as _pd
except Exception:  # pragma: no cover
    _pd = None


def _isna(x) -> bool:
    """Mirror pandas.isna for scalars (None, NaN, pd.NA, NaT)."""
    if x is None:
        return True
    if _pd is not None:
        try:
            return bool(_pd.isna(x))
        except (TypeError, ValueError):  # array-likes
            return False
    return isinstance(x, float) and math.isnan(x)


# =============================================================================
# 1. ROLE SETS  (VERBATIM; identical in every build script, e.g. build_v9.py:38-49)
# =============================================================================

LEG_ROLES = frozenset({"legislator", "chair"})
NONLEG_ROLES = frozenset({
    "minister", "minister_nominee", "minister_acting", "vice_minister",
    "prime_minister", "witness", "testifier", "expert_witness",
    "senior_bureaucrat", "other_official", "local_gov_head",
    "agency_head", "public_corp_head", "org_head", "mid_bureaucrat",
    "nominee", "military", "police", "financial_regulator",
    "audit_official", "election_official", "constitutional_court",
    "assembly_official", "independent_official", "private_sector",
    "research_head", "cultural_institution_head", "broadcasting",
    "cooperative_head",
})
EXCLUDED_ROLES = frozenset({"committee_staff", "other", "unknown"})  # deep_audit.py:46
ALL_ROLES = LEG_ROLES | NONLEG_ROLES | EXCLUDED_ROLES  # 34 labels ('unknown' only from v7 data)

# build_v9.py:52-58
GOVT_ROLES = frozenset({
    "minister", "minister_nominee", "minister_acting", "vice_minister",
    "prime_minister", "agency_head", "senior_bureaucrat", "mid_bureaucrat",
})
PANEL_LINK_ROLES = frozenset({"minister", "minister_acting", "minister_nominee"})


def has_member_id(member_id) -> bool:
    """member_id presence test. build_v6.py:60 treats '', 'nan' and '0' as absent;
    validate_dataset.py:280 treats '', 'nan' (not '0') as absent; build_v5.py:149
    maps 'nan','','None','NaN' to NA. Union used here (conflict noted in report)."""
    if _isna(member_id):
        return False
    return str(member_id).strip() not in ("", "nan", "None", "NaN", "0")


def normalize_member_id_v5(member_id):
    """VERBATIM build_v5.py:141-162 (FIX 1): 'nan', '', 'None', 'NaN' and
    whitespace-only -> None. Applied to the XLSX era only; v6-v8 rows were
    appended afterwards and still carry '' (520,452 v9 rows) and, in v6 only,
    '0' -> NA (build_v6.py:257)."""
    if _isna(member_id):
        return None
    s = str(member_id)
    if s in ("nan", "", "None", "NaN") or s.strip() == "":
        return None
    return member_id


# =============================================================================
# 2. SPEAKER-ROLE CLASSIFICATION
# =============================================================================

# ---- 2a. build_v6 classifier (VERBATIM, build_v6.py:54-153) ------------------
# Docstring there says "Copied from pipeline 01", but it is a SUBSET of the
# cascade documented in docs/v9/PIPELINE_v9.md Stage 2 (no military / police /
# public_corp_head / financial_regulator / independent_official /
# local_gov_head / research_head / cultural / broadcasting / cooperative /
# private_sector branches; 총재/이사장/차장 go to senior_bureaucrat and
# 원장/회장/사장 go to org_head). It was used only for the 42 v6 HTML meetings.

def classify_speaker_build_v6(name, member_id):
    """Return (role, person, affiliation). VERBATIM build_v6.py:54-153."""
    name = str(name).strip()
    if not name:
        return "unknown", "", ""

    has_mid = (not _isna(member_id)) and str(member_id).strip() not in ("", "nan", "0")

    if "위원장" in name:
        person = re.sub(r".*위원장\s*", "", name).strip()
        return "chair", person, name
    if name.endswith("위원"):
        person = name.replace("위원", "").strip()
        return "legislator", person, name
    if has_mid:
        return "legislator", name, name
    if "장관후보자" in name:
        person = re.sub(r".*장관후보자\s*", "", name).strip()
        ministry = re.sub(r"장관후보자.*", "", name).strip()
        return "minister_nominee", person, ministry
    if "장관" in name:
        person = re.sub(r".*장관\s*", "", name).strip()
        ministry = re.sub(r"장관.*", "", name).strip()
        return "minister", person, ministry
    if "총리" in name:
        person = re.sub(r".*총리\s*", "", name).strip()
        if "후보자" in name:
            return "nominee", person, name
        return "prime_minister", person, name
    if "차관" in name:
        person = re.sub(r".*차관\s*", "", name).strip()
        ministry = re.sub(r"차관.*", "", name).strip()
        return "vice_minister", person, ministry
    if "증인" in name:
        return "witness", name.replace("증인", "").strip(), ""
    if "진술인" in name:
        return "testifier", name.replace("진술인", "").strip(), ""
    if "참고인" in name:
        return "expert_witness", name.replace("참고인", "").strip(), ""
    if "전문위원" in name or "수석전문위원" in name:
        person = re.sub(r".*(전문위원|수석전문위원)\s*", "", name).strip()
        return "committee_staff", person, name
    if "후보자" in name:
        person = re.sub(r".*후보자\s*", "", name).strip()
        position = re.sub(r"후보자.*", "", name).strip()
        return "nominee", person, position
    if "청장" in name:
        person = re.sub(r".*청장\s*", "", name).strip()
        return "agency_head", person, name
    if "감사원장" in name or "감사위원" in name:
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "audit_official", person, name
    if "헌법재판소" in name:
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "constitutional_court", person, name
    if "선거관리위원회" in name or "선관위" in name:
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "election_official", person, name
    if any(kw in name for kw in ["국회사무", "국회도서관", "국회예산정책처", "국회입법조사처"]):
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "assembly_official", person, name
    for title in ["본부장", "처장", "사무처장", "국장", "실장", "차장", "총재", "이사장"]:
        if title in name:
            person = name.split()[-1] if len(name.split()) > 1 else name
            return "senior_bureaucrat", person, name
    if any(kw in name for kw in ["원장", "회장", "사장"]):
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "org_head", person, name
    return "other", name, name


# ---- 2b. v3 member_id fix (VERBATIM, fix_and_rebuild.py:44-98) ----------------
LEGISLATIVE_CHAIR_PATTERNS = ("소위원장", "위원장직무대행", "위원장대리", "조정위원장")


def fix_member_id_role_v3(role: str, speaker: str, member_id) -> str:
    """Rows with a member_id but a non-legislator role become chair/legislator.
    fix_and_rebuild.py:55-98. Note the second pass (:90-98) sends ANY
    '위원장' speaker with member_id to chair, so the pattern list is redundant."""
    if not has_member_id(member_id) or role in LEG_ROLES:
        return role
    spk = str(speaker)
    if any(p in spk for p in LEGISLATIVE_CHAIR_PATTERNS):
        return "chair"
    if "위원장" in spk:
        return "chair"
    return "legislator"  # member_id is strongest signal (both remaining branches)


# ---- 2c. v4 'other' reclassification (VERBATIM, build_v4.py:76-97,128-136) ---
OTHER_RECLASS_RULES_V4 = (
    (re.compile(r"사관학교장"), "military"),
    (re.compile(r"사령관|참모총장|참모차장"), "military"),
    (re.compile(r"경찰청|경찰서"), "police"),
    (re.compile(r"교수|연구위원|연구원.*센터"), "expert_witness"),
    (re.compile(r"감독$|선수단"), "private_sector"),
    (re.compile(r"예술감독"), "private_sector"),
    (re.compile(r"국장$|국장\s"), "senior_bureaucrat"),
    (re.compile(r"실장$|실장\s"), "senior_bureaucrat"),
    (re.compile(r"정책관$|정책관\s"), "mid_bureaucrat"),
    (re.compile(r"감사관$|감사관\s"), "mid_bureaucrat"),
    (re.compile(r"교육장\s|교육장$"), "local_gov_head"),
    (re.compile(r"관장\s|관장$"), "org_head"),
    (re.compile(r"소장\s|소장$"), "org_head"),
    (re.compile(r"상임위원\s|상임위원$"), "org_head"),
    (re.compile(r"전무이사|상무이사|기획이사|사업이사|관리이사|운영이사|기금이사|보험관리이사|업무이사|업무상임이사|유통이사|유통담당이사|검정이사|자격검정이사|기반조성본부이사|유지관리본부이사|기획운영이사|선임비상임이사|관리상임이사"), "org_head"),
    (re.compile(r"이사\s|이사$"), "org_head"),
    (re.compile(r"감사\s|감사$"), "org_head"),
    (re.compile(r"통제소장"), "senior_bureaucrat"),  # unreachable: '소장\s' above matches first
)


def reclassify_other_v4(speaker) -> Optional[str]:
    """First matching v4 rule (re.search) or None. Applied only where role=='other'."""
    if not speaker or _isna(speaker):
        return None
    spk = str(speaker)
    for pattern, new_role in OTHER_RECLASS_RULES_V4:
        if pattern.search(spk):
            return new_role
    return None


# ---- 2d. v5 'other' reclassification (VERBATIM, build_v5.py:81-131,276-307) --
# build_v5 applies the rules vectorised in list order, only to still-unassigned
# rows, which is equivalent to "first match wins".
OTHER_RECLASS_RULES_V5 = (
    (re.compile(r"기무사령부|기무부대"), "military"),
    (re.compile(r"군사보좌관"), "military"),
    (re.compile(r"금융통화위원"), "financial_regulator"),
    (re.compile(r"한국정책방송원|KBS|방송통신위원회"), "broadcasting"),
    (re.compile(r"방송위원회"), "broadcasting"),
    (re.compile(r"교육장$|교육장\s"), "local_gov_head"),
    (re.compile(r"학교장\s|학교장$"), "org_head"),
    (re.compile(r"맹학교장|고등학교장|중학교장"), "org_head"),
    (re.compile(r"정책보좌관|정치자문역|자문역"), "other_official"),
    (re.compile(r"보좌관\s|보좌관$"), "other_official"),
    (re.compile(r"홍보관\s|홍보관$"), "other_official"),
    (re.compile(r"대변인\s|대변인$"), "other_official"),
    (re.compile(r"제작소장|자원관장"), "org_head"),
    (re.compile(r"공사.*팀\s|공사.*부\s"), "org_head"),
    (re.compile(r"워킹그룹.*관\s|워킹그룹.*관$"), "other_official"),
    (re.compile(r"연구부\s|연구부$"), "expert_witness"),
    (re.compile(r"교수\s|교수$"), "expert_witness"),
    (re.compile(r"안전평가관\s|안전평가관$"), "mid_bureaucrat"),
    (re.compile(r"어업자원관\s|어업자원관$"), "mid_bureaucrat"),
    (re.compile(r"노사협력관\s|노사협력관$"), "mid_bureaucrat"),
    (re.compile(r"선진화관\s|선진화관$"), "mid_bureaucrat"),
    (re.compile(r"정책과\s|정책과$"), "mid_bureaucrat"),
    (re.compile(r"TF장\s|TF장$"), "mid_bureaucrat"),
    (re.compile(r"소방서|안전센터"), "police"),
    (re.compile(r"대통령직인수위원회위원"), "assembly_official"),  # questionable: transition committee is not the Assembly
    (re.compile(r"㈜|주식회사|\(재\)|\(사\)"), "private_sector"),
    (re.compile(r"선수단감독|감독\s"), "private_sector"),
    (re.compile(r"철인3종|트라이애슬론"), "private_sector"),
)


def reclassify_other_v5(speaker) -> Optional[str]:
    """First matching v5 rule (str.contains == re.search) or None. Only for role=='other'."""
    if not speaker or _isna(speaker):
        return None
    spk = str(speaker)
    for pattern, new_role in OTHER_RECLASS_RULES_V5:
        if pattern.search(spk):
            return new_role
    return None


# ---- 2e. v5 minister acting fix (VERBATIM, build_v5.py:263-273) ---------------
def fix_minister_acting_v5(role: str, speaker) -> str:
    if role == "minister" and "장관직무대리" in str(speaker):
        return "minister_acting"
    return role


# ---- 2f. XLSX-era cascade (RECONSTRUCTED) -------------------------------------
# The documented order (docs/v9/PIPELINE_v9.md:67-82) is kept; substring checks are
# plain `in` tests as in build_v6 (which says it was copied from pipeline 01).
# Branches marked [EMP] are inferred from v9 labels, not from any code.
# Known substring quirks that the original clearly had and that are kept here
# because v9 labels contain them:
#   '검사장'  -> public_corp_head   (contains '사장')        24,796 v9 rows
#   '이사장'  -> public_corp_head   (contains '사장')       ~179,000 v9 rows
#   '법원장'  -> org_head           (contains '원장')
#   '연구원장'-> org_head           (contains '원장'), not research_head
#   '...위원장' without member_id -> independent_official (v2 over-correction)
#   '교육감'  -> independent_official [EMP] (49,241 rows), although PIPELINE_v9.md
#               and the role description list 교육감 under local_gov_head.
# Agreement with v9 on the 8,597,178 XLSX-era rows:
#   first reconstruction (2026-09-24)                 99.549 percent
#   this version, 8 more [EMP] branches (2026-09-25)  99.863 percent (8,585,360 rows)
# The added branches are marked [EMP2]. They were chosen by inspecting the
# largest disagreement cells, so they fit v9; they are not evidence about the
# original code.

_OTHER_OFFICIAL_KEYWORDS = ("총장", "비서관", "대변인", "심의관", "단장", "부장", "대표이사",
                            "과장", "팀장", "센터장", "판사", "공무원", "수석대표", "기획관")


def classify_speaker_xlsx_reconstructed(speaker, member_id) -> str:
    """RECONSTRUCTED pipeline-01 cascade for the XLSX era, BEFORE the v3/v4/v5 patches."""
    s = "" if _isna(speaker) else str(speaker).strip()
    if not s:
        return "unknown"
    if has_member_id(member_id):
        return "chair" if "위원장" in s else "legislator"   # PIPELINE_v9.md steps 1-3 + v3
    if s.endswith("위원") or s.endswith("의원"):
        return "legislator"                                  # step 2 (title-suffix legislators)
    if s.startswith(("위원장 ", "소위원장 ", "위원장대리 ", "위원장직무대행 ")):
        return "chair"                                       # legislative chair without id
    if "장관후보자" in s:
        return "minister_nominee"
    if "후보자" in s:
        return "nominee"                                     # [EMP] precedes 위원장/청장/사장 etc.
    if "장관직무대행" in s:
        return "minister_acting"
    if "장관" in s:
        return "minister"
    if "총리" in s:
        return "prime_minister"
    if "차관" in s:
        return "vice_minister"
    if "증인" in s:
        return "witness"
    if "진술인" in s:
        return "testifier"
    if "참고인" in s:
        return "expert_witness"
    if "전문위원" in s:
        return "committee_staff"
    if "감사원장" in s or "감사위원" in s:
        return "audit_official"
    if "헌법재판소" in s:
        return "constitutional_court"
    if "선거관리위원회" in s or "선관위" in s:
        return "election_official"
    if any(k in s for k in ("국회사무", "국회도서관", "국회예산정책처", "국회입법조사처")):
        return "assembly_official"
    if "위원장" in s or "교육감" in s or "특별감찰관" in s or "입법조사관" in s:
        return "independent_official"                        # [EMP]
    if "국가인권위원회" in s:
        return "independent_official"                        # [EMP2] e.g. 국가인권위원회상임위원
    if "청장" in s:
        return "agency_head"
    if "합동참모본부" in s:
        return "military"                                    # [EMP2] before 본부장/처장/부장
    if any(k in s for k in ("사령관", "참모총장", "참모차장", "합동참모의장", "사관학교장")):
        return "military"
    if "금융감독원" in s:
        return "financial_regulator"
    if "경찰" in s:
        return "police"
    if any(k in s for k in ("세관장", "구치소장", "교도소장")):
        return "senior_bureaucrat"                           # [EMP2] before the v4 '소장' rule
    if "사장" in s or "은행장" in s:
        return "public_corp_head"
    if "시장" in s or "도지사" in s:
        return "local_gov_head"                              # [EMP]
    if "부지사" in s:
        return "local_gov_head"                              # [EMP2] 행정부지사, 정무부지사
    if "연구소장" in s:
        return "research_head"                               # [EMP]
    if any(k in s for k in ("박물관장", "미술관장", "도서관장", "기념관장")):
        return "cultural_institution_head"                   # [EMP]
    if ("협동조합" in s or "농협" in s or "조합중앙회" in s) and ("대표이사" in s or "상무" in s or "이사" in s):
        return "cooperative_head"                            # [EMP]
    if "대표이사" in s:
        return "other_official"                              # [EMP2] ㈜... 대표이사 is other_official in v9
    if "원장" in s or "회장" in s:
        return "org_head"
    if any(k in s for k in ("본부장", "처장", "국장", "실장", "차장", "총재", "대사", "총영사")):
        return "senior_bureaucrat"                           # 대사/총영사 [EMP]
    if any(k in s for k in ("관리관", "지원관")):
        return "mid_bureaucrat"                              # [EMP2] 국방부법무관리관 etc.
    if "정책관" in s or "감사관" in s:
        return "mid_bureaucrat"
    if "방송" in s:
        return "broadcasting"                                # [EMP]
    # [EMP2] no private_sector branch here: in v9 private_sector comes from the
    # v4/v5 'other' rules (㈜, 주식회사, 감독, 선수단), applied after this cascade.
    if any(k in s for k in _OTHER_OFFICIAL_KEYWORDS):
        return "other_official"                              # [EMP] + [EMP2] 과장..기획관
    return "other"


def classify_speaker_v9_chain(speaker, member_id) -> str:
    """Reconstructed cascade followed by the VERBATIM v3, v4, v5 patches in build order."""
    role = classify_speaker_xlsx_reconstructed(speaker, member_id)
    role = fix_member_id_role_v3(role, speaker, member_id)
    if role == "other":
        role = reclassify_other_v4(speaker) or "other"
    role = fix_minister_acting_v5(role, speaker)
    if role == "other":
        role = reclassify_other_v5(speaker) or "other"
    return role


def classify_with_lookup(speaker, member_id, lookup: Mapping) -> str:
    """Exact v9 behaviour for known (speaker, has_member_id) pairs, else the chain.
    `lookup` is built by the caller from v10/interim/04_v9_speaker_role_table_xlsx_era.parquet
    (keys: (speaker, bool has_mid) -> v9 role, majority label if ambiguous)."""
    key = (speaker, has_member_id(member_id))
    if key in lookup:
        return lookup[key]
    return classify_speaker_v9_chain(speaker, member_id)


# =============================================================================
# 3. PERSON TITLE / PERSON NAME
# =============================================================================

# build_v4.py:49-60 (longest first)
PERSON_TITLE_PREFIXES = (
    ("반장직무대행", "반장직무대행"),
    ("반장직무대리", "반장직무대리"),
    ("반장대리", "반장대리"),
    ("직무대행", "직무대행"),
    ("직무대리", "직무대리"),
    ("위원당대리", "위원당대리"),
    ("위윈장대리", "위원장대리"),  # typo in original data
    ("대리위", "대리"),            # parsing artifact
    ("대리", "대리"),
    ("반장", "반장"),
)
# build_v4.py:62-67
PERSON_TITLE_SUFFIXES = (
    (" 의원", ""),
    ("의원", ""),
    (" 위원님", ""),
    ("위원님", ""),
)
# build_v4.py:71 (defined but unused there); build_v5.py:138
PARTY_PAREN_PATTERN = re.compile(r"\([가-힣새한비국평]\)$")
KOREAN_NAME_RE = re.compile(r"([가-힣]{2,4})$")                        # build_v5.py:136 (unused there)
KOREAN_NAME_PAREN_RE = re.compile(r"([가-힣]{2,4}\([가-힣새한비국평]\))$")  # build_v5.py:138 (unused there)

# build_v5.py:51-55
VALID_PERSON_TITLES = frozenset({
    "대리", "반장", "직무대리", "직무대행",
    "반장대리", "반장직무대행", "반장직무대리",
    "위원장대리", "위원당대리",
})


def extract_person_title(person_name_str):
    """VERBATIM build_v4.py:100-125. Returns (clean_name, title_or_None)."""
    if not person_name_str or _isna(person_name_str):
        return person_name_str, None
    name = str(person_name_str).strip()
    title = None
    for pattern, title_label in PERSON_TITLE_PREFIXES:
        if name.startswith(pattern):
            remainder = name[len(pattern):].strip()
            if remainder:
                name = remainder
                title = title_label
                break
    for pattern, _replacement in PERSON_TITLE_SUFFIXES:
        if name.endswith(pattern):
            remainder = name[:-len(pattern)].strip() if pattern else name
            if remainder:
                name = remainder
                break
    return name.strip(), title


def canonical_names_by_member_id(pairs: Iterable[tuple]) -> dict:
    """VERBATIM logic of build_v4.py:258-269: for each member_id with >1 distinct
    non-empty name, canonical = min(sorted(set(names)), key=len) (ties -> alphabetical).
    Input: iterable of (member_id, person_name). Output: {member_id: canonical}.
    Note: run BEFORE member_uid existed, so the 4 homonymous ids are merged here."""
    names: dict = {}
    for mid, n in pairs:
        if _isna(mid) or str(mid).strip() in ("", "nan", "None", "NaN"):
            continue
        if _isna(n):
            continue
        n = str(n).strip()
        if n and n != "nan":
            names.setdefault(str(mid).strip(), set()).add(n)
    out = {}
    for mid, s in names.items():
        uniq = sorted(s)
        if len(uniq) > 1:
            canonical = min(uniq, key=len)
            if canonical:
                out[mid] = canonical
    return out


def title_from_long_variant(old_name: str, canonical: str) -> Optional[str]:
    """build_v4.py:280-285: prefix of a long name variant becomes person_title
    when person_title is empty (source of the 87 contaminated titles fixed in v5)."""
    old = str(old_name).strip()
    if old.endswith(canonical) and len(old) > len(canonical):
        prefix = old[:-len(canonical)].strip()
        return prefix or None
    return None


def decontaminate_person_title(person_title, affiliation_raw):
    """VERBATIM build_v5.py:165-186 (single row). Returns (person_title, affiliation_raw)."""
    if _isna(person_title) or person_title in VALID_PERSON_TITLES:
        return person_title, affiliation_raw
    if not _isna(affiliation_raw) and str(affiliation_raw).strip():
        return None, f"{person_title} {affiliation_raw}"
    return None, person_title


def extract_name_from_speaker_v5(speaker) -> Optional[str]:
    """VERBATIM build_v5.py:201-207: '김부겸 위원장' -> '김부겸'; else None."""
    m = re.match(r"^([가-힣]{2,4})\s+(위원장|위원|의원|장관)", str(speaker).strip())
    return m.group(1) if m else None


_PURE_NAME_RE = re.compile(r"^[가-힣]{2,4}(\([가-힣새한비국평]\))?$")
_ANON_NAME_RE = re.compile(r"^[가-힣]?0+$|^O+$")


def split_affiliation_and_name_v5(person_name, affiliation_raw):
    """VERBATIM semantics of build_v5.py:310-382 for ONE non-legislator row
    (rows with member_id are skipped by the caller). Returns (person_name, affiliation_raw)."""
    name = str(person_name)  # astype(str) in the original
    if (_PURE_NAME_RE.match(name) or name.strip() == "" or name in ("nan", "None")
            or _ANON_NAME_RE.match(name) or re.search(r"[a-zA-Z]", name)):
        return person_name, affiliation_raw
    m = re.match(r"(.+?)\s*([가-힣]{2,4}\([가-힣새한비국평]\))$", name)
    if not m:
        m = re.match(r"(.+?)\s*([가-힣]{2,4})$", name)
    if not m or len(m.group(2)) < 2:
        return person_name, affiliation_raw
    prefix, extracted = m.group(1), m.group(2)
    if prefix is None or prefix.strip() == "":
        return extracted, affiliation_raw
    aff = "" if _isna(affiliation_raw) else str(affiliation_raw)
    if aff.strip() in ("", "nan", "None"):
        return extracted, prefix.strip()
    return extracted, prefix.strip() + " " + aff
    # Caveat: a 4-syllable run at the end is taken as the name, so a title
    # glued to a 2-syllable name ('...국장홍길동') is split greedily/lazily per regex.


# =============================================================================
# 4. MEMBER_UID (homonymous member_id)  VERBATIM build_v5.py:60-77,224-260
# =============================================================================

HOMONYM_MEMBER_IDS = {
    "7407": {"E6S73230": "7407_A", "0W194007": "7407_B"},   # 김영주
    "6182": {"FJ03481D": "6182_A", "KA04352K": "6182_B"},   # 최경환
    "806":  {"ZC87486D": "806_A",  "DTG4846A": "806_B"},    # 김선동
    "878":  {"BQS2021C": "878_A",  "9UW75767": "878_B"},    # 김성태
}


def member_uid_for(member_id, naas_cd, dominant_naas_in_term: Optional[str] = None):
    """member_uid = member_id except for the 4 homonymous ids. When naas_cd is NA the
    original used the modal naas_cd of the same member_id within the same term
    (pass it as dominant_naas_in_term); unresolved rows keep the bare member_id."""
    if _isna(member_id):
        return member_id
    mid = str(member_id)
    if mid not in HOMONYM_MEMBER_IDS:
        return member_id
    m = HOMONYM_MEMBER_IDS[mid]
    if not _isna(naas_cd) and naas_cd in m:
        return m[naas_cd]
    if _isna(naas_cd) and dominant_naas_in_term:
        return m.get(dominant_naas_in_term, mid)
    return member_id


def harmonize_homonym_metadata_v5(rows: Sequence[Mapping]) -> list:
    """VERBATIM semantics of build_v5.py:385-409 (FIX 4b) for the rows of ONE
    member_uid of the 4 homonymous ids: for gender, party and ruling_status, every
    non-null value that differs from the modal value is overwritten by the mode.

    KNOWN-BUG: party and ruling_status legitimately change between terms, and the
    mode is taken across ALL terms of the member_uid. v9 evidence (v9 rows by term):
      7407_B (김영주) term 20 -> party 열린우리당, ruling (the term-17 values)
      6182_A (최경환) terms 18-19 -> 한나라당, opposition (the term-17 values)
      878_A  (김성태) term 18 -> 새누리당 (the term-19 label)
    Returns new dicts; does not mutate the input."""
    out = [dict(r) for r in rows]
    for col in ("gender", "party", "ruling_status"):
        vals = [r.get(col) for r in out if not _isna(r.get(col))]
        if not vals:
            continue
        counts: dict = {}
        for v in vals:
            counts[v] = counts.get(v, 0) + 1
        top = max(counts.values())
        mode_val = sorted(v for v, c in counts.items() if c == top)[0]  # pandas mode() sorts ties
        for r in out:
            if not _isna(r.get(col)) and r[col] != mode_val:
                r[col] = mode_val
    return out


# =============================================================================
# 5. DATES / TEXT  (VERBATIM build_v4.py:139-162)
# =============================================================================

def parse_korean_date(d) -> Optional[str]:
    d = str(d).strip()
    d = re.sub(r"\([^)]*\)$", "", d).strip()
    d = d.replace(" ", "")
    for fmt in ("%Y년%m월%d일", "%Y年%m月%d日", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    return None  # build_v4 then KEEPS the unparsed original string (:346) - silent fallback


def normalize_text(text):
    if _isna(text):
        return text
    return re.sub(r"  +", " ", str(text)).strip()


def concat_speech_parts(parts: Sequence) -> str:
    """PIPELINE_v9.md:59-61: 발언내용1..7 joined with spaces and stripped (original code not in repo)."""
    return " ".join(str(p) for p in parts if not _isna(p) and str(p) != "").strip()


# Raw XLSX layout (의안정보시스템 회의록 데이터셋). NOT from the kr-hearings repo:
# documented in ../assemblykor/data-raw/process_all_speeches.py:24-34 and
# process_audit_speeches.py:23-31 (0-based column index, header in row 1) for 16-20대
# per-committee files; 21-22대 files have one sheet per meeting named
# '{회의번호}_발언내용' read with header=2 (process_audit_speeches.py:96-105,
# process_speeches_21_22.py:4). docs/v9/PIPELINE_v9.md:40-43 says the 21-22대 header row
# is "detected dynamically (search for 발언자 row)".
XLSX_RAW_COLUMNS_16_20 = {
    0: "회의번호", 2: "대수", 4: "위원회", 8: "회의일자", 9: "안건",
    10: "발언자", 11: "의원ID", 12: "발언순번",
    13: "발언내용1", 14: "발언내용2", 15: "발언내용3", 16: "발언내용4",
    17: "발언내용5", 18: "발언내용6", 19: "발언내용7",
}

# v3 dedup (fix_and_rebuild.py:107-125): drop_duplicates(['meeting_id','speech_order'],
# keep='first') with no check that the copies agree; removed 94,347 v2 rows
# (8,691,525 -> 8,597,178; report.json vs report_v3.json).
DEDUP_KEY_V3 = ("meeting_id", "speech_order")

# Term windows used by the old validators (both are approximations).
TERM_DATE_RANGES_DEEP_AUDIT = {  # deep_audit.py:59-67 (constitutional 4-year terms)
    16: ("2000-05-30", "2004-05-29"), 17: ("2004-05-30", "2008-05-29"),
    18: ("2008-05-30", "2012-05-29"), 19: ("2012-05-30", "2016-05-29"),
    20: ("2016-05-30", "2020-05-29"), 21: ("2020-05-30", "2024-05-29"),
    22: ("2024-05-30", "2028-05-29"),
}
TERM_YEAR_RANGES_VALIDATE = {  # validate_dataset.py:144-152, used with +-1 year slack
    16: (2000, 2004), 17: (2004, 2008), 18: (2008, 2012), 19: (2012, 2016),
    20: (2016, 2020), 21: (2020, 2024), 22: (2024, 2028),
}


# =============================================================================
# 6. COMMITTEE HARMONIZATION
# =============================================================================
# Extracted from v9 data (the pipeline-01 map itself is not in the repo).
# Raw -> key is a function of the raw name ONLY within 상임위원회/국정감사;
# for the four v6-v8 hearing types the key is fixed by hearing_type
# (build_v6.py:244, build_v8.py:49-53), so '법제사법위원회' appears under both
# 'judiciary' and 'confirmation_special' in v9.

# Generated 2026-09-24 from data/all_speeches_16_22_v9.parquet (SELECT committee_key, committee ... GROUP BY).
COMMITTEE_KEY_MAP_STANDING = {
    # --- agriculture ---
    '농림수산식품위원회': 'agriculture',  # 국정감사,상임위원회; 211 mtgs; terms 18-19
    '농림축산식품해양수산위원회': 'agriculture',  # 국정감사,상임위원회; 429 mtgs; terms 19-22
    '농림해양수산위원회': 'agriculture',  # 국정감사,상임위원회; 342 mtgs; terms 16-17
    '농림해양수산위원회-제1반': 'agriculture',  # 국정감사; 4 mtgs; terms 17-17
    '농림해양수산위원회-제2반': 'agriculture',  # 국정감사; 5 mtgs; terms 17-17
    # --- assembly_operations ---
    '국회운영위원회': 'assembly_operations',  # 국정감사,상임위원회; 517 mtgs; terms 16-22
    # --- culture ---
    '문화관광위원회': 'culture',  # 국정감사,상임위원회; 353 mtgs; terms 16-17
    '문화관광위원회-제1반': 'culture',  # 국정감사; 1 mtgs; terms 17-17
    '문화관광위원회-제2반': 'culture',  # 국정감사; 1 mtgs; terms 17-17
    '문화체육관광위원회': 'culture',  # 국정감사,상임위원회; 197 mtgs; terms 20-22
    # --- culture_media ---
    '문화체육관광방송통신위원회': 'culture_media',  # 국정감사,상임위원회; 206 mtgs; terms 18-19
    # --- defense ---
    '국방위원회': 'defense',  # 국정감사,상임위원회; 812 mtgs; terms 16-22
    '국방위원회-제1반': 'defense',  # 국정감사; 1 mtgs; terms 17-17
    '국방위원회-제2반': 'defense',  # 국정감사; 1 mtgs; terms 17-17
    # --- education ---
    '교육위원회': 'education',  # 국정감사,상임위원회; 434 mtgs; terms 16-22
    '교육위원회-제1반': 'education',  # 국정감사; 45 mtgs; terms 16-21
    '교육위원회-제2반': 'education',  # 국정감사; 44 mtgs; terms 16-21
    # --- education_culture ---
    '교육문화체육관광위원회': 'education_culture',  # 국정감사,상임위원회; 189 mtgs; terms 19-20
    '교육문화체육관광위원회-제1반': 'education_culture',  # 국정감사; 12 mtgs; terms 19-20
    '교육문화체육관광위원회-제2반': 'education_culture',  # 국정감사; 12 mtgs; terms 19-20
    # --- education_science ---
    '교육과학기술위원회': 'education_science',  # 국정감사,상임위원회; 191 mtgs; terms 18-19
    '교육과학기술위원회-제1반': 'education_science',  # 국정감사; 30 mtgs; terms 18-19
    '교육과학기술위원회-제2반': 'education_science',  # 국정감사; 30 mtgs; terms 18-19
    # --- environment_labor ---
    '환경노동위원회': 'environment_labor',  # 국정감사,상임위원회; 939 mtgs; terms 16-22
    # --- finance ---
    '기획재정위원회': 'finance',  # 국정감사,상임위원회; 549 mtgs; terms 18-22
    '기획재정위원회-제1반': 'finance',  # 국정감사; 35 mtgs; terms 18-21
    '기획재정위원회-제2반': 'finance',  # 국정감사; 31 mtgs; terms 18-21
    '재정경제위원회': 'finance',  # 국정감사,상임위원회; 430 mtgs; terms 16-17
    '재정경제위원회-제1반': 'finance',  # 국정감사; 11 mtgs; terms 16-16
    '재정경제위원회-제2반': 'finance',  # 국정감사; 11 mtgs; terms 16-16
    '재정경제위원회-제3반': 'finance',  # 국정감사; 2 mtgs; terms 16-16
    # --- foreign_affairs ---
    '외교통상통일위원회': 'foreign_affairs',  # 국정감사,상임위원회; 173 mtgs; terms 18-19
    '외교통상통일위원회-구주반': 'foreign_affairs',  # 국정감사; 22 mtgs; terms 18-19
    '외교통상통일위원회-남미반': 'foreign_affairs',  # 국정감사; 3 mtgs; terms 18-18
    '외교통상통일위원회-미주1반': 'foreign_affairs',  # 국정감사; 4 mtgs; terms 18-18
    '외교통상통일위원회-미주2반': 'foreign_affairs',  # 국정감사; 3 mtgs; terms 18-18
    '외교통상통일위원회-미주반': 'foreign_affairs',  # 국정감사; 22 mtgs; terms 18-19
    '외교통상통일위원회-아주반': 'foreign_affairs',  # 국정감사; 19 mtgs; terms 18-19
    '외교통상통일위원회-아프리카․중동반': 'foreign_affairs',  # 국정감사; 20 mtgs; terms 18-19
    '외교통일위원회': 'foreign_affairs',  # 국정감사,상임위원회; 282 mtgs; terms 19-22
    '외교통일위원회-구주A반': 'foreign_affairs',  # 국정감사; 3 mtgs; terms 21-21
    '외교통일위원회-구주B반': 'foreign_affairs',  # 국정감사; 4 mtgs; terms 21-21
    '외교통일위원회-구주반': 'foreign_affairs',  # 국정감사; 39 mtgs; terms 19-21
    '외교통일위원회-미구주반': 'foreign_affairs',  # 국정감사; 2 mtgs; terms 21-21
    '외교통일위원회-미주반': 'foreign_affairs',  # 국정감사; 59 mtgs; terms 19-22
    '외교통일위원회-아주반': 'foreign_affairs',  # 국정감사; 42 mtgs; terms 19-21
    '외교통일위원회-아중동반': 'foreign_affairs',  # 국정감사; 2 mtgs; terms 21-21
    '외교통일위원회-아프리카․중동반': 'foreign_affairs',  # 국정감사; 17 mtgs; terms 19-21
    '통일외교통상위원회': 'foreign_affairs',  # 국정감사,상임위원회; 286 mtgs; terms 16-17
    '통일외교통상위원회-구주․중동반': 'foreign_affairs',  # 국정감사; 4 mtgs; terms 16-16
    '통일외교통상위원회-구주반': 'foreign_affairs',  # 국정감사; 25 mtgs; terms 16-17
    '통일외교통상위원회-미주반': 'foreign_affairs',  # 국정감사; 32 mtgs; terms 16-17
    '통일외교통상위원회-아주반': 'foreign_affairs',  # 국정감사; 24 mtgs; terms 16-17
    '통일외교통상위원회-아태주반': 'foreign_affairs',  # 국정감사; 4 mtgs; terms 16-16
    '통일외교통상위원회-아프리카․중동반': 'foreign_affairs',  # 국정감사; 7 mtgs; terms 17-17
    '통일외교통상위원회-중동반': 'foreign_affairs',  # 국정감사; 4 mtgs; terms 17-17
    # --- gender_family ---
    '여성가족위원회': 'gender_family',  # 국정감사,상임위원회; 242 mtgs; terms 17-22
    '여성위원회': 'gender_family',  # 국정감사,상임위원회; 63 mtgs; terms 16-18
    # --- health_welfare ---
    '보건복지가족위원회': 'health_welfare',  # 국정감사,상임위원회; 64 mtgs; terms 18-18
    '보건복지위원회': 'health_welfare',  # 국정감사,상임위원회; 770 mtgs; terms 16-22
    # --- industry ---
    '산업자원위원회': 'industry',  # 국정감사,상임위원회; 333 mtgs; terms 16-17
    '산업통상자원위원회': 'industry',  # 국정감사,상임위원회; 151 mtgs; terms 19-20
    '산업통상자원중소벤처기업위원회': 'industry',  # 국정감사,상임위원회; 273 mtgs; terms 20-22
    '지식경제위원회': 'industry',  # 국정감사,상임위원회; 209 mtgs; terms 18-19
    '지식경제위원회-제1반': 'industry',  # 국정감사; 2 mtgs; terms 18-19
    '지식경제위원회-제2반': 'industry',  # 국정감사; 2 mtgs; terms 18-19
    '지식경제위원회-제3반': 'industry',  # 국정감사; 1 mtgs; terms 18-18
    # --- intelligence ---
    '정보위원회': 'intelligence',  # 상임위원회; 49 mtgs; terms 16-22
    # --- judiciary ---
    '법제사법위원회': 'judiciary',  # 국정감사,상임위원회; 1384 mtgs; terms 16-22
    '법제사법위원회-제1반': 'judiciary',  # 국정감사; 14 mtgs; terms 16-22
    '법제사법위원회-제2반': 'judiciary',  # 국정감사; 14 mtgs; terms 16-22
    # --- land_transport ---
    '건설교통위원회': 'land_transport',  # 국정감사,상임위원회; 308 mtgs; terms 16-17
    '건설교통위원회-제1반': 'land_transport',  # 국정감사; 4 mtgs; terms 16-16
    '건설교통위원회-제2반': 'land_transport',  # 국정감사; 5 mtgs; terms 16-16
    '국토교통위원회': 'land_transport',  # 국정감사,상임위원회; 345 mtgs; terms 19-22
    '국토교통위원회-제1반': 'land_transport',  # 국정감사; 9 mtgs; terms 19-22
    '국토교통위원회-제2반': 'land_transport',  # 국정감사; 9 mtgs; terms 19-22
    '국토해양위원회': 'land_transport',  # 국정감사,상임위원회; 168 mtgs; terms 18-19
    # --- political_affairs ---
    '정무위원회': 'political_affairs',  # 국정감사,상임위원회; 894 mtgs; terms 16-22
    '정무위원회-동경반': 'political_affairs',  # 국정감사; 1 mtgs; terms 19-19
    '정무위원회-북경반': 'political_affairs',  # 국정감사; 1 mtgs; terms 19-19
    # --- public_admin ---
    '안전행정위원회': 'public_admin',  # 국정감사,상임위원회; 161 mtgs; terms 19-20
    '안전행정위원회-제1반': 'public_admin',  # 국정감사; 20 mtgs; terms 19-19
    '안전행정위원회-제2반': 'public_admin',  # 국정감사; 20 mtgs; terms 19-19
    '행정안전위원회': 'public_admin',  # 국정감사,상임위원회; 512 mtgs; terms 18-22
    '행정안전위원회-제1반': 'public_admin',  # 국정감사; 62 mtgs; terms 18-21
    '행정안전위원회-제2반': 'public_admin',  # 국정감사; 62 mtgs; terms 18-21
    '행정자치위원회': 'public_admin',  # 국정감사,상임위원회; 338 mtgs; terms 16-17
    '행정자치위원회-제1반': 'public_admin',  # 국정감사; 48 mtgs; terms 16-17
    '행정자치위원회-제2반': 'public_admin',  # 국정감사; 50 mtgs; terms 16-17
    # --- science_ict ---
    '과학기술정보방송통신위원회': 'science_ict',  # 국정감사,상임위원회; 268 mtgs; terms 20-22
    '과학기술정보방송통신위원회-제2반': 'science_ict',  # 국정감사; 2 mtgs; terms 21-21
    '과학기술정보통신위원회': 'science_ict',  # 국정감사,상임위원회; 296 mtgs; terms 16-17
    '미래창조과학방송통신위원회': 'science_ict',  # 국정감사,상임위원회; 148 mtgs; terms 19-20
}

SPECIAL_COMMITTEE_RAW_NAMES_V9 = {
    '국정조사': [
        '가습기살균제사고진상규명과피해구제및재발방지대책마련을위한국정조사특별위원회',  # 15 mtgs; terms 20-20
        '개인정보대량유출관련실태조사및재발방지를위한국정조사(정무위원회)',  # 7 mtgs; terms 19-19
        '공공의료정상화를위한국정조사특별위원회',  # 10 mtgs; terms 19-19
        '공적자금국정조사특별위원회',  # 2 mtgs; terms 16-16
        '공적자금의운용실태규명을위한국정조사특별위원회',  # 1 mtgs; terms 16-16
        '국가정보원댓글의혹사건등의진상규명을위한국정조사특별위원회',  # 15 mtgs; terms 19-19
        '국무총리실산하민간인불법사찰및증거인멸사건의진상규명을위한국정조사특별위원회',  # 2 mtgs; terms 19-19
        '미국산쇠고기수입위생조건개정관련한.미기술협의의과정및협정내용의실태규명을위한국정조사특별위원회',  # 15 mtgs; terms 18-18
        '박근혜정부의최순실등민간인에의한국정농단의혹사건진상규명을위한국정조사특별위원회',  # 18 mtgs; terms 20-20
        '세월호침몰사고의진상규명을위한국정조사특별위원회',  # 13 mtgs; terms 19-19
        '쌀관세화유예연장협상의실태규명을위한국정조사특별위원회',  # 10 mtgs; terms 17-17
        '쌀소득보전직접지불금불법수령사건실태규명을위한국정조사특별위원회',  # 11 mtgs; terms 18-18
        '용산이태원참사진상규명과재발방지를위한국정조사특별위원회',  # 9 mtgs; terms 21-21
        '윤석열정부의비상계엄선포를통한내란혐의진상규명국정조사특별위원회',  # 2 mtgs; terms 22-22
        '이라크내테러집단에의한한국인피살사건관련진상조사특별위원회',  # 14 mtgs; terms 17-17
        '저축은행비리의혹진상규명을위한국정조사특별위원회',  # 19 mtgs; terms 18-18
        '정부및공공기관등의해외자원개발진상규명을위한국정조사특별위원회',  # 10 mtgs; terms 19-19
        '최근일련의언론사태진상규명을위한국정조사특별위원회',  # 1 mtgs; terms 16-16
        '한빛은행대출관련의혹사건등의진상조사를위한국정조사특별위원회',  # 17 mtgs; terms 16-16
    ],
    '국회본회의': [
        '국회본회의',  # 1058 mtgs; terms 16-22
    ],
    '예산결산특별위원회': [
        '예산결산특별위원회',  # 832 mtgs; terms 16-22
    ],
    '인사청문특별위원회': [
        '',  # 1 mtgs; terms None-None
        '감사원장(김황식)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '감사원장(양건)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '감사원장(윤성식)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 16-16
        '감사원장(전윤철)임명동의에관한인사청문특별위원회',  # 6 mtgs; terms 16-17
        '감사원장(최재해)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 21-21
        '감사원장(최재형)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 20-20
        '감사원장(황찬현)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 19-19
        '국무총리(김민석)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 22-22
        '국무총리(김부겸)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 21-21
        '국무총리(김석수)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 16-16
        '국무총리(이낙연)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '국무총리(이완구)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 19-19
        '국무총리(이한동)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 16-16
        '국무총리(이해찬)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 17-17
        '국무총리(장대환)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 16-16
        '국무총리(장상)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 16-16
        '국무총리(정세균)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '국무총리(정운찬)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 18-18
        '국무총리(한덕수)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 17-17
        '국무총리(한명숙)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 17-17
        '국무총리(황교안)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 19-19
        '국무총리후보자(고건)에관한인사청문특별위원회',  # 3 mtgs; terms 16-16
        '국무총리후보자(정홍원)에관한인사청문특별위원회',  # 5 mtgs; terms 19-19
        '국무총리후보자(한덕수)에관한인사청문특별위원회',  # 6 mtgs; terms 21-21
        '국무총리후보자(한승수)에관한인사청문특별위원회',  # 4 mtgs; terms 17-17
        '대법관(고영한.김병화.김신.김창석)임명동의에관한인사청문특별위원회',  # 6 mtgs; terms 19-19
        '대법관(고현철)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 16-16
        '대법관(권순일)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 19-19
        '대법관(권영준·서경환)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 21-21
        '대법관(김능환.박일환.안대희.이홍훈.전수안)임명동의에관한인사청문특별위원회',  # 5 mtgs; terms 17-17
        '대법관(김상환)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '대법관(김선수·노정희·이동원)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 20-20
        '대법관(김소영)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 19-19
        '대법관(김영란)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 17-17
        '대법관(김용담)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 16-16
        '대법관(김용덕.박보영)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 18-18
        '대법관(김재형)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '대법관(김지형)임명동의에관한인사청문특별위원회',  # 5 mtgs; terms 17-17
        '대법관(김황식)임명동의에관한인사청문특별위원회',  # 5 mtgs; terms 17-17
        '대법관(노경필·박영재·이숙연)임명동의에관한인사청문특별위원회',  # 6 mtgs; terms 22-22
        '대법관(노태악)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '대법관(마용주)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 22-22
        '대법관(민유숙·안철상)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 20-20
        '대법관(민일영)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '대법관(박병대)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '대법관(박상옥)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 19-19
        '대법관(박시환)임명동의에관한인사청문특별위원회',  # 5 mtgs; terms 17-17
        '대법관(박정화·조재연)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 20-20
        '대법관(신숙희·엄상필)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 21-21
        '대법관(신영철)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '대법관(양승태)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 17-17
        '대법관(양창수)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '대법관(오경미)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 21-21
        '대법관(오석준)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 21-21
        '대법관(이기택)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 19-19
        '대법관(이상훈)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '대법관(이인복)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 18-18
        '대법관(이흥구)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 21-21
        '대법관(조희대)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 19-19
        '대법관(차한성)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 17-17
        '대법관(천대엽)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 21-21
        '대법원장(김명수)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 20-20
        '대법원장(양승태)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 18-18
        '대법원장(이균용)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 21-21
        '대법원장(이용훈)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 17-17
        '대법원장(조희대)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 21-21
        '법제사법위원회',  # 2 mtgs; terms 21-21
        '인사청문특별위원회',  # 19 mtgs; terms 16-19
        '인사청문특별위원회(헌재)',  # 4 mtgs; terms 16-16
        '중앙선거관리위원회위원(김영철)선출에관한인사청문특별위원회',  # 2 mtgs; terms 16-16
        '중앙선거관리위원회위원(남래진)선출에관한인사청문특별위원회',  # 1 mtgs; terms 21-21
        '중앙선거관리위원회위원(문상부)선출에관한인사청문특별위원회',  # 1 mtgs; terms 21-21
        '중앙선거관리위원회위원(유승삼.제갈융우)선출에관한인사청문특별위원회',  # 1 mtgs; terms 17-17
        '중앙선거관리위원회위원(이상환·김용호)선출에관한인사청문특별위원회',  # 2 mtgs; terms 19-19
        '중앙선거관리위원회위원(이한구)선출에관한인사청문특별위원회',  # 1 mtgs; terms 18-18
        '중앙선거관리위원회위원선출에관한인사청문특별위원회',  # 3 mtgs; terms 16-16
        '행정안전위원회',  # 1 mtgs; terms 22-22
        '헌법재판소장(김이수)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 20-20
        '헌법재판소장(박한철)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 19-19
        '헌법재판소장(유남석)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '헌법재판소장(이종석)임명동의에관한인사청문특별위원회',  # 2 mtgs; terms 21-21
        '헌법재판소장(이진성)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '헌법재판소장(전효숙)임명동의및헌법재판소재판관(목영준.이동흡)선출에관한인사청문특별위원회',  # 9 mtgs; terms 17-17
        '헌법재판소재판관(강일원.김이수.안창호)선출에관한인사청문특별위원회',  # 5 mtgs; terms 19-19
        '헌법재판소재판관(마은혁·정계선·조한창)선출에관한인사청문특별위원회',  # 4 mtgs; terms 22-22
        '헌법재판소재판관(이상경)선출에관한인사청문특별위원회',  # 3 mtgs; terms 16-16
        '헌법재판소재판관(조대현)선출에관한인사청문특별위원회',  # 3 mtgs; terms 17-17
        '헌법재판소재판관(조용환)선출에관한인사청문특별위원회',  # 4 mtgs; terms 18-18
        '헌법재판소재판관선출에관한인사청문특별위원회',  # 1 mtgs; terms 20-20
        '헌법재판소재판관후보자를겸하는헌법재판소장(김상환)임명동의에관한인사청문특별위원회',  # 1 mtgs; terms 22-22
        '헌법재판소재판관후보자를겸하는헌법재판소장(이동흡)임명동의에관한인사청문특별위원회',  # 4 mtgs; terms 19-19
        '헌법재판소재판관후보자를겸한헌법재판소장(이강국)임명동의에관한인사청문특별위원회',  # 3 mtgs; terms 17-17
    ],
}


# build_v8.py:49-53 + build_v6.py:244
SPECIAL_HEARING_TYPE_TO_KEY = {
    "인사청문특별위원회": "confirmation_special",
    "국정조사": "investigation",
    "예산결산특별위원회": "budget_special",
    "국회본회의": "plenary",
}

# docs/v9/PIPELINE_v9.md:126: "Subcommittee names are mapped by stripping the suffix".
# In v9 the suffixes present are 국정감사 반 names ('-제1반', '-미주반', '-아프리카․중동반', ...).
SUBCOMMITTEE_SUFFIX_RE = re.compile(r"-[^-]+$")


def harmonize_committee(raw, hearing_type) -> Optional[str]:
    """Key for (raw committee, hearing_type) as in v9; None if unknown (v10 must fail loudly)."""
    if hearing_type in SPECIAL_HEARING_TYPE_TO_KEY:
        return SPECIAL_HEARING_TYPE_TO_KEY[hearing_type]
    if _isna(raw):
        return None
    raw = str(raw).strip()
    if raw in COMMITTEE_KEY_MAP_STANDING:
        return COMMITTEE_KEY_MAP_STANDING[raw]
    parent = SUBCOMMITTEE_SUFFIX_RE.sub("", raw)
    return COMMITTEE_KEY_MAP_STANDING.get(parent)


# =============================================================================
# 7. HEARING TYPES
# =============================================================================

HEARING_TYPES = {
    "상임위원회": dict(
        source="v5 XLSX (의안정보시스템 '제{term}대 국회 상임위원회 회의록 데이터셋')",
        definition="Standing-committee meetings incl. their 소위 and in-committee 인사청문회/공청회 "
                   "(minister nominee hearings are 상임위원회 rows with role minister_nominee)",
        committee_key="standing-committee key (20)", v9_meetings=9674),
    "국정감사": dict(
        source="v5 XLSX ('... 국정감사 회의록 데이터셋')",
        definition="Annual national audit sessions by standing committees, incl. '-제N반' field teams",
        committee_key="standing-committee key (19; no intelligence)", v9_meetings=4805),
    "인사청문특별위원회": dict(
        source="v6 HTML scrape (42 mtgs) + v7 PDF (228 mtgs)",
        definition="Confirmation-hearing SPECIAL committees (PM, justices, BAI head, NEC, 헌재). "
                   "v9 also files 3 standing-committee meetings here (법제사법위원회 2021-01-19/25, "
                   "행정안전위원회 2025-03-06) and meeting 43038 with empty metadata",
        committee_key="confirmation_special", v9_meetings=270),
    "국정조사": dict(
        source="v8 assembly_hearing_pipeline (XML viewer + PDF)",
        definition="Parliamentary investigation special committees (and one 정무위원회-run 국정조사, "
                   "one 진상조사특별위원회)",
        committee_key="investigation", v9_meetings=191),
    "예산결산특별위원회": dict(
        source="v8 assembly_hearing_pipeline", definition="Budget & Accounts special committee incl. 소위",
        committee_key="budget_special", v9_meetings=832),
    "국회본회의": dict(
        source="v8 assembly_hearing_pipeline", definition="Plenary sessions",
        committee_key="plenary", v9_meetings=1058),
}

# enrich_with_vconfdetail.py:97-113 (VCONFDETAIL flag -> column); ran on v5 only,
# the columns were not carried into v6-v9 (build_v7.py:74-93 skips silently).
VCONFDETAIL_FLAG_COLUMNS = {
    "is_confirmation_hearing": "HR_HRG_YN",
    "is_public_hearing": "PBHRG_YN",
    "is_investigation_hearing": "HRG_YN",
    "is_joint_session": "SITG_YN",
    "conf_start_time": "BG_PTM",
    "conf_end_time": "ED_PTM",
    "minutes_pdf_url": "DOWN_URL",
}


def vconfdetail_primary_key_v5(meeting_id) -> str:
    """enrich_with_vconfdetail.py:37,55: both sides str().zfill(6).
    KNOWN-BUG: maps the v6 HTML id '52162' (a CONFER_NUM, 2024-07-22 인사청문) and
    the XLSX id '052162' (a CONF_ID, 2022-09-23 교육위원회) to the same key."""
    return str(meeting_id).zfill(6)


def vconfdetail_flag_value_v5(yn) -> bool:
    """enrich_with_vconfdetail.py:105-107: 'Y' -> True; anything else, INCLUDING an
    unmatched meeting (NA), -> False. Unknown and 'no' are indistinguishable."""
    return (not _isna(yn)) and yn == "Y"
# Fallback join (enrich_with_vconfdetail.py:72-78): VCONFDETAIL deduplicated on
# (CONF_DT, CMIT_NM) keeping the FIRST row, then joined on (date, committee), so
# every same-day meeting of a committee receives the first meeting's record
# (29 CONFER_NUMs are shared by 2-3 XLSX meetings in v5_enriched).


def meeting_id_namespace_v9(meeting_id, hearing_type, v6_html_ids=frozenset()) -> str:
    """Namespace of a v9 meeting_id, as measured against the Open API lists
    (04_meeting_id_namespace.py): 'CONF_ID' for XLSX, v7 PDF 인사청문 and v8 rows;
    'CONFER_NUM' for the 42 v6 HTML 인사청문 meetings (pass their ids; they are
    the ids in all_speeches_16_22_v6 but not in _v5). Pure lookup, no I/O."""
    if hearing_type == "인사청문특별위원회" and str(meeting_id) in v6_html_ids:
        return "CONFER_NUM"
    return "CONF_ID"


def conf_id_key(meeting_id) -> str:
    """CONF_ID as the Open API prints it (6 digits, zero-padded)."""
    return str(meeting_id).zfill(6)


# =============================================================================
# 8. MINISTRY NORMALIZATION + MINISTER PANEL LINKAGE (VERBATIM build_v9.py)
# =============================================================================

MINISTRY_TYPO_MAP = {  # build_v9.py:61-108
    "법부무": "법무부", "범무부": "법무부", "법무무": "법무부",
    "행장자치부": "행정자치부", "안정행정부": "안전행정부", "안전행전부": "안전행정부",
    "해앙수산부": "해양수산부", "해수부": "해양수산부", "정통부": "정보통신부",
    "교육적인자원부": "교육인적자원부", "교육인적자부": "교육인적자원부",
    "재정경재부": "재정경제부", "문환관광부": "문화관광부", "문화관부": "문화관광부",
    "교육기술부": "교육과학기술부", "교육과술부": "교육과학기술부",
    "교육과학기술기술부": "교육과학기술부", "문화체육광부": "문화체육관광부",
    "과학기술정보통부": "과학기술정보통신부", "농림축신식품부": "농림축산식품부",
    "농림축삭식품부": "농림축산식품부", "농림수산식품": "농림수산식품부",
    "산업지원부": "산업자원부", "산업자원통상부": "산업통상자원부",
    "여성가족주": "여성가족부", "통상외교부": "외교통상부", "해농림부": "농림부",
    "여성부가족부": "여성가족부", "여성부가족": "여성가족부",
    "보건복지부가족부": "보건복지부", "보건복지부보건복지부": "보건복지부",
    "보건복지부보": "보건복지부", "국방": "국방부", "과학기술": "과학기술부",
    "부총리겸교육적인자원부": "교육인적자원부", "농림수산식품부제": "농림수산식품부",
    "행정자치부장관후": "행정자치부", "행정자치부장관": "행정자치부",
    "재경경제부제1": "재정경제부", "재경부": "재정경제부", "재정경제": "재정경제부",
    "행전안전부제1": "행정안전부", "문화관광체육부제1": "문화체육관광부",
    "문화체육부제1": "문화체육관광부", "부총리겸재정경재부": "재정경제부",
    "부홍리겸재정경제부": "재정경제부",
}
MINISTRY_HISTORICAL_MAP = {"여성부": "여성가족부", "보건복지가족부": "보건복지부", "특임": "특임장관"}  # :111-115
PERSON_NAME_FIXES = {"졍종환": "정종환", "백영희": "백희영", "윤관웅": "윤광웅"}  # :118-122 (v9 overwrote person_name)

# KNOWN-BUG: build_v9.py:125-133 codes 김대중 as "Conservative" (the panel CSV codes
# him "Progressive"); and leaves 2017-03-11..2017-05-09 (acting president) unmapped.
ADMIN_TERMS_V9 = (
    ("김대중", "Conservative", "1998-02-25", "2003-02-24"),
    ("노무현", "Progressive", "2003-02-25", "2008-02-24"),
    ("이명박", "Conservative", "2008-02-25", "2013-02-24"),
    ("박근혜", "Conservative", "2013-02-25", "2017-03-10"),
    ("문재인", "Progressive", "2017-05-10", "2022-05-09"),
    ("윤석열", "Conservative", "2022-05-10", "2025-06-03"),
    ("이재명", "Progressive", "2025-06-04", "2099-12-31"),
)
# Corrected ideology only (matches minister_panel_comprehensive.csv admin_ideology).
# The 2017-03-11..2017-05-09 gap is left open on purpose: coding it is a research decision.
ADMIN_TERMS_CORRECTED = tuple(
    (a, "Progressive" if a == "김대중" else i, s, e) for a, i, s, e in ADMIN_TERMS_V9
)


def normalize_ministry(raw):
    """VERBATIM build_v9.py:136-254."""
    if _isna(raw) or str(raw).strip() == "":
        return None
    s = str(raw).strip()
    if re.match(r"^리[공실본부팀]", s) or s.startswith("후보장") or s.startswith("후보 "):
        return None
    m = re.match(r"^보\s+(\S+부|국방부)$", s)
    if m:
        s = m.group(1)
    m = re.match(r"^보\s+(\S+부)\S*$", s)
    if m:
        s = m.group(1)
    s = re.sub(r"^부[총홍]리겸", "", s)
    m = re.match(r"^정책보좌관\s+(\S+)$", s)
    if m:
        s = m.group(1)
    s = re.sub(r"장관(후보자|직무대행|직무대리|정책보좌관)?(\s+\S+)?$", "", s)
    s = re.sub(r"차관(보)?(\s+\S+)?$", "", s)
    s = re.sub(r"제[12](차관.*)?$", "", s)
    s = re.sub(r"[12]차관.*$", "", s)
    s = re.sub(r"2$", "", s)
    s = re.sub(r"차관(보)?$", "", s)
    m = re.match(r"^실\S*\s+(.+)$", s)
    if m:
        s = m.group(1)
    m = re.match(r"^\S+관\s+(\S+부)$", s)
    if m:
        s = m.group(1)
    m = re.match(r"^\S+국장\s+(\S+부)$", s)
    if m:
        s = m.group(1)
    m = re.match(r"^\S+\s+(\S+부)$", s)
    if m and not s.endswith("부"):   # dead branch: the pattern requires s to end with '부'
        s = m.group(1)
    s = re.sub(r"(처|청|원|실|본부)장\s+\S+$", r"\1", s)
    s = re.sub(r"^(서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주)\S*(지방|유역)", "", s)
    s = re.sub(r"^(서울|부산|대구|인천|광주|대전|울산|세종)\S*지방", "", s)
    s = s.strip()
    if s in MINISTRY_TYPO_MAP:
        s = MINISTRY_TYPO_MAP[s]
    s = re.sub(r"제[12]$", "", s).strip()
    if s in MINISTRY_HISTORICAL_MAP:
        s = MINISTRY_HISTORICAL_MAP[s]
    if s and re.search(r"(공사|보험|부동산원|㈜|은행총재|한국은행)", s):
        return None
    if s and len(s) >= 10 and not re.search(r"(부|처|청|원|총리|장관|통상|본부|실)$", s):
        return None
    return s if s else None


def infer_admin_from_date(date_str, admin_terms=ADMIN_TERMS_V9):
    """VERBATIM build_v9.py:257-269 (pass ADMIN_TERMS_CORRECTED for the fixed version).
    The original parsed with pd.Timestamp(date_str); v9 dates are all 'YYYY-MM-DD'
    (or null for meeting 43038), for which the two parsers agree."""
    if _isna(date_str) or not date_str:
        return None, None
    try:
        dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None, None
    for admin, ideology, start, end in admin_terms:
        if datetime.strptime(start, "%Y-%m-%d") <= dt <= datetime.strptime(end, "%Y-%m-%d"):
            return admin, ideology
    return None, None


def link_minister_panel_v9(person_name, ministry_normalized, date, candidates: Sequence[Mapping]):
    """VERBATIM matching cascade of build_v9.py:369-418 for ONE (person_name, meeting).

    `candidates`: panel rows for this name, IN FILE ORDER, each a mapping with
    'ministry', 'start_dt', 'end_dt' (datetime; end NA -> 2099-12-31, start NA -> no date match),
    'dual_office', 'admin', 'admin_ideology'. `date`: datetime or None.
    Returns (panel_row or None, rule_name).

    KNOWN-BUG: fallback_2 (ministry only, any date) and fallback_3 (single entry,
    any ministry, any date) assign an administration outside the appointment
    period; 18,111 XLSX-era minister-type rows in v9 carry an admin different
    from the date-implied one (report section 4).
    Note: v9 deduplicated on (person_name, meeting_id) BEFORE matching
    (build_v9.py:350), so the ministry of the first row of that name in the
    meeting is used for all of them."""
    def _dm(c):  # pd.notna(date) and pd.notna(start_dt) and start <= date <= end
        return (not _isna(date) and not _isna(c.get("start_dt"))
                and c["start_dt"] <= date <= c["end_dt"])

    def _mm(c):  # original: `is not None` tests; a NaN ministry never equals a string
        return (ministry_normalized is not None and c.get("ministry") is not None
                and ministry_normalized == c["ministry"])

    for c in candidates:
        if _mm(c) and _dm(c):
            return c, "exact_name_ministry_date"
    for c in candidates:
        if _dm(c):
            return c, "fallback_1_name_date"
    for c in candidates:
        if _mm(c):
            return c, "fallback_2_name_ministry_anydate"
    if len(candidates) == 1:
        return candidates[0], "fallback_3_single_entry"
    return None, "unmatched"
# After linkage, unmatched rows get admin/admin_ideology from infer_admin_from_date
# (build_v9.py:457-477) and dual_office stays None.


# =============================================================================
# 9. LEGISLATOR METADATA JOIN
# =============================================================================
# XLSX era (pipeline 01, not in repo): name_clean/party/ruling_status/seniority/
# gender/naas_cd came from a "National Assembly member database" keyed by
# member_id (docs/v9/CODEBOOK_v9.md:85). Values are TERM-START snapshots (CODEBOOK caveat).
# v6-v8: build_v6.py:281-341 (VERBATIM logic below) and the same join inside
# the v7/v8 upstream pipelines ("mp_metadata enrichment", build_v8.py:13).

MP_META_ENRICH_COLS = ("party", "ruling_status", "seniority", "gender", "naas_cd")


def enrich_legislator_v6(person_name, term, meta_by_name_term: Mapping, current: Mapping) -> dict:
    """VERBATIM semantics of build_v6.py:293-331 for one row.
    meta_by_name_term: {(name, term): {party, ruling_status, seniority, gender, naas_cd}}
    built with drop_duplicates(subset=['name','term']) i.e. FIRST record wins
    (KNOWN-BUG: two legislators with the same name in one term get one record).
    Only fills columns that are currently NA; name_clean := person_name on match.
    NB: v6 applies this to ALL rows, not only legislator roles (the merge is not
    filtered by role), so a minister who shares a name with a sitting legislator
    of that term would receive party metadata."""
    out = dict(current)
    rec = meta_by_name_term.get((person_name, term))
    if rec is None:
        return out
    for col in MP_META_ENRICH_COLS:
        if _isna(out.get(col)) and not _isna(rec.get(col)):
            out[col] = rec[col]
    if _isna(out.get("name_clean")):
        out["name_clean"] = person_name
    return out


def clean_ruling_status_v9(value):
    """VERBATIM build_v9.py:497-514: '' -> None. Only ruling_status was cleaned;
    party, naas_cd, gender, member_id, member_uid keep '' in v6-v8 rows."""
    return None if value == "" else value


# OBSERVED in v9 (not code): ruling_status on legislator rows by (term, party).
# Generated 2026-09-25 from data/all_speeches_16_22_v9.parquet with
#   select term, party, ruling_status, count(*) ... where role in ('legislator','chair')
#   and party <> '' group by all
# It is constant within (term, party) except 18대 한나라당 (1,372 'opposition' rows,
# all member_uid 6182_A, the harmonize_homonym_metadata_v5 artefact). It is NOT a
# term-start snapshot for 20대 (더불어민주당 'ruling', 새누리당 'opposition' for the
# whole term, including 2016-06..2017-05) and it ignores the 2022-05-10 change in 21대
# and the 2025-06-04 change in 22대. Satellite parties are inconsistent (21대
# 더불어시민당 'ruling', 22대 국민의미래 'opposition' while 국민의힘 is 'ruling').
# Values: (status, rows). Reproduction table only; do not reuse as a coding rule.
RULING_STATUS_BY_TERM_PARTY_V9_OBSERVED = {
    (16, "개혁국민정당"): (("ruling", 883),), (16, "무소속"): (("independent", 7044),),
    (16, "민주국민당"): (("opposition", 590),), (16, "새천년민주당"): (("ruling", 170089),),
    (16, "자유민주연합"): (("opposition", 27141),), (16, "한국신당"): (("opposition", 59),),
    (16, "한나라당"): (("opposition", 317561),),
    (17, "국민중심당"): (("opposition", 27),), (17, "국민통합21"): (("opposition", 1913),),
    (17, "대통합민주신당"): (("ruling", 351),), (17, "무소속"): (("independent", 3325),),
    (17, "민주노동당"): (("opposition", 35862),), (17, "민주당"): (("ruling", 4217),),
    (17, "새천년민주당"): (("ruling", 15685),), (17, "열린우리당"): (("ruling", 415465),),
    (17, "자유민주연합"): (("opposition", 8775),), (17, "한나라당"): (("opposition", 418960),),
    (18, "무소속"): (("independent", 92085),), (18, "민주노동당"): (("opposition", 22683),),
    (18, "민주당"): (("opposition", 23268),), (18, "민주통합당"): (("opposition", 2151),),
    (18, "새누리당"): (("ruling", 2333),), (18, "자유선진당"): (("opposition", 55673),),
    (18, "진보신당"): (("opposition", 2396),), (18, "창조한국당"): (("opposition", 12042),),
    (18, "친박연대"): (("opposition", 31964),), (18, "통합민주당"): (("opposition", 331256),),
    (18, "한나라당"): (("ruling", 459053), ("opposition", 1372)),
    (19, "무소속"): (("independent", 14681),), (19, "민주통합당"): (("opposition", 515668),),
    (19, "새누리당"): (("ruling", 413282),), (19, "새정치민주연합"): (("opposition", 7833),),
    (19, "자유선진당"): (("opposition", 16062),), (19, "통합진보당"): (("opposition", 36510),),
    (19, "한나라당"): (("opposition", 1641),),
    (20, "국민의당"): (("opposition", 110593),), (20, "더불어민주당"): (("ruling", 370201),),
    (20, "무소속"): (("independent", 32088),), (20, "바른미래당"): (("opposition", 1057),),
    (20, "새누리당"): (("opposition", 365205),), (20, "열린우리당"): (("ruling", 1569),),
    (20, "자유한국당"): (("opposition", 10220),), (20, "정의당"): (("opposition", 14987),),
    (21, "국민의당"): (("opposition", 6197),), (21, "국민의힘"): (("opposition", 10704),),
    (21, "기본소득당"): (("opposition", 2562),), (21, "더불어민주당"): (("ruling", 458149),),
    (21, "더불어시민당"): (("ruling", 42314),), (21, "무소속"): (("independent", 11635),),
    (21, "미래통합당"): (("opposition", 235702),), (21, "미래한국당"): (("opposition", 42185),),
    (21, "시대전환"): (("opposition", 3880),), (21, "열린민주당"): (("opposition", 10779),),
    (21, "정의당"): (("opposition", 14520),), (21, "진보당"): (("opposition", 534),),
    (22, "개혁신당"): (("opposition", 2132),), (22, "국민의미래"): (("opposition", 10387),),
    (22, "국민의힘"): (("ruling", 73471),), (22, "더불어민주당"): (("opposition", 182199),),
    (22, "더불어민주연합"): (("opposition", 10609),), (22, "새로운미래"): (("opposition", 521),),
    (22, "조국혁신당"): (("opposition", 9420),), (22, "진보당"): (("opposition", 682),),
}


# deep_audit.py:49-57 - listed for completeness; NOT used to build data, and
# wrong as a ruling-party table (16대 ruling party was 새천년민주당, not 한나라당;
# 20대 lists both main parties). Do not reuse.
RULING_PARTIES_BY_TERM_DEEP_AUDIT = {
    16: ["한나라당"], 17: ["열린우리당", "한나라당"], 18: ["한나라당"], 19: ["새누리당"],
    20: ["더불어민주당", "자유한국당"], 21: ["더불어민주당"], 22: ["국민의힘"],
}


# =============================================================================
# 10. DYADS
# =============================================================================

def _side(role):
    if role in LEG_ROLES:
        return "L"
    if role in NONLEG_ROLES:
        return "N"
    return None


def build_dyad_index_pairs(rows: Sequence[Mapping], order: str = "numeric"):
    """Return [(i_leg, i_wit, direction)] over `rows` of ONE meeting.

    order='numeric'        - correct: int(speech_order) (build_v5.py:416-417).
    order='lexicographic'  - reproduces v9 exactly (build_v9.py:529, build_v8.py:77,
                              build_v6.py:163: sort_values on a string column).
    Rows whose speech_order is not an integer are DROPPED in 'numeric' mode, as
    build_v4/v5 did silently via dropna (v10 should raise instead)."""
    if order == "numeric":
        idx = []
        for i, r in enumerate(rows):
            try:
                idx.append((int(str(r["speech_order"]).strip()), i))
            except (ValueError, TypeError):
                continue
        idx.sort()
        seq = [i for _, i in idx]
    elif order == "lexicographic":
        seq = sorted(range(len(rows)), key=lambda i: str(rows[i]["speech_order"]))
    else:
        raise ValueError(order)
    out = []
    for a, b in zip(seq, seq[1:]):
        sa, sb = _side(rows[a]["role"]), _side(rows[b]["role"])
        if sa == "L" and sb == "N":
            out.append((a, b, "question"))
        elif sa == "N" and sb == "L":
            out.append((b, a, "answer"))
    return out
# Note: committee_staff/other/unknown rows stay in the sequence and break
# adjacency (they are 'X', neither side), as in every build script.


# Dyad record layout. v9 (build_v9.py:550-582) takes ALL meeting fields (term,
# committee, committee_key, hearing_type, date, agenda) from the LEGISLATOR row;
# v4/v6/v8 (build_v4.py:178-215, build_v6.py:170-209, build_v8.py:84-123) took them
# from the FIRST row of the pair (the witness row for 'answer' dyads); v5
# (build_v5.py:455-473) from the legislator row. Only agenda can differ within a
# meeting. No version stores the two speech_order values, member_id, naas_cd or a
# speech key, so a dyad cannot be traced back to its speeches except by text.
DYAD_FIELDS_V9 = {
    "meeting_id": ("meeting", "meeting_id"),
    "term": ("leg", "term"), "committee": ("leg", "committee"),
    "committee_key": ("leg", "committee_key"), "hearing_type": ("leg", "hearing_type"),
    "date": ("leg", "date"), "agenda": ("leg", "agenda"),
    "leg_name": ("leg", "person_name"), "leg_speaker_raw": ("leg", "speaker"),
    "leg_member_uid": ("leg", "member_uid|member_id"), "leg_party": ("leg", "party"),
    "leg_ruling_status": ("leg", "ruling_status"), "leg_seniority": ("leg", "seniority"),
    "leg_gender": ("leg", "gender"),
    "witness_name": ("wit", "person_name"), "witness_speaker_raw": ("wit", "speaker"),
    "witness_role": ("wit", "role"), "witness_affiliation": ("wit", "affiliation_raw"),
    "witness_ministry_normalized": ("wit", "ministry_normalized"),
    "witness_dual_office": ("wit", "dual_office"), "witness_admin": ("wit", "admin"),
    "witness_admin_ideology": ("wit", "admin_ideology"),
    "direction": ("pair", "question|answer"),
    "leg_speech": ("leg", "speech_text"), "witness_speech": ("wit", "speech_text"),
}


def make_dyad_record_v9(leg: Mapping, wit: Mapping, meeting_id, direction: str) -> dict:
    """VERBATIM build_v9.py:550-582 (`leg.get('member_uid', leg.get('member_id'))`)."""
    rec = {}
    for out_col, (side, src) in DYAD_FIELDS_V9.items():
        if side == "meeting":
            rec[out_col] = meeting_id
        elif side == "pair":
            rec[out_col] = direction
        elif src == "member_uid|member_id":
            rec[out_col] = leg.get("member_uid", leg.get("member_id"))
        else:
            rec[out_col] = (leg if side == "leg" else wit).get(src)
    return rec
