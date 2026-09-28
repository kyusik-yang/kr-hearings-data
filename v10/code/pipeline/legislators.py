"""legislators.py - link legislator-side speech turns to persons (NAAS_CD) and attach
person / member-term metadata (v10 pipeline, see docs/CODEBOOK.md).

Public API
    build_tables(write=True)          -> dict of DataFrames (persons, person_terms, committee_spells,
                                         memid_crosswalk); written to interim/pipeline/legislators/
    load_reference(refresh=False)     -> dict of reference tables (cached, built on first use)
    enrich(turns, meetings, ref=None) -> turns with added columns (row order preserved)
    split_label(label)                -> (pos, name) helper for labels such as '이수진(비) 위원'

Columns added by enrich (contract): naas_cd, leg_name_hangul, leg_name_hanja, gender, birth_date,
district, elect_type, seniority, id_method, id_confidence. Extra columns: id_candidates (int),
id_note, leg_side (bool), leg_side_basis, leg_title_class, leg_is_term_member, leg_seated_on_date,
leg_stint, leg_record_mem_id, id_label_repair, id_memid_status (what happened to a supplied viewer
mem_id: used / not_used_nonlegislator / not_used_name / not_in_crosswalk / corrected, see MEM_ID_CORRECTIONS), leg_date_basis
(speech_date / meeting_date: the date used for seat and committee checks).

Resolution order (first rule that yields exactly one person wins):
  a. speaker_mem_id -> NAAS_CD through the record member-term crosswalk (19-22대), legislator side
     only, and only when the printed name is the record name or one glyph from it. The id's member
     must be seated on the speech date; when it is not and exactly one same-name member of the term
     is seated, that member is taken instead (mem_id_seat_override, the viewer links 19대 이재영 to
     the 평택을 member after he lost his seat)
  b. (term, hangul name) unique among that term's members; initial-sound spelling (류/유, 리/이, ...)
     as a fallback (name_term_dueum)
  c. (term, hanja name) for hanja labels (16-17대 XML, HWP), with fallbacks: one-character variant,
     surname variant (裴/裵), API hanja names that contain hangul (설松雄 = 偰松雄), and a dropped
     first character (low)
  d. same-name members of a term: seat dates on the speech date, printed area, printed markers
     (elect type '(비)', district/region, party initial checked against the party lineage valid on the
     speech date), committee membership on the speech date, and the complement of a marked label in
     the same meeting
  e. one-syllable typo among members seated on the date and on the meeting's committee (low)
  f. otherwise null, with the reason in id_method
  A turn whose speaker label the parser rates label_confidence 'low' is never linked: the person
  columns are null and id_method is 'unlinked:label_confidence_low' (counted in out.attrs['legislators']).
id_confidence:
  high    record member-term id seated on the date; unique exact name (hangul or hanja) in the term;
          the only same-name member seated on the speech date
  medium  a derived cue: spelling variant, printed area or marker, committee roster, same-meeting
          complement, mem_id seat override, a mem_id whose printed name is one glyph from the record
          name, dual-office or panel-note link, or a name taken from the position/label because the
          name slot held a title (id_label_repair)
  low     conflicting cues, member not seated on the speech date, dropped glyph, one-syllable typo
Non-legislator titles (ministers, nominees) are linked only with evidence from a minister-panel row
of the office named in the title that covers the speech date (Resolver.panel_rows): dual office in
this term (sitting member), a panel note naming this term and only terms of the seated member, or,
for a unique former member, a note naming a term the person served or dual office at appointment in
a term whose seat 의원이력 confirms on the appointment date (all medium). Negated note mentions
('22대 아님') do not count, and an acting title ('...장관직무대행') is matched only to the official's
own other office. Name uniqueness alone is not used. A viewer mem_id on a non-legislator title is not
used. These turns keep their non-legislator role (this module never changes roles).

Reference sources: Open API downloads in interim/pipeline/legislators/api (legislators_fetch.py),
interim/members_record_memid_19_22.parquet, interim/members_memid_crosswalk_19_22.parquet,
interim/members_party_spells_21.parquet, assemblykor members_all_assemblies.csv (cross-check only),
minister-data minister_panel_comprehensive.csv (dual office).
"""
from __future__ import annotations

import datetime as dt
import glob
import json
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

V10 = Path(__file__).resolve().parents[2]
INTERIM = V10 / "interim"
OUTD = INTERIM / "pipeline" / "legislators"
APID = OUTD / "api"
# sibling checkouts of the assemblykor and minister-data repositories (next to this repository),
# overridable with the environment variables below
ASSEMBLYKOR_CSV = Path(os.environ.get("ASSEMBLYKOR_MEMBERS_CSV",
                                      V10.parents[1] / "assemblykor" / "data-raw" / "members_all_assemblies.csv"))
MINISTER_PANEL_CSV = Path(os.environ.get("MINISTER_PANEL_CSV",
                                         V10.parents[1] / "minister-data" / "data" / "minister_panel_comprehensive.csv"))
LINEAGE_CSV = INTERIM / "party_lineage.csv"

TERM_DATES = {  # constitutional terms (members_term_dates_openapi.csv, nokivirranikoinnk)
    16: ("2000-05-30", "2004-05-29"), 17: ("2004-05-30", "2008-05-29"), 18: ("2008-05-30", "2012-05-29"),
    19: ("2012-05-30", "2016-05-29"), 20: ("2016-05-30", "2020-05-29"), 21: ("2020-05-30", "2024-05-29"),
    22: ("2024-05-30", "2028-05-29"),
}

ADDED_COLUMNS = ["naas_cd", "leg_name_hangul", "leg_name_hanja", "gender", "birth_date", "district",
                 "elect_type", "seniority", "id_method", "id_confidence", "id_candidates", "id_note",
                 "leg_side", "leg_side_basis", "leg_title_class", "leg_is_term_member",
                 "leg_seated_on_date", "leg_stint", "leg_record_mem_id", "id_label_repair",
                 "id_memid_status", "leg_date_basis"]

# id_method values that link a person (everything else leaves naas_cd null)
LINKING_METHODS = (
    "mem_id", "mem_id_pos_name_swapped", "mem_id_seat_override", "mem_id_not_seated", "mem_id_term_mismatch",
    "mem_id_name_mismatch", "name_term", "name_term_dueum", "hanja_term", "hanja_term_variant",
    "hanja_term_surname_variant", "hanja_term_hangul_wildcard", "hanja_reading_name", "hanja_term_partial",
    "homonym_seat_dates",
    "homonym_area", "homonym_marker_elect_type", "homonym_marker_district", "homonym_marker_party",
    "homonym_committee", "homonym_meeting_complement", "name_fuzzy_committee",
    "nonleg_dual_office", "nonleg_sitting_member_panel_note", "nonleg_former_member_panel", "mem_id_corrected")

# Corrections of a viewer mem_id that names the other member of a same-name pair, each backed by the minutes:
# (conf_num, printed label, naas_cd given by the viewer mem_id) -> naas_cd of the speaker. id_method
# 'mem_id_corrected', id_memid_status 'corrected'.
MEM_ID_CORRECTIONS = {
    # 20th 헌법개정및정치개혁특별위원회, 2018-03-12. The chair calls on "김성태 대표님", and turn 65 (金成泰 위원) answers
    # "김성태 대표가 아니고 저는 김성태 헌정특위 위원입니다". Turn 67 (김성태 위원) is the same member answering 김경협,
    # but the viewer mem_id names BQS2021C (金聖泰). Found by the kna cross-check of 2026-09-28.
    (42927, "김성태 위원", "BQS2021C"): "9UW75767",
}


def _apply_mem_id_correction(conf_num, label, res: dict) -> dict:
    """res with MEM_ID_CORRECTIONS applied (unchanged when no correction matches). conf_num may arrive as the
    string key of enrich ('42927') or as an integer."""
    try:
        key = (int(str(conf_num).strip()), label, res.get("naas_cd"))
    except (TypeError, ValueError):
        return res
    fix = MEM_ID_CORRECTIONS.get(key)
    if fix is None:
        return res
    res.update(note=f"viewer mem_id gave {res['naas_cd']}; corrected from the minutes", naas_cd=fix,
               method="mem_id_corrected", confidence="high", memid_status="corrected")
    return res

# initial-sound law (두음법칙) spellings of the first syllable of a surname
DUEUM = {"류": "유", "유": "류", "리": "이", "이": "리", "라": "나", "나": "라", "로": "노", "노": "로",
         "림": "임", "임": "림", "량": "양", "양": "량", "려": "여", "여": "려", "렴": "염", "염": "렴",
         "룡": "용", "용": "룡", "륙": "육", "육": "륙", "뢰": "뇌", "뇌": "뢰"}
# hanja surname glyph variants seen in labels vs the API (裴基善 in 16대 XML, 裵基善 in ALLNAMEMBER)
HANJA_SURNAME_VARIANTS = {"裴": "裵", "裵": "裴"}

# ----------------------------------------------------------------------------- normalization

HANJA_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
HANGUL_NAME_RE = re.compile(r"^[가-힣]{2,5}$")
PAREN_RE = re.compile(r"[(（]([^()（）]{1,20})[)）]")
WS_RE = re.compile(r"\s+")


def nfkc(s) -> str:
    """NFKC (folds CJK compatibility ideographs, e.g. U+F9E1 李 -> U+674E) and drop all whitespace."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    return WS_RE.sub("", unicodedata.normalize("NFKC", str(s)))


def has_hanja(s) -> bool:
    return bool(HANJA_RE.search(s or ""))


HANJA_TABLE = V10 / "raw" / "third_party" / "hanja_table_0.15.1.yml"  # hanja 0.15.1: {glyph: reading}
_READINGS = {}


def hanja_readings() -> dict:
    """{hanja glyph: hangul reading} from the hanja 0.15.1 table (read once; empty if missing)."""
    if not _READINGS and HANJA_TABLE.exists():
        rx = re.compile(r'^"(.+?)":\s*"(.+?)"\s*$')
        for line in HANJA_TABLE.read_text(encoding="utf-8").splitlines():
            mm = rx.match(line.strip())
            if mm:
                k, v = json.loads('"' + mm.group(1) + '"'), json.loads('"' + mm.group(2) + '"')
                if len(k) == 1 and len(v) == 1:
                    _READINGS[k] = v
    return _READINGS


def same_reading(a, b, initial=False) -> bool:
    """Two glyphs (hanja or hangul) read the same. The initial-sound alternation (李 = 이/리) is allowed
    only for a word-initial glyph (initial=True): 金德容 (김덕용) is not a variant of 金德龍 (김덕룡)."""
    rd = hanja_readings()
    ra, rb = rd.get(a, a), rd.get(b, b)
    return ra == rb or (initial and DUEUM.get(ra) == rb)


_Y_VOWELS = {2, 3, 6, 7, 12, 17, 20}  # ㅑ ㅒ ㅕ ㅖ ㅛ ㅠ ㅣ


def dueum_form(ch) -> str:
    """Word-initial form of a hangul syllable (두음법칙): ㄹ -> ㅇ before ㅑㅒㅕㅖㅛㅠㅣ, else ㄴ;
    ㄴ -> ㅇ before those vowels. '로' -> '노', '녀' -> '여', '룡' -> '용'."""
    c = ord(ch) - 0xAC00
    if not 0 <= c < 11172:
        return ch
    cho, jung, jong = c // 588, (c % 588) // 28, c % 28
    if cho == 5:
        cho = 11 if jung in _Y_VOWELS else 2
    elif cho == 2 and jung in _Y_VOWELS:
        cho = 11
    return chr(0xAC00 + cho * 588 + jung * 28 + jong)


def reads_as(s, hangul) -> bool:
    """s (hanja, hangul or mixed) reads as the hangul string glyph by glyph, with the initial-sound
    alternation at every position (compound words: 環境勞動 = 환경노동, 女性 = 여성). Used only against
    closed name lists (committee and ministry names), never for person names."""
    if not s or len(s) != len(hangul):
        return False
    rd = hanja_readings()
    for a, b in zip(s, hangul):
        if a == b:
            continue
        ra = rd.get(a)
        if ra is None or dueum_form(ra) != dueum_form(b):
            return False
    return True


def hanja_to_hangul(s):
    """Hangul reading of a hanja name, or None when a glyph has no reading."""
    rd = hanja_readings()
    out = []
    for ch in s:
        if re.match(r"[\uac00-\ud7a3]", ch):
            out.append(ch)
        elif ch in rd:
            out.append(rd[ch])
        else:
            return None
    return "".join(out)


def _jamo(ch):
    c = ord(ch) - 0xAC00
    return (c // 588, (c % 588) // 28, c % 28) if 0 <= c < 11172 else None


def one_jamo_apart(a, b) -> bool:
    """Two hangul strings of equal length that differ in one syllable, by one jamo (초성/중성/종성)."""
    diff = [(x, y) for x, y in zip(a, b) if x != y]
    if len(a) != len(b) or len(diff) != 1:
        return False
    ja, jb = _jamo(diff[0][0]), _jamo(diff[0][1])
    return ja is not None and jb is not None and sum(u != v for u, v in zip(ja, jb)) == 1


_TRAIL_TITLE_RE = re.compile(r"(위원장|위원|의원|委員長|委員|議員|義員|議院|委)$")  # 委: truncated 委員


def clean_name(name):
    """Return (clean_name, markers). Markers are the parenthesized tokens, e.g. '이수진(비)' ->
    ('이수진', ['비']). A glued trailing title ('김성곤위원') is stripped when a 2-4 character name
    remains."""
    s = nfkc(name)
    markers = PAREN_RE.findall(s)
    s = PAREN_RE.sub("", s)
    s = re.sub(r"[()（）;:,.。、·・○◯]", "", s)
    m = _TRAIL_TITLE_RE.search(s)
    if m and 2 <= len(s) - len(m.group(1)) <= 4:
        s = s[: m.start()]
    return s, [m.strip() for m in markers if m.strip()]


# a "name" that is really a title: labels printed '김낙연 위원장' / '노철래 위원님' / '남경필 국회운영위원장대리'
# reach the resolver with the title in the name slot (and the name in the position slot or nowhere)
_TITLE_LIKE_RE = re.compile(r"(위원장|위원|의원|의장|부의장|간사|반장|대리|직무대행|직무대리|위원님|의원님|위윈|전문|"
                            r"委員長|委員|議員|議長|義員|議院|代理|代行|專門)$")
_HANJA_NAME_RE = re.compile(r"^[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]{2,4}$")


_FUSED_RE = re.compile(r"^.*(委員長代理|委員長|委員|議員|議長|위원장대리|위원장|위원|의원|의장)"
                       r"(?P<n>[\uac00-\ud7a3]{2,4}|[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]{2,4})$")


def _plausible_name(c):
    return bool(c) and bool(HANGUL_NAME_RE.match(c) or _HANJA_NAME_RE.match(c)) and not _TITLE_LIKE_RE.search(c)


def repair_name(pos, name, label, force=False):
    """When the printed name is empty, title-like or not name-shaped ('소위원장 김부겸'), the unique
    name-like token of the label or the position ('권경석', '김형오 의장' -> '김형오',
    '農林海洋水産委員長代理崔善榮' -> '崔善榮', '李 協委員' -> '李協'); else None. With force=True the
    search also runs when the printed name is name-shaped (the resolver uses this only when that
    name is not a member: '金孝錫議員' / '간략하게' -> '金孝錫')."""
    nm, _ = clean_name(name)
    if _plausible_name(nm) and not force:
        return None
    if force:
        # only a name fused with a member title in the position slot, in the other script than the
        # printed name ('金孝錫議員' + speech text '간략하게'); '張泰玩委員' + '李南基' is left alone
        c, _ = clean_name(unicodedata.normalize("NFKC", str(pos or "")).replace(" ", ""))
        m = _FUSED_RE.match(c)
        c = m.group("n") if (m and not _plausible_name(c)) else c
        if (_plausible_name(c) and c != nm and has_hanja(c) != has_hanja(nm)
                and _TRAIL_TITLE_RE.search(unicodedata.normalize("NFKC", str(pos or "")).replace(" ", ""))):
            return c
        return None
    toks = []
    for src in (label, pos, name):
        for t in unicodedata.normalize("NFKC", str(src or "")).replace("\u25ef", " ").replace("\u25cb", " ").split():
            c, _ = clean_name(t)
            if not _plausible_name(c):
                m = _FUSED_RE.match(c)
                c = m.group("n") if m else None
            if c and _plausible_name(c) and c != nm and c not in toks:
                toks.append(c)
    if not toks:  # a name split by a space: '李 協委員' -> '李協'
        c, _ = clean_name(re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(label or ""))))
        m = _FUSED_RE.match(c)
        c = m.group("n") if m and not _plausible_name(c) else c
        if _plausible_name(c) and c != nm:
            toks.append(c)
    return toks[0] if len(toks) == 1 else None


_POS_WORDS = [  # hanja position words seen in 16-17대 labels -> hangul
    ("副議長", "부의장"), ("議長", "의장"), ("小委員長", "소위원장"), ("委員長", "위원장"), ("委員", "위원"),
    ("議員", "의원"), ("職務代理", "직무대리"), ("職務代行", "직무대행"), ("代理", "대리"), ("臨時", "임시"),
    ("幹事", "간사"), ("班長", "반장"), ("副總理", "부총리"), ("國務總理", "국무총리"), ("總理", "총리"),
    ("長官", "장관"), ("候補者", "후보자"), ("特別", "특별"), ("兼", "겸"),
]


def pos_norm(pos) -> str:
    s = nfkc(pos)
    s = PAREN_RE.sub("", s)
    for h, k in _POS_WORDS:
        s = s.replace(h, k)
    return s


_LEG_BARE = {"위원", "의원", "위원장", "의장", "부의장", "소위원장", "간사", "반장", "감사반장", "임시의장",
             "위원장대리", "위원장직무대리", "위원장직무대행", "의장대리", "의장직무대리", "의장직무대행",
             "부의장대리", "소위원장대리", "소위원장직무대리", "소위원장직무대행", "반장대리", "감사반장대리",
             "간사대리", "조정위원장", "조정위원장대리", "조정위원장직무대행", "위원겸간사"}
_CHAIR_SUFFIX_RE = re.compile(r"^(?P<pre>.+?)(?P<t>위원장|위원)(?P<a>대리|직무대리|직무대행)?$")
_CABINET_RE = re.compile(r"(장관|국무총리|부총리|총리)(직무대리|직무대행|대리)?$")
_NOMINEE_RE = re.compile(r"후보자$")
# government commissions named '...특별위원회' (not National Assembly committees): 女性特別委員長,
# 中小企業特別委員長 in 16대 minutes
GOVT_SPECIAL_COMMITTEES = ("여성특별위원회", "중소기업특별위원회")


def _is_na_committee(pre, committee_names) -> bool:
    """'X' of an 'X위원장' title names a National Assembly committee: 'X위원회' is a committee name of
    the 위원회경력 spells, read glyph by glyph when X is hanja ('財政經濟' = 재정경제위원회)."""
    name = pre + "위원회"
    if name in committee_names:
        return True
    if not has_hanja(pre):
        return False
    key = (pre, id(committee_names))
    hit = _NA_COMMITTEE_CACHE.get(key)
    if hit is None:
        hit = any(len(c) == len(name) and reads_as(name, c) for c in committee_names)
        _NA_COMMITTEE_CACHE[key] = hit
    return hit


_NA_COMMITTEE_CACHE = {}


def title_class(pos, committee_names=frozenset()) -> str:
    """'legislator' | 'cabinet' | 'nominee' | 'other' | 'none' (empty position)."""
    p = pos_norm(pos)
    if not p:
        return "none"
    if p in _LEG_BARE:
        return "legislator"
    if _NOMINEE_RE.search(p):
        return "nominee"
    if re.search(r"소위원장(대리|직무대리|직무대행)?$", p):
        return "legislator"
    m = _CHAIR_SUFFIX_RE.match(p)
    if m:
        pre = m.group("pre")
        if pre.endswith("특별") and any(reads_as(pre + "위원회", g) for g in GOVT_SPECIAL_COMMITTEES):
            return "other"
        if pre.endswith("특별") or "특별위원회" in pre or pre.endswith("인사청문"):
            return "legislator"
        if _is_na_committee(pre, committee_names):
            # includes 16대 plenary hanja titles: '財政經濟委員長代理' (committee chair reporting); a
            # hanja commission head ('中央勞動委員長', '公正去來委員長') is not a committee: 'other'
            return "legislator"
        return "other"
    if _CABINET_RE.search(p):
        return "cabinet"
    return "other"


# Cabinet offices and their rename lineages. Used only to check that a minister-panel row belongs to
# the office named in a title (노동부 = 고용노동부, 행정자치부 = 행정안전부 = 안전행정부, ...). Same
# entries as government.MINISTRY_LINEAGE (read 2026-09-26); copied so that this module does not
# import another component.
CABINET_LINEAGE = {
    "재정경제부": ("finance_planning",), "기획예산처": ("finance_planning",), "기획재정부": ("finance_planning",),
    "통일부": ("unification",), "외교통상부": ("foreign_affairs",), "외교부": ("foreign_affairs",),
    "법무부": ("justice",), "국방부": ("defense",),
    "행정자치부": ("interior",), "행정안전부": ("interior",), "안전행정부": ("interior",),
    "국민안전처": ("public_safety",),
    "교육부": ("education",), "교육인적자원부": ("education",), "교육과학기술부": ("education", "science_ict"),
    "과학기술부": ("science_ict",), "정보통신부": ("science_ict",), "미래창조과학부": ("science_ict",),
    "과학기술정보통신부": ("science_ict",),
    "문화관광부": ("culture",), "문화체육관광부": ("culture",),
    "농림부": ("agriculture",), "농림수산식품부": ("agriculture", "oceans"), "농림축산식품부": ("agriculture",),
    "산업자원부": ("industry",), "지식경제부": ("industry",), "산업통상자원부": ("industry",),
    "산업통상부": ("industry",),
    "보건복지부": ("health_welfare",), "보건복지가족부": ("health_welfare", "gender_family"),
    "환경부": ("environment",), "기후에너지환경부": ("environment",),
    "노동부": ("labor",), "고용노동부": ("labor",),
    "여성부": ("gender_family",), "여성가족부": ("gender_family",), "성평등가족부": ("gender_family",),
    "건설교통부": ("land_transport",), "국토해양부": ("land_transport", "oceans"), "국토교통부": ("land_transport",),
    "해양수산부": ("oceans",), "중소벤처기업부": ("sme",), "국가보훈부": ("veterans",),
    "특임장관": ("special_affairs",), "국무총리": ("prime_minister",),
}
NONCABINET = "<non-cabinet office>"
_GENERIC_OFFICES = {"", "공직", "公職", "국무위원", "國務委員", "부총리"}


def title_office(pos):
    """(office, acting) named by a cabinet or nominee title. '고용노동부장관직무대행' -> ('고용노동부',
    True); '부총리겸기획재정부장관후보자' -> ('기획재정부', False); '國務總理' -> ('국무총리', False);
    office None when the title names no office ('公職候補者', '장관'); NONCABINET for a nominee to a
    post that is not a cabinet office ('국가정보원장후보자', '방송통신위원장후보자')."""
    p = pos_norm(pos)
    p = re.sub(r"후보자$", "", p)
    m = re.search(r"(직무대행|직무대리|대리)$", p)
    acting = bool(m)
    if m:
        p = p[: m.start()]
    if "겸" in p:
        p = p.split("겸")[-1]
    if p.endswith("국무총리") or p == "총리":
        return "국무총리", acting
    if p == "특임장관":
        return "특임장관", acting
    if p.endswith("장관"):
        return (p[:-2] or None), acting
    if p in _GENERIC_OFFICES:
        return None, acting
    return NONCABINET, acting


def office_key(office):
    """Hangul office name for lineage lookups: a hanja office ('農林部') is read against the cabinet
    list; other names are returned unchanged."""
    if office and has_hanja(office):
        hit = [k for k in CABINET_LINEAGE if reads_as(office, k)]
        if len(hit) == 1:
            return hit[0]
        return hanja_to_hangul(office) or office
    return office


def same_office(office, ministry) -> bool:
    """The title's office and a panel row's ministry are the same office: same name, one name inside
    the other ('보건복지부보' typo, '환경부' / '기후에너지환경부'), or the same rename lineage."""
    if not office or office == NONCABINET or not ministry:
        return False
    a, b = office_key(office), office_key(ministry)
    if a == b or a in b or b in a:
        return True
    la, lb = CABINET_LINEAGE.get(a), CABINET_LINEAGE.get(b)
    return bool(la and lb and set(la) & set(lb))


def ministry_compatible(office, ministry) -> bool:
    """A panel row of `ministry` may describe a title naming `office`: no office named (None), the same
    office, or an office that is not a known cabinet name (a typo such as '행정안정부'). A known office
    of another lineage, or a non-cabinet post, is not compatible."""
    if office is None:
        return True
    if office == NONCABINET:
        return False
    if same_office(office, ministry):
        return True
    return CABINET_LINEAGE.get(office_key(office)) is None


def committee_norm(s) -> str:
    """Committee name key: NFKC, no whitespace, no '제N대' prefix, no punctuation."""
    s = nfkc(s)
    s = re.sub(r"^제\d+대", "", s)
    return re.sub(r"[·․‧・.,、()（）\[\]「」\"'“”‘’]", "", s)


def parent_committee(committee_raw, subcommittee=None) -> str:
    """Parent committee of a (sub)committee label: '교육위원회 예산안심사소위원회' -> '교육위원회'."""
    s = unicodedata.normalize("NFKC", str(committee_raw or "")).strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) > 1 and parts[0].endswith("위원회") and "소위" in "".join(parts[1:]):
        return parts[0]
    m = re.match(r"^(.+?위원회)\s*-\s*\S*반$", s)  # v9 audit teams: '안전행정위원회-제2반', '외교통일위원회-미주반'
    if m:
        return m.group(1)
    return s


_TITLE_TOKEN_RE = re.compile(
    r"(위원장|위원|의원|의장|부의장|간사|반장|대리|직무대행|직무대리|장관|총리|후보자|委員長|委員|議員|議長)(\([^)]*\))?$")


def split_label(label):
    """Split a printed speaker label into (pos, name) for labels of the forms
    'NAME 위원', '위원 NAME', 'NAME(비) 위원', '최경환 위원(국)', '국토교통위원장대리 김병욱'."""
    s = unicodedata.normalize("NFKC", str(label or "")).strip()
    toks = s.split()
    if not toks:
        return None, None
    if len(toks) == 1:
        n, _ = clean_name(toks[0])
        return None, n or None
    name_like = [i for i, t in enumerate(toks)
                 if HANGUL_NAME_RE.match(PAREN_RE.sub("", t)) and not _TITLE_TOKEN_RE.search(PAREN_RE.sub("", t) or "x")]
    if len(toks) == 2:
        a, b = toks
        a0, b0 = PAREN_RE.sub("", a), PAREN_RE.sub("", b)
        if _TITLE_TOKEN_RE.search(b0) and not _TITLE_TOKEN_RE.search(a0):
            return b, a
        if _TITLE_TOKEN_RE.search(a0) and not _TITLE_TOKEN_RE.search(b0):
            return a, b
        # both or neither look like titles: the last token is the name (v9/XML '직위 이름' order)
        return a, b
    i = name_like[-1] if name_like else len(toks) - 1
    return " ".join(toks[:i] + toks[i + 1:]), toks[i]


# ----------------------------------------------------------------------------- build tables

def _load_api(pattern) -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(APID / pattern))):
        rows += json.loads(Path(f).read_text())["rows"]
    return pd.DataFrame(rows)


def _parse_span(s):
    m = re.match(r"^\s*(\d{4})\.(\d{2})\.(\d{2})\s*~\s*(?:(\d{4})\.(\d{2})\.(\d{2}))?\s*$", s or "")
    if not m:
        return None, None
    a = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    b = f"{m.group(4)}-{m.group(5)}-{m.group(6)}" if m.group(4) else None
    return a, b


def _terms_from_eraco(s):
    out = []
    for x in re.split(r",\s*", s or ""):
        x = x.strip()
        if not x:
            continue
        if x == "제헌":
            out.append(1)
        else:
            m = re.fullmatch(r"제(\d+)대", x)
            if m:
                out.append(int(m.group(1)))
    return out


def build_tables(write=True, verbose=True):
    """Build persons, person_terms, committee_spells and memid_crosswalk from the Open API downloads."""
    log = []

    def say(*a):
        msg = " ".join(str(x) for x in a)
        log.append(msg)
        if verbose:
            print(msg)

    alln = _load_api("ALLNAMEMBER_p*.json")
    per = _load_api("npffdutiapkzbfyvr_*_p*.json")
    per["term"] = per.UNIT_CD.str[-2:].astype(int)
    sit = _load_api("nwvrqwxyaytdsfvhu_p*.json")
    sit["term"] = 22
    hist = pd.concat([_load_api("nfzegpkvaclgtscxt_*_p*.json").assign(src="nfzegpkvaclgtscxt"),
                      _load_api("nexgtxtmaamffofof_p*.json").assign(src="nexgtxtmaamffofof")], ignore_index=True)
    cmt = pd.concat([_load_api("nqbeopthavwwfbekw_*_p*.json").assign(src="nqbeopthavwwfbekw"),
                     _load_api("nyzrglyvagmrypezq_p*.json").assign(src="nyzrglyvagmrypezq")], ignore_index=True)
    say("api rows: ALLNAMEMBER", len(alln), "| per-term 16-22(former)", len(per), "| sitting", len(sit),
        "| 의원이력", len(hist), "| 위원회경력", len(cmt))

    # ---- persons (all 3,296 ALLNAMEMBER members, every term they served)
    p = pd.DataFrame({
        "naas_cd": alln.NAAS_CD, "name": alln.NAAS_NM, "name_hanja": alln.NAAS_CH_NM,
        "name_en": alln.NAAS_EN_NM, "gender": alln.NTR_DIV, "birth_date": alln.BIRDY_DT,
        "birth_calendar": alln.BIRDY_DIV_CD, "terms_raw": alln.GTELT_ERACO,
    })
    p["terms_all"] = [sorted(set(_terms_from_eraco(s))) for s in alln.GTELT_ERACO]
    p["n_terms_total"] = p.terms_all.str.len().astype("int16")
    p["terms_16_22"] = [[t for t in ts if 16 <= t <= 22] for ts in p.terms_all]
    p["served_16_22"] = p.terms_16_22.str.len() > 0
    p["name_norm"] = p.name.map(nfkc)
    p["hanja_norm"] = p.name_hanja.map(nfkc)
    say("persons:", len(p), "unique naas_cd:", p.naas_cd.nunique(), "| served 16-22:", int(p.served_16_22.sum()),
        "| empty GTELT_ERACO:", int((p.n_terms_total == 0).sum()))

    # ---- stints (의원이력): one row per (naas_cd, term, stint)
    hist["term"] = hist.PROFILE_SJ.str.extract(r"제(\d+)대")[0].astype(int)
    hist = hist.drop_duplicates(["MONA_CD", "term", "FRTO_DATE"])
    sp = hist.FRTO_DATE.map(_parse_span)
    hist["seat_start"] = [a for a, b in sp]
    hist["seat_end"] = [b for a, b in sp]
    rest = hist.PROFILE_SJ.str.replace(r"^제\d+대\s*", "", regex=True)
    hist["party_at_election"] = rest.str.split().str[0]
    hist["district"] = rest.str.split(n=1).str[1].fillna("").str.strip()
    st = hist[hist.term.between(16, 22)].copy()
    st = st.sort_values(["MONA_CD", "term", "seat_start"])
    st["stint"] = st.groupby(["MONA_CD", "term"]).cumcount().add(1).astype("int16")
    st["elect_type"] = np.where(st.district.str.contains("비례|전국구"), "비례", "지역구")
    say("stints 16-22:", len(st), "| member-terms:", st[["MONA_CD", "term"]].drop_duplicates().shape[0],
        "| unparsed spans:", int(st.seat_start.isna().sum()))

    # per-term service (party label reported for the member-term, district, elect type)
    pts = pd.concat([per[["MONA_CD", "term", "HG_NM", "HJ_NM", "POLY_NM", "ORIG_NM", "ELECT_GBN_NM", "UNITS"]],
                     sit[["MONA_CD", "term", "HG_NM", "HJ_NM", "POLY_NM", "ORIG_NM", "ELECT_GBN_NM", "UNITS"]]])
    pts = pts.drop_duplicates(["MONA_CD", "term"])
    pts = pts.rename(columns={"HG_NM": "name_term", "HJ_NM": "hanja_term", "POLY_NM": "party_term_api",
                              "ORIG_NM": "district_term_api", "ELECT_GBN_NM": "elect_type_term_api",
                              "UNITS": "units_api"})
    pt = st.merge(pts, on=["MONA_CD", "term"], how="outer", indicator=True)
    say("stints x per-term service:", pt._merge.value_counts().to_dict())
    pt = pt.drop(columns="_merge")

    # ALLNAMEMBER per-term elements (party / district type), only where the lists align with the terms
    ae = []
    for r in alln.itertuples():
        ts = _terms_from_eraco(r.GTELT_ERACO)
        parties = [x.strip() for x in (r.PLPT_NM or "").split("/")] if r.PLPT_NM else []
        dtypes = [x.strip() for x in (r.ELECD_DIV_NM or "").split("/")] if r.ELECD_DIV_NM else []
        for k, t in enumerate(ts):
            if 16 <= t <= 22:
                ae.append({"MONA_CD": r.NAAS_CD, "term": t,
                           "party_allnamember": parties[k] if len(parties) == len(ts) else None,
                           "elect_type_allnamember": dtypes[k] if len(dtypes) == len(ts) else None})
    ae = pd.DataFrame(ae).drop_duplicates(["MONA_CD", "term"])
    pt = pt.merge(ae, on=["MONA_CD", "term"], how="left")
    # 의원이력 rows without a district ('제22대 더불어민주연합 ', list successors of 2025): take the
    # district and elect type from the per-term service
    empty = pt.district.fillna("").eq("")
    pt["district_source"] = np.where(empty, "per_term_service", "의원이력")
    pt.loc[empty, "district"] = pt.loc[empty, "ORIG_NM"] if "ORIG_NM" in pt else pt.loc[empty, "district_term_api"]
    et_fill = pt.loc[empty, "elect_type_term_api"].fillna("").str.replace("전국구", "비례대표")
    pt.loc[empty, "elect_type"] = np.where(et_fill.str.contains("비례"), "비례", "지역구")
    say("stints with empty 의원이력 district (filled from the per-term service):", int(empty.sum()))

    # record member-term ids (19-22대)
    cw = pd.read_parquet(INTERIM / "members_memid_crosswalk_19_22.parquet")
    ids = (cw.dropna(subset=["naas_cd"]).groupby(["naas_cd", "term"]).mem_id
           .apply(lambda s: ";".join(str(int(x)) for x in sorted(s))).rename("record_mem_id").reset_index())
    recp = cw.dropna(subset=["naas_cd"]).drop_duplicates(["naas_cd", "term"])[["naas_cd", "term", "party_rec"]]
    pt = pt.merge(ids.rename(columns={"naas_cd": "MONA_CD"}), on=["MONA_CD", "term"], how="left")
    pt = pt.merge(recp.rename(columns={"naas_cd": "MONA_CD", "party_rec": "party_record"}), on=["MONA_CD", "term"],
                  how="left")

    pt = pt.rename(columns={"MONA_CD": "naas_cd", "HG_NM": "name_hist", "HJ_NM": "hanja_hist"})
    pt = pt.merge(p[["naas_cd", "name", "name_hanja"]], on="naas_cd", how="left")
    pt["term_start"] = pt.term.map(lambda t: TERM_DATES[t][0])
    pt["term_end"] = pt.term.map(lambda t: TERM_DATES[t][1])
    pt["entered_mid_term"] = pt.seat_start > pt.term_start
    pt["left_mid_term"] = pt.seat_end.notna() & (pt.seat_end < pt.term_end)
    pt["entry_kind_inferred"] = np.where(~pt.entered_mid_term, "general_election",
                                         np.where(pt.elect_type == "비례", "list_succession", "by_election"))
    # elect type cross-check against the per-term service (only single-stint member-terms)
    nst = pt.groupby(["naas_cd", "term"]).stint.transform("count")
    et_api = pt.elect_type_term_api.fillna("").str.replace("전국구", "비례대표").str.replace("비례대표", "비례")
    mism = (nst == 1) & (et_api != "") & (et_api != pt.elect_type)
    say("elect_type (의원이력 district) vs per-term service disagreements (single-stint):", int(mism.sum()))
    pt["elect_type_conflict"] = mism
    name_diff = (pt.name.map(nfkc) != pt.name_term.map(nfkc)) | (pt.name.map(nfkc) != pt.name_hist.map(nfkc))
    say("member-terms whose ALLNAMEMBER name differs from the per-term or 의원이력 name:", int(name_diff.sum()))
    pt["name_variants"] = [sorted({nfkc(x) for x in (a, b, c) if isinstance(x, str) and x})
                           for a, b, c in zip(pt.name, pt.name_term, pt.name_hist)]
    pt["hanja_variants"] = [sorted({nfkc(x) for x in (a, b, c) if isinstance(x, str) and x})
                            for a, b, c in zip(pt.name_hanja, pt.hanja_term, pt.hanja_hist)]
    pt = pt[["naas_cd", "term", "stint", "name", "name_hanja", "district", "elect_type", "party_at_election",
             "party_term_api", "party_allnamember", "party_record", "record_mem_id", "seat_start", "seat_end",
             "term_start", "term_end", "entered_mid_term", "left_mid_term", "entry_kind_inferred",
             "district_term_api", "elect_type_term_api", "elect_type_allnamember", "elect_type_conflict", "district_source",
             "name_variants", "hanja_variants", "units_api", "src"]].rename(columns={"src": "seat_source"})
    pt["term"] = pt.term.astype("int16")
    pt = pt.sort_values(["term", "name", "naas_cd", "stint"]).reset_index(drop=True)
    say("person_terms rows:", len(pt), "| per term:", pt.groupby("term").naas_cd.nunique().to_dict())

    # ---- cross-check against assemblykor
    if ASSEMBLYKOR_CSV.exists():
        kor = pd.read_csv(ASSEMBLYKOR_CSV)
        kor = kor[kor.assembly.between(16, 22)].rename(columns={"assembly": "term", "member_id": "naas_cd"})
        x = kor[["naas_cd", "term"]].drop_duplicates().merge(pt[["naas_cd", "term"]].drop_duplicates(),
                                                             how="outer", indicator=True)
        say("assemblykor (naas_cd, term) vs person_terms:", x._merge.value_counts().to_dict())

    # ---- committee spells
    cmt["term"] = cmt.PROFILE_SJ.str.extract(r"제(\d+)대")[0].astype(float)
    csp = cmt.FRTO_DATE.map(_parse_span)
    cs = pd.DataFrame({"naas_cd": cmt.MONA_CD, "term": cmt.term, "committee": cmt.PROFILE_SJ.str.replace(
        r"^제\d+대\s*", "", regex=True), "start": [a for a, b in csp], "end": [b for a, b in csp], "source": cmt.src})
    cs = cs.drop_duplicates(["naas_cd", "committee", "start", "end"])
    cs["committee_norm"] = cs.committee.map(committee_norm)
    say("committee spells:", len(cs), "| unparsed spans:", int(cs.start.isna().sum()),
        "| members covered 16-22:", cs[cs.term.between(16, 22)].naas_cd.nunique())

    # ---- mem_id crosswalk re-check against the new tables
    cw2 = cw.merge(pt.drop_duplicates(["naas_cd", "term"])[["naas_cd", "term", "name", "name_hanja", "district"]],
                   on=["naas_cd", "term"], how="left")
    cw2["name_ok"] = [nfkc(a) == nfkc(b) for a, b in zip(cw2.name_rec, cw2.name)]
    cw2["hanja_ok"] = [nfkc(a) == nfkc(b) for a, b in zip(cw2.hanja_rec, cw2.name_hanja)]
    cw2["in_person_terms"] = cw2.name.notna()
    say("memid crosswalk:", len(cw2), "| naas_cd null:", int(cw2.naas_cd.isna().sum()),
        "| (naas_cd, term) in person_terms:", int(cw2.in_person_terms.sum()),
        "| name agrees:", int(cw2.name_ok.sum()), "| hanja agrees (NFKC):", int(cw2.hanja_ok.sum()))
    cwo = cw2[["mem_id", "term", "naas_cd", "name_rec", "hanja_rec", "district_rec", "party_rec", "match",
               "in_person_terms", "name_ok", "hanja_ok"]]

    out = {"persons": p.drop(columns=["name_norm", "hanja_norm"]), "person_terms": pt, "committee_spells": cs,
           "memid_crosswalk": cwo}
    if write:
        OUTD.mkdir(parents=True, exist_ok=True)
        for k, v in out.items():
            v.to_parquet(OUTD / f"{k}.parquet", index=False)
        (OUTD / "build_log.txt").write_text("\n".join(log) + "\n")
    return out


# ----------------------------------------------------------------------------- reference + resolver

_REF = {}


def load_reference(refresh=False):
    if _REF and not refresh:
        return _REF
    need = ["persons", "person_terms", "committee_spells", "memid_crosswalk"]
    if refresh or not all((OUTD / f"{k}.parquet").exists() for k in need):
        build_tables(write=True, verbose=False)
    ref = {k: pd.read_parquet(OUTD / f"{k}.parquet") for k in need}
    spells21 = INTERIM / "members_party_spells_21.parquet"
    ref["party_spells_21"] = pd.read_parquet(spells21) if spells21.exists() else pd.DataFrame()
    ref["minister_panel"] = pd.read_csv(MINISTER_PANEL_CSV) if MINISTER_PANEL_CSV.exists() else pd.DataFrame()
    ref["party_lineage"] = pd.read_csv(LINEAGE_CSV) if LINEAGE_CSV.exists() else pd.DataFrame()
    _REF.clear()
    _REF.update(ref)
    _REF["resolver"] = Resolver(ref)
    return _REF


PARTY_ABBREV = {  # party-initial markers printed after homonyms that are not a prefix of the name
    "자유한국당": ("한", "한국"), "민주평화당": ("평", "민평", "평화"), "더불어민주당": ("민", "더", "민주"),
    "새정치민주연합": ("새정치",), "국민의힘": ("국", "힘"), "더불어시민당": ("시", "시민"),
    "미래통합당": ("통",), "바른미래당": ("바", "미"), "새천년민주당": ("민",), "열린우리당": ("우", "열"),
    "민주통합당": ("민",), "통합진보당": ("진", "통"), "국민의당": ("국",),
}
_YEAR_SUFFIX_RE = re.compile(r"\(\d{4}\)$")


def party_base(label) -> str:
    """'민주당(2008)' -> '민주당'; whitespace removed."""
    return _YEAR_SUFFIX_RE.sub("", nfkc(label))


def _party_marker_match(marker, parties):
    """Strength of a party-marker match: 2 = a party starts with the marker or has it as a known
    abbreviation, 1 = a party contains it, 0 = no match."""
    best = 0
    for p in parties:
        if not p:
            continue
        p = party_base(p)
        if p.startswith(marker) or marker in PARTY_ABBREV.get(p, ()):
            return 2
        if marker in p:
            best = 1
    return best


class Lineage:
    """Party lineage (interim/party_lineage.csv): each label with its validity dates, linked to its
    successor by rename or merger. chain(label) = the label, its ancestors and its descendants (no
    sibling crossing), each with its own validity window."""

    def __init__(self, df):
        self.info = {}
        self.pred = defaultdict(set)
        if df is None or not len(df):
            return
        for r in df.itertuples():
            fr = r.label_from if isinstance(r.label_from, str) and r.label_from else None
            to = r.label_to if isinstance(r.label_to, str) and r.label_to else None
            succ = r.successor if (isinstance(r.successor, str) and r.successor
                                   and r.kind in ("rename", "merger_into")) else None
            self.info[r.label] = (fr, to, succ)
        for lab, (_, _, succ) in self.info.items():
            if succ:
                self.pred[succ].add(lab)

    def labels_for(self, party):
        """Lineage labels for an API party string (exact, else same base name)."""
        p = nfkc(party)
        if p in self.info:
            return [p]
        return [lab for lab in self.info if party_base(lab) == p]

    def chain(self, party):
        out = set()
        for lab in self.labels_for(party):
            stack = [lab]
            while stack:  # ancestors
                x = stack.pop()
                if x in out:
                    continue
                out.add(x)
                stack.extend(self.pred.get(x, ()))
            x = lab
            while x in self.info and self.info[x][2] and self.info[x][2] not in out:  # descendants
                x = self.info[x][2]
                out.add(x)
        return out

    def window(self, label):
        fr, to, _ = self.info.get(label, (None, None, None))
        return fr, to


_NOTE_FLAG_RE = re.compile(r"동명이인|혼동|verify|요확인")
_NOTE_TERM_RE = re.compile(r"(?<!\d)(\d{1,2}(?:\s*[~∼,·/]\s*\d{1,2})*)\s*대")
_NOTE_JOIN_RE = re.compile(r"^\s*(?:[,·/・]|및|와|과)?\s*$")
_NOTE_BOUNDARY_RE = re.compile(r"[;()（）]|으나")
_NOTE_NEG_RE = re.compile(r"아님|아니|없음|없었|낙선|불출마|미출마|실패|배제|낙마|오류")


def panel_note_terms(note) -> set:
    """Terms that a minister-panel note says the official served as a member.
    '15·16대 의원이었으나 22대 아님' -> {15, 16}; '18대 의원이었으나 21대/22대 아님' -> {18};
    '이전 의원직은 16~18대' -> {16, 17, 18}. A mention is negated when the text after it, up to the
    next mention, ';', a parenthesis or a contrast ('...으나'), has a negation ('아님', '없음', '낙선',
    '불출마', '미출마', '실패', '배제', '오류'). Mentions joined only by '/', '·', ',' share the
    polarity of the last one. Notes without '의원', and notes that flag a homonym or an unverified claim
    ('동명이인', '혼동', 'verify', '요확인'), give nothing."""
    if not isinstance(note, str) or "의원" not in note or _NOTE_FLAG_RE.search(note):
        return set()
    ms = list(_NOTE_TERM_RE.finditer(note))
    out, group = set(), []
    for i, m in enumerate(ms):
        nums = [int(x) for x in re.findall(r"\d{1,2}", m.group(1))]
        if len(nums) == 2 and re.search(r"[~∼]", m.group(1)):
            nums = list(range(nums[0], nums[1] + 1))
        group += nums
        nxt = ms[i + 1].start() if i + 1 < len(ms) else len(note)
        seg = note[m.end():nxt]
        if i + 1 < len(ms) and _NOTE_JOIN_RE.match(seg):
            continue
        b = _NOTE_BOUNDARY_RE.search(seg)
        if b:
            seg = seg[: b.start()]
        if not _NOTE_NEG_RE.search(seg):
            out.update(group)
        group = []
    return out


def _shift(iso, days):
    return (dt.date.fromisoformat(iso) + dt.timedelta(days=days)).isoformat() if iso else None


PANEL_PRE_NOMINEE = 120  # days before appointment/confirmation in which a nominee title matches a panel row
PANEL_PRE_CABINET = 30   # the same for a sitting (or acting) cabinet title: panel dates are partly approximate
PANEL_POST = 30          # days after the end of the tenure


class Resolver:
    """Indexes the reference tables and resolves one speaker key at a time."""

    def __init__(self, ref):
        pt = ref["person_terms"].copy()
        self.persons = ref["persons"].set_index("naas_cd")
        self.terms_all = {k: list(v) for k, v in self.persons.terms_all.items()}
        self.pt = pt
        self.lineage = Lineage(ref.get("party_lineage"))
        self.by_name = defaultdict(list)
        self.by_hanja = defaultdict(list)
        self.stints = defaultdict(list)
        sp21 = ref.get("party_spells_21")
        spells21 = defaultdict(list)
        if sp21 is not None and len(sp21):
            for r in sp21.itertuples():
                spells21[r.naas_cd].append((nfkc(r.party), str(r.start)[:10] if isinstance(r.start, str) else None,
                                            str(r.end)[:10] if isinstance(r.end, str) else None))
        for r in pt.itertuples():
            parties = {x for x in (r.party_at_election, r.party_term_api, r.party_allnamember, r.party_record)
                       if isinstance(x, str) and x}
            dated = []  # (party base, from, to): lineage chain of every recorded party
            for x in parties:
                labs = self.lineage.chain(x)
                if labs:
                    dated += [(party_base(lab),) + self.lineage.window(lab) for lab in labs]
                else:
                    dated.append((party_base(x), None, None))
            if int(r.term) == 21:
                parties |= {p for p, _, _ in spells21.get(r.naas_cd, [])}
                dated += [(party_base(p), a, b) for p, a, b in spells21.get(r.naas_cd, [])]
            d = {"naas_cd": r.naas_cd, "term": int(r.term), "stint": int(r.stint), "start": r.seat_start,
                 "end": r.seat_end, "district": nfkc(r.district), "district_raw": r.district,
                 "district_api": nfkc(r.district_term_api), "elect_type": r.elect_type, "parties": parties,
                 "parties_dated": dated, "record_mem_id": r.record_mem_id}
            self.stints[(r.naas_cd, int(r.term))].append(d)
        for (cd, t), lst in self.stints.items():
            row = pt[(pt.naas_cd == cd) & (pt.term == t)].iloc[0]
            for n in row.name_variants:
                if cd not in self.by_name[(t, n)]:
                    self.by_name[(t, n)].append(cd)
            for h in row.hanja_variants:
                if cd not in self.by_hanja[(t, h)]:
                    self.by_hanja[(t, h)].append(cd)
        # all persons by name (former-member rule)
        self.p_by_name = defaultdict(list)
        self.p_by_hanja = defaultdict(list)
        for cd, r in self.persons.iterrows():
            self.p_by_name[nfkc(r["name"])].append(cd)
            if isinstance(r["name_hanja"], str) and r["name_hanja"]:
                self.p_by_hanja[nfkc(r["name_hanja"])].append(cd)
        # names by term for the variant fallbacks
        self.hanja_by_term = defaultdict(list)
        for (t, h), cds in self.by_hanja.items():
            self.hanja_by_term[t].append((h, cds))
        self.names_by_term = defaultdict(list)
        for (t, n), cds in self.by_name.items():
            self.names_by_term[t].append((n, cds))
        # API hanja names that contain hangul (설松雄, 李연淑): a hangul position matches a hanja glyph
        # with that reading
        self.hanja_wild = defaultdict(list)
        for (t, h), cds in self.by_hanja.items():
            if has_hanja(h) and re.search(r"[\uac00-\ud7a3]", h):
                self.hanja_wild[t].append((h, cds))
        # record mem_id
        cw = ref["memid_crosswalk"]
        self.memid = {int(r.mem_id): (r.naas_cd, int(r.term), nfkc(r.name_rec), nfkc(r.hanja_rec))
                      for r in cw.itertuples() if isinstance(r.naas_cd, str)}
        # committee spells
        cs = ref["committee_spells"]
        self.cspells = defaultdict(list)
        for r in cs.itertuples():
            self.cspells[r.naas_cd].append((r.committee_norm, r.start, r.end))
        self.committee_names = frozenset(cs.committee_norm.unique())
        # minister panel (dual office; notes that name the terms a former member served)
        mp = ref.get("minister_panel")
        self.panel = defaultdict(list)
        self.panel_bad_dates = []  # rows whose end precedes the start (reported by the eval)
        if mp is not None and len(mp):
            for r in mp.itertuples():
                start = str(r.start) if isinstance(r.start, str) else None
                conf = str(r.confirmation_date) if isinstance(r.confirmation_date, str) else None
                end = str(r.end) if isinstance(r.end, str) else None
                lo0 = min(x for x in (start, conf) if x) if (start or conf) else None
                bad = bool(start and end and end < start)
                if bad:
                    self.panel_bad_dates.append((r.name, r.ministry, start, end))
                asm = None if pd.isna(r.assembly_num_at_appt) else int(r.assembly_num_at_appt)
                self.panel[nfkc(r.name)].append({
                    "lo_nominee": _shift(lo0, -PANEL_PRE_NOMINEE), "lo_cabinet": _shift(lo0, -PANEL_PRE_CABINET),
                    "hi": _shift(end, PANEL_POST), "undated": lo0 is None and end is None, "bad_dates": bad,
                    "appt": start or conf, "dual": bool(r.dual_office), "assembly": asm,
                    "district": nfkc(r.mp_district), "ministry": nfkc(r.ministry),
                    "note_terms": panel_note_terms(r.notes)})

    # -- helpers
    def seated(self, cd, term, date):
        """(seated_on_date, stint) for a member-term; seated is None when the date is unknown."""
        sts = self.stints.get((cd, term), [])
        if not sts:
            return False, None
        if not date:
            return None, sts[0]
        for s in sts:
            if s["start"] and s["start"] <= date and (s["end"] is None or date <= s["end"]):
                return True, s
        return False, sts[0]

    def on_committee(self, cd, cnorm, date):
        if not cnorm or not date:
            return None
        for c, a, b in self.cspells.get(cd, []):
            if c == cnorm and a and a <= date and (b is None or date <= b):
                return True
        return False

    def seniority(self, cd, term):
        ts = self.terms_all.get(cd)
        if ts is None:
            return None
        return int(sum(1 for t in ts if t <= term))

    def parties_on(self, cd, term, date):
        """Party base names of the member-term's lineage valid on `date` (all when date is unknown)."""
        out = set()
        for s in self.stints.get((cd, term), []):
            for p, a, b in s["parties_dated"]:
                if not date or ((a is None or a <= date) and (b is None or date <= b)):
                    out.add(p)
        return out

    def _names_for(self, term, nm, hanja):
        return list((self.by_hanja if hanja else self.by_name).get((term, nm), []))

    def _variant_candidates(self, term, nm, hanja):
        """Fallback candidates when the exact name is not a member of the term: (cands, method)."""
        if hanja:
            # one-character variant with the same reading (金鐘民/金鍾民): same length, unique in the term
            var = [cds for h, cds in self.hanja_by_term.get(term, [])
                   if len(h) == len(nm) and sum(a != b for a, b in zip(h, nm)) == 1
                   and all(a == b or same_reading(a, b, initial=(i == 0)) for i, (a, b) in enumerate(zip(h, nm)))]
            flat = sorted({c for cds in var for c in cds})
            if len(flat) == 1:
                return flat, "hanja_term_variant"
            if nm[0] in HANJA_SURNAME_VARIANTS:
                c = self._names_for(term, HANJA_SURNAME_VARIANTS[nm[0]] + nm[1:], True)
                if len(c) == 1:
                    return c, "hanja_term_surname_variant"
            wild = sorted({c for h, cds in self.hanja_wild.get(term, [])
                           if len(h) == len(nm)
                           and all(a == b or (re.match(r"[\uac00-\ud7a3]", a) and same_reading(b, a, initial=(i == 0)))
                                   for i, (a, b) in enumerate(zip(h, nm)))
                           for c in cds})
            if len(wild) == 1:
                return wild, "hanja_term_hangul_wildcard"
            # hangul reading of the printed hanja name (李嬿叔 -> 이연숙), with the initial-sound variant
            rdg = hanja_to_hangul(nm)
            if rdg:
                c = self._names_for(term, rdg, False) or (
                    self._names_for(term, DUEUM[rdg[0]] + rdg[1:], False) if rdg[0] in DUEUM else [])
                if len(c) == 1:
                    return c, "hanja_reading_name"
            if len(nm) >= 2 and all(has_hanja(ch) for ch in nm):
                # the first glyph was dropped by the viewer ('建設交通委員長代理 松雄' = 偰松雄)
                part = sorted({c for h, cds in self.hanja_by_term.get(term, [])
                               if len(h) == len(nm) + 1 and h[1:] == nm for c in cds})
                if len(part) == 1:
                    return part, "hanja_term_partial"
            return [], None
        if nm and nm[0] in DUEUM:
            c = self._names_for(term, DUEUM[nm[0]] + nm[1:], False)
            if c:
                return c, "name_term_dueum"
        return [], None

    # -- main
    def resolve(self, term, date, pos, name, mem_id=None, area=None, label=None, committee=None,
                leg_side=True, tclass="legislator"):
        """Return dict(naas_cd, method, confidence, n_cand, note, marked, candidates, label_repair).
        A link made after label repair is at most medium confidence."""
        r = self._resolve(term, date, pos, name, mem_id, area, label, committee, leg_side, tclass)
        if r.get("label_repair") and r.get("confidence") == "high":
            r["confidence"] = "medium"
        return r

    def _resolve(self, term, date, pos, name, mem_id, area, label, committee, leg_side, tclass):
        res = {"naas_cd": None, "method": None, "confidence": None, "n_cand": 0, "note": None,
               "marked": False, "candidates": (), "label_repair": False, "memid_status": None}
        nm, markers = clean_name(name)
        for src in (pos, label):
            markers += [m for m in PAREN_RE.findall(nfkc(src)) if m not in markers]
        markers = [m for m in markers if m not in ("대리", "직무대리", "직무대행")]
        res["marked"] = bool(markers)
        # a. record member-term id (legislator side only: the viewer also attaches member ids to
        # non-members who share a member's name, see roles.py)
        if mem_id is not None and not pd.isna(mem_id) and int(mem_id) != 0:
            hit = self.memid.get(int(mem_id))
            if hit and not leg_side:
                res["note"] = f"mem_id {int(mem_id)} ({hit[0]}) not used: non-legislator title"
                res["memid_status"] = "not_used_nonlegislator"
            elif hit:
                r = self._resolve_memid(hit, term, date, nm, pos, res)
                if r is not None:
                    return {**r, "memid_status": "used"}
                res["note"] = f"mem_id {int(mem_id)} not used: record name {hit[2]}/{hit[3]} vs printed {nm}"
                res["memid_status"] = "not_used_name"
            else:
                res["note"] = f"mem_id {int(mem_id)} not in record crosswalk"
                res["memid_status"] = "not_in_crosswalk"
        if term is None:
            return {**res, "method": "unresolved_no_term" if nm else "unresolved_no_name"}
        hanja = has_hanja(nm)
        cands = self._names_for(term, nm, hanja) if nm else []
        base = "hanja_term" if hanja else "name_term"
        if not cands and nm:
            cands, vbase = self._variant_candidates(term, nm, hanja)
            if cands:
                base = vbase
        if not cands:
            # label repair: the printed name is empty or a title, the name sits in the position/label
            rn = repair_name(pos, name, label) or (repair_name(pos, name, label, force=True) if nm else None)
            if rn:
                h2 = has_hanja(rn)
                c2 = self._names_for(term, rn, h2)
                b2 = "hanja_term" if h2 else "name_term"
                if not c2:
                    c2, v2 = self._variant_candidates(term, rn, h2)
                    b2 = v2 or b2
                if c2 or not nm:
                    nm, hanja, cands, base = rn, h2, c2, b2
                    res["label_repair"] = True
                    res["note"] = f"name repaired from label/position: {rn}"
        if not nm:
            return {**res, "method": "unresolved_no_name"}
        if not leg_side:
            return self._resolve_nonleg(term, date, nm, hanja, cands, tclass, pos, res)
        if not cands:
            fz = self._fuzzy_committee(term, date, nm, committee)
            if fz:
                return {**res, **fz}
            other = [t for t in range(16, 23) if t != term and (self.by_hanja if hanja else self.by_name).get((t, nm))]
            return {**res, "method": "unresolved_no_member_in_term",
                    "note": (f"member of term(s) {other}" if other else res["note"])}
        res["n_cand"] = len(cands)
        res["candidates"] = tuple(cands)
        if len(cands) == 1:
            cd = cands[0]
            seat, _ = self.seated(cd, term, date)
            conf = {"hanja_term_variant": "medium", "hanja_term_surname_variant": "medium",
                    "hanja_term_hangul_wildcard": "medium", "name_term_dueum": "medium",
                    "hanja_reading_name": "medium",
                    "hanja_term_partial": "low"}.get(base, "high")
            if seat is False:
                return {**res, "naas_cd": cd, "method": base, "confidence": "low",
                        "note": "not seated on speech date"}
            return {**res, "naas_cd": cd, "method": base, "confidence": conf}
        return self._disambiguate(term, date, nm, markers, area, committee, cands, res)

    def _resolve_memid(self, hit, term, date, nm, pos, res):
        """Link through the record member-term id, or None when the printed name is neither the record
        name nor a one-glyph variant of it (the caller then resolves the printed name)."""
        cd, t, n0, h0 = hit
        ok_name = nm in (n0, h0) or not nm
        swapped = False
        if not ok_name:
            pn, _ = clean_name(pos)
            if pn and pn in (n0, h0):  # viewer data-pos / data-name swapped ('劉承旼' / '國防委員長')
                ok_name, swapped = True, True
        # a printed name one glyph away from the record name, same first glyph ('염동철' = 염동열,
        # '은수민' = 은수미) is a typo of that member; any other name is not linked through the id
        near = (not ok_name) and any(x and len(x) == len(nm) >= 2 and x[0] == nm[0]
                                     and sum(a != b for a, b in zip(x, nm)) == 1 for x in (n0, h0))
        if not ok_name and not near:
            return None
        if term is not None and t != term:
            return {**res, "naas_cd": cd, "method": "mem_id_term_mismatch", "confidence": "medium",
                    "n_cand": 1, "note": f"mem_id term {t} != meeting term {term}"}
        if not ok_name:
            return {**res, "naas_cd": cd, "method": "mem_id_name_mismatch", "confidence": "medium",
                    "n_cand": 1, "note": f"record name {n0}/{h0} vs printed {nm} (one glyph)"}
        seat, _ = self.seated(cd, t, date)
        if seat is False:
            same = set(self.by_name.get((t, n0), []))
            if h0:
                same |= set(self.by_hanja.get((t, h0), []))
            same = sorted(same)
            on = [c for c in same if c != cd and self.seated(c, t, date)[0]]
            if len(on) == 1:
                return {**res, "naas_cd": on[0], "method": "mem_id_seat_override", "confidence": "medium",
                        "n_cand": len(same), "candidates": tuple(same),
                        "note": f"mem_id member {cd} not seated on {date}; same-name member {on[0]} seated"}
            return {**res, "naas_cd": cd, "method": "mem_id_not_seated", "confidence": "medium", "n_cand": 1,
                    "note": f"mem_id member not seated on {date}"}
        return {**res, "naas_cd": cd, "method": "mem_id_pos_name_swapped" if swapped else "mem_id",
                "confidence": "high", "n_cand": 1}

    def _fuzzy_committee(self, term, date, nm, committee):
        """One-jamo typo in one syllable ('윤호증' = 윤호중, '우체창' = 우제창): same surname, unique among
        members at that distance who are seated on the date and sit on the meeting's committee on that
        date. Hangul only (a hanja glyph with the same reading is handled as a variant; other hanja
        glyph differences are not linked: '鄭夢憲' is not 鄭夢準)."""
        cn = committee_norm(committee) if committee else ""
        if not (cn and date and HANGUL_NAME_RE.match(nm or "") and len(nm) >= 3):
            return None
        near = sorted({c for n, cds in self.names_by_term.get(term, [])
                       if len(n) == len(nm) and n[0] == nm[0] and one_jamo_apart(n, nm) for c in cds})
        hit = [c for c in near if self.seated(c, term, date)[0] and self.on_committee(c, cn, date)]
        if len(hit) == 1:
            n0 = self.persons.at[hit[0], "name"]
            return {"naas_cd": hit[0], "method": "name_fuzzy_committee", "confidence": "low", "n_cand": 1,
                    "candidates": tuple(hit), "note": f"printed {nm} ~ {n0} (one jamo), on {committee}"}
        return None

    def _disambiguate(self, term, date, nm, markers, area, committee, cands, res):
        decisions = []
        # seat dates
        seated = {cd: self.seated(cd, term, date)[0] for cd in cands}
        on = [cd for cd in cands if seated[cd]]
        if date and len(on) == 1:
            decisions.append(("homonym_seat_dates", on[0]))
        pool = on if (date and len(on) >= 1) else cands
        # printed area (XML 19대+)
        if area:
            a = nfkc(area)
            hit = [cd for cd in pool if any(a and (a == s["district"] or a == s["district_api"] or
                                                   (a == "비례대표" and s["elect_type"] == "비례"))
                                            for s in self.stints[(cd, term)])]
            if len(hit) == 1:
                decisions.append(("homonym_area", hit[0]))
        # printed markers
        for m in markers:
            if m in ("비", "비례", "전국구", "전"):
                hit = [cd for cd in pool if any(s["elect_type"] == "비례" for s in self.stints[(cd, term)])]
                if len(hit) == 1:
                    decisions.append(("homonym_marker_elect_type", hit[0]))
                    continue
            if m in ("지", "지역", "지역구"):
                hit = [cd for cd in pool if any(s["elect_type"] == "지역구" for s in self.stints[(cd, term)])]
                if len(hit) == 1:
                    decisions.append(("homonym_marker_elect_type", hit[0]))
                    continue
            if len(m) >= 2:
                hit = [cd for cd in pool if any(m in s["district"] or m in s["district_api"]
                                                for s in self.stints[(cd, term)])]
                if len(hit) == 1:
                    decisions.append(("homonym_marker_district", hit[0]))
                    continue
            # party initial: lineage labels valid on the speech date first, then any recorded party
            done = False
            for parties_of in (lambda cd: self.parties_on(cd, term, date),
                               lambda cd: set().union(*[s["parties"] for s in self.stints[(cd, term)]])):
                sc = {cd: _party_marker_match(m, parties_of(cd)) for cd in pool}
                for lvl in (2, 1):
                    hit = [cd for cd in pool if sc[cd] >= lvl]
                    if len(hit) == 1:
                        decisions.append(("homonym_marker_party", hit[0]))
                        done = True
                    if hit:
                        break
                if done or any(sc.values()):
                    break
        # committee membership on the speech date
        cn = committee_norm(committee) if committee else ""
        if cn and date:
            hit = [cd for cd in pool if self.on_committee(cd, cn, date)]
            if len(hit) == 1:
                decisions.append(("homonym_committee", hit[0]))
        picks = {cd for _, cd in decisions}
        note = ";".join(f"{k}={v}" for k, v in decisions) or None
        if len(picks) == 1:
            cd = decisions[0][1]
            conf = "high" if decisions[0][0] == "homonym_seat_dates" else "medium"
            return {**res, "naas_cd": cd, "method": decisions[0][0], "confidence": conf, "note": note}
        if len(picks) > 1:
            # conflicting cues: take the first cue in this order, at low confidence
            order = ["homonym_seat_dates", "homonym_area", "homonym_marker_elect_type", "homonym_marker_district",
                     "homonym_marker_party", "homonym_committee"]
            k, cd = sorted(decisions, key=lambda x: order.index(x[0]))[0]
            return {**res, "naas_cd": cd, "method": k, "confidence": "low", "note": "conflict:" + note}
        return {**res, "method": "unresolved_ambiguous", "note": "candidates=" + ",".join(cands)}

    def panel_rows(self, name, date, pos, tclass):
        """Minister-panel rows of this (hangul) name that can describe this title on this date.
        - The row's ministry must be the office the title names (rename lineage and one-name-inside-
          the-other typos accepted; see ministry_compatible). A title that names no office accepts any
          row; a nominee to a non-cabinet post accepts none.
        - The row must cover the date: from the appointment or confirmation date (whichever is first)
          minus PANEL_PRE_NOMINEE days for a nominee title, minus PANEL_PRE_CABINET days otherwise, to
          the end plus PANEL_POST days. A row of the ministry named in the title (its name inside the
          printed title) counts at any date: panel end dates are partly wrong (김희정 여성가족부 ends
          2015-03-12 in the panel, she speaks as minister until 2015-11; 김진표 교육인적자원부 ends
          before it starts; nominees never appointed have no dates: 강선우).
        - An acting title ('고용노동부장관직무대행') is held by an official of another office: only rows
          of another office that cover the date (cabinet window) count."""
        office, acting = title_office(pos)
        pn = pos_norm(pos)
        out = []
        for r in self.panel.get(name, []):
            lo = r["lo_nominee"] if (tclass == "nominee" and not acting) else r["lo_cabinet"]
            covers = bool(date) and not r["undated"] and (lo is None or lo <= date) and (r["hi"] is None or date <= r["hi"])
            if acting:
                if covers and not same_office(office, r["ministry"]):
                    out.append(r)
                continue
            if (covers and ministry_compatible(office, r["ministry"])) or (r["ministry"] and r["ministry"] in pn):
                out.append(r)
        return out

    def _resolve_nonleg(self, term, date, nm, hanja, cands, tclass, pos, res):
        """Non-legislator titles: dual-office ministers (sitting members) and former members."""
        if tclass not in ("cabinet", "nominee"):
            if cands:
                return {**res, "method": "nonleg_name_collision", "n_cand": len(cands),
                        "note": "title is not a member title; not linked"}
            return {**res, "method": "nonleg_not_member"}
        lookup = nm  # the panel is in hangul: a hanja label is looked up by its unique person's name
        if hanja and len(self.p_by_hanja.get(nm, [])) == 1:
            lookup = nfkc(self.persons.at[self.p_by_hanja[nm][0], "name"])
        panel = self.panel_rows(lookup, date, pos, tclass)
        seated = [cd for cd in cands if self.seated(cd, term, date)[0]]
        dual = [r for r in panel if (r["assembly"] == term if r["assembly"] is not None else r["dual"])]
        if dual and seated:
            pick = seated
            if len(pick) > 1:
                d = dual[0]["district"]
                pick = [cd for cd in seated if d and any(d[:4] in s["district"] or d[:4] in s["district_api"]
                                                         for s in self.stints[(cd, term)])]
            if len(pick) == 1:
                return {**res, "naas_cd": pick[0], "method": "nonleg_dual_office", "confidence": "medium",
                        "n_cand": len(seated), "note": f"minister panel dual office ({dual[0]['ministry']})"}
            return {**res, "method": "nonleg_dual_office_ambiguous", "n_cand": len(seated),
                    "note": "candidates=" + ",".join(seated)}
        note_terms = set().union(*[r["note_terms"] for r in panel]) if panel else set()
        if seated and not dual:
            # the panel note says the official was a member of this term, and every term it names is a
            # term of the seated member (이달곤: 18대 비례, resigned 2009-02-03, heard as 후보자 while seated)
            if (len(seated) == 1 and term in note_terms
                    and note_terms <= set(self.terms_all.get(seated[0], []))):
                return {**res, "naas_cd": seated[0], "method": "nonleg_sitting_member_panel_note",
                        "confidence": "medium", "n_cand": 1, "note": f"minister panel note names term {term}"}
            # a sitting member shares the name, but no panel evidence: not linked (e.g. 김영주 2007)
            return {**res, "method": "nonleg_name_collision", "n_cand": len(seated),
                    "note": "sitting member with the same name, no dual-office record"}
        # former member (of an earlier term, or of this term and no longer seated), unique in all persons
        pool = self.p_by_hanja.get(nm, []) if hanja else self.p_by_name.get(nm, [])
        if len(pool) != 1:
            return {**res, "method": "nonleg_not_member" if not pool else "nonleg_former_ambiguous",
                    "n_cand": len(pool), "note": ("candidates=" + ",".join(pool)) if pool else None}
        cd = pool[0]
        ts = self.terms_all.get(cd, [])
        starts = [s["start"] for t in range(16, 23) for s in self.stints.get((cd, t), []) if s["start"]]
        served_before = [t for t in ts if t < term] or (date and starts and min(starts) <= date)
        if not ts or not served_before:
            return {**res, "method": "nonleg_future_member", "n_cand": 1,
                    "note": f"only a member later ({cd}, terms {[int(x) for x in ts]}); not linked"}
        b = self.persons.at[cd, "birth_date"]
        if date and isinstance(b, str) and len(b) >= 4:
            age = int(date[:4]) - int(b[:4])
            if not 35 <= age <= 80:
                return {**res, "method": "nonleg_former_implausible_age", "n_cand": 1,
                        "note": f"{cd} age {age}; not linked"}
        # evidence that the unique former member is this official, from a panel row that describes this
        # title on this date: (i) its note names terms, all of which the person served (a note can name
        # another person's term: '대구 달성군은 추경호 21대'); (ii) it records dual office in term A at
        # appointment, and 의원이력 has the person seated in term A on the appointment date (진영, 유은혜,
        # 박영선: 20대 dual-office ministers still in office in 21대)
        hit_note = sorted(int(x) for x in note_terms) if (note_terms and note_terms <= set(ts)) else []
        hit_dual = sorted({r["assembly"] for r in panel if r["dual"] and r["assembly"] in ts and r["appt"]
                           and self.seated(cd, r["assembly"], r["appt"])[0]})
        if hit_note or hit_dual:
            ev = ([f"note names term(s) {hit_note}"] if hit_note else []) + (
                [f"dual office at appointment in term(s) {hit_dual}, seat verified"] if hit_dual else [])
            return {**res, "naas_cd": cd, "method": "nonleg_former_member_panel", "confidence": "medium",
                    "n_cand": 1, "note": "minister panel " + "; ".join(ev)}
        # name uniqueness alone is not evidence (국무총리 김황식 2010-13 is not the 16대 member 김황식)
        return {**res, "method": "nonleg_former_unverified", "n_cand": 1,
                "note": f"unique person {cd} named {nm}, terms {[int(x) for x in ts]}; no panel evidence; not linked"}


# ----------------------------------------------------------------------------- enrich

_MEETING_COLS = ["term", "class_name", "hearing_type", "committee_raw", "subcommittee", "date"]
_KCOLS = ["conf_num", "term", "date", "committee_raw", "subcommittee", "pos", "name", "mem_id", "area", "label",
          "role_group"]
_DATE_RE = re.compile(r"^(\d{4})[-./]?(\d{1,2})[-./]?(\d{1,2})")


def iso_date(x):
    """'YYYY-MM-DD' for a date-like value ('2019-10-01', '2019.10.01', '20191001', date, Timestamp,
    '2019-10-01T09:00'), else None (null, empty string, unparseable or impossible date)."""
    if x is None or x is pd.NA or x is pd.NaT or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (dt.date, pd.Timestamp)):
        try:
            return x.strftime("%Y-%m-%d")
        except ValueError:
            return None
    m = _DATE_RE.match(str(x).strip())
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
    except ValueError:
        return None


def _map_unique(values, fn):
    """fn applied once per distinct value; object ndarray (null input -> fn(None))."""
    codes, uniq = pd.factorize(pd.Series(values, dtype=object), use_na_sentinel=True)
    vals = np.array([fn(v) for v in uniq] + [fn(None)], dtype=object)
    return vals[codes]


def _conf_key(s: pd.Series) -> pd.Series:
    """Meeting key as a string, so that int, float and str conf_num values meet (45604 = 45604.0 =
    '45604'). Contract dtype is int64; the XLSX validation passes v9 meeting ids as strings."""
    if pd.api.types.is_numeric_dtype(s.dtype):
        try:
            return pd.Series(pd.array(s, dtype="Int64"), index=s.index).astype("string")
        except (TypeError, ValueError):
            pass
    return s.astype("string").str.strip().str.replace(r"^(\d+)\.0+$", r"\1", regex=True)


def enrich(turns: pd.DataFrame, meetings: pd.DataFrame, ref=None) -> pd.DataFrame:
    """Add legislator identity columns to `turns` (row order, index and input columns unchanged).

    Uses role_group (roles.py) when present to decide the legislator side, else the title rule.
    Meeting columns (term, date, committee_raw, subcommittee) come from `turns` when present there,
    with nulls filled from `meetings`; conf_num is matched as a string key, so int and str ids meet.
    The date used is speech_date when it parses as a date, else the meeting date (leg_date_basis says
    which). Each key is resolved independently of the other meetings in the call (the only cross-row
    rule, the same-meeting complement, stays within one meeting). Input problems are reported with
    warnings.warn and counted per row: turns without a meetings row, term filled from meetings,
    speech_date values that do not parse as a date."""
    import warnings
    ref = ref or load_reference()
    R = ref["resolver"]
    n = len(turns)
    idx = turns.index

    def col(c):
        return turns[c] if c in turns.columns else pd.Series([None] * n, index=idx, dtype=object)

    tk = _conf_key(col("conf_num"))
    m = None
    if meetings is not None and len(meetings) and "conf_num" in meetings.columns:
        m = meetings.assign(_k=_conf_key(meetings["conf_num"]).values).drop_duplicates("_k").set_index("_k")
        miss = int((tk.notna() & ~tk.isin(m.index)).sum() + tk.isna().sum())
        if miss:
            warnings.warn(f"legislators.enrich: {miss} of {n} turns have no meetings row (conf_num)", stacklevel=2)

    def mcol(c):
        """Meeting-level value per turn: turns' own column with nulls filled from meetings."""
        f = tk.map(m[c]) if (m is not None and c in m.columns) else None
        if c not in turns.columns:
            return f if f is not None else col(c)
        v = turns[c]
        if f is None:
            return v
        k = int((v.isna() & f.notna()).sum())
        if k:
            warnings.warn(f"legislators.enrich: {k} turns have a null '{c}' filled from meetings", stacklevel=3)
        return v.where(v.notna(), f)

    sd = _map_unique(col("speech_date").values, iso_date)
    md = _map_unique(mcol("date").values, iso_date)
    sd_given = _map_unique(col("speech_date").values,
                           lambda v: v is not None and v is not pd.NaT and not (isinstance(v, float) and np.isnan(v))
                           and str(v).strip() != "")
    bad_sd = int((sd_given.astype(bool) & pd.isna(sd)).sum())
    if bad_sd:
        warnings.warn(f"legislators.enrich: {bad_sd} speech_date values do not parse as dates; meeting date used",
                      stacklevel=2)
    has_sd = ~pd.isna(sd)
    date = np.where(has_sd, sd, md)
    basis = np.where(has_sd, "speech_date", np.where(pd.isna(md), None, "meeting_date"))
    term = pd.array(pd.to_numeric(mcol("term"), errors="coerce"), dtype="Int64")
    if n and pd.isna(term).all():
        warnings.warn("legislators.enrich: no turn has a term (turns.term / meetings.term)", stacklevel=2)
    keys = pd.DataFrame({
        "conf_num": tk.values, "term": term, "date": date,
        "committee_raw": mcol("committee_raw").values, "subcommittee": mcol("subcommittee").values,
        "pos": col("speaker_pos").values, "name": col("speaker_name").values,
        "mem_id": pd.array(pd.to_numeric(col("speaker_mem_id"), errors="coerce"), dtype="Int64"),
        "area": col("speaker_area").values, "label": col("speaker_label_raw").values,
        "role_group": col("role_group").values,
    })
    codes = keys.groupby(_KCOLS, sort=False, dropna=False).ngroup().to_numpy() if n else np.zeros(0, dtype=int)
    first = np.unique(codes, return_index=True)[1]
    uniq = keys.iloc[first].reset_index(drop=True)
    del keys
    uniq = uniq.astype(object).where(uniq.notna(), None)
    results = []
    for r in uniq.itertuples(index=False):
        tc = title_class(r.pos, R.committee_names)
        if isinstance(r.role_group, str):
            leg_side, basis_k = r.role_group == "legislator", "role_group"
        else:
            leg_side, basis_k = tc == "legislator", "title_rule"
        t = int(r.term) if r.term is not None else None
        res = R.resolve(t, r.date, r.pos, r.name, r.mem_id, r.area, r.label,
                        parent_committee(r.committee_raw, r.subcommittee), leg_side, tc)
        res.update(leg_side=leg_side, basis=basis_k, tclass=tc)
        results.append(res)
    uniq["resobj"] = results
    _complement_pass(uniq, R)
    # metadata per unique key, broadcast to the rows through the key codes
    G = len(uniq)
    arr = {c: np.full(G, None, dtype=object) for c in ADDED_COLUMNS}
    for g, r in enumerate(uniq.itertuples(index=False)):
        res = _apply_mem_id_correction(r.conf_num, r.label, r.resobj)
        cd = res["naas_cd"]
        arr["naas_cd"][g], arr["id_method"][g], arr["id_confidence"][g] = cd, res["method"], res["confidence"]
        arr["id_candidates"][g], arr["id_note"][g] = res["n_cand"], res["note"]
        arr["leg_side"][g], arr["leg_side_basis"][g], arr["leg_title_class"][g] = res["leg_side"], res["basis"], res["tclass"]
        arr["id_label_repair"][g] = bool(res.get("label_repair"))
        arr["id_memid_status"][g] = res.get("memid_status")
        if cd is not None:
            t = int(r.term) if r.term is not None else None
            p = R.persons.loc[cd]
            seat, stint = R.seated(cd, t, r.date) if t else (None, None)
            arr["leg_name_hangul"][g], arr["leg_name_hanja"][g] = p["name"], p["name_hanja"]
            arr["gender"][g], arr["birth_date"][g] = p["gender"], p["birth_date"]
            arr["seniority"][g] = R.seniority(cd, t) if t else None
            arr["leg_is_term_member"][g], arr["leg_seated_on_date"][g] = bool(stint), seat
            if stint:
                arr["district"][g], arr["elect_type"][g] = stint["district_raw"], stint["elect_type"]
                arr["leg_stint"][g], arr["leg_record_mem_id"][g] = stint["stint"], stint["record_mem_id"]
    # a speaker label the parser rates 'low' (build_turns label_confidence) is never linked (R2): the person
    # columns are nulled after resolution and id_method says why; the unresolved reasons stay as they are
    lowc = (col("label_confidence").astype("string").eq("low").fillna(False).to_numpy(dtype=bool)
            if "label_confidence" in turns.columns else np.zeros(n, dtype=bool))
    linked_row = np.array([x is not None for x in arr["naas_cd"]], dtype=bool)[codes] if n else np.zeros(0, dtype=bool)
    blocked = lowc & linked_row
    out = turns.copy(deep=False)
    for c in ADDED_COLUMNS:
        if c == "leg_date_basis":
            v = basis
        else:
            a = arr[c]
            if c in _STR_COLS:
                a = np.array([None if (x is None or (isinstance(x, float) and np.isnan(x))) else str(x) for x in a],
                             dtype=object)
            elif c in _BOOL_COLS:
                a = np.array([None if x is None else bool(x) for x in a], dtype=object)
            v = a[codes]
            if blocked.any() and c in _LOW_CONF_NULLED:
                v = v.copy()
                v[blocked] = "unlinked:label_confidence_low" if c == "id_method" else None
        out[c] = pd.array(v, dtype=_OUT_DTYPES.get(c, "string"))
    out.attrs["legislators"] = {"rows": int(n), "label_confidence_low_rows": int(lowc.sum()),
                                "label_confidence_low_links_blocked": int(blocked.sum())}
    return out


# columns emptied when a label_confidence 'low' turn would have been linked (id_method gets the reason)
_LOW_CONF_NULLED = ("naas_cd", "leg_name_hangul", "leg_name_hanja", "gender", "birth_date", "district", "elect_type",
                    "seniority", "id_method", "id_confidence", "leg_is_term_member", "leg_seated_on_date", "leg_stint",
                    "leg_record_mem_id")


_STR_COLS = {"naas_cd", "leg_name_hangul", "leg_name_hanja", "gender", "birth_date", "district", "elect_type",
             "id_method", "id_confidence", "id_note", "leg_side_basis", "leg_title_class", "leg_record_mem_id",
             "id_memid_status"}
_BOOL_COLS = {"leg_is_term_member", "leg_seated_on_date", "leg_side", "id_label_repair"}
_OUT_DTYPES = {**{c: "string" for c in _STR_COLS}, **{c: "boolean" for c in _BOOL_COLS},
               "seniority": "Int16", "id_candidates": "Int16", "leg_stint": "Int16", "leg_date_basis": "string"}


def _complement_pass(uniq, R):
    """Within one meeting, an unmarked label of a two-person homonym refers to the person that the
    marked label does not (21대 '이수진(비)' / '이수진')."""
    grp = defaultdict(list)
    for i, r in enumerate(uniq.itertuples(index=False)):
        nm, _ = clean_name(r.name)
        grp[(r.conf_num, nm)].append(i)
    for (cn, nm), idx in grp.items():
        if len(idx) < 2:
            continue
        rs = [uniq.at[i, "resobj"] for i in idx]
        marked = {r["naas_cd"] for r in rs if r["marked"] and r["naas_cd"] and r["method"].startswith("homonym_marker")}
        for i, r in zip(idx, rs):
            if r["method"] == "unresolved_ambiguous" and not r["marked"] and len(r["candidates"]) == 2 and len(marked) == 1:
                other = [c for c in r["candidates"] if c not in marked]
                if len(other) == 1:
                    r.update(naas_cd=other[0], method="homonym_meeting_complement", confidence="medium",
                             note=f"marked label in the same meeting = {next(iter(marked))}")


if __name__ == "__main__":
    build_tables(write=True, verbose=True)
