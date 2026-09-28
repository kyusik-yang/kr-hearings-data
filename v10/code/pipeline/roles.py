"""roles.py - speaker role classification for the v10 pipeline (component 'roles').

Public API
----------
enrich(turns, meetings, *, roster=None, na_committees=None, v9_lookup=None, na_committees_by_term=None) -> DataFrame
    Returns `turns` (same rows, same order, same index; a shallow copy, the input is not
    modified) with the contract columns
    role, role_group, role_rule, role_v9_compat, affiliation_raw, person_title
    and the extra columns title_raw, pos_hangul, pos_fix, role_v9_compat_src. Counts (turns
    without a meeting row, distinct keys, dropped duplicate meeting rows, reference-file load
    status) are in out.attrs['roles_enrich']. Conflicting duplicate meeting rows raise ValueError.

Column semantics
    affiliation_raw  the institution printed in the title (v9 semantics for ministers: '국방부장관
                     김관진' -> '국방부'): printed title minus acting / nominee words and one office
                     word; null when the title is only an office word (위원, 증인, 국무총리 …); the
                     whole title when no office word is recognised. See affiliation_from_title().
    title_raw        the printed title (whitespace removed, Hanja kept).
    pos_fix          label repairs applied, '+'-joined. 'two_speakers' marks a label that fuses two
                     speakers' labels around a marker; role and name come from the last label and
                     which speaker the text belongs to is not known. 'two_names' marks
                     'NAME委員NAME2' (first name kept).
    role_v9_compat   v9 lookup / chain answer. For XLSX turns the lookup key is the XLSX label and
                     v9's member_id (source_member_id). On v9 XLSX rows a lookup hit reproduces v9
                     by construction; the chain alone is the out-of-lookup estimate.
classify(pos, name=None, *, label_raw=None, mem_id=None, term=None, class_name=None,
         hearing_type=None, is_subcommittee=None, committee_raw=None, subcommittee=None,
         roster=None, na_committees=None) -> RoleResult
    One speaker turn. Pure function of its arguments plus the lazily loaded default
    roster / committee list (both injectable).
split_label(label) -> (pos, name)
    Split an XLSX/HWP style label ('홍길동 위원', '국방부장관 이종섭', '위원장 홍길동').
normalize_title(s) -> (hangul, n_unmapped_hanja)
    Hanja -> Hangul (CJK compatibility ideographs folded first), 두음법칙 fixes.
rule_table() -> DataFrame
    The ordered rule table (legislator stage + non-legislator cascade) as data.
v9_compat(pos_hangul, name, has_mid, lookup=None) -> (role, src)
    What the v9 XLSX-era cascade (legacy_rules.classify_with_lookup) gives for the
    same speaker rendered as an XLSX label.

Design (see the report of the roles component for counts)
-------------------------------------------------------
1. Label repair (`pos_fix`): leading junk before '◯', swapped data-pos / data-name,
   name fused to the title ('環境部長官金明子'), bare names, a title printed in the name field.
2. Legislator stage (ordered, first match wins):
   empty label -> unknown; member titles (위원, 의원, 委員, 議員) -> legislator;
   presiding titles -> chair, with context: 소위원장 / 조정위원장 preside only in a
   subcommittee (or 안건조정위원회) meeting, otherwise they report (legislator);
   의장 / 부의장 preside only in plenary (국회본회의, 전원위원회);
   '{NA committee}위원장(대리)' presides in its own committee and reports elsewhere
   (plenary); 반장 (audit team leader) presides.
   A '{X}위원장' is an NA committee chair when X names an NA committee (meeting universe),
   abbreviates a standing committee (>= 4 characters), contains 인사청문/국정조사/임명동의/
   선출에관한, or is a '…특별' name sharing its first four characters with an NA special
   committee of the same term. Otherwise it is a government commission head. Outside
   plenary, a committee-like title whose speaker is not in that term's roster (Hangul,
   Hanja, Hangul reading of the Hanja name) is a government body of the same name.
   mem_id > 0 is decisive for member titles and bare names. It does not override a printed
   non-member title: executive titles keep the executive role (dual office) and a non-NA
   '{X}위원장' keeps the commission role ('memid.title_conflict'), because the viewer also
   links homonymous officials (22대 방송통신위원장 김태규/이진숙 carry members' mem_ids).
3. Non-legislator cascade: an ordered table of regex rules on the Hangul title, applied
   to each '겸' segment with acting/designate suffixes removed; first rule that matches
   any segment wins. Then the verbatim v4/v5 'other' rules, then TAIL_RULES for titles no
   earlier rule recognises. Rules marked `changes_v9` deliberately differ from v9.
   rule_table() returns all of them in evaluation order.

No network access. File reads (all lazy, cached; a missing or unreadable file is recorded in
LOAD_STATUS / load_status() and raises a RuntimeWarning, never an exception): the Hanja table in
v10/raw/third_party/, the roster v10/interim/members_term_16_22.parquet, committee names
from v10/interim/meeting_universe_api.parquet and the v9 speaker-role table
v10/interim/04_v9_speaker_role_table_xlsx_era.parquet.
"""
from __future__ import annotations

import functools
import json
import math
import os
import re
import sys
import unicodedata
import warnings
from dataclasses import dataclass
from typing import Mapping, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
V10 = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(V10, "code"))

import legacy_rules as LR  # noqa: E402  (verbatim v3-v9 rules)

HANJA_TABLE = os.path.join(V10, "raw", "third_party", "hanja_table_0.15.1.yml")
ROSTER_PATH = os.path.join(V10, "interim", "members_term_16_22.parquet")
UNIVERSE_PATH = os.path.join(V10, "interim", "meeting_universe_api.parquet")
V9_TABLE_PATH = os.path.join(V10, "interim", "04_v9_speaker_role_table_xlsx_era.parquet")
PERSON_TERMS_PATH = os.path.join(V10, "interim", "pipeline", "legislators", "person_terms.parquet")

# --------------------------------------------------------------------------- taxonomy

LEG_ROLES = LR.LEG_ROLES
NONLEG_ROLES = LR.NONLEG_ROLES
EXCLUDED_ROLES = LR.EXCLUDED_ROLES        # committee_staff, other, unknown
ALL_ROLES = LR.ALL_ROLES                  # 33 v9 roles + 'unknown' (empty label only)


def role_group(role: str) -> str:
    if role in LEG_ROLES:
        return "legislator"
    if role in NONLEG_ROLES:
        return "nonlegislator"
    return "excluded"


# --------------------------------------------------------------------------- reference-file status

# One entry per lazily loaded reference file: {'ok': bool, 'n': entries loaded, 'path': ..., 'error': ...}.
# A missing or unreadable file never raises. It is recorded here and a RuntimeWarning is issued,
# because an empty roster or committee list changes named-chair and bare-name decisions.
LOAD_STATUS: dict = {}


def _record_load(name: str, path: str, n: int, error=None) -> None:
    LOAD_STATUS[name] = {"ok": error is None, "n": int(n), "path": path, "error": None if error is None else repr(error)}
    if error is not None:
        warnings.warn(f"roles: reference file '{name}' not loaded ({path}): {error!r}", RuntimeWarning, stacklevel=3)
    elif n == 0:
        warnings.warn(f"roles: reference file '{name}' loaded 0 entries ({path})", RuntimeWarning, stacklevel=3)


def load_status() -> dict:
    """Copy of LOAD_STATUS (which reference files were loaded, with entry counts and errors)."""
    return {k: dict(v) for k, v in LOAD_STATUS.items()}


# --------------------------------------------------------------------------- input normalisation

def _isnull(x) -> bool:
    """None, float NaN, pandas NA / NaT (without importing pandas)."""
    if x is None:
        return True
    if isinstance(x, float):
        return x != x
    return type(x).__name__ in ("NAType", "NaTType")


def _clean_str(x) -> Optional[str]:
    if _isnull(x):
        return None
    s = str(x).strip()
    return s or None


def mem_id_present(x) -> bool:
    """True when a viewer mem_id is present: a positive number (int, float, numeric string such
    as '1234' or '1234.0'). 0, 0.0, '0', '0.0', negative, empty, NaN, None, pd.NA are absent.
    A non-numeric non-empty string (never seen in viewer data) counts as present."""
    if _isnull(x):
        return False
    if isinstance(x, bool):
        return x
    try:
        v = float(str(x).strip())
    except (TypeError, ValueError):
        return str(x).strip().lower() not in ("", "nan", "none", "<na>", "null", "nat")
    return math.isfinite(v) and v > 0


_TRUE_STR = frozenset({"true", "t", "1", "yes", "y"})
_FALSE_STR = frozenset({"false", "f", "0", "no", "n"})


def to_bool_or_none(x) -> Optional[bool]:
    """bool / 0-1 number / 'True'/'False'/'1'/'0' strings -> bool; anything else (null, '', other
    strings or numbers) -> None (context unknown)."""
    if _isnull(x):
        return None
    if isinstance(x, bool) or type(x).__name__ in ("bool_", "bool"):
        return bool(x)
    if isinstance(x, str):
        t = x.strip().lower()
        return True if t in _TRUE_STR else (False if t in _FALSE_STR else None)
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return True if v == 1 else (False if v == 0 else None)


# --------------------------------------------------------------------------- normalization

_WS = re.compile(r"\s+")
_HANJA_CHAR = re.compile(r"[㐀-䶿一-鿿豈-﫿]")
_COMPAT = re.compile(r"[豈-﫿]")
_NAME_CHARS = r"[가-힣㐀-䶿一-鿿豈-﫿]"
_MIDDOTS = str.maketrans({c: "ㆍ" for c in "·․‧∙・•"})

# 두음법칙 for the first syllable of a title converted from Hanja (standard Korean orthography).
_DUEUM_INITIAL = dict(zip(
    "녀뇨뉴니랴려례료류리라래로뢰루르락란람랑래략량렬렴렵령록론롱룡륜률륭륵름릉린림립력련녁년념녕뉵",
    "여요유이야여예요유이나내노뇌누느낙난남낭내약양열염엽영녹논농용윤율융늑늠능인임입역연역연염영육"))
# Word-internal morphemes whose Hanja reading needs 두음법칙 because they start a word
# inside a compound title (applied only to titles that contained Hanja).
_DUEUM_WORDS = (
    ("리사장", "이사장"), ("상임리사", "상임이사"), ("상무리사", "상무이사"), ("로동", "노동"),
    ("녀성", "여성"), ("립법", "입법"), ("륙군", "육군"), ("련합", "연합"), ("류통", "유통"),
    ("력사", "역사"), ("림업", "임업"), ("락농", "낙농"), ("리사회", "이사회"), ("례산", "예산"),
    ("로인", "노인"), ("리재", "이재"), ("량곡", "양곡"), ("립지", "입지"), ("류치", "유치"),
)
# Hanja words whose first character takes the initial-sound rule inside a compound title
# (總領事 = 총영사, 私立學校敎職員年金 = …연금, 勞使協力官 = 노사협력관, 輸出入銀行理事 = …이사).
# Replaced at the Hanja level before the per-character reading, two characters for two
# syllables, so index positions stay aligned with the raw title.
_HANJA_WORDS = {
    "領事": "영사", "年金": "연금", "勞使": "노사", "勞組": "노조", "勞務": "노무", "勞動": "노동",
    "勞働": "노동", "理事": "이사", "女性": "여성", "立法": "입법", "陸軍": "육군", "聯合": "연합",
    "流通": "유통", "歷史": "역사", "林業": "임업", "酪農": "낙농", "老人": "노인", "糧穀": "양곡",
    "立地": "입지", "旅券": "여권", "旅客": "여객", "料金": "요금", "利用": "이용", "領土": "영토",
    "領海": "영해", "來日": "내일", "例規": "예규", "禮式": "예식", "論山": "논산", "綠色": "녹색",
    "陸上": "육상", "良心": "양심", "旅行": "여행", "冷凍": "냉동", "兩院": "양원",
}
# 理事 is a word only when 理 does not close a preceding 理-word (管理事務所 = 관리사무소);
# 領事 not after 統 (大統領事務… would be 대통령사무…).
_HANJA_WORD_BLOCK_PREV = {
    "理事": set("管處代總審整修經料倫心物原論合道義受調地眞病推攝事辨司助條治"),
    "領事": set("統要占首"),
}
# Surname readings: the family name keeps its conventional Hangul spelling (金 = 김) and both
# spellings are accepted for surnames written with and without the initial-sound rule.
_SURNAME_READINGS = {
    "金": ("김",), "李": ("이", "리"), "柳": ("유", "류"), "劉": ("유", "류"), "林": ("임", "림"),
    "羅": ("나", "라"), "盧": ("노", "로"), "呂": ("여", "려"), "陸": ("육", "륙"), "龍": ("용", "룡"),
    "廉": ("염", "렴"), "梁": ("양", "량"), "樑": ("양", "량"), "兪": ("유",),
    "庾": ("유",), "南宮": ("남궁",), "諸葛": ("제갈",), "皇甫": ("황보",),
    "鮮于": ("선우",), "司空": ("사공",), "獨孤": ("독고",), "東方": ("동방",), "西門": ("서문",),
}


@functools.lru_cache(maxsize=1)
def _hanja_map() -> dict:
    """{hanja char: hangul syllable} from the hanja 0.15.1 table (27,497 entries)."""
    m = {}
    try:
        rx = re.compile(r'^"(.+?)":\s*"(.+?)"\s*$')
        with open(HANJA_TABLE, encoding="utf-8") as fh:
            for line in fh:
                mm = rx.match(line.strip())
                if mm:
                    k = json.loads('"' + mm.group(1) + '"')
                    v = json.loads('"' + mm.group(2) + '"')
                    if len(k) == 1 and len(v) == 1:
                        m[k] = v
    except Exception as e:  # recorded and warned, see LOAD_STATUS
        _record_load("hanja_table", HANJA_TABLE, 0, e)
        return {}
    _record_load("hanja_table", HANJA_TABLE, len(m))
    return m


@functools.lru_cache(maxsize=200_000)
def normalize_title(s: Optional[str]) -> tuple:
    """Hangul, whitespace-free rendering of a printed title. Returns (text, n_unmapped).
    Character-for-character (index positions are preserved), so a split point found on
    the Hangul text is valid on the raw text with whitespace removed."""
    if s is None:
        return "", 0
    s = _WS.sub("", str(s)).translate(_MIDDOTS)
    if not s:
        return "", 0
    had_hanja = bool(_HANJA_CHAR.search(s))
    if not had_hanja:
        return s, 0
    hm = _hanja_map()
    out, unmapped = [], 0
    folded = "".join((unicodedata.normalize("NFKC", ch) if _COMPAT.match(ch) else ch) for ch in s)
    if len(folded) != len(s):
        folded = s
    i = 0
    while i < len(folded):
        w = folded[i:i + 2]
        rd = _HANJA_WORDS.get(w)
        if rd is not None and not (i > 0 and folded[i - 1] in _HANJA_WORD_BLOCK_PREV.get(w, ())):
            out.extend(rd)
            i += 2
            continue
        c = folded[i]
        if _HANJA_CHAR.match(c):
            r = hm.get(c)
            if r is None:
                unmapped += 1
                out.append(c)
            else:
                out.append(r)
        else:
            out.append(c)
        i += 1
    t = "".join(out)
    if _HANJA_CHAR.match(s[0]) and t[0] in _DUEUM_INITIAL:
        t = _DUEUM_INITIAL[t[0]] + t[1:]
    for a, b in _DUEUM_WORDS:
        if a in t:
            t = t.replace(a, b)
    if t.endswith("리사") and len(t) > 2:
        t = t[:-2] + "이사"
    return t, unmapped


# --------------------------------------------------------------------------- label parsing

MEMBER_TITLES = frozenset({"위원", "의원"})                 # after Hanja conversion (委員, 議員)
# printed variants -> canonical title (after Hanja conversion; '義員' is a typo of 議員)
MEMBER_TITLE_TYPOS = {"의원": "의원", "義員": "의원", "위원님": "위원", "의원님": "의원",
                      "위원당대리": "위원장대리", "위윈장대리": "위원장대리", "위원장대리위": "위원장대리",
                      "소위원잔": "소위원장", "소위원자": "소위원장", "위윈": "위원",
                      # HWP 18대 misprints of the presiding subcommittee chair / deputy chair and of
                      # 위원 (each checked against the same person's other turns in the meeting)
                      "소위원쟝": "소위원장", "소위원님": "소위원장", "소위원회": "소위원장",
                      "위원원장대리": "위원장대리", "의위원": "위원",
                      # misprinted non-member titles seen in XML / HWP (exact strings)
                      "국모총리": "국무총리", "국무총식": "국무총리", "진실인": "진술인", "참조인": "참고인"}
# misprinted final syllable of '…청장' / '…실장' after an institution name (HWP / XML typos)
TITLE_SUFFIX_TYPOS = (("청정", "청장"), ("청창", "청장"), ("정창", "청장"), ("실정", "실장"), ("실잘", "실장"),
                      ("차창", "차장"))
ACTING_RE = re.compile(r"(직무대행자|직무대행|직무대리|권한대행|대행|대리|서리)$")
DESIGNATE_RE = re.compile(r"(임명예정자)$")
NOMINEE_RE = re.compile(r"(후보자|내정자)$")
FORMER_RE = re.compile(r"^\((전|前)\)")
LEAD_JUNK_RE = re.compile(r"^[^◯○]*[◯○]")
PAREN_TAG_RE = re.compile(r"\([^()]{0,3}\)$")          # '이수진(비)', '최경환(국)'

# Strong title endings used to recognise a title and to split a name fused to it.
TITLE_TOKENS = (
    "위원장", "부위원장", "소위원장", "위원", "의원", "의장", "부의장", "장관", "차관보", "차관", "총리",
    "청장", "처장", "원장", "사장", "이사장", "회장", "본부장", "실장", "국장", "심의관", "정책관",
    "관리관", "감사관", "기획관", "비서관", "대사", "총영사", "사령관", "참모총장", "교육감", "지사",
    "시장", "구청장", "군수", "증인", "진술인", "참고인", "전문위원", "조사관", "후보자", "내정자",
    "대리", "대행", "서리", "교수", "반장", "차장", "단장", "부장", "과장", "팀장", "소장", "관장",
    "총장", "총재", "대변인", "검사장", "이사", "감사", "상무", "전무", "대표이사", "공사", "영사",
    "판사", "검사", "감정인", "방청인", "변호인", "대통령", "간사", "사회자", "진행자", "통역인", "통역",
    "속기사", "발표자", "토론자", "발제자", "위원장대리",
)
_TITLE_END_RE = re.compile("(" + "|".join(sorted(map(re.escape, TITLE_TOKENS), key=len, reverse=True)) + ")$")
_NAME_ONLY_RE = re.compile(rf"^{_NAME_CHARS}{{1,4}}(\([^()]{{0,3}}\))?$")
# name-first labels with an empty data-name ('宋榮珍議員', '김성곤위원', typo '李在五義員');
# only plain member titles (never 위원장, whose prefixes are committee names).
_NAME_FIRST_MEMBER_RE = re.compile(rf"^(?P<name>{_NAME_CHARS}{{2,4}})(?P<title>委員|議員|義員|委原|위원|의원)$")
# 'NAME委員NAME2': two member labels fused, the second one cut ('金晟祚委員李忠馥')
_TWO_NAMES_RE = re.compile(rf"^(?P<name>{_NAME_CHARS}{{2,4}})(?P<title>委員|議員|위원|의원)(?P<name2>{_NAME_CHARS}{{2,4}})$")
# member label whose title lost its last character ('李漢久委'); accepted only for roster names
_NAME_TRUNC_MEMBER_RE = re.compile(rf"^(?P<name>{_NAME_CHARS}{{2,4}})(?P<title>委|議)$")
# Words that precede 위원 in the titles of non-member committee members ('민간위원', '공익위원',
# '國務委員'); a 'NAME위원' split whose name part starts with one of these is refused.
# Checked on the Hangul reading, so Hanja forms are covered.
_NAME_FIRST_BLOCK = ("국무", "전문", "수석", "감사", "상임", "비상임", "상근", "비상근", "연구", "금융", "통화",
                     "선거", "교육", "운영", "조정", "전원", "심사", "자문", "분과", "평가", "심의", "징계",
                     "윤리", "위원", "민간", "공익", "근로자", "사용자", "정부", "명예", "외부", "위촉", "당연직",
                     "실무", "특별", "추천", "선임", "객원", "초빙", "겸임", "책임", "대표", "조사", "심판",
                     "소위", "예결", "청문", "간사")


def _blocked_name_part(name_raw: str) -> bool:
    return normalize_title(name_raw)[0].startswith(_NAME_FIRST_BLOCK)


# a 'TITLE + NAME' split is refused when the 'name' contains an organisational unit word
# ('대통령비서실' -> '비서실', '감사원사무처' -> '원사무처'); single final characters such as 원/실
# are not used because they end common given names (박지원)
_UNIT_WORD_RE = re.compile(r"사무|비서|본부|위원|연구|정책|기획|총괄|담당|센터")


def looks_like_title(t: str) -> bool:
    """t is a Hangul-normalized string."""
    return bool(t) and bool(_TITLE_END_RE.search(ACTING_RE.sub("", t) or t))


def looks_like_name(raw: str) -> bool:
    if not raw:
        return False
    s = _WS.sub("", raw)
    if not _NAME_ONLY_RE.match(s):
        return False
    h, _ = normalize_title(PAREN_TAG_RE.sub("", s))
    return h not in MEMBER_TITLES and not looks_like_title(h)


def split_label(label) -> tuple:
    """XLSX / HWP label -> (pos, name). Members print 'NAME 위원' (name first), everyone
    else 'TITLE NAME'. One token -> (token, None) unless it is a bare name."""
    if label is None:
        return None, None
    s = str(label).strip()
    if "◯" in s or "○" in s:
        s = LEAD_JUNK_RE.sub("", s).strip()
    if not s:
        return None, None
    toks = s.split()
    if len(toks) == 1:
        return (None, toks[0]) if looks_like_name(toks[0]) else (toks[0], None)
    last_h, _ = normalize_title(PAREN_TAG_RE.sub("", toks[-1]))
    if last_h in ("위원", "의원", "위원장", "위원님", "의원님") and looks_like_name(toks[0]) and len(toks) == 2:
        return toks[-1], toks[0]
    return toks[0], " ".join(toks[1:])


def _fused_split(raw: str, hangul: str):
    """Title with a name glued to its end: return (title_raw, name_raw) or None."""
    m = _NAME_FIRST_MEMBER_RE.match(raw)
    if m and not _blocked_name_part(m.group("name")):
        return ("NAME_FIRST", m.group("title"), m.group("name"))
    if looks_like_title(hangul):
        return None
    best = None  # rightmost end position of a title token that leaves a 2-4 character name
    for i in range(2, len(hangul) - 1):
        if not _TITLE_END_RE.search(hangul[:i]):
            continue
        rest_h = hangul[i:]
        if 2 <= len(rest_h) <= 4 and re.fullmatch(_NAME_CHARS + "+", raw[i:]) and not looks_like_title(rest_h) \
                and not _UNIT_WORD_RE.search(rest_h):
            best = i
    if best is None:
        return None
    return ("TITLE_FIRST", raw[:best], raw[best:])


@dataclass
class Parsed:
    pos_raw: Optional[str]      # printed title (affiliation_raw), whitespace collapsed
    name_raw: Optional[str]
    pos_h: str                  # Hangul, whitespace-free
    fix: str                    # repair applied
    unmapped_hanja: int
    pos_h_printed: str = ""     # Hangul title before typo / party-tag repairs (for v9_compat)


# Common Korean surnames (Hanja and Hangul), used only to arbitrate between two splits of a fused label
_SURNAME_CHARS = frozenset(
    "金李朴崔鄭姜趙尹張林韓吳徐申權黃安宋柳全洪高文梁孫裵白許劉南沈盧河丁郭成車朱禹具辛任羅田閔兪陳池嚴元蔡千方孔"
    "康玄咸卞廉楊邊呂秋魯都蘇愼石宣薛馬吉周延表魏明奇潘王琴玉陸印孟諸卓秦魚殷片龍芮慶奉程史夫皇甫太睦桂"
    "김이박최정강조윤장임한오서신권황안송류유전홍고문양손배백허남심노하곽성차주우구민진지엄원채천방공현함변염여"
    "추도소석선설마길연표위명기반왕금옥육인맹제탁어은편용예경봉사부태목계")
_MARKER_RE = re.compile(r"[◯○]")
_TIME_JUNK_RE = re.compile(r"[0-9\s()\[\]:.,;·ㆍ시분초~\-]+")
_PUNCT = ";:,.·ㆍ・•‧∙"
_TRAIL_PUNCT_RE = re.compile(f"[{_PUNCT}]+$")
_LEAD_PUNCT_RE = re.compile(f"^[{_PUNCT}]+")
_MIDDOT_MEMBER_RE = re.compile(rf"^(?P<name>{_NAME_CHARS}{{2,4}})[·ㆍ・•‧∙](?P<title>委員|議員|위원|의원)$")
_LABEL_EXTRA_RE = re.compile(rf"{_NAME_CHARS}+(\([^()]{{0,3}}\))?")


def parse_speaker(pos, name, label_raw=None) -> Parsed:
    pos = _clean_str(pos)
    name = _clean_str(name)
    label_raw = _clean_str(label_raw)
    fixes = []
    if pos is None and name is None and label_raw is not None:
        pos, name = split_label(label_raw)
        fixes.append("from_label_raw")
    # Two labels fused around a speaker marker: the end of the previous speaker's label, the
    # marker, then a complete label ('임해규 의원◯행정자치부장관 박명재', '國家報勳處長 李在達○徐相燮委員').
    # Title and name are both taken from the label after the last marker, so a title is never
    # paired with the other speaker's name. Flagged 'two_speakers': which speaker the text belongs
    # to is not known. A marker preceded only by a time stamp ('10시40분)◯議長') is ordinary junk.
    lab = label_raw if label_raw is not None else " ".join(x for x in (pos, name) if x)
    if lab and _MARKER_RE.search(lab):
        idx = max(lab.rfind("◯"), lab.rfind("○"))
        before, after = lab[:idx], lab[idx + 1:].strip()
        if after and len(re.findall(_NAME_CHARS, _TIME_JUNK_RE.sub("", before))) >= 2:
            pos, name = split_label(after)
            fixes.append("two_speakers")
    if pos and ("◯" in pos or "○" in pos):
        new = LEAD_JUNK_RE.sub("", pos).strip()
        if new != pos:
            pos = new or None
            fixes.append("lead_junk")
    if name and ("◯" in name or "○" in name) and LEAD_JUNK_RE.match(name):
        name = LEAD_JUNK_RE.sub("", name).strip() or None
        fixes.append("name_lead_junk")
    # stray punctuation around a label ('安商守委員;')
    for which in ("pos", "name"):
        v = pos if which == "pos" else name
        if v:
            nv = _TRAIL_PUNCT_RE.sub("", _LEAD_PUNCT_RE.sub("", v)).strip()
            if nv != v:
                if which == "pos":
                    pos = nv or None
                else:
                    name = nv or None
                fixes.append("punct")
    # An upstream split of a fused 'TITLENAME' label ('副總理兼財政經濟部長官陳稔' split as '…部長' +
    # '官陳稔') is re-done with this module's title tokens, only when the upstream name does not
    # start with a common surname and this module's name does ('保健福祉部次官張錫準' keeps 張錫準).
    if pos and name and label_raw is not None and not _WS.search(label_raw) and "two_speakers" not in fixes:
        labc = _TRAIL_PUNCT_RE.sub("", label_raw)
        pc, nc = _WS.sub("", pos), _WS.sub("", name)
        if labc == pc + nc and nc[:1] not in _SURNAME_CHARS:
            fs = _fused_split(labc, normalize_title(labc)[0])
            if fs and fs[0] == "TITLE_FIRST" and (fs[1], fs[2]) != (pc, nc) and fs[2][:1] in _SURNAME_CHARS:
                pos, name = fs[1], fs[2]
                fixes.append("resplit_fused_label")
    # A title printed with a space ('서울 高等檢察廳檢事長 鄭鎭圭') whose first word became the
    # position: the label minus its last word is the title when that reads as a title and the
    # current position does not.
    if pos and label_raw is not None and "two_speakers" not in fixes:
        toks = LEAD_JUNK_RE.sub("", label_raw).split() if _MARKER_RE.search(label_raw) else label_raw.split()
        if len(toks) >= 3 and looks_like_name(toks[-1]):
            th = normalize_title("".join(toks[:-1]))[0]
            ph = normalize_title(pos)[0]
            if looks_like_title(th) and not looks_like_title(ph) and ph not in MEMBER_TITLES \
                    and MEMBER_TITLE_TYPOS.get(ph) is None and (name is None or not looks_like_name(name)):
                pos, name = "".join(toks[:-1]), toks[-1]
                fixes.append("label_title_with_space")
    # the data-pos holds only the first part of a label whose rest is in label_raw
    # ('薛' / '薛 勳委員', '尹景湜' / '尹景湜 議員', '韓國輸出保險公社社長' / '… 李英雨')
    if pos and name is None and label_raw is not None and "two_speakers" not in fixes \
            and "from_label_raw" not in fixes:
        labc, posc = _WS.sub("", LEAD_JUNK_RE.sub("", label_raw) if _MARKER_RE.search(label_raw) else label_raw), \
            _WS.sub("", pos)
        if labc != posc and labc.startswith(posc):
            extra = labc[len(posc):]
            if len(extra) <= 6 and _LABEL_EXTRA_RE.fullmatch(extra):
                p2, n2 = split_label(LEAD_JUNK_RE.sub("", label_raw).strip())
                if p2 or n2:
                    pos, name = p2, n2
                    fixes.append("label_raw_extra")
    if pos and name is None:
        m = _MIDDOT_MEMBER_RE.match(pos)   # '우제창·위원'
        if m:
            pos, name = m.group("title"), m.group("name")
            fixes.append("middot_split")
    if pos and _WS.search(pos):
        # label and spoken text fused in the position field (HWP 'label_only' lines such as
        # '위원장 김영선 발언권 드리기 전에…', '한국토지공사사장 이종상 ……'): keep title + name
        toks = pos.split()
        t0h = normalize_title(toks[0])[0]
        if len(toks) >= 2 and looks_like_name(toks[1]) and (t0h in MEMBER_TITLES or looks_like_title(t0h)) \
                and (name is None or not looks_like_name(name)):
            pos, name = toks[0], toks[1]
            fixes.append("pos_multi_token")
        elif len(toks) >= 2 and looks_like_name(toks[0]) and normalize_title(toks[1])[0] in MEMBER_TITLES \
                and (name is None or not looks_like_name(name)):
            pos, name = toks[1], toks[0]
            fixes.append("pos_multi_token")
    if pos:
        pos = _WS.sub("", pos)
    # title printed in the name field (parser 'pos=name_only' misfire, or swapped attributes)
    if name:
        nh, _ = normalize_title(PAREN_TAG_RE.sub("", _WS.sub("", name)))
        name_is_title = nh in MEMBER_TITLES or looks_like_title(nh) or MEMBER_TITLE_TYPOS.get(nh) in MEMBER_TITLES
        if name_is_title and (pos is None or looks_like_name(pos)):
            pos, name = _WS.sub("", name), pos
            fixes.append("swap" if name else "name_as_title")
            if "◯" in pos or "○" in pos:
                pos = LEAD_JUNK_RE.sub("", pos).strip() or None
                fixes.append("lead_junk")
            # a name cut before its last character: data-pos '李', data-name '協委員';
            # data-pos '南宮', data-name '晳議員'
            m1 = re.match(rf"^(?P<x>{_NAME_CHARS})(?P<t>委員|議員|委原|위원|의원)$", pos or "")
            nm = _WS.sub("", name or "")
            if m1 and nm and m1.group("x") not in "소부전상議委義" and (
                    len(nm) == 1 or (len(nm) <= 3 and all(_HANJA_CHAR.match(c) for c in nm + m1.group("x")))):
                name, pos = nm + m1.group("x"), m1.group("t")
                fixes.append("name_cut")
    if pos and name and not looks_like_name(pos):
        # member label printed whole in data-pos, junk in data-name ('元喜龍委員' / '금년')
        m2 = _NAME_FIRST_MEMBER_RE.match(pos)
        if m2 and not _blocked_name_part(m2.group("name")) and _HANJA_CHAR.search(m2.group("name")):
            pos, name = m2.group("title"), m2.group("name")
            fixes.append("fused_name_first_over_name")
    if pos and name and len(_WS.sub("", name)) == 1 and len(pos) >= 3 and re.fullmatch(_NAME_CHARS, pos[-1]):
        # surname printed with the title, given name separate ('韓國勞動敎育院長李' / '銑',
        # '副總理兼財政經濟部長官陳' / '稔')
        if looks_like_title(normalize_title(pos[:-1])[0]) and not looks_like_title(normalize_title(pos)[0]):
            name, pos = pos[-1] + _WS.sub("", name), pos[:-1]
            fixes.append("surname_in_title")
    if pos and name is None:
        h, _ = normalize_title(pos)
        m3 = _TWO_NAMES_RE.match(pos)
        m4 = _NAME_TRUNC_MEMBER_RE.match(pos)
        m3 = m3 if (m3 and not _blocked_name_part(m3.group("name"))
                    and not looks_like_title(normalize_title(m3.group("name2"))[0])) else None
        fs = None if m3 else _fused_split(pos, h)
        if m3:
            # 'NAME委員NAME2': two member labels fused; the first name is kept (flagged)
            pos, name = m3.group("title"), m3.group("name")
            fixes.append("two_names")
        elif fs:
            kind, t, n = fs
            pos, name = t, n
            fixes.append("fused_name_first" if kind == "NAME_FIRST" else "fused_name")
        elif m4 and not _blocked_name_part(m4.group("name")):
            # '李漢久委': member title cut after its first character; classify() accepts it
            # only when the name is in the term's roster
            pos, name = m4.group("title") + ("員" if m4.group("title") in "委議" else ""), m4.group("name")
            fixes.append("title_truncated")
        elif looks_like_name(pos) and h not in MEMBER_TITLES:
            pos, name = None, pos
            fixes.append("bare_name")
    if pos and name and pos.endswith(_WS.sub("", name)) and len(pos) > len(_WS.sub("", name)) + 1:
        pos = pos[: -len(_WS.sub("", name))]
        fixes.append("name_in_pos")
    h, unm = normalize_title(pos)
    h_printed = h
    if MEMBER_TITLE_TYPOS.get(h, h) != h:
        h = MEMBER_TITLE_TYPOS[h]
        fixes.append("title_typo")
    elif len(h) >= 4:
        for a, b in TITLE_SUFFIX_TYPOS:
            if h.endswith(a):
                h = h[: -len(a)] + b
                fixes.append("title_suffix_typo")
                break
    h0 = PAREN_TAG_RE.sub("", h)
    if h0 != h and MEMBER_TITLE_TYPOS.get(h0, h0) in MEMBER_TITLES:   # '위원(국)': party tag on the title
        h = MEMBER_TITLE_TYPOS.get(h0, h0)
        fixes.append("title_party_tag")
    return Parsed(pos, name, h, "+".join(fixes) or "none", unm, h_printed)


# --------------------------------------------------------------------------- context data

def _norm_key(s) -> str:
    """Comparison key for committee names: Hangul only (Hanja converted), no punctuation."""
    if s is None:
        return ""
    h, _ = normalize_title(str(s))
    return re.sub(r"[^가-힣0-9A-Za-z]", "", h)


@functools.lru_cache(maxsize=1)
def default_na_committees() -> frozenset:
    """Normalized names of every National Assembly committee / subcommittee in the
    Open API meeting universe plus the v9 committee map keys."""
    names = set()
    err = None
    try:
        import pandas as pd
        u = pd.read_parquet(UNIVERSE_PATH, columns=["COMM_NAME", "title_body", "v_CMIT_NM", "v_SB_CMIT_NM"])
        for col in u.columns:
            for v in u[col].dropna().unique():
                for part in str(v).split():
                    names.add(part)
    except Exception as e:  # recorded and warned; the v9 committee map keys are still used
        err = e
    n_universe = len(names)
    names.update(LR.COMMITTEE_KEY_MAP_STANDING.keys())
    out = set()
    for n in names:
        n = re.sub(r"-[^-]+$", "", str(n))
        k = _norm_key(n)
        if k.endswith("위원회") and len(k) > 3:
            out.add(k)
    _record_load("na_committees_universe", UNIVERSE_PATH, n_universe, err)
    return frozenset(out)


def _fold_compat(n: str) -> str:
    return "".join(unicodedata.normalize("NFKC", c) if _COMPAT.match(c) else c for c in n)


@functools.lru_cache(maxsize=1)
def default_na_committees_by_term() -> dict:
    """{term: frozenset(normalized committee names)} from the Open API meeting universe."""
    out: dict = {}
    err = None
    try:
        import pandas as pd
        u = pd.read_parquet(UNIVERSE_PATH, columns=["DAE_NUM", "COMM_NAME", "title_body", "v_CMIT_NM", "v_SB_CMIT_NM"])
        for col in ("COMM_NAME", "title_body", "v_CMIT_NM", "v_SB_CMIT_NM"):
            for t, v in zip(u["DAE_NUM"], u[col]):
                if _isnull(v) or _isnull(t):
                    continue
                for part in str(v).split():
                    k = _norm_key(re.sub(r"-[^-]+$", "", part))
                    if k.endswith("위원회") and len(k) > 3:
                        out.setdefault(int(t), set()).add(k)
    except Exception as e:  # recorded and warned
        err = e
    _record_load("na_committees_by_term", UNIVERSE_PATH, sum(len(v) for v in out.values()), err)
    return {t: frozenset(v) for t, v in out.items()}


@functools.lru_cache(maxsize=1)
def default_roster() -> dict:
    """{term: frozenset(names)} with Hangul and Hanja names of every member of term 16-22,
    plus the name / Hanja variants listed by the legislators component (person_terms.parquet,
    read only when present) and compatibility-ideograph folded forms."""
    out: dict = {}
    err = None
    try:
        import pandas as pd
        r = pd.read_parquet(ROSTER_PATH, columns=["term", "name", "name_hanja"])
        for t, g in r.groupby("term"):
            s = set(g["name"].dropna().astype(str).str.replace(r"\s+", "", regex=True))
            s |= set(g["name_hanja"].dropna().astype(str).str.replace(r"\s+", "", regex=True))
            out[int(t)] = set(x for x in s if x)
    except Exception as e:  # recorded and warned
        err = e
    _record_load("roster_members_term", ROSTER_PATH, sum(len(v) for v in out.values()), err)
    err, n_pt = None, 0
    try:
        import pandas as pd
        pt = pd.read_parquet(PERSON_TERMS_PATH, columns=["term", "name_variants", "hanja_variants"])
        for t, nv, hv in pt.itertuples(index=False):
            for arr in (nv, hv):
                if arr is None or (isinstance(arr, float) and arr != arr):
                    continue
                for x in list(arr):
                    x = _WS.sub("", str(x))
                    if x:
                        out.setdefault(int(t), set()).add(x)
                        n_pt += 1
    except Exception as e:  # optional file (legislators component); recorded and warned
        err = e
    _record_load("roster_person_terms", PERSON_TERMS_PATH, n_pt, err)
    return {t: frozenset(v | {_fold_compat(x) for x in v}) for t, v in out.items()}


def in_roster(name, term, roster) -> Optional[bool]:
    """True/False, or None when the check cannot be made (no name, no term, no roster)."""
    if not name or term is None or roster is None:
        return None
    try:
        t = int(term)
    except (TypeError, ValueError):
        return None
    names = roster.get(t)
    if not names:
        return None
    n = PAREN_TAG_RE.sub("", _WS.sub("", str(name)))
    if len(n) < 2:
        # empty or a one-character name cut off in the source ('産業資源委員長代理 李'): no check
        return None
    if n in names:
        return True
    if _fold_compat(n) in names:  # compatibility ideographs
        return True
    if _HANJA_CHAR.search(n):
        # Hangul reading of a Hanja name (variant characters such as 嬿/姸, 雋/儁 are common in
        # 16대 pages); the roster holds the Hangul names too
        if any(h in names for h in hanja_name_readings(n)):
            return True
    return False


def hanja_name_readings(n: str) -> frozenset:
    """Hangul readings of a Hanja person name: per-character readings (no title word rules),
    with the surname read by its conventional spellings (金 = 김; 柳 = 유 or 류; 李 = 이 or 리)
    and the initial-sound rule both applied and not applied to the surname. Empty when a
    character has no reading."""
    n = _fold_compat(PAREN_TAG_RE.sub("", _WS.sub("", str(n))))
    hm = _hanja_map()
    for k in (2, 1):
        sur = n[:k]
        if sur in _SURNAME_READINGS and len(n) > k:
            heads, rest = _SURNAME_READINGS[sur], n[k:]
            break
    else:
        r0 = hm.get(n[:1]) if _HANJA_CHAR.match(n[:1] or " ") else (n[:1] or None)
        if r0 is None:
            return frozenset()
        heads, rest = tuple({r0, _DUEUM_INITIAL.get(r0, r0)}), n[1:]
    tail = []
    for c in rest:
        if _HANJA_CHAR.match(c):
            r = hm.get(c)
            if r is None:
                return frozenset()
            tail.append(r)
        else:
            tail.append(c)
    t = "".join(tail)
    return frozenset(h + t for h in heads)


# --------------------------------------------------------------------------- rule tables

# Legislator stage. Each rule: (rule_id, role or '@presiding', regex on the acting-stripped
# Hangul title, note). '@...' roles are resolved by context in _legislator_stage.
LEG_RULES = (
    ("leg.member", "legislator", r"^(위원|의원)$", "member title (위원/의원/委員/議員)"),
    ("leg.chair.committee", "@committee_chair", r"^위원장$", "bare 위원장 (+대리/직무대리/직무대행): presiding"),
    ("leg.chair.subcommittee", "@subcommittee_chair",
     r"^(([가-힣0-9]*(법안|법률안|청원|예산|결산|기금|심사|조정|제[0-9]))?소)위원장$",
     "소위원장 (bare or an NA subcommittee name): presides in a subcommittee meeting, reports elsewhere"),
    ("leg.chair.adjustment", "@adjustment_chair", r"^(안건)?조정위원장$", "안건조정위원장: presides in the 조정위 meeting, reports elsewhere"),
    ("leg.chair.speaker", "@speaker", r"^(국회)?(부)?의장$", "의장/부의장: presides in plenary"),
    ("leg.chair.audit_team", "chair", r"^반장$", "국정감사 반장 (team leader) presides over the team audit"),
    ("leg.chair.named", "@named_chair", r"^(?P<prefix>.+?)위원장$", "'{committee}위원장': NA committee chair (presides in own committee, reports in plenary) or government commission head"),
)

# Non-legislator cascade: (rule_id, role, regex, note, changes_v9). Regexes are searched on
# each '겸' segment of the title after acting/designate suffixes are removed.
NONLEG_RULES = (
    # hearing roles
    ("hear.witness", "witness", r"^증인|증인$", "증인, 증인(NAME)대리, 증인(NAME)변호인, NAME증인대리", False),
    ("hear.testifier", "testifier", r"^진술인", "", False),
    ("hear.expert", "expert_witness", r"^(참고인|감정인)", "감정인 was 'other' in v9 (10 rows)", True),
    # committee staff
    ("staff.committee", "committee_staff", r"^(수석)?전문위원$|^입법(조사|심의)관(보)?$",
     "committee staff; v9: 입법조사관 independent_official, (수석)전문위원 legislator in v8 types", True),
    ("staff.committee_named", "committee_staff", r"^(?P<na>.+위원회)((수석)?전문위원|입법조사관)$",
     "'{NA committee}수석전문위원' (only if the prefix is an NA committee)", False),
    ("gov.expert_member", "other_official", r"전문위원$",
     "전문위원 of a non-NA body (v9 committee_staff by substring)", True),
    # nominees
    ("nom.minister", "minister_nominee", r"장관(후보자|내정자)$", "", False),
    ("nom.other", "nominee", r"(후보자|내정자)$", "v9 matched 후보자 anywhere (e.g. …사장후보자(X)인사청문준비단장)", True),
    # executive
    ("exec.prime_minister", "prime_minister", r"^(대통령권한대행)?국무총리((직무대행|권한대행|서리)[가-힣]*)?$",
     "sitting / acting PM only; v9 coded PM-office staff (국무총리실장, 국무총리비서실장, …) prime_minister", True),
    ("exec.minister", "@minister", r"장관$|^부총리$",
     "title ends with 장관 (v9: 장관 anywhere, e.g. …장관정책보좌관, …도매시장관리공사…); acting suffix -> minister_acting", True),
    ("exec.vice_minister", "vice_minister", r"차관$", "", False),
    ("exec.assistant_minister", "senior_bureaucrat", r"차관보$", "차관보 (1급) was vice_minister in v9", True),
    ("exec.president", "other_official", r"^대통령$", "president (시정연설); no v9 role exists", True),
    # institutions (prefix / keyword)
    ("inst.audit", "audit_official", r"^감사원", "감사원 staff; v9 only 감사원장 and '감사위원' anywhere", True),
    ("inst.constitutional_court", "constitutional_court", r"헌법재판소|헌법재판관", "", False),
    ("inst.election", "election_official", r"선거관리위원|선관위", "includes 중앙선거관리위원장 (v9 independent_official)", True),
    ("inst.assembly", "assembly_official", r"^국회(사무|도서관|예산정책처|입법조사처|의정연수원|미래연구원)",
     "", False),
    ("inst.assembly_bare", "@assembly_bare",
     r"^(사무총장|사무차장|입법차장|의사국장|국제국장|관리국장|법제실장|기획조정실장|의정연수원장|예산정책처장|입법조사처장|도서관장)$",
     "bare NA-secretariat title in 국회운영위원회 or plenary -> assembly_official", True),
    ("inst.transition_committee", "other_official", r"대통령직인수위원회",
     "v5 rule sent 대통령직인수위원회위원 to assembly_official", True),
    ("org.union", "org_head", r"(노동조합(?!과)|노조)[가-힣]*(부)?위원장$",
     "trade-union (vice) chairs (…노동조합위원장) are heads of organisations, not government commission heads "
     "(v9 independent_official via '위원장'); other union posts keep the general rules", True),
    ("gov.commission_head", "independent_official", r"(부)?위원장$",
     "government commission head (non-NA '…위원장'); as v9", False),
    ("gov.special_inspector", "independent_official", r"특별감찰관", "", False),
    ("gov.human_rights", "independent_official", r"^국가인권위원회", "", False),
    ("local.education", "local_gov_head", r"(부)?교육감$|교육장$",
     "교육감 is local_gov_head per the codebook (v9: independent_official)", True),
    ("judicial.prosecution_office", "agency_head", r"검사장$",
     "head of a prosecutors' office, coded like other regional-office heads (지청장, 지방경찰청장); v9 public_corp_head via '사장'", True),
    ("judicial.court", "other_official", r"법원장$|법원[가-힣]*지원장$",
     "court presidents (v9 org_head via '원장'); judiciary has no own role, v9 codes 판사 other_official", True),
    ("military", "military",
     r"^(국방부)?(육군|해군|공군|해병대|국군)|합동참모|사령관|참모총장|참모차장|사관학교장|기무사령부|기무부대|군사보좌관|"
     r"(국방|육군|해군|공군)무관$",
     "armed-forces prefix and defence attaches added (v9: 육군본부…, 해군검찰단장, …대사관국방무관 elsewhere)", True),
    ("local.head", "local_gov_head",
     r"(시장|도지사|부지사|부시장)(\([^)]*\))?$|(부)?구청장$|(부)?군수$",
     "시장/도지사/부지사/구청장/군수 at the end (v9: '시장' anywhere; 구청장 agency_head)", True),
    ("agency.head", "agency_head", r"청장$", "", False),
    ("financial", "financial_regulator", r"금융감독원|금융통화위원|^금융위원회|^금융감독위원회",
     "FSS / FSC staff and 금융통화위원 (commission heads stay independent_official, as v9)", False),
    ("police", "police", r"경찰|소방서|119안전센터", "경찰, 소방서, 119안전센터 (v5 '안전센터' alone also hit e.g. …방사선안전센터장)", False),
    ("senior.customs_prison", "senior_bureaucrat", r"세관장|구치소장|교도소장|세무서장$",
     "세관장/구치소장/교도소장 as v9; 세무서장 (district tax office head) added by analogy with 세관장", True),
    ("corp.board_chair.public", "public_corp_head",
     r"(공단|공사|기금|은행|거래소|금고|제주국제자유도시개발센터)(\([^)]*\))?(부)?이사장(보)?$",
     "이사장 (and 부이사장/부이사장보) of a public corporation-type institution (공단, 공사, 기금, 은행, 거래소)", False),
    ("corp.board_chair.other", "org_head", r"(부)?이사장(보)?$",
     "이사장 of a foundation, council, mutual-aid society, school corporation etc. (v9 public_corp_head via '사장')", True),
    ("corp.head", "public_corp_head", r"사장$|은행장$|부행장(보)?$", "사장, 은행장, (수석)부행장(보)", False),
    ("research.head", "research_head", r"(연구원|연구소|과학원)(원)?장$|농업과학기술원장$",
     "연구원장/과학원장 are research_head (v9 org_head via '원장'); 연구소장 as v9; 농업과학기술원장 (RDA research "
     "institute; other …과학기술원장 such as KAIST stay org_head)", True),
    ("culture.head", "cultural_institution_head", r"(박물관|미술관|도서관|기념관|기록관|과학관|문화전당|극장)장$",
     "museum, gallery, library, memorial, archive, science museum, 국립아시아문화전당, 국립극장 heads "
     "(과학관/전당/극장 added)", True),
    ("coop", "@coop", r"협동조합|농협|수협|조합중앙회|산림조합|축협",
     "cooperative staff and executives -> cooperative_head, except heads (…회장/원장 -> org_head) and "
     "senior posts (본부장/처장/국장/실장/차장 -> senior_bureaucrat), as in v9", False),
    ("org.head", "org_head", r"원장|회장", "contains, as v9", False),
    ("diplomatic_staff", "other_official",
     r"^(?!.*공사(참사관)?$).*(대사관|총영사관|대표부|영사관)[가-힣0-9ㆍ]*(참사관|서기관|(?<!총)영사|실무관|[가-힣]관)(\([^)]*\))?$"
     r"|^(부)?영사$",
     "embassy / consulate staff below 공사 (참사관, 1-3등서기관, 영사, 실무관, 재경관, 교육관 …) and a bare (부)영사 "
     "(副領事); v9 senior_bureaucrat via '대사' inside '대사관'. 대사, 총영사, 공사, 공사참사관 stay senior_bureaucrat", True),
    ("senior", "senior_bureaucrat", r"본부장|처장|(?<!한)국장|실장|차장|총재|대사|총영사",
     "contains, as v9, except '국장' inside '한국장…' (한국장애인…, 한국장학…)", True),
    ("mid", "mid_bureaucrat", r"(관리관|지원관|정책관|감사관)(?!리)",
     "contains, as v9, but not inside '…관리' (…정책관리과 is a unit name, not a 정책관)", True),
    ("org.internal_auditor", "org_head", r"(상임|상근)?감사위원$",
     "internal auditors of corporations (v9 audit_official via '감사위원'); v4 convention 감사 -> org_head", True),
    ("private.company", "private_sector", r"㈜|주식회사|\(주\)|\(유\)",
     "company markers (v9: ㈜…대표이사 other_official, 주식회사… private_sector)", True),
    ("gov.field_office", "other_official", r"사무소장$", "government field offices (…사무소장), other_official as v9", False),
    ("broadcast", "broadcasting",
     r"^(국정홍보처)?(방송위원회|방송위원|방송통신위원회|방송통신심의위원회|한국방송공사|한국방송광고(진흥)?공사|"
     r"한국교육방송공사|한국정책방송원|방송문화진흥회)|KBS|MBC|EBS|교통방송",
     "broadcasting bodies by name (v9: '방송' anywhere, which also hit 한국방송통신대학교, ministry "
     "전파방송 divisions and 도로교통공단방송관리팀장)", True),
    ("other_official.kw", "other_official",
     r"총장|비서관|대변인|심의관(?!리)|단장|부장|대표|과장|팀장|센터장|판사|공무원$|기획관(?!리)|조정관(?!리)|사무관|참사관|서기관|이사관",
     "v9 keyword list plus 대표/조정관/사무관/참사관/서기관/이사관 (v9 other_official for these); 공무원 only as the "
     "final word, so 공무원연금공단… keeps its own rule", False),
    ("private.v5", "private_sector", r"선수단감독|감독$|철인3종|트라이애슬론|예술감독",
     "v4/v5 'other' rules for coaches and art directors; '(재)'/'(사)' are left to the v4/v5 fallback so that "
     "v4 이사/감사/소장 rules win first, as in v9", False),
)
# v4 then v5 'other' reclassification rules (verbatim patterns) are applied after this
# table as rules 'v4.<i>' and 'v5.<i>', skipping the v5 대통령직인수위원회 rule (see above).
# TAIL_RULES run last, after v4/v5, for titles that no earlier rule recognises. They exist
# only in v10 (v9 left these strings 'other'); placing them after v4/v5 keeps every v4/v5
# outcome unchanged (e.g. v5 '노사협력관' -> mid_bureaucrat, '정책과' -> mid_bureaucrat).
TAIL_RULES = (
    ("tail.bok_branch", "other_official", r"^한국은행[가-힣]*지점장$",
     "Bank of Korea branch heads (韓國銀行大田支店長, 한국은행부산지점장); v9 left them 'other', which the run-2 "
     "hand audit judged wrong (sample row 185)", True),
    ("tail.gov_officer", "other_official",
     r"[가-힣]{2,}(담당관|협력관|총괄관|[가-힣]관|관실|[가-힣]과|담당|[가-힣]팀|계장|[가-힣]단)$|검사$",
     "government officer or unit titles ending in 관 / 관실 / 과 / 담당 / 팀 / 계장 / 단 (…담당관, …협력관, "
     "…제도과, …기금담당, …방역팀) and prosecutors (…검사); other_official = 'other government official' "
     "as in v9 (v9 codes 과장/팀장 other_official)", True),
    ("tail.research_staff_grade", "other_official", r"(책임|선임|수석)행정원$",
     "administrative staff grades at government research institutes (국가보안기술연구소책임행정원); run-2 hand audit "
     "row 176 judged 'other' wrong", True),
    ("tail.corp_executive", "org_head", r"(상무|전무)$|(이사|상무|전무)대우$",
     "corporate executive titles without 이사 (…심사상무) and director-grade titles (…이사대우, 韓國輸出入銀行理事待遇); "
     "v4 codes 상무이사/전무이사/…이사 org_head", True),
)
_TAIL_EXCLUDE_RE = re.compile(r"(장관|차관|도서관|박물관|미술관|기념관|기록관|회관|대사관|영사관|문화관|전시관|체육관|과학관)$")

_LEG_COMPILED = tuple((rid, role, re.compile(rx), note) for rid, role, rx, note in LEG_RULES)
_NONLEG_COMPILED = tuple((rid, role, re.compile(rx), note, ch) for rid, role, rx, note, ch in NONLEG_RULES)
_TAIL_COMPILED = tuple((rid, role, re.compile(rx), note, ch) for rid, role, rx, note, ch in TAIL_RULES)
PRESIDING_CLASSES = frozenset({"국회본회의", "전원위원회"})
_NA_SPECIAL_NAME_RE = re.compile(r"인사청문|국정조사|임명동의|선출에관한")
_NON_STANDING_RE = re.compile(r"특별|인사청문|국정조사|소위원회|조정위원회|심사|분과")


def rule_table():
    import pandas as pd
    rows = [dict(stage="legislator", order=i + 1, rule_id=r[0], role=r[1], pattern=r[2], note=r[3], changes_v9=None)
            for i, r in enumerate(LEG_RULES)]
    rows += [dict(stage="nonlegislator", order=i + 1, rule_id=r[0], role=r[1], pattern=r[2], note=r[3], changes_v9=r[4])
             for i, r in enumerate(NONLEG_RULES)]
    n = len(NONLEG_RULES)
    rows += [dict(stage="nonlegislator", order=n + i + 1, rule_id=f"v4.{i}", role=role, pattern=p.pattern,
                  note="legacy_rules.OTHER_RECLASS_RULES_V4 (verbatim)", changes_v9=False)
             for i, (p, role) in enumerate(LR.OTHER_RECLASS_RULES_V4)]
    n += len(LR.OTHER_RECLASS_RULES_V4)
    rows += [dict(stage="nonlegislator", order=n + i + 1, rule_id=f"v5.{i}", role=role, pattern=p.pattern,
                  note="legacy_rules.OTHER_RECLASS_RULES_V5 (verbatim)", changes_v9=False)
             for i, (p, role) in enumerate(LR.OTHER_RECLASS_RULES_V5) if role != "assembly_official"]
    rows += [dict(stage="nonlegislator", order=None, rule_id=r[0], role=r[1], pattern=r[2], note=r[3],
                  changes_v9=r[4]) for r in TAIL_RULES]
    k = 0
    for r in rows:  # contiguous evaluation order within the non-legislator stage
        if r["stage"] == "nonlegislator":
            k += 1
            r["order"] = k
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- classification

@dataclass
class RoleResult:
    role: str
    role_group: str
    role_rule: str
    affiliation_raw: Optional[str]
    person_title: Optional[str]
    pos_hangul: str
    pos_fix: str
    name: Optional[str]
    pos_hangul_printed: str = ""
    title_raw: Optional[str] = None   # the printed title (whitespace removed)


_ACTING_WORDS = "직무대행자|직무대행|직무대리|권한대행|대행|대리|서리"
# '겸' joins two offices. Not a split: '겸하는/겸한' (the verb inside a committee name,
# '헌법재판소재판관후보자를겸하는헌법재판소장(…)임명동의에관한인사청문특별위원장'), the words 겸용/겸임/
# 겸업/겸직 ('국방과학연구소민군겸용기술센터장'), and '겸' before a bare acting word
# ('영화진흥위원회부위원장겸직무대행' = deputy chair, also acting chair).
_GYEOM_SPLIT_RE = re.compile(rf"(?<=[가-힣)]{{2}})겸(?![하용임업직])(?!(?:{_ACTING_WORDS})$)(?=[가-힣]{{2}})")
_ACTING_GYEOM_RE = re.compile(rf"겸({_ACTING_WORDS})$")


def _segments(t: str) -> list:
    """Split on '겸' (both sides >= 2 chars) and strip acting / designate suffixes and a
    leading '(전)'. A trailing '겸' (the second office cut off) is dropped. Returns [(core, acting)]."""
    t = FORMER_RE.sub("", t)
    if t.endswith("겸") and len(t) > 3:
        t = t[:-1]
    t = _ACTING_GYEOM_RE.sub(r"\1", t)
    parts = [p for p in _GYEOM_SPLIT_RE.split(t) if p] or [t]
    out = []
    for p in parts:
        p = DESIGNATE_RE.sub("", p) or p
        a = ACTING_RE.search(p)
        core = p[: a.start()] if a and a.start() > 0 else p
        out.append((core, a.group(1) if a and a.start() > 0 else None))
    # '부총리겸교육인적자원부차관': '부총리겸…부' names the ministry a deputy prime minister heads, so
    # a post below the minister keeps its own role. '부총리겸…부장관' and '부총리겸…부' (the office
    # word left out) are the deputy prime minister himself.
    if len(out) > 1 and out[0][0] == "부총리" and not out[-1][0].endswith(("장관", "부")):
        out = out[1:]
    return out


def _same_term_special(pk: str, term, na_by_term) -> bool:
    """An NA special committee of `term` whose name shares the first four characters of pk."""
    try:
        names = (na_by_term or {}).get(int(term))
    except (TypeError, ValueError):
        return False
    if not names:
        return False
    head = pk[:4]
    return any("특별" in n and n.startswith(head) for n in names)


def _is_plenary(class_name, hearing_type) -> Optional[bool]:
    vals = [v for v in (class_name, hearing_type) if isinstance(v, str) and v]
    if not vals:
        return None
    return any(v in PRESIDING_CLASSES for v in vals)


def _committee_root(c) -> str:
    if not c:
        return ""
    c = str(c).split()[0]
    return _norm_key(re.sub(r"-[^-]+$", "", c))


def _legislator_stage(core, acting, ctx, name, roster, na_comm, na_by_term=None):
    """Returns (role, rule_id) or None when the title is not a legislator title."""
    for rid, role, rx, _ in _LEG_COMPILED:
        m = rx.match(core)
        if not m:
            continue
        if role == "legislator":
            if acting:  # '위원대리'? not a member title
                return None
            return "legislator", rid
        if role == "chair":
            return "chair", rid
        plen = _is_plenary(ctx["class_name"], ctx["hearing_type"])
        sub = ctx["is_subcommittee"]
        if role == "@committee_chair":
            # a bare '위원장' in a subcommittee meeting is coded as presiding like elsewhere; the
            # rule id marks it because such meetings also have a presiding 소위원장 (researcher decision)
            return "chair", rid + (".in_subcommittee" if sub else "") + (".acting" if acting else "")
        if role == "@subcommittee_chair":
            if sub is None:
                return "chair", rid + ".presiding_unknown_ctx"
            return ("chair", rid + ".presiding") if sub else ("legislator", rid + ".reporting")
        if role == "@adjustment_chair":
            comm = " ".join(str(x) for x in (ctx["committee_raw"], ctx["subcommittee"]) if x)
            in_adj = "조정위원회" in comm
            if sub is None and not comm:
                return "chair", rid + ".presiding_unknown_ctx"
            return ("chair", rid + ".presiding") if (in_adj or sub) else ("legislator", rid + ".reporting")
        if role == "@speaker":
            if plen is None:
                return "chair", rid + ".presiding_unknown_ctx"
            return ("chair", rid + ".presiding") if plen else ("legislator", rid + ".nonplenary")
        if role == "@named_chair":
            prefix = m.group("prefix")
            if prefix.endswith("부") or prefix in ("", "소"):
                return None  # 부위원장 = commission deputy; 소위원장 handled above
            key = _norm_key(prefix + "위원회")
            pk = _norm_key(prefix)
            if key in na_comm:
                is_na_by = "na_committee"
            elif len(pk) >= 4 and any(n.startswith(pk) and not _NON_STANDING_RE.search(n) for n in na_comm):
                # abbreviated standing-committee name, e.g. 농림해양 -> 농림해양수산위원회. Special and
                # sub-committees are excluded: 방송통신 would match 방송통신특별위원회.
                is_na_by = "na_committee_prefix"
            elif _NA_SPECIAL_NAME_RE.search(pk):
                # confirmation-hearing and investigation committees exist only in the Assembly
                is_na_by = "na_pattern"
            elif "특별" in pk and len(pk) >= 4 and _same_term_special(pk, ctx["term"], na_by_term):
                # an NA special committee of this term printed under a variant name
                # (과거사진상조사특별위원장 = 16대 과거사진상규명에관한특별위원회). Government
                # '…특별위원회' heads (16대 중소기업특별위원장) have no NA namesake in that term.
                is_na_by = "na_special_same_term"
            elif ctx["has_mid"]:
                # A non-NA committee title with a viewer mem_id: the viewer links members by name,
                # and it also links homonymous officials (22대 방송통신위원장(직무대행) 김태규, 이진숙).
                # The printed title decides; the person link is legislators.py's job.
                return "@memid_title_conflict", rid
            else:
                return None  # government commission head -> non-legislator cascade
            # The roster is used only to refute: a committee-like title whose speaker is not a
            # member of this term is a government body of the same name (16대 여성특별위원회).
            # Not applied in plenary, where a title naming an NA committee is always its chair
            # reporting (16대 plenary pages print variant or truncated Hanja names).
            # The same-term special-committee match (shared first four characters) is refuted the
            # same way: 20대 '가습기살균제사건과4ㆍ16세월호참사특별조사위원장 장완익' shares its opening with
            # the NA 가습기살균제사고진상규명…국정조사특별위원회 but heads a government commission.
            if (is_na_by in ("na_committee", "na_committee_prefix", "na_special_same_term") and not ctx["has_mid"]
                    and not plen and in_roster(name, ctx["term"], roster) is False):
                return "@refuted", f"{rid}.{is_na_by}.name_not_in_roster"
            own = _committee_root(ctx["committee_raw"])
            if plen:
                return "legislator", f"{rid}.{is_na_by}.reporting_plenary"
            # same committee: exact name, or an abbreviated prefix of >= 4 characters
            # ('농림해양위원장' in 농림해양수산위원회)
            same = own == key or (len(pk) >= 4 and own.startswith(pk))
            if own and not same and not (ctx["subcommittee"] and _committee_root(ctx["subcommittee"]) == key):
                return "legislator", f"{rid}.{is_na_by}.reporting_other_committee"
            return "chair", f"{rid}.{is_na_by}.presiding"
    return None


def _nonleg_cascade(segs, ctx, na_comm, v9_string=""):
    """Returns (role, rule_id). `v9_string` is the title as v9 saw it ('TITLE NAME', acting
    suffix kept) for the verbatim v4/v5 fallback rules, whose patterns test for a following
    name with '\\s'."""
    cores = [c for c, _ in segs]
    acting_any = any(a for _, a in segs)
    for rid, role, rx, _, _ in _NONLEG_COMPILED:
        hit = None
        for c in cores:
            m = rx.search(c)
            if m:
                hit = (c, m)
                break
        if not hit:
            continue
        c, m = hit
        if rid == "staff.committee_named":
            if _norm_key(m.group("na")) not in na_comm:
                continue
        if role == "@minister":
            return ("minister_acting" if acting_any else "minister"), rid + (".acting" if acting_any else "")
        if role == "@assembly_bare":
            comm = _committee_root(ctx["committee_raw"])
            plen = _is_plenary(ctx["class_name"], ctx["hearing_type"])
            if plen or comm == "국회운영위원회":
                return "assembly_official", rid
            continue
        if role == "@coop":
            if not re.search(r"(회장|원장)$", cores[-1]) and not re.search(r"본부장|처장|국장|실장|차장", "겸".join(cores)):
                return "cooperative_head", rid
            continue
        return role, rid
    full = v9_string or "겸".join(cores)
    for i, (p, role) in enumerate(LR.OTHER_RECLASS_RULES_V4):
        if p.search(full):
            return role, f"v4.{i}"
    for i, (p, role) in enumerate(LR.OTHER_RECLASS_RULES_V5):
        if role == "assembly_official":
            continue
        if p.search(full):
            return role, f"v5.{i}"
    for rid, role, rx, _, _ in _TAIL_COMPILED:
        for c in cores:
            if rx.search(c) and not _TAIL_EXCLUDE_RE.search(c):
                return role, rid
    return "other", "fallback.other"


_EXEC_ROLES = frozenset({"minister", "minister_acting", "minister_nominee", "prime_minister", "nominee",
                         "vice_minister"})


def _person_title(role, rule, segs, core0) -> Optional[str]:
    acting = next((a for _, a in segs if a), None)
    if rule.startswith("leg.chair.audit_team"):
        return "반장" + (acting or "")
    if acting in ("직무대행자",):
        acting = "직무대행"
    return acting


# --------------------------------------------------------------------------- affiliation

# affiliation_raw = the institution printed in the title: the title as printed (whitespace
# removed, Hanja kept) with a leading '(전)', trailing acting / designate / nominee words and one
# office word removed. '국방부장관 김관진' -> '국방부', '경찰청장' -> '경찰청', '한국전력공사사장' ->
# '한국전력공사', '서울특별시장' -> '서울특별시', '주일본국대한민국대사관공사' -> '주일본국대한민국대사관'.
# For a dual title ('…겸…') the last office is used. Titles that are only an office word (위원,
# 위원장, 증인, 참고인, 국무총리, 전문위원) give null. A title with no recognised office word is kept
# whole (a unit name such as '기획재정부국제조세제도과'). The printed title itself is `title_raw`.
_AFF_TRAIL_RE = re.compile(rf"(겸)?({_ACTING_WORDS}|임명예정자|후보자|내정자)$")
_AFF_OFFICE_RES = tuple(re.compile(x) for x in (
    r"^(?P<off>(증인|진술인|참고인|감정인|방청인|변호인|통역인|통역|속기사|발표자|토론자|발제자|진행자|사회자)"
    r"(\([^()]*\))?(변호인|대리)?)$",
    r"(?P<off>(제\d+)?차관보?)$",
    r"(?P<off>장관|부총리|국무총리|대통령)$",
    r"(?P<off>(수석)?전문위원|입법조사관보?|입법심의관)$",
    r"(?P<off>(부|소)?위원장)$",
    r"(?P<off>((상임|비상임|상근|비상근)?감사위원|(상임|비상임|상근|비상근)?위원|의원|(부)?의장))$",
    r"(?P<off>(부)?이사장(보)?|대표이사|(상임|비상임|상무|전무|기획|사업|관리|운영|업무)?이사|(상임|상근)?감사|상무|전무)$",
    r"(?P<off>사무(총장|차장))$",
    r"(?P<off>검사장|교육장|(부)?교육감|(행정|정무|경제)?(제\d)?부(시장|지사)|(부)?사령관|참모(총장|차장)|(부)?총재|(부)?총장)$",
    r"시(?P<off>장)$",
    r"도(?P<off>지사)$",
    r"군(?P<off>수)$",
    r"대사관(?P<off>공사참사관|공사|참사관|\d등서기관|서기관|실무관|(국방|육군|해군|공군)?무관|영사|[가-힣]{1,2}관)$",
    r"영사관(?P<off>영사|[가-힣]{1,2}관)$",
    r"(?P<off>대사|총영사)$",
    r"(?P<off>부(원장|청장|처장|국장|실장|소장|단장|본부장|센터장|관장|회장|사장))$",
    r"(?:협회|중앙회|연합회|공제회|마사회|학회|협의회|위원회|연구회|진흥회)(?P<off>장)$",
    r"(?P<off>회장)$",
    r"(?P<off>사장|(수석)?부행장(보)?)$",
    r"(?P<off>차장)$",
    r"(?:(?<=[청처원국실과소관단부팀반서대])|(?<=본부|센터|학교|지원|은행))(?P<off>장)$",
    r"(?P<off>심의관|정책관|관리관|감사관|기획관|조정관|지원관|담당관|협력관|총괄관|비서관|대변인|사무관|이사관|서기관|"
    r"조사관|주무관|감독|교수|연구위원|비서|간사|대표|계장|판사|검사)$",
))
_NO_AFF = frozenset({"위원", "의원", "위원장", "위원장대리", "소위원장", "의장", "부의장", "반장"})


def affiliation_from_title(pos_raw) -> Optional[str]:
    """The institution part of a printed title (see the comment above), or None."""
    raw = _clean_str(pos_raw)
    if raw is None:
        return None
    raw = _WS.sub("", raw)
    h = normalize_title(raw)[0]
    if len(h) != len(raw):          # alignment lost (never expected): work on the Hangul text
        raw = h
    ht = MEMBER_TITLE_TYPOS.get(h, h)
    if ht in _NO_AFF or ht in MEMBER_TITLES:
        return None
    for a, b in TITLE_SUFFIX_TYPOS:  # same-length misprints ('조달청정')
        if len(h) >= 4 and h.endswith(a):
            h = h[: -len(a)] + b
            break
    b0 = 0
    m = FORMER_RE.match(h)
    if m:
        b0 = m.end()
    e = len(h)
    while True:
        m = _AFF_TRAIL_RE.search(h[b0:e])
        if not m or m.start() == 0:
            break
        e = b0 + m.start()
    seg = [b0] + [b0 + mm.end() for mm in _GYEOM_SPLIT_RE.finditer(h[b0:e])]
    b = seg[-1]
    core = h[b:e]
    for rx in _AFF_OFFICE_RES:
        m = rx.search(core)
        if m:
            e = b + m.start("off")
            break
    out = _TRAIL_PUNCT_RE.sub("", raw[b:e]).strip()
    return out or None


# --------------------------------------------------------------------------- classify

def classify(pos, name=None, *, label_raw=None, mem_id=None, term=None, class_name=None,
             hearing_type=None, is_subcommittee=None, committee_raw=None, subcommittee=None,
             roster=None, na_committees=None, na_committees_by_term=None) -> RoleResult:
    """One speaker turn. Input values are normalised first: mem_id counts as present only when it
    is a positive number (0, 0.0, '0.0', NaN, pd.NA are absent); is_subcommittee accepts bools,
    0/1 and 'True'/'False' strings (anything else is unknown); null-like pos / name / context
    values (None, NaN, pd.NA) are treated as missing."""
    roster = default_roster() if roster is None else roster
    na_comm = default_na_committees() if na_committees is None else na_committees
    na_by_term = default_na_committees_by_term() if na_committees_by_term is None else na_committees_by_term
    p = parse_speaker(pos, name, label_raw)
    has_mid = mem_id_present(mem_id)
    is_subcommittee = to_bool_or_none(is_subcommittee)
    term = None if _isnull(term) else term
    class_name, hearing_type = _clean_str(class_name), _clean_str(hearing_type)
    committee_raw, subcommittee = _clean_str(committee_raw), _clean_str(subcommittee)
    ctx = dict(term=term, class_name=class_name, hearing_type=hearing_type, is_subcommittee=is_subcommittee,
               committee_raw=committee_raw, subcommittee=subcommittee, has_mid=has_mid)

    def out(role, rule, segs=()):
        pt = _person_title(role, rule, segs, p.pos_h) if segs else None
        return RoleResult(role, role_group(role), rule, affiliation_from_title(p.pos_raw), pt, p.pos_h, p.fix,
                          p.name_raw, p.pos_h_printed, p.pos_raw)

    if not p.pos_h:
        if not p.name_raw:
            return out("unknown", "empty_label")
        if has_mid:
            return out("legislator", "memid.no_title")
        r = in_roster(p.name_raw, term, roster)
        if r:
            return out("legislator", "bare_name.roster")
        return out("other", "bare_name.unmatched" if r is False else "bare_name.unchecked")

    if "title_truncated" in p.fix and not has_mid and not in_roster(p.name_raw, term, roster):
        # '李漢久委' is a member label only if 李漢久 is a member of this term
        return out("other", "title_truncated.name_not_in_roster")

    segs = _segments(p.pos_h)
    core0, acting0 = segs[0]
    # '財政經濟委員會長代理': '{NA committee}委員會長' misprints '{NA committee}委員長'
    mh = re.match(r"^(?P<p>.+)위원회장$", core0)
    if len(segs) == 1 and mh and _norm_key(mh.group("p") + "위원회") in na_comm:
        core0 = mh.group("p") + "위원장"
        segs = [(core0, acting0)]
        p.fix = (p.fix + "+" if p.fix != "none" else "") + "committee_hoejang"
    v9s = p.pos_h + (" " + _WS.sub(" ", p.name_raw).strip() if p.name_raw else "")
    refuted = None
    memid_conflict = False
    if len(segs) == 1:
        leg = _legislator_stage(core0, acting0, ctx, p.name_raw, roster, na_comm, na_by_term)
        if leg is not None and leg[0] == "@refuted":
            refuted, leg = leg[1], None
        elif leg is not None and leg[0] == "@memid_title_conflict":
            memid_conflict, leg = True, None
    else:
        leg = None
    if has_mid:
        nl_role, nl_rule = _nonleg_cascade(segs, ctx, na_comm, v9s)
        if memid_conflict:
            return out(nl_role, "memid.title_conflict." + nl_rule, segs)
        if nl_role in _EXEC_ROLES and leg is None:
            return out(nl_role, "memid.dual_office." + nl_rule, segs)
        if leg is not None:
            return out(leg[0], leg[1] + ".memid", segs)
        if nl_role != "other":
            # A printed non-legislator title (증인, 참고인, 경찰청장, 전문위원 …) keeps its role: the
            # viewer attaches mem_ids to homonymous non-members. The person link is legislators.py's job.
            return out(nl_role, "memid.title_conflict." + nl_rule, segs)
        # an unrecognised title ('간사', '국회의원'): the mem_id decides
        return out("legislator", "memid.decisive.title_" + nl_rule, segs)
    if leg is not None:
        return out(leg[0], leg[1], segs)
    role, rule = _nonleg_cascade(segs, ctx, na_comm, v9s)
    if refuted:
        rule = refuted + ">" + rule
    if role == "other" and looks_like_name(p.pos_raw or "") and not p.name_raw:
        r = in_roster(p.pos_raw, term, roster)
        if r:
            return out("legislator", "bare_name.roster", segs)
    return out(role, rule, segs)


# --------------------------------------------------------------------------- v9 compat

@functools.lru_cache(maxsize=1)
def default_v9_lookup() -> dict:
    """{(speaker, has_mid): majority v9 role} over v9 XLSX-era rows."""
    try:
        import pandas as pd
        t = pd.read_parquet(V9_TABLE_PATH, columns=["speaker", "has_mid", "role", "n"])
    except Exception as e:  # recorded and warned; v9_compat then uses the chain for every label
        _record_load("v9_speaker_role_table", V9_TABLE_PATH, 0, e)
        return {}
    t = t.groupby(["speaker", "has_mid", "role"], as_index=False)["n"].sum()
    t = t.sort_values(["speaker", "has_mid", "n", "role"], ascending=[True, True, False, True])
    t = t.drop_duplicates(["speaker", "has_mid"])
    _record_load("v9_speaker_role_table", V9_TABLE_PATH, len(t))
    return {(s, bool(h)): r for s, h, r in zip(t["speaker"], t["has_mid"], t["role"])}


def xlsx_style_label(pos_h, name) -> str:
    pos_h = pos_h or ""
    name = _WS.sub(" ", str(name)).strip() if name else ""
    if pos_h in MEMBER_TITLES and name:
        return f"{name} {pos_h}"
    return f"{pos_h} {name}".strip()


def v9_compat(pos_h, name, has_mid, lookup=None, pos_h_printed=None, label_exact=None) -> tuple:
    """v9 role for the speaker rendered as an XLSX label. `label_exact` is the label exactly as v9
    saw it (XLSX turns: speaker_label_raw) and is tried first. Otherwise the label is built from
    the title as printed (before v10 typo repairs), then from the repaired title. Labels not in
    the lookup go through the reconstructed v9 chain (legacy_rules.classify_speaker_v9_chain).
    `has_mid` must follow v9's member_id (for XLSX turns: source_member_id).

    Note on accuracy: the lookup is the v9 XLSX-era speaker table itself, so on v9 XLSX rows
    a lookup hit reproduces v9 by construction. The chain alone is the out-of-lookup estimate."""
    lookup = default_v9_lookup() if lookup is None else lookup
    cands = []
    le = _clean_str(label_exact)
    if le is not None:
        cands.append(le)
    for ph in (pos_h_printed, pos_h):
        if ph is None:
            continue
        lab = xlsx_style_label(ph, name)
        if lab and lab not in cands:
            cands.append(lab)
    if not cands:
        return "unknown", "empty"
    for label in cands:
        key = (label, bool(has_mid))
        if key in lookup:
            return lookup[key], "lookup"
    return LR.classify_speaker_v9_chain(cands[0], "1" if has_mid else None), "chain"


# --------------------------------------------------------------------------- enrich

CONTRACT_COLS = ("role", "role_group", "role_rule", "role_v9_compat", "affiliation_raw", "person_title")
FORMER_TITLE_RE = re.compile(r"^\s*[\(（]\s*(?:전|前)\s*[\)）]|^前(?=[一-鿿])")
EXTRA_COLS = ("title_raw", "pos_hangul", "pos_fix", "role_v9_compat_src")
# within-meeting label consistency (R2, 2026-09-26): bool flags and the meeting-majority title
CONSISTENCY_COLS = ("label_inconsistent_in_meeting", "label_repaired", "label_meeting_majority")
_MEETING_COLS = ("conf_num", "term", "class_name", "hearing_type", "is_subcommittee", "committee_raw", "subcommittee")

# Role classes compared for label consistency: presiding and member titles of one legislator are the
# same person-side class (a member who chairs part of a meeting is not an inconsistency).
_UNRECOGNISED_ROLES = ("other", "unknown")     # a title the rules do not recognise: the only repair target
FUSED_RULE_PREFIX = "label_fused_excluded"


def _role_class(role):
    return "legislator" if role in LEG_ROLES else role


def _within_one_edit(a: str, b: str) -> bool:
    """Levenshtein distance(a, b) <= 1."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la > lb:
        a, b, la, lb = b, a, lb, la
    i = 0
    while i < la and a[i] == b[i]:
        i += 1
    return a[i + 1:] == b[i + 1:] if la == lb else a[i:] == b[i + 1:]


def title_typo_kind(t: str, majority: str) -> Optional[str]:
    """How t looks like a typo of the meeting-majority title: 'prefix' (a strict prefix of it,
    '문화체육관광부제1차' of '문화체육관광부제1차관', '財政經濟部長' of '財政經濟部長官'), 'one_edit'
    (Levenshtein distance 1, e.g. '氣象廳豫' / '氣象廳長', but also '법무부장관' / '법무부차관'), else None.
    Empty titles are never typos."""
    if not t or not majority or t == majority:
        return None
    if majority.startswith(t):
        return "prefix"
    return "one_edit" if _within_one_edit(t, majority) else None


def is_title_typo_of(t: str, majority: str) -> bool:
    """t is a strict prefix of, or one edit away from, the meeting-majority title."""
    return title_typo_kind(t, majority) is not None


def enrich(turns, meetings, *, roster=None, na_committees=None, v9_lookup=None, na_committees_by_term=None):
    """Add role columns to `turns`. Rows, order and index are preserved; the input is not modified.

    classify() runs once per distinct (speaker_pos, speaker_name, speaker_label_raw, has mem_id,
    v9 has member_id, XLSX source, meeting context); rows are mapped to their key with integer
    factorisation (no string casts). `meetings` must have one context per conf_num: exact
    duplicate rows are dropped and counted, conflicting duplicates raise ValueError. Counts are in
    `out.attrs['roles_enrich']` (turns without a meeting row, distinct keys, dropped duplicate
    meeting rows, reference-file load status)."""
    import numpy as np
    import pandas as pd
    out = turns.copy(deep=False)
    n = len(out)
    stats = {"rows": n}
    if n == 0:
        for c in CONTRACT_COLS + EXTRA_COLS + CONSISTENCY_COLS:
            out[c] = pd.Series(dtype="boolean" if c in CONSISTENCY_COLS[:2] else "string")
        out.attrs["roles_enrich"] = stats
        return out

    def col(c):
        return out[c] if c in out.columns else pd.Series([None] * n, index=out.index, dtype=object)

    # ---- meeting context, one row per conf_num
    mcols = [c for c in _MEETING_COLS if c in meetings.columns]
    m = meetings[mcols]
    m = m[m["conf_num"].isin(pd.unique(out["conf_num"]))]
    stats["meetings_exact_duplicate_rows_dropped"] = 0
    if m["conf_num"].duplicated().any():
        md = m.drop_duplicates()
        stats["meetings_exact_duplicate_rows_dropped"] = int(len(m) - len(md))
        if md["conf_num"].duplicated().any():
            bad = sorted(md.loc[md["conf_num"].duplicated(keep=False), "conf_num"].unique().tolist())
            raise ValueError(f"roles.enrich: meetings has conflicting context rows for {len(bad)} conf_num "
                             f"(first: {bad[:10]}); one row per conf_num is required")
        m = md
    m = m.reset_index(drop=True)
    ctx_pos = pd.Index(m["conf_num"]).get_indexer(out["conf_num"]).astype(np.int64)
    stats["turns_without_meeting"] = int((ctx_pos < 0).sum())

    # ---- per-row key parts as integer codes
    pos_c = pd.factorize(col("speaker_pos"))[0]
    name_c = pd.factorize(col("speaker_name"))[0]
    lab_c = pd.factorize(col("speaker_label_raw"))[0]
    midn = pd.to_numeric(col("speaker_mem_id"), errors="coerce")
    has = (midn.notna() & (midn > 0)).to_numpy(dtype=bool)
    sm_c, sm_u = pd.factorize(col("source_member_id"))
    sm_ok = np.array([LR.has_member_id(u) for u in sm_u], dtype=bool)
    has_v9 = has | ((sm_c >= 0) & (sm_ok[np.maximum(sm_c, 0)] if len(sm_ok) else False))
    src_c, src_u = pd.factorize(col("source"))
    is_x_u = np.array([str(u) == "xlsx" for u in src_u], dtype=bool)
    is_x = (src_c >= 0) & (is_x_u[np.maximum(src_c, 0)] if len(is_x_u) else False)
    stats["rows_has_mem_id"] = int(has.sum())
    stats["rows_v9_has_member_id"] = int(has_v9.sum())
    stats["rows_xlsx"] = int(is_x.sum())

    key = ctx_pos + 1
    for c in (pos_c, name_c, lab_c, has, has_v9, is_x):
        c = np.asarray(c, dtype=np.int64) + 1
        key = pd.factorize(key * (int(c.max()) + 1) + c)[0].astype(np.int64)
    k = int(key.max()) + 1
    first = np.empty(k, dtype=np.int64)
    first[key[::-1]] = np.arange(n - 1, -1, -1, dtype=np.int64)
    stats["distinct_keys"] = k

    roster = default_roster() if roster is None else roster
    na_comm = default_na_committees() if na_committees is None else na_committees
    na_by_term = default_na_committees_by_term() if na_committees_by_term is None else na_committees_by_term
    lookup = default_v9_lookup() if v9_lookup is None else v9_lookup

    P = col("speaker_pos").to_numpy(dtype=object)[first]
    N = col("speaker_name").to_numpy(dtype=object)[first]
    L = col("speaker_label_raw").to_numpy(dtype=object)[first]
    H, HV, X = has[first], has_v9[first], is_x[first]
    cp = ctx_pos[first]
    ctxv = {}
    for c in _MEETING_COLS[1:]:
        arr = m[c].to_numpy(dtype=object) if c in m.columns else np.full(len(m), None, dtype=object)
        v = np.full(k, None, dtype=object)
        ok = cp >= 0
        v[ok] = arr[cp[ok]]
        ctxv[c] = v
    res = {c: np.empty(k, dtype=object) for c in CONTRACT_COLS + EXTRA_COLS}
    names = np.empty(k, dtype=object)

    def _classify(i, pos, name, label):
        return classify(pos, name, label_raw=label, mem_id=1 if H[i] else None, term=ctxv["term"][i],
                        class_name=ctxv["class_name"][i], hearing_type=ctxv["hearing_type"][i],
                        is_subcommittee=ctxv["is_subcommittee"][i], committee_raw=ctxv["committee_raw"][i],
                        subcommittee=ctxv["subcommittee"][i], roster=roster, na_committees=na_comm,
                        na_committees_by_term=na_by_term)

    for i in range(k):
        r = _classify(i, P[i], N[i], L[i])
        v9r, v9src = v9_compat(r.pos_hangul, r.name, bool(HV[i]), lookup, r.pos_hangul_printed,
                               label_exact=L[i] if X[i] else None)
        for c, v in (("role", r.role), ("role_group", r.role_group), ("role_rule", r.role_rule),
                     ("role_v9_compat", v9r), ("affiliation_raw", r.affiliation_raw),
                     ("person_title", r.person_title), ("title_raw", r.title_raw), ("pos_hangul", r.pos_hangul),
                     ("pos_fix", r.pos_fix), ("role_v9_compat_src", v9src)):
            res[c][i] = v
        names[i] = r.name

    # ---- fused labels (label_fused: a label that joins two speakers' labels around a marker) are
    # excluded: role 'other', role_group 'excluded', the last label's role kept inside role_rule
    fused = pd.array(col("label_fused"), dtype="boolean").fillna(False).to_numpy(dtype=bool)
    stats["label_fused_rows"] = int(fused.sum())

    # ---- within-meeting label consistency (key level; a key never spans meetings)
    conf_c = pd.factorize(col("conf_num"))[0]
    nonfused_n = np.bincount(key, weights=(~fused).astype(np.float64), minlength=k).astype(np.int64)
    flag_k, rep_k, maj_k, cstats = _meeting_label_consistency(conf_c[first], names, res, nonfused_n, P)
    stats.update(cstats)
    for i, j in rep_k.items():                   # repair: classify the majority label with this key's mem_id
        r = _classify(i, P[j], N[j], L[j])
        res["role"][i], res["role_group"][i] = r.role, r.role_group
        res["role_rule"][i] = "meeting_majority_repair>" + r.role_rule
        res["affiliation_raw"][i], res["person_title"][i], res["pos_hangul"][i] = \
            r.affiliation_raw, r.person_title, r.pos_hangul
        res["pos_fix"][i] = (res["pos_fix"][i] + "+" if res["pos_fix"][i] not in (None, "none") else "") + \
            "meeting_majority_repair"

    rows = {c: res[c][key] for c in CONTRACT_COLS + EXTRA_COLS}      # fancy indexing: row-level copies
    if fused.any():
        pf = rows["pos_fix"][fused].astype(str)
        stats["label_fused_two_speakers_rows"] = int(sum("two_speakers" in x for x in pf))
        stats["label_fused_role_group_before"] = {
            str(g): int(c) for g, c in zip(*np.unique(rows["role_group"][fused].astype(str), return_counts=True))}
        rows["role_rule"][fused] = np.array([f"{FUSED_RULE_PREFIX}[{a}]>{b}" for a, b in
                                             zip(rows["role"][fused], rows["role_rule"][fused])], dtype=object)
        rows["role"][fused] = "other"
        rows["role_group"][fused] = "excluded"
    n_empty = 0
    for c in CONTRACT_COLS + EXTRA_COLS:
        vals = np.array(rows[c], dtype=object)
        blank = np.fromiter((isinstance(x, str) and not x.strip() for x in vals), dtype=bool, count=len(vals))
        n_empty += int(blank.sum())
        vals[blank] = None                                           # NULL policy: never an empty string
        out[c] = pd.array(vals, dtype="string")
    stats["empty_string_to_null"] = n_empty
    out["label_inconsistent_in_meeting"] = pd.array(flag_k[key] & ~fused, dtype="boolean")
    # a title printed as a former office ('(전)육군참모총장'); role still follows the office in the title
    pos_arr = col("speaker_pos").to_numpy(dtype=object)
    out["is_former_title"] = pd.array([bool(FORMER_TITLE_RE.match(x)) if isinstance(x, str) else False
                                       for x in pos_arr], dtype="boolean")
    stats["former_title_rows"] = int(out["is_former_title"].sum())
    rep_mask = np.zeros(k, dtype=bool)
    rep_mask[list(rep_k)] = True
    out["label_repaired"] = pd.array(rep_mask[key] & ~fused, dtype="boolean")
    out["label_meeting_majority"] = pd.array(np.where(fused, None, maj_k[key]), dtype="string")
    stats["label_inconsistent_rows"] = int(out["label_inconsistent_in_meeting"].sum())
    stats["label_repaired_rows"] = int(out["label_repaired"].sum())
    # completeness: the majority is taken over the turns passed in; a meeting split across calls
    # would get a different majority (run_all never splits meetings)
    if "n_turns" in meetings.columns:
        got = col("conf_num").value_counts()
        exp = pd.to_numeric(meetings.drop_duplicates("conf_num").set_index("conf_num")["n_turns"],
                            errors="coerce").reindex(got.index)
        inc = int((exp.notna() & (got < exp)).sum())
        stats["label_consistency_meetings_incomplete"] = inc
        if inc:
            warnings.warn(f"roles.enrich: {inc} meetings are passed with fewer turns than meetings.n_turns; "
                          "label_inconsistent_in_meeting uses only the turns passed", RuntimeWarning, stacklevel=2)
    stats["load_status"] = load_status()
    out.attrs["roles_enrich"] = stats
    return out


def _meeting_label_consistency(conf_k, names, res, n_k, P):
    """Key-level within-meeting label consistency.

    Per (meeting, person name) the turns' titles are counted (fused labels, empty names and empty
    labels left out). The majority title is the title with the most turns (strictly more than any
    other title); its role class is the class of most of its turns (role class: 'legislator' for
    legislator and chair, else the role). A key whose class differs from the majority class is
      repaired (label_repaired) when its title is an obvious typo of the majority title: a strict
        prefix of it (any role except a member / presiding title), or one edit away from it while its own role is unrecognised
        ('other' / 'unknown'); the role then comes from the majority label. One edit between two
        recognised titles ('법무부장관' / '법무부차관') is a different office, not a typo;
      flagged (label_inconsistent_in_meeting) otherwise; the printed role is kept.
    When the top titles tie and their classes differ there is no majority and every key of that
    person in the meeting is flagged.
    Returns (flag per key, {key: representative majority key}, majority printed title per key
    (flagged and repaired keys), stats)."""
    import numpy as np
    import pandas as pd
    k = len(names)
    flag = np.zeros(k, dtype=bool)
    maj = np.full(k, None, dtype=object)
    rep = {}
    st = {"label_groups_checked": 0, "label_groups_with_several_classes": 0, "label_groups_no_majority": 0,
          "label_inconsistent_keys": 0, "label_repaired_keys": 0, "label_repaired_keys_prefix": 0,
          "label_repaired_keys_one_edit": 0, "label_typo_like_but_recognised_keys": 0}
    nk = np.array([unicodedata.normalize("NFKC", _WS.sub("", x)) if isinstance(x, str) else "" for x in names],
                  dtype=object)
    tk = np.array([_WS.sub("", x) if isinstance(x, str) else "" for x in res["pos_hangul"]], dtype=object)
    role = res["role"]
    cls = np.array([_role_class(r) for r in role], dtype=object)
    K = pd.DataFrame({"conf": conf_k, "nk": nk, "tk": tk, "cls": cls, "n": n_k, "i": np.arange(k)})
    K = K[(K.n > 0) & (K.nk != "") & (role != "unknown")]
    if K.empty:
        return flag, rep, maj, st
    st["label_groups_checked"] = int(K.groupby(["conf", "nk"]).ngroups)
    K = K[K.groupby(["conf", "nk"])["cls"].transform("nunique") > 1]
    for (_, _), g in K.groupby(["conf", "nk"], sort=False):
        st["label_groups_with_several_classes"] += 1
        tt = g.groupby("tk")["n"].sum().sort_values(ascending=False, kind="stable")
        tcls = g.groupby(["tk", "cls"])["n"].sum().reset_index().sort_values(
            ["tk", "n", "cls"], ascending=[True, False, True]).drop_duplicates("tk").set_index("tk")["cls"]
        top = tt[tt == tt.iloc[0]].index.tolist()
        if len(top) == 1:
            mt, mc = top[0], tcls[top[0]]
        elif len({tcls[t] for t in top}) == 1:
            mt, mc = None, tcls[top[0]]
        else:
            mt, mc = None, None
        if mc is None:
            st["label_groups_no_majority"] += 1
            flag[g.i.to_numpy()] = True
            continue
        j = int(g[g.tk == mt].sort_values("n", ascending=False, kind="stable").i.iloc[0]) if mt is not None else None
        for r in g.itertuples(index=False):
            if r.cls == mc:
                continue
            if mt is not None:
                maj[r.i] = P[j]
            kind = title_typo_kind(r.tk, mt) if mt is not None else None
            # a member / presiding title ('위원') is a complete title, never a truncated one
            if (kind == "prefix" and r.cls != "legislator") or (kind == "one_edit" and role[r.i] in _UNRECOGNISED_ROLES):
                rep[r.i] = j
                st["label_repaired_keys_" + kind] += 1
            else:
                flag[r.i] = True
                if kind == "one_edit":
                    st["label_typo_like_but_recognised_keys"] += 1
    st["label_inconsistent_keys"] = int(flag.sum())
    st["label_repaired_keys"] = len(rep)
    return flag, rep, maj, st
