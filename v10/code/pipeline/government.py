"""government.py - government-side metadata for speech turns (kr-hearings-data v10).

enrich(turns, meetings) adds, without reordering rows:

  ministry_normalized  central-government organisation named in `speaker_pos`
                       (period name, typo-fixed; e.g. '보건복지가족부', '국세청', '국무총리')
  ministry_family      rename-lineage key ('education', 'labor', 'food_drug_safety', ...): cabinet
                       lineage (MINISTRY_LINEAGE, also used for panel matching), else ORG_FAMILY
                       (renamed agencies / commissions; regional offices and overseas missions
                       get their parent ministry's lineage); null when no lineage is recorded
  ministry_rule        which normalisation rule fired (see normalize_ministry)
  minister_panel_id    panel v2 (default): the linked spell_id of minister-data v2 spells.csv, or the
                       acting_id of acting_heads.csv for an acting head. panel legacy_296: the
                       '{name}|{ministry}|{start}' key of minister-data/data/minister_panel_comprehensive.csv
  minister_spell_id, minister_nomination_id, minister_acting_id, minister_person_id, minister_lineage
                       v2 link keys (spells / nominations / acting_heads ids, person_id, release lineage
                       key such as 'finance', 'pm'); null in legacy_296
  dual_office          v2: the linked spell holds a National Assembly seat on the SPEECH DATE
                       (dual_office_start <= date <= dual_office_end; null for acting heads and for nominations
                       without a spell). legacy_296: the panel's row-level dual_office (nullable bool)
  link_method          how the panel link was made or why it failed (null = role not linked)
  gov_link_name        v2: Hangul name of the linked spell / nominee / acting head (null when unlinked).
                       legacy_296: Hangul name used for the panel lookup (Hanja names converted)
  admin                administration on the speech date from president_calendar.csv
                       ('박근혜', '권한대행(황교안)', ...)
  admin_ideology       'Progressive' / 'Conservative', null in acting windows
  gov_date_source      'speech_date' or 'meeting_date' (fallback: turns.date, else meetings.date),
                       'unparseable_speech_date' / 'unparseable_meeting_date' (chosen date is not
                       a valid YYYY-MM-DD date: admin null), or null (no date)
  presidency_state     only added when the input does not already carry it (party_timeline
                       owns it); when present, agreement is checked and counted. Values normal,
                       partyless (president in office without a party, researcher decision 6),
                       suspended, acting; no 'vacant' (every post-removal calendar row names an
                       acting president, load_calendar refuses one without)

Rules (researcher decisions of 2026-09-25 and the component brief):
  * admin comes from the speech date and the president calendar, never from the panel.
    김대중/노무현/문재인/이재명 Progressive, 이명박/박근혜/윤석열 Conservative.
  * A suspended president stays the administration (admin = the suspended president,
    presidency_state 'suspended'); after a removal the acting government is labelled
    '권한대행(NAME)' with null ideology and presidency_state 'acting'. The suspended-window
    choice is a parameter (suspended_admin='acting' flips it).
  * Panel linkage only for roles minister, minister_acting, minister_nominee,
    prime_minister. A link needs name AND ministry (exact name or same cabinet lineage)
    AND the speech date inside a window around the appointment. There are no date-free
    fallbacks (v9 fallback_2 / fallback_3 are gone).
    Never linked (R2, 2026-09-26): a turn whose printed title disagrees with the person's majority
    title in the meeting (roles.py label_inconsistent_in_meeting; link_method
    'unlinked:label_inconsistent_in_meeting') and a turn whose speaker label the parser rates
    label_confidence 'low' ('unlinked:label_confidence_low'). A title repaired to the meeting
    majority (roles.py label_repaired) is normalised from the repaired title
    (label_meeting_majority).
      - minister, prime_minister: [start, end] ('tenure'), else [start-B, end+B] ('buffer')
      - minister_nominee: [start-N, start+M] ('nominee'): the hearing precedes the appointment,
        but many panel start dates are nomination or approximate dates, so M > 0; else
        (start+M, end] ('nominee_in_tenure', a conflict label: panel says already in office)
      - minister_acting: never linked. The panel has no acting periods. Label
        'unmatched:acting_inside_own_tenure' when the date falls in the person's own panel
        tenure (conflict), else 'unmatched:acting_no_acting_record'. A prime_minister turn
        whose title is a bare '국무총리직무대행' (no own post printed) is linked the same way.
    B = BUFFER_DAYS, N = NOMINEE_PRE_DAYS, M = NOMINEE_POST_DAYS. Panel rows with end < start or no start are
    never matched; a missing end is capped at the day before the next inauguration after
    the start (open, end_eff = date.max, for the sitting administration; window arithmetic
    saturates) and flagged ':end_imputed'.
    The rules in this bullet are the legacy_296 panel (config switchable.government.panel), kept to
    reproduce the numbers before 2026-09-28.
  * Panel v2 (default since the researcher's approval of 2026-09-28; SpellIndex): the minister-data
    v2.0.0 snapshot in v10/interim/external/ (config minister_release, sha256-checked against its
    MANIFEST.json; rc3 was approved and released as v2.0.0 the same day, same linking tables). Agreed interface of 2026-09-26/27:
      - person: speaker_name (NFKC, whitespace removed; Hanja too) equals spells.name or name_hanja, or a
        person_name_variants.csv variant_string valid for the lineage and date
      - lineage: the printed title (speaker_pos, also with 후보자 / 직무대행 / 직무대리 stripped), else
        ministry_normalized, looked up in ministry_alias.csv rows valid on the date; person-scoped rows
        (person_id set) only for that person; the first candidate string with a valid row decides, and a
        vice-minister title_form row or an out_of_scope lineage never links; role prime_minister and titles
        starting with 국무총리 / 國務總理 are lineage 'pm'
      - spells (minister, prime_minister): spell_start - B <= date <= (spell_end or cutoff) + B,
        B = spell_buffer_days (default 1; 'spell:exact' inside, else 'spell:buffer')
      - nominees (minister_nominee, and role 'nominee' whose title resolves to a cabinet lineage, e.g.
        '국무총리후보자'): nominations.csv (nominee, lineage) with a hearing date within 1 day
        ('nomination:hearing'); spell_id null for withdrawn / rejected nominations. A role-'nominee'
        title that names no office ('公職候補者') in a PM confirmation hearing committee is read as
        '국무총리후보자' ('nomination:committee_title')
      - acting heads: role prime_minister with a 직무대행 / 직무대리 title -> acting_heads.csv rows acting for
        'pm' ('acting_head:pm'); minister_acting -> rows of the resolved lineage ('acting_head:lineage',
        coverage 'incidental, not exhaustive'); same person (name), from <= date <= (to or cutoff)
      - the three gates above (label_inconsistent_in_meeting, label_confidence 'low', former title) apply
      - unlinked reasons: unlinked:name_not_in_panel, lineage_unresolved, vice_minister_title,
        lineage_out_of_scope, person_in_other_lineage, outside_spell, outside_hearing, not_in_acting_heads,
        outside_acting_period, no_name, no_date, plus the gates

No I/O at import time. Pure pandas; the panel, snapshot, config and calendar are read lazily.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import itertools
import json
import os
import re
import unicodedata
from collections import namedtuple
from functools import lru_cache
from typing import Iterable, Optional

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
V10 = os.path.abspath(os.path.join(HERE, "..", ".."))
REPO = os.path.abspath(os.path.join(V10, ".."))
# legacy_296 panel only (minister-data working tree). The default panel (v2) is the snapshot named by
# config.yaml switchable.government.minister_release. PANEL_PATH is kept as the legacy name (run_all.py
# fingerprints it).
LEGACY_PANEL_PATH = os.path.abspath(os.path.join(REPO, "..", "minister-data", "data",
                                                 "minister_panel_comprehensive.csv"))
PANEL_PATH = LEGACY_PANEL_PATH
CONFIG_PATH = os.path.join(HERE, "config.yaml")
CALENDAR_PATH = os.path.join(V10, "interim", "president_calendar.csv")
MEMBERS_PATH = os.path.join(V10, "interim", "members_allnamember_16_22.parquet")
# Optional: reading table of the PyPI package hanja 0.15.1 (vendored by another component; licence
# not verified, see raw/third_party/README.md). Used only as an extra reading source when present.
VENDORED_HANJA_TABLE = os.path.join(V10, "raw", "third_party", "hanja_table_0.15.1.yml")

LINK_ROLES = ("minister", "minister_acting", "minister_nominee", "prime_minister")
LEG_ROLES = ("legislator", "chair")
FORMER_TITLE_RE = re.compile(r"^\s*[\(（]\s*(?:전|前)\s*[\)）]|^前(?=[一-鿿])")   # same rule as roles.FORMER_TITLE_RE
BUFFER_DAYS = 7          # tolerance around panel start/end (panel dates are partly approximate)
NOMINEE_PRE_DAYS = 60    # a confirmation hearing is matched to an appointment starting <= 60 days later
NOMINEE_POST_DAYS = 60   # ... or starting <= 60 days earlier (many panel starts are nomination dates)

ADMIN_IDEOLOGY = {
    "김대중": "Progressive", "노무현": "Progressive", "문재인": "Progressive", "이재명": "Progressive",
    "이명박": "Conservative", "박근혜": "Conservative", "윤석열": "Conservative",
}

V2_LINK_COLUMNS = ("minister_spell_id", "minister_nomination_id", "minister_acting_id", "minister_person_id",
                   "minister_lineage")
ADDED_COLUMNS = ("ministry_normalized", "ministry_family", "ministry_rule", "minister_panel_id",
                 "dual_office", "link_method", "gov_link_name", "admin", "admin_ideology",
                 "gov_date_source") + V2_LINK_COLUMNS

# ---------------------------------------------------------------------------------------
# 1. Hanja -> Hangul (16대 and early 17대 viewer pages print positions and names in Hanja)
# ---------------------------------------------------------------------------------------
HANJA_RE = re.compile("[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")

# Standard (non-initial) readings. Covers every Hanja character seen in viewer positions
# of the 640 locally available pages (2026-09-25) plus minister-name characters.
_HANJA_READINGS_STR = (
    "員원長장議의委위部부官관理리務무國국總총人인政정證증副부法법統통通통財재經경濟제交교社사"
    "兼겸室실廳청事사檢검動동一일公공代대次차設설建건局국光광小소外외商상保보觀관大대企기劃획"
    "防방勞로産산健건察찰資자韓한地지方방秘비書서行행農농算산源원治치自자豫예管관領령水수文문"
    "化화的적會회處처道도福복祉지林림審심鐵철育육團단敎교康강險험參참海해陳진述술調조策책整정"
    "報보洋양査사民민考고物물高고稅세業업流류本본送송放방院원住주宅택首수席석技기術술學학科과"
    "家가環환境경計계等등金금勤근評평價가專전門문弘홍情정職직同동諮자問문制제謀모合합信신司사"
    "監감州주括괄李리狀상況황株주式식土토作작基기雇고傭용準준速속空공洙수冷랭藏장戰전田전陸륙"
    "軍군航항令령護호融융常상運운營영安안開개發발警경力력能능特특別별允윤補보決결全전年년任임"
    "靑청少소立립障장性성燦찬烈렬都도山산市시第제川천原원映영女녀鄭정際제郭곽鎬호獲획得득課과"
    "班반督독振진興흥仁인東동北북所소兵병美미永영場장畵화廢폐棄기明명消소費비中중擔담當당像상"
    "刊간製제子자域역涉섭央앙硏연究구聽청聞문支지辯변春춘謙겸鷺로粱량津진路로平평港항改개革혁"
    "徵징主주和화命명意의略략勳훈西서倫륜協협選선廣광權권張장碩석納납南남判판上상施시村촌申신"
    "溪계輪륜規규京경食식糧량候후對대裁재出출元원援원生생談담稔념灣만荷하役역宮궁礎초薛설姜강"
    "錫석氣기變변約약機기構구輸수淸청工공吳오個개憲헌鉉현共공衛위擧거在재喆철尹윤歐구度도漢한"
    "曺조現현過과去거眞진相상糾규體체煥환需수擴확推추進진澤택秀수非비五오義의崔최善선榮영孫손"
    "忠충悳덕花화卉훼販판喜희龍룡苦고衷충聖성植식潭담景경湜식樹수木목災재害해者자哲철劇극胞포"
    "洲주鐸탁璨찬泓홍滄창晶정潘반云운丞승玗우鎰일圻기蔣장鱗린署서館관限한鑑감定정關관達달象상"
    "許허品품醫의藥약來래腐부敗패知지柳류桓환"
)
HANJA_READINGS = {_HANJA_READINGS_STR[i]: _HANJA_READINGS_STR[i + 1]
                  for i in range(0, len(_HANJA_READINGS_STR), 2)}

# Word-level readings where the initial-sound rule (두음법칙) applies inside compounds.
HANJA_WORD_READINGS = {
    "勞動": "노동", "勞使": "노사", "勞政": "노정", "女性": "여성", "流通": "유통", "陸軍": "육군",
    "陸上": "육상", "林業": "임업", "立法": "입법", "理事": "이사", "年金": "연금", "冷藏": "냉장",
    "老人": "노인", "副總理": "부총리", "大統領": "대통령", "領事": "영사",
}


def _jamo_split(ch):
    code = ord(ch) - 0xAC00
    if not 0 <= code < 11172:
        return None
    return code // 588, (code % 588) // 28, code % 28


def _jamo_join(cho, jung, jong):
    return chr(0xAC00 + cho * 588 + jung * 28 + jong)


# vowel indices of ㅑ ㅕ ㅖ ㅛ ㅠ ㅣ (before which initial ㄹ/ㄴ become ㅇ)
_Y_VOWELS = {2, 6, 7, 12, 17, 20}


def initial_sound_rule(syl: str) -> str:
    """두음법칙 for one Hangul syllable: 리->이, 로->노, 녀->여, 류->유 ..."""
    parts = _jamo_split(syl) if syl else None
    if parts is None:
        return syl
    cho, jung, jong = parts
    if cho == 5:          # ㄹ
        return _jamo_join(11 if jung in _Y_VOWELS else 2, jung, jong)
    if cho == 2 and jung in _Y_VOWELS:   # ㄴ
        return _jamo_join(11, jung, jong)
    return syl


def hanja_to_hangul(text: Optional[str]) -> tuple:
    """Convert Hanja in a position string to Hangul. Returns (converted, n_unconverted)."""
    if text is None:
        return None, 0
    s = unicodedata.normalize("NFKC", str(text))
    if not HANJA_RE.search(s):
        return s, 0
    out = []
    i = 0
    first = True
    words = sorted(HANJA_WORD_READINGS, key=len, reverse=True)
    while i < len(s):
        w = next((w for w in words if s.startswith(w, i)), None)
        if w is not None:
            out.append(HANJA_WORD_READINGS[w])
            i += len(w)
        else:
            ch = s[i]
            r = HANJA_READINGS.get(ch)
            if r is None and HANJA_RE.match(ch):
                r = _vendored_readings().get(ch)
            if r is not None and first:
                r = initial_sound_rule(r)
            out.append(r if r is not None else ch)
            i += 1
        first = False
    res = "".join(out)
    return res, len(HANJA_RE.findall(res))


@lru_cache(maxsize=1)
def _vendored_readings(path: str = VENDORED_HANJA_TABLE) -> dict:
    """{hanja char: hangul syllable} from the optional vendored table ({} when absent)."""
    import json
    m = {}
    if not os.path.exists(path):
        return m
    rx = re.compile(r'^"(.+?)":\s*"(.+?)"\s*$')
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            mm = rx.match(line.strip())
            if mm:
                k, v = json.loads('"' + mm.group(1) + '"'), json.loads('"' + mm.group(2) + '"')
                if len(k) == 1 and len(v) == 1:
                    m[k] = v
    return m


@lru_cache(maxsize=1)
def _legislator_name_readings(path: str = MEMBERS_PATH):
    """{hanja_char: set(readings)} for surname (first syllable) and given-name positions,
    aligned syllable by syllable from NAAS_CH_NM / NAAS_NM of all 16-22대 members."""
    sur, giv = {}, {}
    if not os.path.exists(path):
        return sur, giv
    m = pd.read_parquet(path, columns=["NAAS_NM", "NAAS_CH_NM"])
    for a, b in zip(m["NAAS_CH_NM"].fillna(""), m["NAAS_NM"].fillna("")):
        a = unicodedata.normalize("NFKC", a).replace(" ", "")
        b = b.replace(" ", "")
        if not a or len(a) != len(b) or not all(HANJA_RE.match(c) for c in a):
            continue
        sur.setdefault(a[0], set()).add(b[0])
        for x, y in zip(a[1:], b[1:]):
            giv.setdefault(x, set()).add(y)
    return sur, giv


def hangul_name_candidates(name: Optional[str], limit: int = 64) -> list:
    """All plausible Hangul readings of a (partly) Hanja person name. Hangul names are
    returned unchanged. A character with no known reading yields no candidates."""
    if name is None:
        return []
    s = unicodedata.normalize("NFKC", str(name)).replace(" ", "")
    if not s:
        return []
    if not HANJA_RE.search(s):
        return [s]
    sur, giv = _legislator_name_readings()
    options = []
    for i, ch in enumerate(s):
        if not HANJA_RE.match(ch):
            options.append([ch])
            continue
        std = HANJA_READINGS.get(ch) or _vendored_readings().get(ch)
        if ch == "金":
            std = "김" if i == 0 else "금"
        # preferred reading first (surname: initial-sound rule applied, 李 -> 이 not 리), so the
        # first candidate is the conventional spelling when no panel name matches
        first = ([initial_sound_rule(std)] if i == 0 else [std]) if std else []
        rest = sorted(set((sur if i == 0 else giv).get(ch, set())) | ({std} if std else set()))
        opts = list(dict.fromkeys(first + rest))
        if not opts:
            return []
        options.append(opts)
    out = []
    for combo in itertools.product(*options):
        out.append("".join(combo))
        if len(out) >= limit:
            break
    return out


# ---------------------------------------------------------------------------------------
# 2. Ministry normalisation
# ---------------------------------------------------------------------------------------
# Cabinet-level offices (ministries, ministerial 처, 특임장관, 국무총리) -> lineage keys.
# The first key is the reported ministry_family; all keys count for panel matching.
MINISTRY_LINEAGE = {
    "재정경제부": ("finance_planning",), "기획예산처": ("finance_planning",),
    "기획재정부": ("finance_planning",),
    "통일부": ("unification",), "외교통상부": ("foreign_affairs",), "외교부": ("foreign_affairs",),
    "법무부": ("justice",), "국방부": ("defense",),
    "행정자치부": ("interior",), "행정안전부": ("interior",), "안전행정부": ("interior",),
    "국민안전처": ("public_safety",),
    "교육부": ("education",), "교육인적자원부": ("education",),
    "교육과학기술부": ("education", "science_ict"),
    "과학기술부": ("science_ict",), "정보통신부": ("science_ict",), "미래창조과학부": ("science_ict",),
    "과학기술정보통신부": ("science_ict",),
    "문화관광부": ("culture",), "문화체육관광부": ("culture",),
    "농림부": ("agriculture",), "농림수산식품부": ("agriculture", "oceans"),
    "농림축산식품부": ("agriculture",),
    "산업자원부": ("industry",), "지식경제부": ("industry",), "산업통상자원부": ("industry",),
    "보건복지부": ("health_welfare",), "보건복지가족부": ("health_welfare", "gender_family"),
    "환경부": ("environment",), "기후에너지환경부": ("environment",),
    "노동부": ("labor",), "고용노동부": ("labor",),
    "여성부": ("gender_family",), "여성가족부": ("gender_family",), "성평등가족부": ("gender_family",),
    "건설교통부": ("land_transport",), "국토해양부": ("land_transport", "oceans"),
    "국토교통부": ("land_transport",),
    "해양수산부": ("oceans",), "중소벤처기업부": ("sme",), "국가보훈부": ("veterans",),
    "특임장관": ("special_affairs",), "국무총리": ("prime_minister",),
    "산업통상부": ("industry",),   # 2025-10 reorganisation name seen in 22대 pages
}

# Other central bodies (executive agencies, presidential and PM offices, constitutional and
# judicial administration bodies). Name -> canonical name.
_OTHER_ORGS = (
    # PM and presidential offices
    "국무총리실", "국무총리비서실", "국무조정실", "국무총리국무조정실", "국무총리행정조정실",
    "대통령비서실", "대통령실", "대통령경호실", "대통령경호처", "국가안보실", "국가안전보장회의",
    "국민경제자문회의", "민주평화통일자문회의", "국가과학기술자문회의",
    # 처
    "법제처", "국가보훈처", "국정홍보처", "식품의약품안전처", "인사혁신처", "고위공직자범죄수사처",
    "공보처", "총무처", "비상기획위원회",
    # 청 / 본부
    "국세청", "관세청", "조달청", "통계청", "대검찰청", "검찰청", "병무청", "방위사업청", "경찰청",
    "소방청", "소방방재청", "기상청", "문화재청", "국가유산청", "농촌진흥청", "산림청", "특허청",
    "중소기업청", "식품의약품안전청", "해양경찰청", "철도청", "행정중심복합도시건설청",
    "새만금개발청", "재외동포청", "질병관리청", "질병관리본부", "우주항공청", "해양경비안전본부",
    "우정사업본부", "국립과학수사연구원",
    # 위원회
    "공정거래위원회", "금융위원회", "금융감독위원회", "방송통신위원회", "방송위원회",
    "국민권익위원회", "국가청렴위원회", "부패방지위원회", "국민고충처리위원회", "원자력안전위원회",
    "개인정보보호위원회", "국가인권위원회", "중앙인사위원회", "규제개혁위원회", "기획예산위원회",
    "여성특별위원회", "중소기업특별위원회", "중앙노동위원회", "경제사회노동위원회", "노사정위원회",
    "경제사회발전노사정위원회",
    "공적자금관리위원회", "국가교육위원회", "저출산고령사회위원회", "국가청소년위원회", "청소년위원회",
    "진실화해를위한과거사정리위원회", "국가과학기술위원회", "국가균형발전위원회", "지방시대위원회",
    "국가경찰위원회", "방송미디어통신위원회", "탄소중립녹색성장위원회",
    # 2025-10 reorganisation (seen in 22대 pages)
    "국가데이터처", "지식재산처",
    # constitutional / judicial / audit / intelligence / election
    "감사원", "국가정보원", "중앙선거관리위원회", "헌법재판소", "법원행정처", "대법원",
    # v10 additions (frequent uncovered titles in the v9 XLSX era, 2026-09-25)
    "특별감찰관", "국립보건원",
)
ORG_LEXICON = {k: k for k in MINISTRY_LINEAGE}
ORG_LEXICON.update({k: k for k in _OTHER_ORGS})
ORG_LEXICON.update({"특임장관실": "특임장관", "검찰청": "검찰청", "대검찰청": "검찰청", "검찰총장": "검찰청",
                    "대통령정책실": "대통령비서실"})
# 'X위원장' (head of a commission) is printed without 회: alias to 'X위원회'.
ORG_LEXICON.update({k[:-1] + "장": k for k in list(ORG_LEXICON) if k.endswith("위원회")})

# VERBATIM from legacy_rules.MINISTRY_TYPO_MAP (build_v9.py:61-108): typo -> ministry.
MINISTRY_TYPO_MAP = {
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
# v10 additions (typos and short forms seen in viewer pages and v9 XLSX titles, 2026-09-25).
MINISTRY_TYPO_MAP_V10 = {"정부통신부": "정보통신부", "경철청": "경찰청",
                         "특임": "특임장관",            # '특임차관' (v9 historical map had 특임 -> 특임장관)
                         "해양수산": "해양수산부",      # '해양수산차관보'
                         "식품의약청안전청": "식품의약품안전청", "복건복지부": "보건복지부",
                         "국무총식": "국무총리", "국모총리": "국무총리"}
_TYPO = {**MINISTRY_TYPO_MAP, **MINISTRY_TYPO_MAP_V10}
# v9 typo keys that merely extend a correctly spelt lexicon key with the same target
# ('행정자치부장관', '농림수산식품부제') are not used as prefix keys: the lexicon key yields the
# same ministry, and the rule is then labelled 'lexicon' instead of 'typo_map'.
_TYPO_SHADOWED = frozenset(k for k in _TYPO if any(k.startswith(lk) and ORG_LEXICON[lk] == _TYPO[k]
                                                   for lk in ORG_LEXICON))
_PREFIX_KEYS = sorted(set(ORG_LEXICON) | (set(_TYPO) - _TYPO_SHADOWED), key=len, reverse=True)
# a typo key only counts when a title follows it ('국방장관' yes, '국방과학연구소장' no) ...
_TYPO_REST_RE = re.compile(r"^(?:장관|차관|제[12]|[12]차관|장$|장직무|장권한|장대리|차장|후보|직무|권한|대리|$)")
# ... except a misspelt full ministry name ending in 부, which is accepted before any sub-unit
# ('산업자원통상부에너지산업정책관'), like a lexicon ministry.
_TYPO_ANY_REST = frozenset(k for k in _TYPO if k.endswith("부") and len(k) >= 3)
# a generic '...부/처/청/위원회' org only counts when a title or sub-unit follows it
_GENERIC_REST_RE = re.compile(r"^(?:장|차관|차장|실|국|과|본부|정책|심의|기획|대변|제\d|대리|직무|사무|감사|비서|"
                              r"(?:공동|부)?위원장|상임위원|위원$|[가-힣]{0,8}(?:과장|국장|단장|관)$|$)")

_ACTING_HEAD_RE = re.compile(r"^(대통령)(권한대행|직무대행)")
_ACTING_PM_RE = re.compile(r"^(국무총리)(직무대행|권한대행|직무대리)(?=.)")
_DEPUTY_PM_RE = re.compile(r"^(?:경제|사회)?부[총홍]리\s*겸")
_POLICY_ADVISER_RE = re.compile(r"^정책보좌관")

# Regional offices: (regex on the whole string, normalized name). Agency regionals map to
# the parent agency; ministry regionals keep a generic office-type name.
_REGION = r"[가-힣]{1,12}?"   # any place prefix; only reached after the lexicon failed
REGIONAL_OFFICES = (
    (re.compile(r"^" + _REGION + r"해양경찰청"), "해양경찰청"),
    (re.compile(r"^" + _REGION + r"경찰청"), "경찰청"),
    (re.compile(r"^" + _REGION + r"국세청"), "국세청"),
    (re.compile(r"^" + _REGION + r"검찰청"), "검찰청"),
    (re.compile(r"^" + _REGION + r"(?:본부)?세관"), "관세청"),
    (re.compile(r"^" + _REGION + r"병무청"), "병무청"),
    (re.compile(r"^" + _REGION + r"조달청"), "조달청"),
    (re.compile(r"^" + _REGION + r"통계청"), "통계청"),
    (re.compile(r"^" + _REGION + r"기상청"), "기상청"),
    (re.compile(r"^" + _REGION + r"산림청"), "산림청"),
    (re.compile(r"^" + _REGION + r"(?:지방)?고용노동청"), "지방고용노동청"),
    (re.compile(r"^" + _REGION + r"(?:지방)?노동청"), "지방노동청"),
    (re.compile(r"^" + _REGION + r"국토관리청"), "지방국토관리청"),
    (re.compile(r"^" + _REGION + r"유역환경청"), "유역환경청"),
    (re.compile(r"^" + _REGION + r"(?:대기)?환경청"), "지방환경청"),
    (re.compile(r"^" + _REGION + r"환경관리청"), "지방환경청"),   # pre-2002 name ('원주지방환경관리청')
    (re.compile(r"^" + _REGION + r"(?:구치소|교도소)"), "교정기관"),
    (re.compile(r"^" + _REGION + r"공정거래사무소"), "공정거래위원회"),
    (re.compile(r"^" + _REGION + r"체신청"), "지방체신청"),
    (re.compile(r"^" + _REGION + r"해양수산청"), "지방해양수산청"),
    (re.compile(r"^" + _REGION + r"(?:해양항만청|해운항만청)"), "지방해양항만청"),
    (re.compile(r"^" + _REGION + r"항공청"), "지방항공청"),
    (re.compile(r"^" + _REGION + r"보훈청"), "지방보훈청"),
    (re.compile(r"^" + _REGION + r"(?:식품의약품안전청|식약청)"), "지방식품의약품안전청"),
    (re.compile(r"^" + _REGION + r"(?:중소벤처기업청|중소기업청)"), "지방중소벤처기업청"),
    (re.compile(r"^" + _REGION + r"우정청"), "지방우정청"),
    (re.compile(r"^" + _REGION + r"교정청"), "지방교정청"),
)
_LOCAL_GOV_RE = re.compile(
    r"^[가-힣]{1,8}?(?:특별시|광역시|특별자치시|특별자치도|도|시|군|구)"
    r"(?:장|지사|교육감|교육청|청|의회|행정부시장|정무부시장|부시장|행정부지사|정무부지사|부지사|"
    r"경제부지사|부교육감|기획조정실|소방재난본부|소방본부|경제부시장|부군수|부구청장|$)"
    r"|^[가-힣]{1,6}(?:특별시|광역시|특별자치시|특별자치도)"
    r"|^(?:경기|강원|충청[남북]|전라[남북]|경상[남북]|제주|충[남북]|전[남북]|경[남북])도")
_ASSEMBLY_RE = re.compile(r"^(?:국회(?:사무처|사무총장|도서관|예산정책처|입법조사처|의장|부의장|사무차장|의사국|법제실)"
                          r"|예산정책처|입법조사처)")
_MILITARY_RE = re.compile(r"(육군|해군|공군|합동참모|해병대|국군|사관학교|사령부|사령관)")
_JUDICIARY_RE = re.compile(r"법원")
# Korean missions abroad ('주미합중국대한민국대사', '주뉴욕대한민국총영사관영사'). Not '주한...' (foreign
# missions in Korea), not '주식회사...' (a '(주)' / '(株)' company marker, see _COMPANY_MARK_RE), not
# '...대한민국무역진흥공사...' (KOTRA), and '영사' not followed by 장 ('...공영사장' is a company head).
_OVERSEAS_RE = re.compile(r"^주(?!한|식회사)[가-힣A-Za-z]+?(?:대한민국(?!무역)|대사|총영사|영사(?!장)|대표부)")
_NON_GOV_RE = re.compile(r"^(?:한국|대한)|(?:공사|공단|은행|주식회사|재단|협회|조합|연구원|연구소|대학교|대학|병원|"
                         r"진흥원|평가원|보험|방송|신문|학회|센터|기금|거래소|부동산원|회사(?!무)|그룹|노조|노동조합|마사회)")
# company markers '(주)' / '(株)' (NFKC also maps ㈜ U+321C and ㈱ U+3231 to these): rewritten to
# '주식회사' before the separators (which include the parentheses) are removed
_COMPANY_MARK_RE = re.compile(r"\(\s*(?:주|株)\s*\)")
# ('회사(?!무)': '...위원회사무처장' is '위원회' + '사무처장', not a company)
_GENERIC_ORG_RE = re.compile(r"^([가-힣0-9]{2,16}?(?:위원회|부|처|청))")
# checked on the org name without its final suffix (so the 원 of 위원회 does not count)
_GENERIC_BAD_RE = re.compile(r"원|본부|공사|공단|회|학교|기념관|예술의전당|박물관|미술관|경제자유구역")
# head of a government commission printed without 회 ('영화진흥위원장' -> 영화진흥위원회); NA committee
# names and special / sub-committees are excluded (see _na_committee_names)
_GENERIC_COMMISSION_RE = re.compile(r"^([가-힣0-9]{2,24}?위원)장(?:직무대행|직무대리|권한대행|대리)?$")
# a generic org ending in 부 (not 본부) followed by a head title is the 부 of a deputy title
# ('금융부위원장', '세월호인양추진단부단장'), not an org suffix
_DEPUTY_TITLE_REST_RE = re.compile(r"^(?:위원장|단장|원장|청장|처장|총장|총재|사장|이사장|회장|시장|지사|소장|관장|대표|의장)")
# separators removed before matching (after NFKC; NFKC maps U+318D (ㆍ) to U+119E, so both are
# listed), including bullets and circle markers printed before a position (∙ • ○ ◯)
_SEP_RE = re.compile("[\\s·․‧ㆍᆞ・.,()∙•○◯]")

# Rename lineages of organisations that are not panel ministries (agencies, commissions, offices),
# and parent-ministry lineage of ministry-internal units (regional offices, overseas missions).
# Used only for the ministry_family column; panel matching uses MINISTRY_LINEAGE alone.
ORG_FAMILY = {
    "식품의약품안전청": "food_drug_safety", "식품의약품안전처": "food_drug_safety",
    "지방식품의약품안전청": "food_drug_safety",
    "소방방재청": "fire_disaster", "소방청": "fire_disaster",
    "해양경찰청": "coast_guard", "해양경비안전본부": "coast_guard",
    "문화재청": "cultural_heritage", "국가유산청": "cultural_heritage",
    "질병관리본부": "disease_control", "질병관리청": "disease_control",
    "중소기업청": "sme", "중소기업특별위원회": "sme", "지방중소벤처기업청": "sme",
    "국가보훈처": "veterans", "지방보훈청": "veterans",
    "공보처": "public_information", "국정홍보처": "public_information",
    "중앙인사위원회": "civil_service", "인사혁신처": "civil_service",
    "부패방지위원회": "anti_corruption", "국가청렴위원회": "anti_corruption",
    "국민고충처리위원회": "anti_corruption", "국민권익위원회": "anti_corruption",
    "방송위원회": "broadcasting_commission", "방송통신위원회": "broadcasting_commission",
    "방송미디어통신위원회": "broadcasting_commission",
    "금융감독위원회": "financial_services_commission", "금융위원회": "financial_services_commission",
    "국무조정실": "pm_office", "국무총리실": "pm_office", "국무총리국무조정실": "pm_office",
    "국무총리행정조정실": "pm_office",
    "대통령비서실": "presidential_office", "대통령실": "presidential_office",
    "대통령경호실": "presidential_security", "대통령경호처": "presidential_security",
    "통계청": "statistics", "국가데이터처": "statistics",
    "특허청": "intellectual_property", "지식재산처": "intellectual_property",
    "청소년위원회": "youth_commission", "국가청소년위원회": "youth_commission",
    "노사정위원회": "social_dialogue", "경제사회발전노사정위원회": "social_dialogue",
    "경제사회노동위원회": "social_dialogue",
    "국가균형발전위원회": "balanced_development", "지방시대위원회": "balanced_development",
    "여성특별위원회": "gender_family", "기획예산위원회": "finance_planning",
    # ministry-internal units -> parent ministry lineage
    "지방고용노동청": "labor", "지방노동청": "labor", "지방국토관리청": "land_transport",
    "지방항공청": "land_transport", "유역환경청": "environment", "지방환경청": "environment",
    "지방해양수산청": "oceans", "지방해양항만청": "oceans", "지방교정청": "justice", "교정기관": "justice",
    "국립보건원": "disease_control",
    "재외공관": "foreign_affairs",
}


@lru_cache(maxsize=1)
def _na_committee_names() -> frozenset:
    """NA standing-committee names (legacy_rules.COMMITTEE_KEY_MAP_STANDING, team suffix removed)."""
    try:
        import legacy_rules as _lr
    except ImportError:
        import sys
        sys.path.insert(0, os.path.join(HERE, ".."))
        import legacy_rules as _lr
    return frozenset(k.split("-")[0] for k in _lr.COMMITTEE_KEY_MAP_STANDING)


def _prefix_org(s: str):
    """Longest prefix of s that is a lexicon org, or a typo key followed by a title.
    -> (canonical, rule, rest)."""
    for k in _PREFIX_KEYS:
        if s.startswith(k):
            rest = s[len(k):]
            if k in ORG_LEXICON:
                return ORG_LEXICON[k], "lexicon", rest
            if k in _TYPO_ANY_REST or _TYPO_REST_RE.match(rest):
                return _TYPO[k], "typo_map", rest
    return None, None, s


@lru_cache(maxsize=200_000)
def normalize_ministry(pos: Optional[str]) -> tuple:
    """speaker_pos -> (ministry_normalized, ministry_rule).

    Order: Hanja conversion; acting prefixes (대통령권한대행, 국무총리직무대행 + own post);
    부총리겸 prefix; longest lexicon / v9-typo prefix; regional offices; local government,
    National Assembly and non-government bodies (-> None); generic '...부/처/청/위원회'
    prefix; government commission head ('X위원장' -> 'X위원회', NA committees excluded);
    else None. Overseas missions (주...대사/총영사/대표부) -> '재외공관'. Rule names:
    'lexicon', 'typo_map', 'regional_office', 'overseas_mission', 'generic_suffix',
    'generic_commission', with modifiers 'hanja+', 'acting_for_president+', 'acting_for_pm+',
    'deputy_pm+'; None reasons 'empty', 'assembly_body', 'military', 'judiciary',
    'non_government', 'local_government', 'no_org', 'hanja_unconverted'."""
    if pos is None or (isinstance(pos, float) and pos != pos) or pos is pd.NA:
        return None, "empty"
    s = _COMPANY_MARK_RE.sub("주식회사", unicodedata.normalize("NFKC", str(pos)))
    s = _SEP_RE.sub("", s)
    mods = []
    if HANJA_RE.search(s):
        s, n_left = hanja_to_hangul(s)
        mods.append("hanja")
    else:
        n_left = 0
    s = _SEP_RE.sub("", s)
    if not s:
        return None, "empty"
    m = _ACTING_HEAD_RE.match(s)
    if m and len(s) > m.end():
        s = s[m.end():]
        mods.append("acting_for_president")
    m = _ACTING_PM_RE.match(s)
    if m:
        rest = s[m.end():]
        org, _, _ = _prefix_org(_DEPUTY_PM_RE.sub("", rest))
        if org is not None:
            s = rest
            mods.append("acting_for_pm")
    if _DEPUTY_PM_RE.match(s):
        s = _DEPUTY_PM_RE.sub("", s)
        mods.append("deputy_pm")
    s = _POLICY_ADVISER_RE.sub("", s)
    pre = "+".join(mods) + "+" if mods else ""
    org, rule, _ = _prefix_org(s)
    if org is not None:
        return org, pre + rule
    for rx, name in REGIONAL_OFFICES:
        if rx.match(s):
            return name, pre + "regional_office"
    if _ASSEMBLY_RE.match(s):
        return None, "assembly_body"
    if _OVERSEAS_RE.match(s):
        return "재외공관", pre + "overseas_mission"
    if _MILITARY_RE.search(s):
        return None, "military"
    if _JUDICIARY_RE.search(s):
        return None, "judiciary"
    if _NON_GOV_RE.search(s):
        return None, "non_government"
    if _LOCAL_GOV_RE.match(s):
        return None, "local_government"
    m = _GENERIC_ORG_RE.match(s)
    if m and not s.startswith(("장관", "차관")):
        org, rest = m.group(1), s[m.end():]
        if org.endswith("부") and not org.endswith("본부") and _DEPUTY_TITLE_REST_RE.match(rest):
            m = None      # the 부 of a deputy title ('...단부단장'), not an org suffix: no generic org
        if m is not None and _GENERIC_REST_RE.match(rest):
            core = org[:-3] if org.endswith("위원회") else org[:-1]
            if not _GENERIC_BAD_RE.search(core) and not (
                    org.endswith("위원회") and (org in _na_committee_names() or "특별위원" in org or org.endswith("소위원회"))):
                return org, pre + "generic_suffix"
    m = _GENERIC_COMMISSION_RE.match(s)
    head = m.group(1) if m else ""
    if head.endswith("부위원") and len(head) >= 5:
        head = head[:-3] + "위원"              # deputy head: '금융부위원장' -> 금융위원회
    if m and "특별위원" not in head and not head.endswith("소위원") and "위원회" not in head:
        org = head + "회"
        if org in ORG_LEXICON:
            return ORG_LEXICON[org], pre + "lexicon"
        if org not in _na_committee_names():
            return org, pre + "generic_commission"
    if n_left:
        return None, "hanja_unconverted"
    return None, "no_org"


def ministry_lineage(ministry: Optional[str]) -> tuple:
    """Cabinet lineage keys (panel matching only)."""
    if ministry is None:
        return ()
    return MINISTRY_LINEAGE.get(ministry, ())


def ministry_family(ministry: Optional[str]) -> Optional[str]:
    """Reported lineage key: cabinet lineage first, else ORG_FAMILY, else None."""
    lin = ministry_lineage(ministry)
    if lin:
        return lin[0]
    return ORG_FAMILY.get(ministry) if ministry is not None else None


# ---------------------------------------------------------------------------------------
# 3. President calendar -> admin, admin_ideology, presidency_state
# ---------------------------------------------------------------------------------------
PRESIDENCY_STATE_BY_STATUS = {"in_office": "normal", "suspended_impeachment": "suspended",
                              "vacant_after_removal": "acting"}
# 'in_office' without pres_party_formal -> 'partyless' (researcher decision 6, the same rule as party_timeline)


@lru_cache(maxsize=4)
def load_calendar(path: str = CALENDAR_PATH) -> pd.DataFrame:
    cal = pd.read_csv(path, dtype=str)
    cal["start_d"] = pd.to_datetime(cal["start"]).dt.date
    cal["end_d"] = pd.to_datetime(cal["end"]).dt.date   # NaT for the open row
    cal = cal.sort_values("start_d").reset_index(drop=True)
    for r in cal.itertuples(index=False):
        if r.status == "vacant_after_removal" and not (isinstance(r.acting_president, str) and r.acting_president):
            raise ValueError(f"president calendar row {r.start}: office vacant without an acting president")
    for i in range(1, len(cal)):   # contiguous, non-overlapping (checked in tests too)
        prev_end = cal.loc[i - 1, "end_d"]
        if pd.isna(prev_end) or prev_end + _dt.timedelta(days=1) != cal.loc[i, "start_d"]:
            raise ValueError(f"president calendar not contiguous at row {i}")
    return cal


def _to_date(x):
    if x is None or (isinstance(x, float) and x != x):
        return None
    if isinstance(x, _dt.datetime):
        return x.date()
    if isinstance(x, _dt.date):
        return x
    try:
        return _dt.date.fromisoformat(str(x)[:10])
    except ValueError:
        return None


def admin_for_date(d, calendar: Optional[pd.DataFrame] = None, suspended_admin: str = "president"):
    """-> (admin, admin_ideology, presidency_state) for one date (None triple if outside)."""
    d = _to_date(d)
    if d is None:
        return None, None, None
    cal = load_calendar() if calendar is None else calendar
    for r in cal.itertuples(index=False):
        end = r.end_d
        if r.start_d <= d and (pd.isna(end) or d <= end):
            state = PRESIDENCY_STATE_BY_STATUS.get(r.status)
            if state is None:
                raise ValueError(f"unknown calendar status {r.status!r}")
            pres = r.president if isinstance(r.president, str) and r.president else None
            acting = r.acting_president if isinstance(r.acting_president, str) and r.acting_president else None
            if state == "normal":
                party = getattr(r, "pres_party_formal", None)
                return pres, ADMIN_IDEOLOGY[pres], ("normal" if isinstance(party, str) and party else "partyless")
            if state == "suspended":
                if suspended_admin == "acting":
                    return f"권한대행({acting})", None, state
                return pres, ADMIN_IDEOLOGY[pres], state
            return f"권한대행({acting})", None, state        # acting president (load_calendar guarantees one)
    return None, None, None


def next_inauguration_after(d, calendar: Optional[pd.DataFrame] = None):
    """First date after d on which a different president starts an in_office row."""
    d = _to_date(d)
    cal = load_calendar() if calendar is None else calendar
    cur = admin_for_date(d, cal)[0]
    for r in cal.itertuples(index=False):
        if r.start_d > d and r.status == "in_office" and r.president != cur:
            return r.start_d
    return None


# ---------------------------------------------------------------------------------------
# 4. Minister panel: load, audit, index, link
# ---------------------------------------------------------------------------------------
PERSON_NAME_FIXES = {"졍종환": "정종환", "백영희": "백희영", "윤관웅": "윤광웅"}  # build_v9.py:118-122


def load_panel(path: str = PANEL_PATH, calendar: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Panel with parsed dates, validity flags and the effective end used for matching."""
    p = pd.read_csv(path, dtype={"start": str, "end": str})
    p = p.reset_index().rename(columns={"index": "panel_row"})
    p["start_d"] = p["start"].map(_to_date)
    p["end_d"] = p["end"].map(_to_date)
    p["minister_panel_id"] = (p["name"].astype(str) + "|" + p["ministry"].astype(str) + "|"
                              + p["start"].fillna("NA").astype(str))
    issues = []
    end_eff, imputed, valid = [], [], []
    for r in p.itertuples(index=False):
        iss = []
        if r.start_d is None:
            iss.append("missing_start")
        if r.end_d is None:
            iss.append("missing_end")
        if r.start_d is not None and r.end_d is not None and r.end_d < r.start_d:
            iss.append("end_before_start")
        ok = r.start_d is not None and "end_before_start" not in iss
        e, imp = r.end_d, False
        if ok and e is None:
            nxt = next_inauguration_after(r.start_d, calendar)
            e = (nxt - _dt.timedelta(days=1)) if nxt is not None else _dt.date.max
            imp = True
        if r.ministry not in MINISTRY_LINEAGE:
            iss.append("ministry_not_in_lineage_table")
        issues.append(";".join(iss))
        end_eff.append(e)
        imputed.append(imp)
        valid.append(ok)
    p["issues"] = issues
    p["end_eff"] = end_eff
    p["end_imputed"] = imputed
    p["valid_dates"] = valid
    return p


def audit_panel(panel: pd.DataFrame, calendar: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """One row per (panel_row, issue). Checks the brief's known problems plus: duplicate ids,
    same name in several rows, overlapping tenures of one ministry lineage, panel admin that
    differs from the calendar administration on the start date, and ministries missing from
    MINISTRY_LINEAGE."""
    cal = load_calendar() if calendar is None else calendar
    rows = []
    for r in panel.itertuples(index=False):
        for iss in filter(None, r.issues.split(";")):
            rows.append((r.panel_row, r.name, r.ministry, r.start, r.end, iss, ""))
        if r.start_d is not None:
            a, _, st = admin_for_date(r.start_d, cal)
            if a != r.admin:
                rows.append((r.panel_row, r.name, r.ministry, r.start, r.end,
                             "panel_admin_differs_from_calendar_at_start", f"calendar={a} ({st})"))
            if ADMIN_IDEOLOGY.get(r.admin) != r.admin_ideology:
                rows.append((r.panel_row, r.name, r.ministry, r.start, r.end,
                             "panel_ideology_differs_from_admin_map", str(r.admin_ideology)))
    dup = panel["minister_panel_id"].duplicated(keep=False)
    for r in panel[dup].itertuples(index=False):
        rows.append((r.panel_row, r.name, r.ministry, r.start, r.end, "duplicate_panel_id", ""))
    multi = panel.groupby("name")["panel_row"].transform("size") > 1
    for r in panel[multi].itertuples(index=False):
        rows.append((r.panel_row, r.name, r.ministry, r.start, r.end, "name_in_several_rows", ""))
    ok = panel[panel["valid_dates"]].copy()
    ok["fam"] = ok["ministry"].map(ministry_family)
    for fam, g in ok.groupby("fam"):
        g = g.sort_values("start_d")
        recs = list(g.itertuples(index=False))
        for a, b in itertools.combinations(recs, 2):
            if a.name == b.name:
                continue
            lo, hi = max(a.start_d, b.start_d), min(a.end_eff, b.end_eff)
            if lo < hi and a.ministry == b.ministry:
                days = (hi - lo).days
                rows.append((a.panel_row, a.name, a.ministry, a.start, a.end,
                             "tenure_overlaps_other_holder", f"{b.name} {b.start}~{b.end} ({days} d)"))
    return pd.DataFrame(rows, columns=["panel_row", "name", "ministry", "start", "end", "issue", "detail"])


def _shift(d: _dt.date, delta: _dt.timedelta) -> _dt.date:
    """d + delta, saturating at date.min / date.max (open panel rows carry end_eff = date.max)."""
    try:
        return d + delta
    except OverflowError:
        return _dt.date.max if delta > _dt.timedelta(0) else _dt.date.min


class PanelIndex:
    """name -> panel rows, with the matching windows precomputed."""

    def __init__(self, panel: pd.DataFrame, buffer_days: int = BUFFER_DAYS,
                 nominee_pre_days: int = NOMINEE_PRE_DAYS, nominee_post_days: int = NOMINEE_POST_DAYS):
        self.panel = panel
        self.buffer = _dt.timedelta(days=buffer_days)
        self.nom_pre = _dt.timedelta(days=nominee_pre_days)
        self.nom_post = _dt.timedelta(days=nominee_post_days)
        self.by_name = {}
        for r in panel.itertuples(index=False):
            self.by_name.setdefault(str(r.name).strip(), []).append(r)
        self.names = frozenset(self.by_name)

    def resolve_name(self, name) -> tuple:
        """-> (hangul_name or None, how). how: 'hangul', 'name_fix', 'hanja', 'hanja_no_panel_hit',
        'hanja_ambiguous', 'hanja_unreadable', 'empty'."""
        if name is None or (isinstance(name, float) and name != name) or not str(name).strip():
            return None, "empty"
        s = unicodedata.normalize("NFKC", str(name)).replace(" ", "")
        if not HANJA_RE.search(s):
            if s in PERSON_NAME_FIXES:
                return PERSON_NAME_FIXES[s], "name_fix"
            return s, "hangul"
        cands = hangul_name_candidates(s)
        if not cands:
            return None, "hanja_unreadable"
        hits = sorted({c for c in cands if c in self.names})
        if len(hits) == 1:
            return hits[0], "hanja"
        if len(hits) > 1:
            return None, "hanja_ambiguous"
        return cands[0], "hanja_no_panel_hit"

    def link(self, name: Optional[str], ministry: Optional[str], d, role: str) -> tuple:
        """-> (panel_row or None, link_method). `name` must already be Hangul."""
        d = _to_date(d)
        if d is None:
            return None, "unmatched:no_date"
        if not name:
            return None, "unmatched:no_name"
        if ministry is None:
            return None, "unmatched:no_ministry"
        cands = self.by_name.get(name)
        if not cands:
            return None, "unmatched:name_not_in_panel"
        lin = set(ministry_lineage(ministry))
        compat = []
        for c in cands:
            if c.ministry == ministry:
                compat.append((c, "exact"))
            elif lin and lin & set(ministry_lineage(c.ministry)):
                compat.append((c, "lineage"))
        if role == "minister_acting":
            # The panel holds appointments only (no acting periods), so an acting minister is
            # never linked. A speech inside the person's own panel tenure of the same ministry
            # contradicts the panel (flagged; seen where a panel start date is approximate).
            if any(c.valid_dates and c.start_d <= d <= c.end_eff for c, _ in compat):
                return None, "unmatched:acting_inside_own_tenure"
            return None, "unmatched:acting_no_acting_record"
        if not compat:
            return None, "unmatched:ministry_mismatch"
        valid = [(c, how) for c, how in compat if c.valid_dates]
        if not valid:
            return None, "unmatched:panel_dates_invalid"
        best = []   # (tier_rank, distance_days, -start_ordinal, candidate, label)
        for c, how in valid:
            s, e = c.start_d, c.end_eff
            rank_m = 0 if how == "exact" else 1
            if role == "minister_nominee":
                if _shift(s, -self.nom_pre) <= d <= _shift(s, self.nom_post):
                    best.append((0 + rank_m, abs((s - d).days), -s.toordinal(), c, f"nominee:{how}"))
                elif s <= d <= e:
                    # hearing inside the panel tenure beyond the post-window: the literal
                    # [start, end] rule holds, but it contradicts 'nominee' (flagged label)
                    best.append((2 + rank_m, (d - s).days, -s.toordinal(), c, f"nominee_in_tenure:{how}"))
                continue
            inside = s <= d <= e
            if inside:
                best.append((0 + rank_m, 0, -s.toordinal(), c, f"tenure:{how}"))
            elif _shift(s, -self.buffer) <= d <= _shift(e, self.buffer):
                dist = (s - d).days if d < s else (d - e).days
                best.append((2 + rank_m, dist, -s.toordinal(), c, f"buffer:{how}"))
        if not best:
            return None, "unmatched:outside_window"
        best.sort(key=lambda t: (t[0], t[1], t[2]))
        if len(best) > 1 and best[0][:3] == best[1][:3] and best[0][3].panel_row != best[1][3].panel_row:
            return None, "unmatched:ambiguous"
        c, label = best[0][3], best[0][4]
        if c.end_imputed:
            label += ":end_imputed"
        return c, label


@lru_cache(maxsize=4)
def _default_index(path: Optional[str] = None, buffer_days: int = BUFFER_DAYS,
                   nominee_pre_days: int = NOMINEE_PRE_DAYS, nominee_post_days: int = NOMINEE_POST_DAYS,
                   panel: Optional[str] = None):
    """The panel index enrich uses when none is passed. `panel` None = config.yaml
    switchable.government.panel. v2: SpellIndex over the configured snapshot (the legacy window
    arguments, which run_all.py passes, are ignored). legacy_296: PanelIndex over `path` (default the
    296-row minister_panel_comprehensive.csv) with the legacy windows."""
    panel = government_settings()["panel"] if panel is None else panel
    if panel == "legacy_296":
        return PanelIndex(load_panel(path or LEGACY_PANEL_PATH), buffer_days, nominee_pre_days, nominee_post_days)
    if panel not in PANEL_MODES:
        raise ValueError(f"unknown minister panel {panel!r}; implemented: {list(PANEL_MODES)}")
    st = government_settings()
    if st["panel"] != panel:
        raise ValueError(f"panel {panel!r} requested but config.yaml configures {st['panel']!r} "
                         "(minister_release belongs to the configured panel)")
    return SpellIndex(st["minister_release"], spell_buffer_days=st["spell_buffer_days"],
                      expect_version=PANEL_RELEASE_VERSION[panel])


# ---------------------------------------------------------------------------------------
# 4b. minister-data v2 release (panel v2, the default since 2026-09-28)
# ---------------------------------------------------------------------------------------
PANEL_MODES = ("v2", "legacy_296")
PANEL_RELEASE_VERSION = {"v2": "v2.0.0"}     # MANIFEST.json 'version' the snapshot must carry
EXTERNAL_ROOT = os.path.join(V10, "interim", "external")
V2_FILES = ("spells.csv", "ministry_alias.csv", "person_name_variants.csv", "nominations.csv", "acting_heads.csv")
SPELL_BUFFER_DAYS = 1          # spell_start - B <= speech_date <= (spell_end or cutoff) + B
NOMINATION_HEARING_DAYS = 1    # |speech_date - hearing date| <= this
V2_TITLE_SUFFIXES = ("후보자", "직무대행", "직무대리", "候補者", "職務代行", "職務代理")
_V2_ACTING_TITLE_RE = re.compile("직무대행|직무대리|職務代行|職務代理")
_V2_VICE_TITLE_RE = re.compile("차관|次官|借款")        # vice-minister title_form rows (借款: printed for 次官)
_V2_PM_PREFIXES = ("국무총리", "國務總理")
# link_method prefixes of a successful link, per panel
LINKED_PREFIXES = {"v2": ("spell:", "nomination:", "acting_head:"),
                   "legacy_296": ("tenure:", "buffer:", "nominee:", "nominee_in_tenure:")}
_LINEAGE_MISS = {"vice_minister_title": "unlinked:vice_minister_title",
                 "out_of_scope": "unlinked:lineage_out_of_scope"}


@lru_cache(maxsize=4)
def government_settings(path: str = CONFIG_PATH) -> dict:
    """switchable.government of config.yaml, read by government.py itself (run_all.py passes only
    suspended_admin and the legacy windows). -> {'panel', and for v2 panels 'minister_release' (absolute
    path) and 'spell_buffer_days'}. The snapshot must lie under v10/interim/external/ (never the
    minister-data working tree)."""
    import yaml
    with open(path, encoding="utf-8") as fh:
        g = ((yaml.safe_load(fh) or {}).get("switchable") or {}).get("government") or {}
    panel = g.get("panel")
    if panel not in PANEL_MODES:
        raise ValueError(f"switchable.government.panel = {panel!r}; implemented: {list(PANEL_MODES)}")
    out = {"panel": panel}
    if panel == "legacy_296":
        return out
    rel = g.get("minister_release")
    if not isinstance(rel, str) or not rel or os.path.isabs(rel):
        raise ValueError(f"switchable.government.minister_release must be a path relative to v10/, got {rel!r}")
    ab = os.path.normpath(os.path.join(V10, rel))
    root = os.path.realpath(EXTERNAL_ROOT)
    if os.path.commonpath([os.path.realpath(ab), root]) != root:
        raise ValueError(f"minister_release {rel!r} is not a snapshot under v10/interim/external/")
    b = g.get("spell_buffer_days")
    if not isinstance(b, int) or isinstance(b, bool) or b < 0:
        raise ValueError(f"switchable.government.spell_buffer_days must be a non-negative integer, got {b!r}")
    out.update(minister_release=ab, spell_buffer_days=b)
    return out


def _nk(s) -> str:
    """NFKC, all whitespace removed ('' for null). The key of every v2 name / title comparison."""
    if s is None or s is pd.NA or (isinstance(s, float) and s != s):
        return ""
    return "".join(unicodedata.normalize("NFKC", str(s)).split())


def _nv(x):
    """null-like (None, NaN, pd.NA) -> None."""
    return None if x is None or x is pd.NA or (isinstance(x, float) and x != x) else x


def load_release(release_dir: str, expect_version: Optional[str] = None, verify: bool = True) -> dict:
    """MANIFEST.json and the V2_FILES tables of a minister-data v2 snapshot. Every table's sha256 must
    equal its MANIFEST entry (verify=True) and the MANIFEST version must be `expect_version` when given.
    Tables are read as strings; only empty cells are null."""
    with open(os.path.join(release_dir, "MANIFEST.json"), encoding="utf-8") as fh:
        man = json.load(fh)
    if expect_version is not None and man.get("version") != expect_version:
        raise ValueError(f"{release_dir}: MANIFEST version {man.get('version')!r}, expected {expect_version!r}")
    files = man.get("files") or {}
    out = {"manifest": man}
    for f in V2_FILES:
        p = os.path.join(release_dir, f)
        if f not in files:
            raise ValueError(f"{release_dir}: {f} is not listed in MANIFEST.json")
        if verify:
            h = hashlib.sha256()
            with open(p, "rb") as fh:
                for b in iter(lambda: fh.read(1 << 20), b""):
                    h.update(b)
            if h.hexdigest() != files[f]["sha256"]:
                raise ValueError(f"{release_dir}: {f} sha256 differs from MANIFEST.json")
        df = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""])
        out[f[:-4]] = df.astype(object).where(df.notna(), None)
    return out


_Spell = namedtuple("_Spell", "spell_id person_id name lineage start end end_eff dual_start dual_end")
_Acting = namedtuple("_Acting", "names lo hi acting_id person_id name coverage")
V2Link = namedtuple("V2Link", "method spell_id nomination_id acting_id person_id lineage dual_office name detail")


def _v2_miss(method, lineage=None, detail=None) -> V2Link:
    return V2Link(method, None, None, None, None, lineage, None, None, detail)


class SpellIndex:
    """minister-data v2 release: person x lineage x date linking of government turns (panel v2)."""

    def __init__(self, release_dir: str, spell_buffer_days: int = SPELL_BUFFER_DAYS,
                 hearing_days: int = NOMINATION_HEARING_DAYS, expect_version: Optional[str] = None,
                 verify: bool = True):
        rel = load_release(release_dir, expect_version, verify)
        man = rel["manifest"]
        self.release_dir = release_dir
        self.version = man.get("version")
        self.cutoff = _to_date((man.get("window") or [None, None])[1])
        if self.cutoff is None:
            raise ValueError(f"{release_dir}: MANIFEST.json has no window end (release cutoff)")
        self.buffer = _dt.timedelta(days=spell_buffer_days)
        self.hearing_days = int(hearing_days)
        self.spells, self.by_name, self.by_person, self.hangul_by_key = {}, {}, {}, {}
        for r in rel["spells"].to_dict("records"):
            s, e = _to_date(r["spell_start"]), _to_date(r["spell_end"])
            if s is None or (e is not None and e < s):
                continue                      # none in v2.0.0; such a spell could never be matched
            rec = _Spell(r["spell_id"], r["person_id"], r["name"], r["lineage"], s, e, e or self.cutoff,
                         _to_date(r["dual_office_start"]), _to_date(r["dual_office_end"]))
            self.spells[rec.spell_id] = rec
            self.by_person.setdefault(rec.person_id, []).append(rec)
            for k in {_nk(r["name"]), _nk(r["name_hanja"])} - {""}:
                self.by_name.setdefault(k, []).append(rec)
                self.hangul_by_key.setdefault(k, set()).add(_nk(r["name"]))
        self.variants = {}   # nk(variant) -> [(person_id, nk(canonical name), lineage, lo, hi)]
        for r in rel["person_name_variants"].to_dict("records"):
            self.variants.setdefault(_nk(r["variant_string"]), []).append(
                (r["person_id"], _nk(r["name"]), r["lineage"], _to_date(r["valid_from"]) or _dt.date.min,
                 _to_date(r["valid_to"]) or _dt.date.max))
        self.alias = {}      # nk(alias) -> [(lo, hi, lineage, alias_type, person_id, is_vice_title)]
        for r in rel["ministry_alias"].to_dict("records"):
            k = _nk(r["alias_string"])
            vice = r["alias_type"] == "title_form" and bool(_V2_VICE_TITLE_RE.search(k))
            self.alias.setdefault(k, []).append((_to_date(r["valid_from"]) or _dt.date.min,
                                                 _to_date(r["valid_to"]) or _dt.date.max,
                                                 r["lineage"], r["alias_type"], r["person_id"], vice))
        self.noms, self.nominee_names = {}, set()   # (nk(nominee), lineage) -> [(hearing dates, id, spell_id, name)]
        for r in rel["nominations"].to_dict("records"):
            hd = tuple(sorted({x for x in (_to_date(p.strip()) for p in re.split(r"[;,|]", r["hearing_dates"] or ""))
                               if x is not None}))
            k = _nk(r["nominee"])
            self.noms.setdefault((k, r["lineage"]), []).append((hd, r["nomination_id"], r["spell_id"], r["nominee"]))
            self.nominee_names.add(k)
        self.acting = {}     # acting_for_lineage -> [_Acting]
        for r in rel["acting_heads"].to_dict("records"):
            lo = _to_date(r["from"])
            if lo is None:
                continue
            self.acting.setdefault(r["acting_for_lineage"], []).append(_Acting(
                frozenset({_nk(r["name"]), _nk(r["name_hanja"])} - {""}), lo, _to_date(r["to"]) or self.cutoff,
                r["acting_id"], r["person_id"], r["name"], r["coverage"]))

    # -- lineage ---------------------------------------------------------------------
    def resolve_lineage(self, title, ministry, d, role=None, person_ids=frozenset()) -> tuple:
        """-> (lineage or None, how). The candidates are the printed title, the title without a
        V2_TITLE_SUFFIXES suffix and ministry_normalized; the first candidate with an alias row valid on
        `d` (person-scoped rows only for `person_ids`, preferred over general rows) decides. how:
        'pm_title', 'alias:<alias_type>', 'vice_minister_title', 'out_of_scope', 'unresolved', 'no_date'."""
        t = _nk(title)
        if role == "prime_minister" or t.startswith(_V2_PM_PREFIXES):
            return "pm", "pm_title"
        d = _to_date(d)
        if d is None:
            return None, "no_date"
        cands = [t] + [t[:-len(s)] for s in V2_TITLE_SUFFIXES if t.endswith(s) and len(t) > len(s)] + [_nk(ministry)]
        for c in dict.fromkeys(x for x in cands if x):
            rows = [a for a in self.alias.get(c, ()) if a[0] <= d <= a[1] and (a[4] is None or a[4] in person_ids)]
            if not rows:
                continue
            lo, hi, lin, typ, pid, vice = sorted(rows, key=lambda a: a[4] is None)[0]
            if vice:
                return None, "vice_minister_title"
            if not lin or lin == "out_of_scope":
                return None, "out_of_scope"
            return lin, f"alias:{typ}"
        return None, "unresolved"

    def _person_ids(self, nkey, d) -> frozenset:
        return frozenset({s.person_id for s in self.by_name.get(nkey, ())}
                         | {v[0] for v in self.variants.get(nkey, ()) if v[3] <= d <= v[4]})

    def nominee_title_lineage(self, name, title, ministry, d) -> Optional[str]:
        """Lineage of a role-'nominee' turn whose title is a cabinet nominee title ('국무총리후보자'); None
        for other nominees (대법관후보자, 검찰총장후보자, ...), which are not linked."""
        d = _to_date(d)
        if d is None:
            return None
        return self.resolve_lineage(title, ministry, d, "nominee", self._person_ids(_nk(name), d))[0]

    def name_keys(self, nkey, lineage, d) -> set:
        """Hangul name keys of a printed name: itself, the Hangul name of spells whose Hanja name it is, and
        the canonical name of person_name_variants rows valid for `lineage` on `d`."""
        keys = {nkey} | self.hangul_by_key.get(nkey, set())
        keys |= {v[1] for v in self.variants.get(nkey, ()) if v[2] == lineage and v[3] <= d <= v[4]}
        return keys

    def _dual(self, s, d) -> bool:
        return s.dual_start is not None and s.dual_start <= d <= (s.dual_end or s.end_eff)

    # -- link --------------------------------------------------------------------------
    def link(self, name, title, ministry, d, role) -> V2Link:
        """One government turn -> V2Link. role: minister / prime_minister (spells; a prime_minister with a
        직무대행 / 직무대리 title: acting_heads for 'pm'), minister_acting (acting_heads of its lineage),
        minister_nominee / nominee (nominations). `name`, `title` as printed; `ministry` =
        ministry_normalized."""
        d = _to_date(d)
        if d is None:
            return _v2_miss("unlinked:no_date")
        nkey = _nk(name)
        if not nkey:
            return _v2_miss("unlinked:no_name")
        lin, how = self.resolve_lineage(title, ministry, d, role, self._person_ids(nkey, d))
        if role == "minister_acting" or (role == "prime_minister" and _V2_ACTING_TITLE_RE.search(_nk(title))):
            return self._link_acting(nkey, lin, how, d)
        if role in ("minister_nominee", "nominee"):
            return self._link_nomination(nkey, lin, how, d)
        return self._link_spell(nkey, lin, how, d)

    def _link_spell(self, nkey, lin, how, d) -> V2Link:
        direct = self.by_name.get(nkey, [])
        var = [v for v in self.variants.get(nkey, ()) if v[3] <= d <= v[4]]
        if not direct and not var:
            return _v2_miss("unlinked:name_not_in_panel", lin)
        if lin is None:
            return _v2_miss(_LINEAGE_MISS.get(how, "unlinked:lineage_unresolved"))
        cands = {s.spell_id: s for s in direct}
        cands.update({s.spell_id: s for v in var if v[2] == lin for s in self.by_person.get(v[0], ())})
        same = [s for s in cands.values() if s.lineage == lin]
        if not same:
            return _v2_miss("unlinked:person_in_other_lineage", lin,
                            ";".join(sorted(f"{s.spell_id}[{s.start}..{s.end or ''}]" for s in cands.values())))
        best = []
        for s in same:
            if s.start <= d <= s.end_eff:
                best.append((0, 0, -s.start.toordinal(), s))
            elif s.start - self.buffer <= d <= s.end_eff + self.buffer:
                best.append((1, (s.start - d).days if d < s.start else (d - s.end_eff).days, -s.start.toordinal(), s))
        if not best:
            near = min(same, key=lambda s: (s.start - d).days if d < s.start else (d - s.end_eff).days)
            days = (near.start - d).days if d < near.start else (d - near.end_eff).days
            return _v2_miss("unlinked:outside_spell", lin, f"{near.spell_id}[{near.start}..{near.end or ''}] {days}d")
        tier, _, _, s = min(best, key=lambda b: b[:3])
        return V2Link("spell:exact" if tier == 0 else "spell:buffer", s.spell_id, None, None, s.person_id, lin,
                      self._dual(s, d), s.name, None)

    def _link_nomination(self, nkey, lin, how, d) -> V2Link:
        keys = self.name_keys(nkey, lin, d)
        if not keys & self.nominee_names:
            return _v2_miss("unlinked:name_not_in_panel", lin)
        if lin is None:
            return _v2_miss(_LINEAGE_MISS.get(how, "unlinked:lineage_unresolved"))
        cands = [n for k in sorted(keys) for n in self.noms.get((k, lin), ())]
        if not cands:
            return _v2_miss("unlinked:person_in_other_lineage", lin)
        hits = []
        for hd, nid, sid, nominee in cands:
            dist = min((abs((d - h).days) for h in hd), default=None)
            if dist is not None and dist <= self.hearing_days:
                hits.append((dist, nid, sid, nominee))
        if not hits:
            return _v2_miss("unlinked:outside_hearing", lin,
                            ";".join(f"{nid}[{','.join(map(str, hd))}]" for hd, nid, _, _ in cands))
        _, nid, sid, nominee = min(hits, key=lambda h: (h[0], h[1]))
        s = self.spells.get(sid) if sid else None
        return V2Link("nomination:hearing", sid, nid, None, s.person_id if s else None, lin,
                      self._dual(s, d) if s else None, nominee, None)

    def _link_acting(self, nkey, lin, how, d) -> V2Link:
        if lin is None:
            return _v2_miss(_LINEAGE_MISS.get(how, "unlinked:lineage_unresolved"))
        keys = self.name_keys(nkey, lin, d)
        rows = [a for a in self.acting.get(lin, ()) if a.names & keys]
        if not rows:
            return _v2_miss("unlinked:not_in_acting_heads", lin)
        inside = [a for a in rows if a.lo <= d <= a.hi]
        if not inside:
            return _v2_miss("unlinked:outside_acting_period", lin,
                            ";".join(f"{a.acting_id}[{a.lo}..{a.hi}]" for a in rows))
        a = max(inside, key=lambda a: (a.lo, a.acting_id))
        return V2Link("acting_head:pm" if lin == "pm" else "acting_head:lineage", None, None, a.acting_id,
                      a.person_id, lin, None, a.name, a.coverage)


# ---------------------------------------------------------------------------------------
# 5. enrich
# ---------------------------------------------------------------------------------------
_BARE_ACTING_PM_RE = re.compile(r"^국무총리(?:직무대행|권한대행|직무대리)$")


@lru_cache(maxsize=10_000)
def is_bare_acting_pm(pos: Optional[str]) -> bool:
    """'국무총리직무대행' (or 권한대행 / 직무대리) printed without the speaker's own post: the
    speaker stands in for the PM (e.g. a deputy PM), so the turn is linked like an acting
    minister (never to a PM appointment). '국무총리서리' is not acting (the panel lists 서리)."""
    if pos is None or (isinstance(pos, float) and pos != pos):
        return False
    s = _SEP_RE.sub("", str(pos))
    if HANJA_RE.search(unicodedata.normalize("NFKC", s)):
        s = hanja_to_hangul(s)[0]
    s = _SEP_RE.sub("", unicodedata.normalize("NFKC", s))
    return bool(_BARE_ACTING_PM_RE.match(s))


# A nominee title that names no office ('公職候補者', printed in the PM confirmation hearings of the 16th
# Assembly) takes its office from the committee: in a PM confirmation hearing committee
# ('국무총리(이한동)임명동의에관한인사청문특별위원회') it is read as '국무총리후보자'. The nomination is still matched
# by name and hearing date; link_method 'nomination:committee_title' marks these links.
_OFFICELESS_NOMINEE_TITLES = frozenset({"公職候補者", "공직후보자"})
_PM_HEARING_COMMITTEE_RE = re.compile(r"^국무총리\(.+\)임명동의에관한인사청문특별위원회$")
COMMITTEE_PM_NOMINEE_TITLE = "국무총리후보자"


def _committee_titles(out: pd.DataFrame, meetings: Optional[pd.DataFrame]) -> np.ndarray:
    """Positional array: COMMITTEE_PM_NOMINEE_TITLE for a role-'nominee' turn with an office-less title
    in a PM confirmation hearing committee (meetings.committee_raw), else None (the printed title holds)."""
    res = np.full(len(out), None, dtype=object)
    if meetings is None or "committee_raw" not in meetings.columns or "conf_num" not in out.columns:
        return res
    m = out["role"].eq("nominee").fillna(False).to_numpy(dtype=bool) & np.fromiter(
        (_nk(x) in _OFFICELESS_NOMINEE_TITLES for x in out["speaker_pos"].to_numpy()), dtype=bool, count=len(out))
    if not m.any():
        return res
    comm = out["conf_num"].map(meetings.drop_duplicates("conf_num").set_index("conf_num")["committee_raw"])
    pm_comm = np.fromiter((bool(_PM_HEARING_COMMITTEE_RE.match(_nk(c))) for c in comm.to_numpy()), dtype=bool,
                          count=len(out))
    res[m & pm_comm] = COMMITTEE_PM_NOMINEE_TITLE
    return res


def _v2_nominee_scope(out: pd.DataFrame, idx: "SpellIndex", dkeys: pd.Series,
                      titles: Optional[np.ndarray] = None) -> np.ndarray:
    """Positional mask of role-'nominee' turns whose title is a cabinet nominee title on the date
    (SpellIndex.nominee_title_lineage, e.g. '국무총리후보자' -> pm). They are linked like
    minister_nominee; other nominees (대법관, 검찰총장, ...) keep link_method null. `titles`: positional
    replacement titles (None = the printed speaker_pos), see _committee_titles."""
    m = out["role"].isin(["nominee"]).to_numpy(dtype=bool)
    res = np.zeros(len(out), dtype=bool)
    pos = np.flatnonzero(m)
    if not len(pos):
        return res
    printed = out["speaker_pos"].to_numpy()
    if titles is not None:
        printed = np.where(pd.notna(titles), titles, printed)
    cols = [out["speaker_name"].to_numpy()[pos], printed[pos], out["ministry_normalized"].to_numpy()[pos]]
    cache = {}
    for j, t in zip(pos, zip(*cols, dkeys.to_numpy()[pos])):
        t = tuple(_nv(x) for x in t)
        if t not in cache:
            cache[t] = idx.nominee_title_lineage(*t) is not None
        res[j] = cache[t]
    return res


def _role_group(role) -> Optional[str]:
    if role is None or (isinstance(role, float) and role != role):
        return None
    return "legislator" if role in LEG_ROLES else "other"


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _iso_date_key(k) -> Optional[str]:
    """k when it is a valid 'YYYY-MM-DD' date, else None."""
    if not isinstance(k, str) or not _ISO_DATE_RE.match(k):
        return None
    try:
        _dt.date.fromisoformat(k)
    except ValueError:
        return None
    return k


def _bool_mask(s) -> np.ndarray:
    """Nullable bool column -> numpy bool (null = False)."""
    return pd.array(s, dtype="boolean").fillna(False).to_numpy(dtype=bool)


def enrich(turns: pd.DataFrame, meetings: Optional[pd.DataFrame] = None, *,
           panel_index=None, calendar: Optional[pd.DataFrame] = None,
           suspended_admin: str = "president") -> pd.DataFrame:
    """Add government-side columns to `turns` (same rows, same order).

    panel_index: a SpellIndex (panel v2) or a PanelIndex (legacy_296); None = _default_index()
    (config.yaml switchable.government.panel). Both panels add the same columns (ADDED_COLUMNS).
    Requires turn columns `speaker_pos`, `speaker_name`, `speech_date`, `role` (roles.py).
    Date choice (the same as party_timeline.enrich): speech_date when it is 10 characters long,
    else the meeting date, taken from a `date` column of `turns` (CONTRACT: joined from meetings)
    or else from `meetings` (conf_num, date). A chosen date that is not a valid 'YYYY-MM-DD'
    date gets gov_date_source 'unparseable_<source>' and null admin (counted, never guessed).
    Diagnostics are returned in out.attrs['government'] (counts of every fallback and failure).
    The input frame is not modified (shallow copy: text columns are not duplicated)."""
    missing = [c for c in ("speaker_pos", "speaker_name", "speech_date", "role") if c not in turns.columns]
    if missing:
        raise KeyError(f"government.enrich needs columns {missing} (role comes from roles.py)")
    idx = panel_index if panel_index is not None else _default_index()
    cal = load_calendar() if calendar is None else calendar
    out = turns.copy(deep=False)
    n = len(out)
    diag = {"n_turns": n}

    # dates (positional numpy arrays: any index, including duplicates)
    sd = out["speech_date"].astype("string")
    has_sd = (sd.str.len() == 10).fillna(False).to_numpy(dtype=bool)
    keys = np.full(n, None, dtype=object)
    src = np.full(n, None, dtype=object)
    keys[has_sd] = sd.to_numpy(dtype=object)[has_sd]
    src[has_sd] = "speech_date"
    need = ~has_sd
    fallback = {"turns.date": 0, "meetings.date": 0}
    if "date" in out.columns:
        td = out["date"]
        f = need & td.notna().to_numpy(dtype=bool)
        keys[f] = td.astype(str).str[:10].to_numpy(dtype=object)[f]
        src[f] = "meeting_date"
        fallback["turns.date"] = int(f.sum())
        need = need & ~f
    if meetings is not None and "date" in meetings.columns and "conf_num" in out.columns and need.any():
        mdate = out["conf_num"].map(meetings.drop_duplicates("conf_num").set_index("conf_num")["date"])
        f = need & mdate.notna().to_numpy(dtype=bool)
        keys[f] = mdate.astype(str).str[:10].to_numpy(dtype=object)[f]
        src[f] = "meeting_date"
        fallback["meetings.date"] = int(f.sum())
        need = need & ~f
    ok_key = {k: _iso_date_key(k) for k in pd.unique(keys[keys != None])}   # noqa: E711
    bad = np.array([k is not None and ok_key[k] is None for k in keys], dtype=bool)
    src[bad] = np.array(["unparseable_" + x for x in src[bad]], dtype=object)
    keys[bad] = None
    out["gov_date_source"] = pd.array(src, dtype="string")
    diag["date_source"] = out["gov_date_source"].value_counts(dropna=False).to_dict()
    diag["date_fallback"] = fallback

    # admin by date
    dkeys = pd.Series(pd.array(keys, dtype="string"), index=out.index)
    amap = {k: admin_for_date(k, cal, suspended_admin) for k in pd.unique(keys[keys != None])}  # noqa: E711
    trip = [amap[k] if k is not None else (None, None, None) for k in keys]
    out["admin"] = pd.array([t[0] for t in trip], dtype="string")
    out["admin_ideology"] = pd.array([t[1] for t in trip], dtype="string")
    pstate = pd.Series(pd.array([t[2] for t in trip], dtype="string"), index=out.index)
    has_key = np.array([k is not None for k in keys], dtype=bool)
    st_null = np.array([t[2] is None for t in trip], dtype=bool)
    adm_null = out["admin"].isna().to_numpy()
    diag["admin_null"] = {
        "no_date": int((adm_null & ~has_key & ~bad).sum()),
        "unparseable_date": int((adm_null & bad).sum()),
        "outside_calendar": int((adm_null & has_key & st_null).sum()),
    }
    if "presidency_state" in out.columns:
        given = out["presidency_state"].astype("string")
        both = given.notna() | pstate.notna()
        diag["presidency_state_disagreements"] = int((both & (given.fillna("<NA>") != pstate.fillna("<NA>"))).sum())
    else:
        out["presidency_state"] = pstate
        diag["presidency_state_disagreements"] = None

    # ministry (a null role_group is not 'legislator')
    rg = out["role_group"] if "role_group" in out.columns else out["role"].map(_role_group)
    is_leg = (rg.astype(object).eq("legislator").fillna(False).astype(bool)
              | out["role"].isin(LEG_ROLES)).to_numpy(dtype=bool)
    posv = np.array([p if isinstance(p, str) else None for p in out["speaker_pos"].to_numpy(dtype=object)], dtype=object)
    # a title repaired to the meeting majority (roles.py) is normalised from the repaired title
    if "label_repaired" in out.columns and "label_meeting_majority" in out.columns:
        rep = _bool_mask(out["label_repaired"])
        maj = out["label_meeting_majority"].to_numpy(dtype=object)
        ok = rep & np.array([isinstance(x, str) and x != "" for x in maj], dtype=bool)
        posv[ok] = maj[ok]
        diag["ministry_from_repaired_label"] = int(ok.sum())
    upos = pd.unique(posv[~is_leg & (posv != None)])   # noqa: E711
    nm = {p: normalize_ministry(p) for p in upos}
    mn = np.array([nm[p][0] if p in nm else None for p in posv], dtype=object)
    mr = np.array([nm[p][1] if p in nm else "empty" for p in posv], dtype=object)
    mn[is_leg] = None
    mr[is_leg] = "legislator"
    out["ministry_normalized"] = pd.array(mn, dtype="string")
    out["ministry_family"] = pd.array([ministry_family(m) if m is not None else None for m in mn], dtype="string")
    out["ministry_rule"] = pd.array(mr, dtype="string")

    # panel linkage (positional assignment: works with any index, including duplicates)
    v2 = isinstance(idx, SpellIndex)
    diag["panel"] = "v2" if v2 else "legacy_296"
    diag["panel_release"] = idx.version if v2 else os.path.basename(LEGACY_PANEL_PATH)
    link_mask = out["role"].isin(LINK_ROLES).fillna(False).to_numpy(dtype=bool)
    ctitle = _committee_titles(out, meetings) if v2 else np.full(n, None, dtype=object)
    if v2:
        nom_scope = _v2_nominee_scope(out, idx, dkeys, ctitle)
        diag["nominee_cabinet_title_turns"] = int(nom_scope.sum())
        diag["nominee_committee_title_turns"] = int(pd.notna(ctitle).sum())
        link_mask = link_mask | nom_scope
    pid = np.full(n, None, dtype=object)
    dual = np.full(n, None, dtype=object)
    meth = np.full(n, None, dtype=object)
    lname = np.full(n, None, dtype=object)
    v2cols = {c: np.full(n, None, dtype=object) for c in V2_LINK_COLUMNS}
    # never linked: label inconsistent with the person's majority title in the meeting, or a label the
    # parser rates low (link_method says why; the role column is unchanged)
    incons = _bool_mask(out["label_inconsistent_in_meeting"]) if "label_inconsistent_in_meeting" in out.columns \
        else np.zeros(n, dtype=bool)
    lowc = (out["label_confidence"].astype("string").eq("low").fillna(False).to_numpy(dtype=bool)
            if "label_confidence" in out.columns else np.zeros(n, dtype=bool))
    blocked_low = link_mask & lowc
    blocked_inc = link_mask & incons & ~lowc
    # a former official printed with a '(전)' / '(前)' title ('(전)환경부장관') is a witness about a past
    # office, never the office holder on the speech date (2026-09-28, minister-data adjudication)
    former = np.fromiter((bool(FORMER_TITLE_RE.match(x)) if isinstance(x, str) else False
                          for x in out["speaker_pos"].to_numpy()), dtype=bool, count=n) \
        if "speaker_pos" in out.columns else np.zeros(n, dtype=bool)
    blocked_former = link_mask & former & ~blocked_low & ~blocked_inc
    meth[blocked_low] = "unlinked:label_confidence_low"
    meth[blocked_inc] = "unlinked:label_inconsistent_in_meeting"
    meth[blocked_former] = "unlinked:former_title"
    diag["link_blocked"] = {"label_confidence_low": int(blocked_low.sum()),
                            "label_inconsistent_in_meeting": int(blocked_inc.sum()),
                            "former_title": int(blocked_former.sum())}
    posn = np.flatnonzero(link_mask & ~blocked_low & ~blocked_inc & ~blocked_former)
    if v2 and len(posn):
        names = out["speaker_name"].to_numpy()[posn]
        poss = np.where(pd.notna(ctitle[posn]), ctitle[posn], out["speaker_pos"].to_numpy()[posn])
        mins = out["ministry_normalized"].to_numpy()[posn]
        dks = dkeys.to_numpy()[posn]
        rls = out["role"].to_numpy()[posn]
        cache = {}
        for j, t in zip(posn, zip(names, poss, mins, dks, rls)):
            t = tuple(_nv(x) for x in t)
            r = cache.get(t)
            if r is None:
                r = cache[t] = idx.link(*t)
            pid[j] = r.spell_id or r.acting_id
            dual[j], meth[j], lname[j] = r.dual_office, r.method, r.name
            if ctitle[j] is not None and r.method == "nomination:hearing":
                meth[j] = "nomination:committee_title"
            v2cols["minister_spell_id"][j], v2cols["minister_nomination_id"][j] = r.spell_id, r.nomination_id
            v2cols["minister_acting_id"][j], v2cols["minister_person_id"][j] = r.acting_id, r.person_id
            v2cols["minister_lineage"][j] = r.lineage
    elif len(posn):
        def _v(x):
            return None if x is None or (isinstance(x, float) and x != x) or x is pd.NA else x
        names = out["speaker_name"].to_numpy()[posn]
        mins = out["ministry_normalized"].to_numpy()[posn]
        dks = dkeys.to_numpy()[posn]
        rls = out["role"].to_numpy()[posn].copy()
        poss = out["speaker_pos"].to_numpy()[posn]
        bare = np.array([r == "prime_minister" and is_bare_acting_pm(p) for r, p in zip(rls, poss)], dtype=bool)
        rls[bare] = "minister_acting"          # link semantics only; the role column is unchanged
        diag["prime_minister_bare_acting_linked_as_acting"] = int(bare.sum())
        cache = {}
        hows = {}
        for j, t in zip(posn, zip(names, mins, dks, rls)):
            t = tuple(_v(x) for x in t)
            if t not in cache:
                hname, how = idx.resolve_name(t[0])
                c, lab = idx.link(hname, t[1], t[2], t[3])
                cache[t] = (c.minister_panel_id if c is not None else None,
                            bool(c.dual_office) if c is not None else None, lab, hname, how)
            r = cache[t]
            pid[j], dual[j], meth[j], lname[j] = r[0], r[1], r[2], r[3]
            hows[r[4]] = hows.get(r[4], 0) + 1
        diag["link_name_resolution"] = hows
    out["minister_panel_id"] = pd.array(pid, dtype="string")
    out["dual_office"] = pd.array(dual, dtype="boolean")
    out["link_method"] = pd.array(meth, dtype="string")
    out["gov_link_name"] = pd.array(lname, dtype="string")
    for c in V2_LINK_COLUMNS:                     # null in legacy_296 (stable schema across panels)
        out[c] = pd.array(v2cols[c], dtype="string")
    meth_s = out["link_method"][link_mask]
    diag["link_method"] = meth_s.value_counts(dropna=False).to_dict()
    diag["ministry_rule"] = out["ministry_rule"].value_counts(dropna=False).to_dict()
    diag["suspended_admin"] = suspended_admin
    out.attrs["government"] = diag
    assert len(out) == n
    return out
