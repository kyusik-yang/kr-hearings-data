"""Legislator-witness dyads for v10 (docs/CODEBOOK.md, section "5. dyads").

A dyad is one pair of NUMERICALLY adjacent speaker turns (turn_seq i, i+1) inside one
meeting where one turn has role_group='legislator' and the other role_group='nonlegislator'.
Turns with role_group='excluded' (committee staff, other, unknown) stay in the sequence
and therefore break adjacency (v5 semantics, build_v5.py:416-417). A turn whose
role_group is null is treated the same way as 'excluded' and is counted.
Sittings (CONTRACT turn column sitting_seq): the two turns of a dyad must have the same non-null
sitting_seq, so a meeting document that holds several sittings is never paired across a sitting
boundary (pairs blocked this way are counted: n_pairs_blocked_by_sitting). A null sitting_seq breaks
adjacency like a null role_group (counted). When the turns have no sitting_seq column at all, every
meeting is one sitting and the stats say so (sitting_seq_missing).
after_end_marker (turn printed after a meeting-end marker): carried as leg_/wit_after_end_marker.
With exclude_after_end_marker=True (a parameter, default False = keep, config.yaml) such turns break
adjacency like 'excluded' (pairs blocked: n_pairs_blocked_by_after_end).

    build_dyads(turns_df, meetings_df)          -> DataFrame   (in memory, via duckdb)
    build_dyads_file(turns_parquet, out_path, meetings_path)
                                                -> stats dict  (parquet in, parquet out, streamed in
                                                   conf_num chunks of at most chunk_turns turns)
    dyad_sql(...)                               -> the single SQL used by both
    legacy_v9_dyads(v9_speeches_df)             -> DataFrame   (bit-for-bit v9 string-sort dyads)
    build_legacy_v9_dyads_file(...)             -> stats dict
    verify_legacy_v9(n_meetings=100)            -> dict        (vs data/dyads_16_22_v9.parquet)

Output columns (slim release layout, researcher decision 8 of 2026-09-26; SLIM_LAYOUT below, in this order):
    conf_num, term, date (meeting date), hearing_type, class_name, committee_key, is_subcommittee,
    sitting_seq, leg_turn_seq, wit_turn_seq,
    direction ('question' when the legislator turn comes first, else 'answer'),
    speech_date, leg_naas_cd, leg_name, leg_role, leg_is_chair, leg_party, leg_party_camp,
    leg_ruling_status, presidency_state, leg_seniority, leg_gender,
    wit_name, wit_role, wit_role_group, wit_title_raw, wit_ministry_normalized, wit_minister_panel_id,
    wit_dual_office, admin, admin_ideology, leg_text, wit_text, leg_is_procedural,
    wit_is_legislator_title, any_after_end_marker, any_low_label_confidence, any_time_regress,
    any_label_inconsistent.
  Meeting columns come from the meetings table. sitting_seq is the sitting of both turns (never differs).
  speech_date and the date-derived columns presidency_state, admin, admin_ideology are those of the
  legislator turn. leg_name = the linked member's Hangul name (turn leg_name_hangul), else the printed
  speaker_name; wit_name = the printed speaker_name of the witness turn. leg_text / wit_text = turn `text`.
  any_* = true when either turn has the flag (after_end_marker, label_confidence = 'low', time_regress,
  the roles label-consistency flag); a turn whose value is null counts as false; the column is null only
  when the turns lack the source column altogether (counted in the stats: slim_sources_missing).
  Every other turn attribute (text_raw, party_lineage, affiliation_raw, ...) is joined from turns on
  (conf_num, turn_seq) = (conf_num, leg_turn_seq) or (conf_num, wit_turn_seq). No column is copied twice.
  `extra_turn_cols` / `extra_meeting_cols` (internal samples only, never in the release) add
  leg_<c>/wit_<c> copies or unprefixed meeting columns.

Flags
- leg_is_chair: leg_role = 'chair' when a `role` column exists (roles.py, v9 taxonomy);
  otherwise the position regex CHAIR_POS_RE on speaker_pos.
- leg_is_procedural: the legislator-side spoken text (`text`, stage parentheticals removed)
  is at most PROCEDURAL_MAX_CHARS characters and EVERY sentence matches one of the
  procedural formulas in PROCEDURAL_PATTERNS (recognition of the next questioner, thanks
  and closing, time management, answer requests, session and vote formulas, vocatives).
  Turns made only of fillers ('예.', '네, 알겠습니다.') count only when the legislator
  side is the chair (a questioning member's '예.' is a back-channel, not floor management).
- wit_is_legislator_title: the witness-side position is a legislator title
  (위원, 의원, 위원장, 소위원장, 의장, 부의장, 간사, Hanja 委員/議員/議長...). Sanity flag.

Meeting-level columns come from the meetings table (joined on conf_num; a dyad whose meeting has no
meetings row keeps nulls and is counted in n_dyads_meeting_missing). When no meetings table is given,
the meeting-level columns are taken from the legislator turn when the turns carry them, else they are
null, and the missing ones are listed in the stats (meeting_cols_missing); the CLI reads
interim/pipeline/meetings/meetings.parquet by default.

No per-row Python loops: pairing is a hash self-join on (conf_num, turn_seq + 1) in duckdb. The file
builder never holds the corpus in one query: each conf_num chunk is copied to a temporary table, paired,
sorted (conf_num, first turn) and streamed to the output parquet, so memory stays bounded.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[3]
V10 = REPO / "v10"
OUT_DIR = V10 / "interim" / "pipeline" / "dyads"
V9_SPEECHES = REPO / "data" / "all_speeches_16_22_v9.parquet"
V9_DYADS = REPO / "data" / "dyads_16_22_v9.parquet"
SEED = 8374

ROLE_GROUPS = ("legislator", "nonlegislator", "excluded")

# Turn columns that may hold the roles label-consistency flag (same person printed with a title that
# disagrees with their majority title in the meeting). The first one present
# feeds any_label_inconsistent; none present -> the dyad column is null (counted).
LABEL_INCONSISTENT_COLS = ("label_inconsistent", "speaker_label_inconsistent", "label_inconsistent_in_meeting",
                           "title_inconsistent", "label_title_inconsistent")

# Slim release layout (researcher decision 8, 2026-09-26). (output column, side, source):
#   'P' pair key / direction, 'M' meetings row (else the legislator turn when the turns carry it),
#   'L' legislator turn, 'W' witness turn, 'F' flag computed here,
#   'A' true when either turn is true (source = column name, or 'label_confidence' for label_confidence = 'low',
#       or a tuple of candidate columns, the first present is used).
#   A tuple source on 'L'/'W' = coalesce of the columns present, in order.
SLIM_LAYOUT = (
    ("conf_num", "P", None),
    ("term", "M", "term"),
    ("date", "M", "date"),
    ("hearing_type", "M", "hearing_type"),
    ("class_name", "M", "class_name"),
    ("committee_key", "M", "committee_key"),
    ("is_subcommittee", "M", "is_subcommittee"),
    ("sitting_seq", "L", "sitting_seq"),
    ("leg_turn_seq", "P", None),
    ("wit_turn_seq", "P", None),
    ("direction", "P", None),
    ("speech_date", "L", "speech_date"),
    ("leg_naas_cd", "L", "naas_cd"),
    ("leg_name", "L", ("leg_name_hangul", "speaker_name")),
    ("leg_role", "L", "role"),
    ("leg_is_chair", "F", None),
    ("leg_party", "L", "party"),
    ("leg_party_camp", "L", "party_camp"),
    ("leg_ruling_status", "L", "ruling_status"),
    ("presidency_state", "L", "presidency_state"),
    ("leg_seniority", "L", "seniority"),
    ("leg_gender", "L", "gender"),
    ("wit_name", "W", "speaker_name"),
    ("wit_role", "W", "role"),
    ("wit_role_group", "W", "role_group"),
    ("wit_title_raw", "W", "title_raw"),
    ("wit_ministry_normalized", "W", "ministry_normalized"),
    ("wit_minister_panel_id", "W", "minister_panel_id"),
    ("wit_dual_office", "W", "dual_office"),
    ("admin", "L", "admin"),
    ("admin_ideology", "L", "admin_ideology"),
    ("leg_text", "L", "text"),
    ("wit_text", "W", "text"),
    ("leg_is_procedural", "F", None),
    ("wit_is_legislator_title", "F", None),
    ("any_after_end_marker", "A", "after_end_marker"),
    ("any_low_label_confidence", "A", "label_confidence"),
    ("any_time_regress", "A", "time_regress"),
    ("any_label_inconsistent", "A", LABEL_INCONSISTENT_COLS),
)
SLIM_COLUMNS = tuple(c for c, _, _ in SLIM_LAYOUT)
# SQL type of each slim column when its source is absent (the column is then all null)
SLIM_NULL_TYPES = {
    "term": "SMALLINT", "date": "VARCHAR", "hearing_type": "VARCHAR", "class_name": "VARCHAR",
    "committee_key": "VARCHAR", "is_subcommittee": "BOOLEAN", "sitting_seq": "SMALLINT", "speech_date": "VARCHAR",
    "leg_naas_cd": "VARCHAR", "leg_name": "VARCHAR", "leg_role": "VARCHAR", "leg_party": "VARCHAR",
    "leg_party_camp": "VARCHAR", "leg_ruling_status": "VARCHAR", "presidency_state": "VARCHAR",
    "leg_seniority": "SMALLINT", "leg_gender": "VARCHAR", "wit_name": "VARCHAR", "wit_role": "VARCHAR",
    "wit_role_group": "VARCHAR", "wit_title_raw": "VARCHAR", "wit_ministry_normalized": "VARCHAR",
    "wit_minister_panel_id": "VARCHAR", "wit_dual_office": "BOOLEAN", "admin": "VARCHAR", "admin_ideology": "VARCHAR",
    "leg_text": "VARCHAR", "wit_text": "VARCHAR", "any_after_end_marker": "BOOLEAN",
    "any_low_label_confidence": "BOOLEAN", "any_time_regress": "BOOLEAN", "any_label_inconsistent": "BOOLEAN",
}
ANY_FLAGS = tuple(c for c, s, _ in SLIM_LAYOUT if s == "A")
# meeting-level columns of the slim layout (joined from `meetings`; carried once, unprefixed)
MEETING_LEVEL_COLS = tuple(c for c, s, _ in SLIM_LAYOUT if s == "M")

# ---------------------------------------------------------------- flag regexes (RE2 syntax)
# Everything below is used inside duckdb (RE2). Keep to the common RE2/Python subset.
CHAIR_POS_RE = (r"(부?의장|의장직무대행|(소|분과)?위원장(대리|직무대행|직무대리)?|조정위원장|반장"
                r"|副?議長|委員長)")
LEG_TITLE_POS_RE = (r"(부?의장|(소|분과)?위원장(대리|직무대행|직무대리)?|조정위원장|위원|의원|간사|반장"
                    r"|副?議長|委員長|委員|議員)")
# label fallback when speaker_pos is null: '위원장 홍길동', '소위원장 홍길동', '홍길동 위원', '宋榮珍議員'
LEG_TITLE_LABEL_RE = (r"((부?의장|(소|분과)?위원장(대리|직무대행)?|위원|의원|간사)\s+[가-힣]{2,4}"
                      r"|[가-힣㐀-䶿一-鿿]{2,4}\s*(\([^)]{1,3}\))?\s*(위원|의원|委員|議員|委員長|議長))")

_ADDR = (r"(위원|의원|위원장|소위원장|부의장|의장|간사|장관|차관|차관보|청장|처장|원장|실장|국장|총장|총리|부총리"
         r"|증인|참고인|진술인|공술인|후보자|사장|이사장|대변인|총재|관장|본부장|지사|시장|교육감|사무총장|대표|회장"
         r"|은행장|단장|소장|위원들|선배님|교수|박사|선생|변호사)")
_H = r"[가-힣㐀-䶿一-鿿·ㆍ()\s]"                     # name / organisation characters
_TOK = r"[가-힣㐀-䶿一-鿿·ㆍ()]"
_ADDRESSEE = (r"((" + _TOK + r"+[\s,]+){0,3}" + _TOK + r"*?" + _ADDR + r"\s*(님|님들)?|여러분|위원님들|의원님들)")
# officials only (a report is requested from officials or committee staff, never from members)
_ADDR_OFFICIAL = (r"(위원장|전문위원|수석전문위원|장관|차관|차관보|청장|처장|원장|실장|국장|총장|총리|부총리"
                  r"|증인|참고인|진술인|공술인|후보자|사장|이사장|대변인|총재|관장|본부장|지사|시장|교육감|사무총장|대표|회장"
                  r"|은행장|단장|소장|교수|박사)")
_INTJ = r"((예|네|자|그럼|그러면|좋습니다|알겠습니다|잘\s*알겠습니다)[,\s]*)?"
_CONN = r"((그러면|그럼|이어서|이번에는|계속해서|먼저|다음(은|에는|에|으로|으로는)?|마지막으로|끝으로|이제|수고하셨습니다)[,\s]*)*"
_REQ = (r"(해\s*주시기\s*바랍니다|해\s*주십시오|하여\s*주십시오|하여\s*주시기\s*바랍니다|해\s*주시겠습니다"
        r"|하시기\s*바랍니다|하십시오|하세요|하시겠습니다|해\s*주세요|해\s*주시죠|해\s*주시지요|하시죠|하시지요"
        r"|해\s*보세요|해\s*보시죠|해\s*보시지요|해\s*보십시오|시작하겠습니다|시작하도록\s*하겠습니다"
        r"|부탁드립니다|부탁합니다|하십시다)")
_THANKS = (r"(수고\s*(많이\s*|들\s*)?(하셨습니다|하셨고요|하셨어요|하셨는데요|했습니다|하셨습니다만)"
           r"|수고\s*많으셨습니다|수고\s*많았습니다|감사합니다|감사드립니다|고맙습니다|고생\s*(하셨습니다|많으셨습니다))")
_ACT = (r"(답변|대답|말씀|질의|발언|설명|보고|신문|질의\s*계속|계속|자료\s*요청|자료\s*제출\s*요청|인사|인사말씀"
        r"|선서|제안설명|현황보고|업무보고|보충질의|추가질의|질문|진술|의견\s*진술|의견을\s*진술)")
_ADV = r"((간단히|간단하게|짧게|먼저|계속|빨리|바로|이어서|서면으로|요약해서|간략히|간략하게|핵심만|나오셔서|마이크\s*앞에\s*나오셔서|앞에\s*나오셔서)\s*)*"

# (name, pattern, is_filler). Each is matched with regexp_full_match on one trimmed sentence
# (sentences split on . ? ! … and newlines, parentheticals removed).
PROCEDURAL_PATTERNS: tuple = (
    # thanks / closing: '박상은 위원님께서 수고하셨습니다', '수고 많으셨습니다', '국세청장님 그동안 수고 많으셨습니다'
    ("thanks", _INTJ + r"((존경하는\s*)?" + _ADDRESSEE + r"\s*(께서|께|도|들)?[,\s]*)?"
     r"((정말|대단히|아주|모두|그동안|오늘)\s*)*" + _THANKS, False),
    # recognition of the next questioner: '다음은 존경하는 한나라당 김옥이 위원님 질의하시겠습니다'
    ("next_speaker", _INTJ + _CONN + r"[^.?!]{0,60}?(위원|의원|부의장|간사)\s*(님)?\s*(께서|이|가|부터|의)?[,\s]*(나오셔서\s*)?"
     r"((주\s*|보충\s*|추가\s*)?(질의|질문|신문|발언|의사진행\s*발언|말씀|질의\s*순서)\s*(를|을|가|이)?\s*)?"
     r"(" + _REQ + r"|입니다|순서입니다|차례입니다|이십니다|질의하시겠습니다|질의하겠습니다|말씀하시겠습니다)", False),
    # a witness called to report / greet: '다음은 민갑룡 경찰청장님 인사해 주시기 바랍니다'
    ("call_report", _INTJ + _CONN + r"[^.?!]{0,60}?" + _ADDR_OFFICIAL + r"(직무대행|직무대리|대행|대리)?(님)?(께서|은|는|이|가)?([,\s]+)(나오셔서\s*)?[^.?!]{0,40}?"
     r"(인사|인사말씀|보고|현황보고|업무보고|제안설명|업무현황\s*보고|간부\s*소개|선서)\s*(를|을)?\s*"
     r"(해|하여)?\s*(주시기\s*바랍니다|주십시오|주세요|하시기\s*바랍니다)", False),
    # vocative call: '이용섭 위원님!', '국토부장관님', '한국공항공사 손창완 사장님'
    ("vocative", _INTJ + r"(존경하는\s*)?" + _ADDRESSEE + r"\s*!*", False),
    # request to answer / proceed: '장관님 답변해 주십시오', '질의 계속하십시오', '말씀하세요'
    ("proceed", _INTJ + r"(" + _ADDRESSEE + r"\s*(께서|은|는|이|가)?[,\s]*)?" + _ADV + _ACT + r"\s*(을|를)?\s*" + _REQ, False),
    # time management
    ("time", _INTJ + r"((질의\s*)?시간이\s*(다\s*)?(됐|되었|끝났|지났|초과되었|초과됐)습니다"
     r"|(질의\s*)?시간\s*(을|을\s*좀)?\s*(엄수|지켜)\s*" + _REQ +
     r"|(이제\s*)?(질의를?\s*|말씀을?\s*|질문을?\s*)?(마무리|정리)\s*(를|을)?\s*" + _REQ +
     r"|[0-9]+\s*(분|초)\s*(만|더|간)?\s*(드리겠습니다|드릴게요|드립니다|더\s*드리겠습니다)"
     r"|(추가|보충)\s*질의\s*(를|을|시간을)?\s*(활용해|이용해)\s*(주시기\s*바랍니다|주십시오|주시고요|주세요)"
     r"|(질의|답변|말씀)?\s*(을|를)?\s*(다\s*)?(마치셨습니까|끝나셨습니까|마치셨어요|끝났습니까|마치셨지요))", False),
    # session formulas
    ("session", r".{0,80}(개의|산회|정회|속개|개회|폐회|회의\s*중지|감사\s*중지|감사\s*종료|감사\s*개시|감사\s*계속|조사\s*중지|조사\s*계속)\s*(를|을)?\s*선포합니다", False),
    ("session_move", r"((그러면|그럼|이상으로|이것으로|다음은|계속해서|지금부터|수고하셨습니다)[,\s]*)*[^.?!]{0,40}?"
     r"(회의|감사|질의|질의와\s*답변|질의\s*답변|신문|조사|토론|심사)\s*(를|을)\s*"
     r"(계속|속개|시작|마치|종료|진행)\s*(하겠습니다|하도록\s*하겠습니다|합니다|토록\s*하겠습니다)", False),
    ("session_pause", r"[^.?!]{0,60}?(감사|회의|질의|조사)\s*(를|을)?\s*(잠시\s*)?(중지|정회)\s*(하였다가|했다가|하고)[^.?!]{0,40}?"
     r"(계속|속개)\s*(하도록\s*하겠습니다|하겠습니다)", False),
    ("hear_report", r"[^.?!]{0,40}?(보고|설명|제안설명|의견|답변)\s*(를|을)\s*(듣도록|듣겠습니다|듣기로|받도록|받겠습니다)\s*(하겠습니다)?", False),
    ("dismiss", r"[^.?!]{0,30}?((이석|퇴장|귀가)\s*(하셔도|하셔도\s*좋고)|돌아가셔도|들어가셔도)\s*(되겠습니다|좋습니다|됩니다)"
     r"|(자리에\s*)?(앉아|착석해)\s*(주십시오|주시기\s*바랍니다|주세요)", False),
    ("seats", r"(의석을|장내를|감사장을|회의장을)\s*정돈(해\s*주시기\s*바랍니다|해\s*주십시오|하여\s*주시기\s*바랍니다)", False),
    ("agenda_item", r"(그러면\s*)?(의사일정\s*)?제\s*[0-9]+\s*항[^?!]{0,300}(상정합니다|상정하겠습니다|일괄\s*상정합니다|상정하도록\s*하겠습니다)", False),
    ("vote_result", r".{0,120}(가결|부결|의결|채택|승인)\s*(되었음을|됐음을|되었습니다)\s*(선포합니다)?", False),
    ("objection", r"((다른\s*)?(이의|의견|질의)\s*(없으십니까|없으시지요|없으시죠|있으십니까|없습니까|없으신가요|있으신가요)"
     r"|(「|｢)?없습니다(」|｣)?\s*하는\s*위원\s*있음"
     r"|(더\s*)?(질의|보충질의|추가질의|신문|발언)\s*하실\s*(분|위원|위원님)(\s*(계십니까|있으십니까|없으십니까))?)", False),
    # fillers (count only for the chair)
    ("filler", r"((예|네)[,\s]*)?(예|네|자|예\s*예|네\s*네|그래요|좋습니다|됐습니다|(잘\s*)?알겠습니다|그러세요|그렇게\s*하세요|그렇게\s*하십시오"
     r"|그렇게\s*해\s*주시지요|그렇게\s*해\s*주십시오|그렇게\s*하시지요|하십시오|하세요|잠깐만요|잠깐만|잠시만요|잠깐"
     r"|미안합니다|죄송합니다|뭐라고요)", True),
)
PROCEDURAL_MAX_CHARS = 400


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _sentences_sql(text_expr: str) -> str:
    """duckdb list of trimmed, non-empty sentences of `text_expr` with parentheticals removed."""
    cleaned = f"regexp_replace(coalesce({text_expr}, ''), '\\([^()]*\\)', ' ', 'g')"
    split = f"regexp_split_to_array({cleaned}, '[.?!…]+|\\n')"
    return (f"list_filter(list_transform({split}, lambda s: trim(s)), "
            f"lambda s: s <> '' AND NOT regexp_full_match(s, '[\\s,·ㆍ\"“”‘’]*'))")


def procedural_sql_reference(text_expr: str, is_chair_expr: str) -> str:
    """Reference (slow, direct) SQL for leg_is_procedural. Kept for the equivalence test of
    the fast form (procedural_codes_sql + procedural_from_codes_sql)."""
    sents = _sentences_sql(text_expr)
    any_proc = " OR ".join(f"regexp_full_match(s, {_sql_str(p)})" for _, p, _f in PROCEDURAL_PATTERNS)
    nonfill = " OR ".join(f"regexp_full_match(s, {_sql_str(p)})" for _, p, f in PROCEDURAL_PATTERNS if not f)
    return (
        f"(CASE WHEN {text_expr} IS NULL OR length({text_expr}) > {PROCEDURAL_MAX_CHARS} THEN false "
        f"WHEN len({sents}) = 0 THEN false "
        f"WHEN NOT list_bool_and(list_transform({sents}, lambda s: {any_proc})) THEN false "
        f"WHEN list_bool_or(list_transform({sents}, lambda s: {nonfill})) THEN true "
        f"ELSE coalesce({is_chair_expr}, false) END)"
    )


# Cheap necessary conditions evaluated before an expensive pattern (CASE evaluates THEN only
# where WHEN holds). Each guard is implied by its pattern, so results are unchanged.
_GUARDS = {
    "next_speaker": r"(위원|의원|부의장|간사)",
    "call_report": r"(인사|보고|설명|소개|선서)",
    "session": r"선포합니다$",
    "session_move": r"(하겠습니다|합니다)$",
    "session_pause": r"(중지|정회)",
    "hear_report": r"(듣|받)",
    "dismiss": r"(되겠습니다|좋습니다|됩니다|주십시오|바랍니다|주세요)$",
    "vote_result": r"(가결|부결|의결|채택|승인)",
    "agenda_item": r"항",
}


def sentence_code_sql(s: str = "s") -> str:
    """Per-sentence code: 2 = matches a non-filler procedural pattern, 1 = filler only, 0 = none."""
    parts = []
    for name, p, is_filler in PROCEDURAL_PATTERNS:
        if is_filler:
            continue
        g = _GUARDS.get(name)
        cond = f"regexp_full_match({s}, {_sql_str(p)})"
        if g:
            cond = f"regexp_matches({s}, {_sql_str(g)}) AND {cond}"
        parts.append(f"WHEN {cond} THEN 2")
    fill = [p for _, p, f in PROCEDURAL_PATTERNS if f]
    parts += [f"WHEN regexp_full_match({s}, {_sql_str(p)}) THEN 1" for p in fill]
    return "CASE " + " ".join(parts) + " ELSE 0 END"


def procedural_codes_sql(text_expr: str) -> str:
    """List of per-sentence codes (see sentence_code_sql); NULL when the text is null or longer
    than PROCEDURAL_MAX_CHARS. Sentences are split once, each sentence coded once."""
    sents = _sentences_sql(text_expr)
    return (f"(CASE WHEN {text_expr} IS NULL OR length({text_expr}) > {PROCEDURAL_MAX_CHARS} THEN NULL "
            f"ELSE list_transform({sents}, lambda s: {sentence_code_sql('s')}) END)")


def procedural_from_codes_sql(codes_expr: str, is_chair_expr: str) -> str:
    """leg_is_procedural from the codes list (same result as procedural_sql_reference)."""
    return (f"(CASE WHEN {codes_expr} IS NULL OR len({codes_expr}) = 0 THEN false "
            f"WHEN list_min({codes_expr}) = 0 THEN false WHEN list_max({codes_expr}) = 2 THEN true "
            f"ELSE coalesce({is_chair_expr}, false) END)")


def procedural_flags(texts: Sequence[Optional[str]], is_chair: Sequence[bool], reference: bool = False) -> list:
    """Python helper (tests, samples): leg_is_procedural for given texts."""
    con = _connect()
    df = pd.DataFrame({"i": range(len(texts)), "text": list(texts), "ch": list(is_chair)})
    con.register("_pt", df)
    if reference:
        q = f"SELECT i, {procedural_sql_reference('text', 'ch')} AS f FROM _pt ORDER BY i"
    else:
        q = (f"SELECT i, {procedural_from_codes_sql('c', 'ch')} AS f FROM "
             f"(SELECT i, ch, {procedural_codes_sql('text')} AS c FROM _pt) ORDER BY i")
    r = con.execute(q).fetchdf()
    con.close()
    return [bool(x) for x in r["f"]]


def procedural_rule_sql(text_expr: str) -> str:
    """Name of the first pattern matching each sentence (list), for inspection/samples."""
    sents = _sentences_sql(text_expr)
    case = " ".join(f"WHEN regexp_full_match(s, {_sql_str(p)}) THEN {_sql_str(n)}" for n, p, _ in PROCEDURAL_PATTERNS)
    return f"list_transform({sents}, lambda s: CASE {case} ELSE 'none' END)"


def _q(c: str) -> str:
    return '"' + c.replace('"', '""') + '"'


def pairing_group_sql(columns: Sequence[str], exclude_after_end_marker: bool = False, alias: str = "") -> str:
    """The role_group used for pairing: role_group, or 'excluded_after_end' for a turn printed after a
    meeting-end marker when exclude_after_end_marker is set (such a turn then breaks adjacency)."""
    a = f"{alias}." if alias else ""
    if exclude_after_end_marker and "after_end_marker" in columns:
        return f"(CASE WHEN coalesce({a}after_end_marker, false) THEN 'excluded_after_end' ELSE {a}role_group END)"
    return f"{a}role_group"


def sitting_sql(columns: Sequence[str], alias: str = "") -> str:
    """sitting_seq expression (constant 1 when the turns have no sitting_seq column)."""
    return f"{alias + '.' if alias else ''}sitting_seq" if "sitting_seq" in columns else "CAST(1 AS SMALLINT)"


def _slim_source(side: str, src, cols: Sequence[str]):
    """The turn column(s) a slim column reads on a side ('L'/'W'/'A'): a column name, or None when absent."""
    if isinstance(src, tuple):
        have = [c for c in src if c in cols]
        return have or None
    return src if src in cols else None


def slim_plan(turn_columns: Sequence[str], meetings_columns: Sequence[str] = (),
              extra_turn_cols: Sequence[str] = (), extra_meeting_cols: Sequence[str] = ()) -> dict:
    """Where every output column comes from: {'sources': {col: 'meetings.x' | 'leg_turn.x' | 'wit_turn.x' |
    'flag' | 'pair' | 'either_turn.x' | None}, 'slim_sources_missing': [...], 'meeting_cols_missing': [...]}."""
    cols = list(turn_columns)
    srcs, missing, mmiss = {}, [], []
    for c, side, src in SLIM_LAYOUT:
        if side == "P":
            srcs[c] = "pair"
        elif side == "F":
            srcs[c] = "flag"
        elif side == "M":
            if src in meetings_columns:
                srcs[c] = f"meetings.{src}"
            elif src in cols:
                srcs[c] = f"leg_turn.{src}"
            else:
                srcs[c] = None
                mmiss.append(c)
        else:
            if side == "A" and src == "label_confidence":
                have = "label_confidence" if "label_confidence" in cols else None
            else:
                have = _slim_source(side, src, cols)
            if have is None:
                srcs[c] = None
            elif isinstance(have, list):
                srcs[c] = ("leg_turn." if side == "L" else "wit_turn." if side == "W" else "either_turn.") + \
                    ("coalesce(" + ",".join(have) + ")" if side in ("L", "W") and len(have) > 1 else have[0])
            else:
                srcs[c] = ("leg_turn." if side == "L" else "wit_turn." if side == "W" else "either_turn.") + have
        if srcs[c] is None and c not in mmiss:
            missing.append(c)
    for c in extra_meeting_cols:
        srcs[c] = f"meetings.{c}" if c in meetings_columns else (f"leg_turn.{c}" if c in cols else None)
    for c in extra_turn_cols:
        srcs["leg_" + c] = f"leg_turn.{c}" if c in cols else None
        srcs["wit_" + c] = f"wit_turn.{c}" if c in cols else None
    return {"sources": srcs, "slim_sources_missing": missing, "meeting_cols_missing": mmiss}


def dyad_sql(src: str, columns: Sequence[str], meetings_src: Optional[str] = None,
             meetings_columns: Sequence[str] = (), exclude_after_end_marker: bool = False,
             extra_turn_cols: Sequence[str] = (), extra_meeting_cols: Sequence[str] = ()) -> str:
    """The one dyad query (slim layout). `src` is a relation (table name or subquery) of enriched turns with at
    least conf_num, turn_seq, role_group; `columns` are its column names. `meetings_src` (optional) is a
    relation keyed by conf_num with `meetings_columns`; meeting-level columns are taken from it when it has
    them, else from the legislator turn, else null. The wide turn relation is joined directly (no
    materialized CTE)."""
    cols = list(columns)
    for req in ("conf_num", "turn_seq", "role_group"):
        if req not in cols:
            raise ValueError(f"turns lack required column {req!r}")
    mcols = list(meetings_columns) if meetings_src is not None else []
    has_role = "role" in cols
    pos = "speaker_pos" in cols
    label = "speaker_label_raw" in cols
    chair_expr = ("(L.role = 'chair')" if has_role else
                  (f"regexp_full_match(coalesce(L.speaker_pos, ''), {_sql_str(CHAIR_POS_RE)})" if pos else "NULL"))
    wit_pos = ("W.speaker_pos" if pos else "NULL")
    wit_label = ("W.speaker_label_raw" if label else "NULL")
    wit_title_expr = (
        f"(CASE WHEN {wit_pos} IS NOT NULL AND trim({wit_pos}) <> '' "
        f"THEN regexp_full_match(trim({wit_pos}), {_sql_str(LEG_TITLE_POS_RE)}) "
        f"ELSE coalesce(regexp_full_match(trim({wit_label}), {_sql_str(LEG_TITLE_LABEL_RE)}), false) END)"
    )
    codes = procedural_codes_sql("S.text") if "text" in cols else "NULL"
    sel = []
    for c, side, s in SLIM_LAYOUT:
        if side == "P":
            sel.append(f"P.{c}")
        elif side == "F":
            if c == "leg_is_chair":
                sel.append(f"coalesce({chair_expr}, false) AS leg_is_chair")
            elif c == "leg_is_procedural":
                sel.append(f"{procedural_from_codes_sql('LP.pc', chair_expr)} AS leg_is_procedural")
            elif c == "wit_is_legislator_title":
                sel.append(f"{wit_title_expr} AS wit_is_legislator_title")
            else:
                raise ValueError(c)
        elif side == "M":
            if s in mcols:
                sel.append(f"M.{_q(s)} AS {_q(c)}")
            elif s in cols:
                sel.append(f"L.{_q(s)} AS {_q(c)}")
            else:
                sel.append(f"CAST(NULL AS {SLIM_NULL_TYPES[c]}) AS {_q(c)}")
        elif side in ("L", "W"):
            have = _slim_source(side, s, cols)
            if have is None:
                sel.append(f"CAST(NULL AS {SLIM_NULL_TYPES[c]}) AS {_q(c)}")
            elif isinstance(have, list):
                ex = [f"{side}.{_q(x)}" for x in have]
                sel.append((f"coalesce({', '.join(ex)})" if len(ex) > 1 else ex[0]) + f" AS {_q(c)}")
            else:
                sel.append(f"{side}.{_q(have)} AS {_q(c)}")
        elif side == "A":
            if s == "label_confidence":
                e = ("(coalesce(L.label_confidence = 'low', false) OR coalesce(W.label_confidence = 'low', false))"
                     if "label_confidence" in cols else None)
            else:
                have = _slim_source(side, s, cols)
                col = have[0] if isinstance(have, list) else have
                e = (f"(coalesce(CAST(L.{_q(col)} AS BOOLEAN), false) OR coalesce(CAST(W.{_q(col)} AS BOOLEAN), false))"
                     if col else None)
            sel.append(f"{e} AS {_q(c)}" if e else f"CAST(NULL AS BOOLEAN) AS {_q(c)}")
        else:
            raise ValueError(side)
    for c in extra_meeting_cols:
        sel.append(f"M.{_q(c)} AS {_q(c)}" if c in mcols else (f"L.{_q(c)} AS {_q(c)}" if c in cols else f"NULL AS {_q(c)}"))
    for c in extra_turn_cols:
        if c in cols:
            sel += [f"L.{_q(c)} AS {_q('leg_' + c)}", f"W.{_q(c)} AS {_q('wit_' + c)}"]
    mjoin = f"LEFT JOIN {meetings_src} M ON M.conf_num = P.conf_num" if mcols else ""
    return f"""
WITH A AS (SELECT conf_num, turn_seq, {pairing_group_sql(cols, exclude_after_end_marker)} AS role_group,
                  {sitting_sql(cols)} AS sitting_seq FROM {src}),
P AS (
  SELECT a.conf_num,
         CASE WHEN a.role_group = 'legislator' THEN a.turn_seq ELSE b.turn_seq END AS leg_turn_seq,
         CASE WHEN a.role_group = 'legislator' THEN b.turn_seq ELSE a.turn_seq END AS wit_turn_seq,
         CASE WHEN a.role_group = 'legislator' THEN 'question' ELSE 'answer' END AS direction
  FROM A a JOIN A b ON a.conf_num = b.conf_num AND b.turn_seq = a.turn_seq + 1 AND a.sitting_seq = b.sitting_seq
  WHERE (a.role_group = 'legislator' AND b.role_group = 'nonlegislator')
     OR (a.role_group = 'nonlegislator' AND b.role_group = 'legislator')
),
LK AS (SELECT DISTINCT conf_num, leg_turn_seq FROM P),
LP AS (
  SELECT S.conf_num, S.turn_seq, {codes} AS pc
  FROM {src} S SEMI JOIN LK ON S.conf_num = LK.conf_num AND S.turn_seq = LK.leg_turn_seq
)
SELECT {', '.join(sel)}
FROM P
JOIN {src} L ON L.conf_num = P.conf_num AND L.turn_seq = P.leg_turn_seq
JOIN {src} W ON W.conf_num = P.conf_num AND W.turn_seq = P.wit_turn_seq
LEFT JOIN LP ON LP.conf_num = P.conf_num AND LP.turn_seq = P.leg_turn_seq
{mjoin}
ORDER BY P.conf_num, least(P.leg_turn_seq, P.wit_turn_seq)
"""


def meeting_cols_plan(turn_columns: Sequence[str], meetings_columns: Sequence[str] = (),
                      meeting_cols: Optional[Sequence[str]] = None) -> dict:
    """Where each meeting-level column comes from ('meetings' / 'turns') and which are missing."""
    wanted = list(MEETING_LEVEL_COLS if meeting_cols is None else meeting_cols)
    src = {c: ("meetings" if c in meetings_columns else "turns") for c in wanted
           if c in meetings_columns or c in turn_columns}
    return {"meeting_cols_source": src, "meeting_cols_missing": [c for c in wanted if c not in src]}


def input_diagnostics_sql(src: str, columns: Optional[Sequence[str]] = None,
                          exclude_after_end_marker: bool = False) -> str:
    """Counts the builder reports (never silently drops): null/unknown role_group, duplicate
    (conf_num, turn_seq), turn_seq gaps (non-contiguous positions), null sitting_seq, meetings with
    several sittings, legislator/nonlegislator neighbours not paired because the sitting changes
    (n_pairs_blocked_by_sitting) or because one side is after an end marker and excluded
    (n_pairs_blocked_by_after_end)."""
    cols = list(columns) if columns is not None else ["conf_num", "turn_seq", "role_group"]
    has_sit = "sitting_seq" in cols
    has_ae = "after_end_marker" in cols
    ae = "coalesce(after_end_marker, false)" if has_ae else "false"
    return f"""
WITH T AS (SELECT conf_num, turn_seq, role_group, {sitting_sql(cols)} AS sitting_seq, {ae} AS ae FROM {src}),
G AS (SELECT conf_num, count(*) n, count(DISTINCT turn_seq) nd, min(turn_seq) mn, max(turn_seq) mx,
             count(DISTINCT sitting_seq) ns FROM T GROUP BY 1),
N AS (SELECT a.sitting_seq s1, b.sitting_seq s2, a.ae ae1, b.ae ae2 FROM T a JOIN T b
      ON a.conf_num = b.conf_num AND b.turn_seq = a.turn_seq + 1
      WHERE (a.role_group = 'legislator' AND b.role_group = 'nonlegislator')
         OR (a.role_group = 'nonlegislator' AND b.role_group = 'legislator'))
SELECT
  (SELECT count(*) FROM T) AS n_turns,
  (SELECT count(*) FROM G) AS n_meetings,
  (SELECT count(*) FROM T WHERE role_group IS NULL) AS n_role_group_null,
  (SELECT count(*) FROM T WHERE role_group IS NOT NULL AND role_group NOT IN ('legislator','nonlegislator','excluded')) AS n_role_group_unknown,
  (SELECT count(*) FROM T WHERE role_group = 'legislator') AS n_legislator,
  (SELECT count(*) FROM T WHERE role_group = 'nonlegislator') AS n_nonlegislator,
  (SELECT count(*) FROM T WHERE role_group = 'excluded') AS n_excluded,
  (SELECT coalesce(sum(n - nd), 0) FROM G) AS n_duplicate_positions,
  (SELECT count(*) FROM G WHERE mn <> 1 OR mx <> nd OR n <> nd) AS n_meetings_noncontiguous,
  (SELECT count(*) FROM T WHERE turn_seq IS NULL OR conf_num IS NULL) AS n_null_keys,
  (SELECT count(*) FROM T WHERE sitting_seq IS NULL) AS n_sitting_seq_null,
  (SELECT count(*) FROM G WHERE ns > 1) AS n_meetings_several_sittings,
  (SELECT count(*) FROM T WHERE ae) AS n_turns_after_end_marker,
  (SELECT count(*) FROM N WHERE s1 IS DISTINCT FROM s2) AS n_pairs_blocked_by_sitting,
  (SELECT count(*) FROM N WHERE s1 IS NOT DISTINCT FROM s2 AND (ae1 OR ae2)
     AND {'true' if exclude_after_end_marker else 'false'}) AS n_pairs_blocked_by_after_end
"""


def _connect(memory_limit: str = "6GB", threads: int = 4) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=true")
    con.execute("SET enable_progress_bar=false")
    return con


def _restore_dtypes(out: pd.DataFrame, turns: pd.DataFrame, meetings: Optional[pd.DataFrame] = None,
                    extra_turn_cols: Sequence[str] = ()) -> pd.DataFrame:
    """Give columns copied verbatim from a turn / meetings column the dtype they had in the input frame."""
    plan = slim_plan(list(turns.columns), list(meetings.columns) if meetings is not None else (),
                     extra_turn_cols)["sources"]
    for c in out.columns:
        s = plan.get(c)
        if c in ("leg_turn_seq", "wit_turn_seq"):
            frame, src = turns, "turn_seq"
        elif isinstance(s, str) and s.startswith("meetings.") and meetings is not None:
            frame, src = meetings, s.split(".", 1)[1]
        elif isinstance(s, str) and (s.startswith("leg_turn.") or s.startswith("wit_turn.")) and "(" not in s:
            frame, src = turns, s.split(".", 1)[1]
        else:
            continue
        if src not in frame.columns:
            continue
        dt = frame[src].dtype
        if dt == object or str(out[c].dtype) == str(dt):
            continue
        try:
            out[c] = out[c].astype(dt)
        except (TypeError, ValueError):
            pass
    for c in ("leg_is_chair", "leg_is_procedural", "wit_is_legislator_title"):
        if c in out.columns:
            out[c] = out[c].astype(bool)
    return out


def build_dyads(turns: pd.DataFrame, meetings: Optional[pd.DataFrame] = None, meeting_cols: Optional[Sequence[str]] = None,
                return_stats: bool = False, con: Optional[duckdb.DuckDBPyConnection] = None,
                exclude_after_end_marker: bool = False, extra_turn_cols: Sequence[str] = ()):
    """Build slim dyads from an in-memory enriched turns frame (meeting-level columns joined from
    `meetings` when given). `meeting_cols` = extra meeting columns carried unprefixed (internal samples).
    Raises on duplicate positions or null keys (they would make adjacency ambiguous)."""
    own = con is None
    con = con or _connect()
    tbl = pa.Table.from_pandas(turns, preserve_index=False)
    con.register("_turns_in", tbl)
    diag = con.execute(input_diagnostics_sql("_turns_in", list(turns.columns), exclude_after_end_marker)).fetchdf().iloc[0].to_dict()
    diag = {k: int(v) for k, v in diag.items()}
    diag["sitting_seq_missing"] = "sitting_seq" not in turns.columns
    diag["exclude_after_end_marker"] = bool(exclude_after_end_marker)
    if diag["n_duplicate_positions"] or diag["n_null_keys"]:
        con.unregister("_turns_in")
        raise ValueError(f"turns have duplicate (conf_num, turn_seq) or null keys: {diag}")
    extra_m = [c for c in (meeting_cols or ()) if c not in MEETING_LEVEL_COLS]
    mcols = []
    if meetings is not None:
        mcols = [c for c in tuple(MEETING_LEVEL_COLS) + tuple(extra_m) if c in meetings.columns]
        if meetings["conf_num"].duplicated().any():
            raise ValueError("meetings has duplicate conf_num")
        con.register("_meetings_in", pa.Table.from_pandas(meetings[["conf_num"] + mcols], preserve_index=False))
    q = dyad_sql("_turns_in", list(turns.columns), "_meetings_in" if meetings is not None else None, mcols,
                 exclude_after_end_marker=exclude_after_end_marker, extra_turn_cols=extra_turn_cols,
                 extra_meeting_cols=extra_m)
    out = con.execute(q).arrow()
    if hasattr(out, "read_all"):
        out = out.read_all()
    out = out.to_pandas(types_mapper={pa.int64(): pd.Int64Dtype(), pa.int32(): pd.Int32Dtype(),
                                      pa.int16(): pd.Int16Dtype(), pa.bool_(): pd.BooleanDtype()}.get)
    con.unregister("_turns_in")
    if meetings is not None:
        con.unregister("_meetings_in")
    if own:
        con.close()
    out = _restore_dtypes(out, turns, meetings[["conf_num"] + mcols] if meetings is not None else None, extra_turn_cols)
    for c in ("conf_num",):
        if out[c].isna().sum() == 0:
            out[c] = out[c].astype("int64")
    for c in ("leg_turn_seq", "wit_turn_seq"):
        if out[c].isna().sum() == 0:
            out[c] = out[c].astype("int32")
    if return_stats:
        st = dict(diag)
        st.update(_dyad_counts(out))
        plan = slim_plan(list(turns.columns), mcols, extra_turn_cols, extra_m)
        st.update({"slim_sources": plan["sources"], "slim_sources_missing": plan["slim_sources_missing"],
                   "meeting_cols_missing": plan["meeting_cols_missing"]})
        if meetings is not None:
            st["n_dyads_meeting_missing"] = int((~out["conf_num"].isin(meetings["conf_num"])).sum())
        return out, st
    return out


def _dyad_counts(d: pd.DataFrame) -> dict:
    out = {
        "n_dyads": int(len(d)),
        "n_question": int((d["direction"] == "question").sum()),
        "n_answer": int((d["direction"] == "answer").sum()),
        "n_leg_is_chair": int(d["leg_is_chair"].sum()),
        "n_leg_is_procedural": int(d["leg_is_procedural"].sum()),
        "n_wit_is_legislator_title": int(d["wit_is_legislator_title"].sum()),
        "n_meetings_with_dyads": int(d["conf_num"].nunique()),
    }
    for c in ANY_FLAGS:
        if c in d.columns:
            out[f"n_{c}"] = int(d[c].fillna(False).astype(bool).sum())
            out[f"n_{c}_null"] = int(d[c].isna().sum())
    return out


def _counts_sql(path: str) -> str:
    anyc = "".join(f", count(*) FILTER (WHERE {c}) AS n_{c}, count(*) FILTER (WHERE {c} IS NULL) AS n_{c}_null"
                   for c in ANY_FLAGS)
    return f"""SELECT count(*) n_dyads, count(*) FILTER (WHERE direction='question') n_question,
        count(*) FILTER (WHERE direction='answer') n_answer, count(*) FILTER (WHERE leg_is_chair) n_leg_is_chair,
        count(*) FILTER (WHERE leg_is_procedural) n_leg_is_procedural,
        count(*) FILTER (WHERE wit_is_legislator_title) n_wit_is_legislator_title,
        count(DISTINCT conf_num) n_meetings_with_dyads{anyc} FROM read_parquet({_sql_str(path)})"""


def build_dyads_file(turns_path: str | Sequence[str], out_path: str | os.PathLike,
                     meetings_path: Optional[str | os.PathLike] = None,
                     meeting_cols: Optional[Sequence[str]] = None, memory_limit: str = "6GB",
                     threads: int = 4, chunk_turns: int = 300_000, exclude_after_end_marker: bool = False,
                     exclude_conf_nums: Sequence[int] = (), only_conf_nums: Optional[Sequence[int]] = None) -> dict:
    """Parquet in, parquet out (never materializes texts in pandas). Meetings are processed in
    conf_num chunks of at most `chunk_turns` turns (a larger single meeting is its own chunk); each
    chunk is copied to a temporary table, paired, sorted and streamed to the output. Returns stats.
    exclude_conf_nums / only_conf_nums restrict the meetings paired (duplicate copies: run_all builds
    them into a separate file); the counts of turns left out are reported."""
    t0 = time.time()
    con = _connect(memory_limit, threads)
    con.execute("SET preserve_insertion_order=true")
    paths = [str(turns_path)] if isinstance(turns_path, (str, os.PathLike)) else [str(p) for p in turns_path]
    base = "read_parquet(" + "[" + ", ".join(_sql_str(p) for p in paths) + "], union_by_name=true)"
    con.execute("CREATE TEMP TABLE _excl AS SELECT unnest($1::BIGINT[]) AS conf_num", [[int(x) for x in exclude_conf_nums]])
    n_excluded_turns = int(con.execute(f"SELECT count(*) FROM {base} WHERE conf_num IN (SELECT conf_num FROM _excl)").fetchone()[0]) \
        if exclude_conf_nums else 0
    where = ["conf_num NOT IN (SELECT conf_num FROM _excl)"] if exclude_conf_nums else []
    if only_conf_nums is not None:
        con.execute("CREATE TEMP TABLE _only AS SELECT unnest($1::BIGINT[]) AS conf_num", [[int(x) for x in only_conf_nums]])
        where.append("conf_num IN (SELECT conf_num FROM _only)")
    src = f"(SELECT * FROM {base}{' WHERE ' + ' AND '.join(where) if where else ''})"
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    diag = con.execute(input_diagnostics_sql(src, cols, exclude_after_end_marker)).fetchdf().iloc[0].to_dict()
    diag = {k: int(v) for k, v in diag.items()}
    diag["sitting_seq_missing"] = "sitting_seq" not in cols
    diag["exclude_after_end_marker"] = bool(exclude_after_end_marker)
    diag["n_meetings_excluded"] = len(set(int(x) for x in exclude_conf_nums))
    diag["n_turns_excluded"] = n_excluded_turns
    if diag["n_duplicate_positions"] or diag["n_null_keys"]:
        raise ValueError(f"turns have duplicate (conf_num, turn_seq) or null keys: {diag}")
    extra_m = [c for c in (meeting_cols or ()) if c not in MEETING_LEVEL_COLS]
    mcols = []
    if meetings_path is not None:
        mall = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_sql_str(str(meetings_path))})").fetchall()]
        mcols = [c for c in tuple(MEETING_LEVEL_COLS) + tuple(extra_m) if c in mall]
        con.execute(f"CREATE TEMP TABLE _meetings AS SELECT conf_num, {', '.join(_q(c) for c in mcols)} "
                    f"FROM read_parquet({_sql_str(str(meetings_path))})" if mcols else
                    f"CREATE TEMP TABLE _meetings AS SELECT conf_num FROM read_parquet({_sql_str(str(meetings_path))})")
        if con.execute("SELECT count(*) - count(DISTINCT conf_num) FROM _meetings").fetchone()[0]:
            raise ValueError("meetings has duplicate conf_num")
    # chunk plan on conf_num only
    per = con.execute(f"SELECT conf_num, count(*) n FROM {src} GROUP BY 1 ORDER BY 1").fetchall()
    chunks, cur, lo = [], 0, None
    for cn, n in per:
        if lo is None:
            lo = cn
        if cur and cur + n > chunk_turns:
            chunks.append((lo, prev))
            lo, cur = cn, 0
        cur += n
        prev = cn
    if lo is not None:
        chunks.append((lo, prev))
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(out_path) + ".tmp")
    q = dyad_sql("_chunk", cols, "_meetings" if meetings_path is not None else None, mcols,
                 exclude_after_end_marker=exclude_after_end_marker, extra_meeting_cols=extra_m)
    writer = None
    n_rows = 0
    try:
        for lo, hi in chunks:
            con.execute(f"CREATE OR REPLACE TEMP TABLE _chunk AS SELECT * FROM {src} WHERE conf_num BETWEEN {int(lo)} AND {int(hi)}")
            rdr = con.execute(q).to_arrow_reader(50_000)
            for batch in rdr:
                if writer is None:
                    writer = pq.ParquetWriter(str(tmp), batch.schema, compression="zstd")
                if batch.schema.equals(writer.schema):
                    writer.write_batch(batch)
                else:   # same columns; a chunk may differ only in nullability / metadata
                    writer.write_table(pa.Table.from_batches([batch]).cast(writer.schema))
                n_rows += batch.num_rows
            con.execute("DROP TABLE _chunk")
        if writer is None:   # no dyads at all: write the empty result with its schema
            con.execute(f"CREATE OR REPLACE TEMP TABLE _chunk AS SELECT * FROM {src} WHERE false")
            empty = con.execute(q).arrow()
            if hasattr(empty, "read_all"):
                empty = empty.read_all()
            pq.write_table(empty, str(tmp), compression="zstd")
        else:
            writer.close()
            writer = None
        os.replace(tmp, out_path)
    finally:
        if writer is not None:
            writer.close()
    counts = con.execute(_counts_sql(str(out_path))).fetchdf().iloc[0].to_dict()
    st = dict(diag)
    st.update({k: int(v) for k, v in counts.items()})
    plan = slim_plan(cols, mcols, (), extra_m)
    st.update({"slim_sources": plan["sources"], "slim_sources_missing": plan["slim_sources_missing"],
               "meeting_cols_missing": plan["meeting_cols_missing"]})
    if meetings_path is not None:
        st["n_dyads_meeting_missing"] = int(con.execute(
            f"SELECT count(*) FROM read_parquet({_sql_str(str(out_path))}) d ANTI JOIN _meetings m USING (conf_num)").fetchone()[0])
    con.close()
    if st["n_dyads"] != n_rows:
        raise RuntimeError(f"written rows {n_rows} != counted {st['n_dyads']}")
    st["n_chunks"] = len(chunks)
    st["seconds"] = round(time.time() - t0, 1)
    st["out_file"] = Path(out_path).name
    return st


def procedural_rules(texts: Sequence[str]) -> list:
    """Per-sentence rule names for a few texts (inspection and the hand-check sample)."""
    con = _connect()
    df = pd.DataFrame({"i": range(len(texts)), "text": list(texts)})
    con.register("_t", df)
    r = con.execute(f"SELECT i, {procedural_rule_sql('text')} AS rules FROM _t ORDER BY i").fetchdf()
    con.close()
    return [list(x) for x in r["rules"]]


# =============================================================================
# Legacy: v9 string-sort dyads (build_v9.py:516-582), vectorized, bit-for-bit
# =============================================================================
# VERBATIM role sets of validation/build_v9.py:38-49 (= legacy_rules.LEG_ROLES / NONLEG_ROLES;
# test_dyads.py asserts equality with legacy_rules).
LEG_ROLES_V9 = ("legislator", "chair")
NONLEG_ROLES_V9 = (
    "minister", "minister_nominee", "minister_acting", "vice_minister",
    "prime_minister", "witness", "testifier", "expert_witness",
    "senior_bureaucrat", "other_official", "local_gov_head",
    "agency_head", "public_corp_head", "org_head", "mid_bureaucrat",
    "nominee", "military", "police", "financial_regulator",
    "audit_official", "election_official", "constitutional_court",
    "assembly_official", "independent_official", "private_sector",
    "research_head", "cultural_institution_head", "broadcasting",
    "cooperative_head",
)
V9_SPEECH_COLS = ("meeting_id", "term", "committee", "committee_key", "hearing_type", "date", "agenda",
                  "person_name", "speaker", "member_uid", "party", "ruling_status", "seniority", "gender",
                  "role", "affiliation_raw", "ministry_normalized", "dual_office", "admin",
                  "admin_ideology", "speech_text", "speech_order")
# (output column, side, source column) = legacy_rules.DYAD_FIELDS_V9 / build_v9._make_dyad
V9_DYAD_LAYOUT = (
    ("meeting_id", "m", "meeting_id"), ("term", "L", "term"), ("committee", "L", "committee"),
    ("committee_key", "L", "committee_key"), ("hearing_type", "L", "hearing_type"), ("date", "L", "date"),
    ("agenda", "L", "agenda"), ("leg_name", "L", "person_name"), ("leg_speaker_raw", "L", "speaker"),
    ("leg_member_uid", "L", "member_uid"), ("leg_party", "L", "party"),
    ("leg_ruling_status", "L", "ruling_status"), ("leg_seniority", "L", "seniority"),
    ("leg_gender", "L", "gender"), ("witness_name", "W", "person_name"),
    ("witness_speaker_raw", "W", "speaker"), ("witness_role", "W", "role"),
    ("witness_affiliation", "W", "affiliation_raw"),
    ("witness_ministry_normalized", "W", "ministry_normalized"),
    ("witness_dual_office", "W", "dual_office"), ("witness_admin", "W", "admin"),
    ("witness_admin_ideology", "W", "admin_ideology"), ("direction", "d", None),
    ("leg_speech", "L", "speech_text"), ("witness_speech", "W", "speech_text"),
)


def _v9_side_codes(role: pd.Series) -> np.ndarray:
    r = role.to_numpy(dtype=object)
    side = np.full(len(r), 0, dtype=np.int8)  # 0 = X (excluded/other), 1 = L, 2 = N
    side[np.isin(r, LEG_ROLES_V9)] = 1
    side[np.isin(r, NONLEG_ROLES_V9)] = 2
    return side


def legacy_v9_dyads(sp: pd.DataFrame, order: str = "lexicographic") -> pd.DataFrame:
    """Reproduce build_v9.phase4_build_dyads on a frame of v9 speeches (any subset of whole
    meetings). order='lexicographic' is the published behaviour (sort on the VARCHAR
    speech_order); order='numeric' gives the corrected v5 order for comparison.

    Equivalences with the original loop (documented, verified in verify_legacy_v9):
    - df.groupby('meeting_id') iterates meetings in sorted key order and drops null keys;
      here: stable sort on (meeting_id, speech_order) as Python strings, null meeting_id dropped.
    - group.sort_values('speech_order') is not stable, but v9 has no duplicate
      (meeting_id, speech_order) and no null speech_order (checked), so the order is unique.
    - Dyads are appended in order of the first row of each pair; same here.
    - Output dtypes follow the published parquet: term and leg_seniority DOUBLE (the full v9
      frame had a null term, so pandas held term as float64), witness_dual_office nullable bool.
    """
    missing = [c for c in V9_SPEECH_COLS if c not in sp.columns]
    if missing:
        raise ValueError(f"v9 speeches lack {missing}")
    d = sp.loc[sp["meeting_id"].notna()].copy()
    if d["speech_order"].isna().any():
        raise ValueError("null speech_order: the original sort would put these last; not reproduced")
    if order == "lexicographic":
        key = d["speech_order"].astype(str)
    elif order == "numeric":
        key = d["speech_order"].astype(str).str.strip().astype("int64")
    else:
        raise ValueError(order)
    d = d.assign(_k=key)
    if d.duplicated(["meeting_id", "_k"]).any():
        raise ValueError("duplicate (meeting_id, speech_order): original order would be sort-unstable")
    d = d.sort_values(["meeting_id", "_k"], kind="mergesort").reset_index(drop=True)
    side = _v9_side_codes(d["role"])
    mid = d["meeting_id"].to_numpy(dtype=object)
    n = len(d)
    if n < 2:
        idx_first = np.array([], dtype=np.int64)
    else:
        same = mid[:-1] == mid[1:]
        s0, s1 = side[:-1], side[1:]
        q = same & (s0 == 1) & (s1 == 2)
        a = same & (s0 == 2) & (s1 == 1)
        idx_first = np.flatnonzero(q | a)
    is_q = side[idx_first] == 1
    leg_i = np.where(is_q, idx_first, idx_first + 1)
    wit_i = np.where(is_q, idx_first + 1, idx_first)
    out = {}
    for col, s, src in V9_DYAD_LAYOUT:
        if s == "m":
            out[col] = d[src].to_numpy(dtype=object)[idx_first]
        elif s == "d":
            out[col] = np.where(is_q, "question", "answer").astype(object)
        else:
            arr = d[src].to_numpy()
            out[col] = arr[leg_i if s == "L" else wit_i]
    res = pd.DataFrame(out)
    res["term"] = pd.to_numeric(res["term"], errors="coerce").astype("float64")
    res["leg_seniority"] = pd.to_numeric(res["leg_seniority"], errors="coerce").astype("float64")
    res["witness_dual_office"] = res["witness_dual_office"].astype("boolean")
    return res


def _v9_speech_select() -> str:
    return ", ".join(V9_SPEECH_COLS)


def build_legacy_v9_dyads_file(out_path: str | os.PathLike, speeches_path: str | os.PathLike = V9_SPEECHES,
                               batch_meetings: int = 400, order: str = "lexicographic") -> dict:
    """Stream v9 speeches meeting-batch by meeting-batch (sorted meeting_id order, as the
    original groupby) and write the legacy dyads. Memory stays bounded."""
    t0 = time.time()
    con = _connect()
    mids = [r[0] for r in con.execute(
        f"SELECT DISTINCT meeting_id FROM read_parquet({_sql_str(str(speeches_path))}) WHERE meeting_id IS NOT NULL").fetchall()]
    mids = sorted(mids)  # Python str order = groupby key order
    writer = None
    n = 0
    schema = None
    for i in range(0, len(mids), batch_meetings):
        chunk = mids[i:i + batch_meetings]
        con.execute("CREATE OR REPLACE TEMP TABLE _ids AS SELECT unnest($1) AS meeting_id", [chunk])
        sp = con.execute(f"SELECT {_v9_speech_select()} FROM read_parquet({_sql_str(str(speeches_path))}) "
                         f"WHERE meeting_id IN (SELECT meeting_id FROM _ids)").fetchdf()
        dy = legacy_v9_dyads(sp, order=order)
        tbl = pa.Table.from_pandas(dy, preserve_index=False)
        if writer is None:
            schema = _v9_dyad_schema()
            writer = pq.ParquetWriter(str(out_path), schema, compression="zstd")
        writer.write_table(tbl.cast(schema))
        n += len(dy)
        del sp, dy, tbl
    if writer is not None:
        writer.close()
    con.close()
    return {"n_dyads": n, "n_meetings": len(mids), "seconds": round(time.time() - t0, 1), "out_path": str(out_path)}


def _v9_dyad_schema() -> pa.Schema:
    f = []
    for col, _, _ in V9_DYAD_LAYOUT:
        if col in ("term", "leg_seniority"):
            f.append(pa.field(col, pa.float64()))
        elif col == "witness_dual_office":
            f.append(pa.field(col, pa.bool_()))
        else:
            f.append(pa.field(col, pa.string()))
    return pa.schema(f)


def _frames_equal(a: pd.DataFrame, b: pd.DataFrame) -> dict:
    """Exact comparison (null == null), column by column, in row order."""
    res = {"rows_a": len(a), "rows_b": len(b), "columns_equal": list(a.columns) == list(b.columns),
           "mismatch_by_column": {}}
    if len(a) != len(b) or not res["columns_equal"]:
        res["equal"] = False
        return res
    for c in a.columns:
        x = a[c].reset_index(drop=True)
        y = b[c].reset_index(drop=True)
        xn, yn = x.isna().to_numpy(), y.isna().to_numpy()
        xv = x.astype(object).where(~x.isna(), None).to_numpy()
        yv = y.astype(object).where(~y.isna(), None).to_numpy()
        both = ~xn & ~yn
        neq = (xn != yn)
        if both.any():
            neq[both] = np.array([xv[i] != yv[i] for i in np.flatnonzero(both)], dtype=bool) \
                if x.dtype == object or y.dtype == object else (x[both].to_numpy() != y[both].to_numpy())
        k = int(neq.sum())
        if k:
            res["mismatch_by_column"][c] = k
    res["equal"] = not res["mismatch_by_column"]
    return res


def verify_legacy_v9(n_meetings: int = 100, seed: int = SEED, speeches_path=V9_SPEECHES,
                     dyads_path=V9_DYADS, meeting_ids: Optional[Sequence[str]] = None) -> dict:
    """Rebuild legacy dyads for a seeded sample of v9 meetings and compare with the published
    rows of those meetings, in published file order (file_row_number), bit for bit."""
    con = _connect()
    if meeting_ids is None:
        allm = pd.DataFrame({"meeting_id": [r[0] for r in con.execute(
            f"SELECT DISTINCT meeting_id FROM read_parquet({_sql_str(str(speeches_path))})").fetchall()]})
        allm = allm.sort_values("meeting_id").reset_index(drop=True)
        meeting_ids = allm.sample(n=n_meetings, random_state=seed)["meeting_id"].tolist()
    con.execute("CREATE OR REPLACE TEMP TABLE _ids AS SELECT unnest($1) AS meeting_id", [list(meeting_ids)])
    sp = con.execute(f"SELECT {_v9_speech_select()} FROM read_parquet({_sql_str(str(speeches_path))}) "
                     f"WHERE meeting_id IN (SELECT meeting_id FROM _ids)").fetchdf()
    pub = con.execute(f"SELECT * FROM read_parquet({_sql_str(str(dyads_path))}, file_row_number=true) "
                      f"WHERE meeting_id IN (SELECT meeting_id FROM _ids) ORDER BY file_row_number").fetchdf()
    con.close()
    rn = pub.pop("file_row_number").to_numpy()
    lex = legacy_v9_dyads(sp, "lexicographic")
    num = legacy_v9_dyads(sp, "numeric")
    # published rows of one meeting are contiguous and in build order?
    contiguous = True
    for _, g in pd.DataFrame({"m": pub["meeting_id"].to_numpy(), "r": rn}).groupby("m"):
        r = g["r"].to_numpy()
        if len(r) and (r.max() - r.min() + 1 != len(r)):
            contiguous = False
    # meetings in published file order are in sorted meeting_id order?
    first_row = pd.DataFrame({"m": pub["meeting_id"], "r": rn}).groupby("m")["r"].min().sort_values()
    sorted_order = list(first_row.index) == sorted(first_row.index)
    cmp_lex = _frames_equal(lex.reset_index(drop=True), pub.reset_index(drop=True))
    per_meeting = []
    for m in meeting_ids:
        a = lex[lex["meeting_id"] == m].reset_index(drop=True)
        b = pub[pub["meeting_id"] == m].reset_index(drop=True)
        c = num[num["meeting_id"] == m].reset_index(drop=True)
        per_meeting.append({"meeting_id": m, "n_published": len(b), "n_legacy": len(a), "n_numeric": len(c),
                            "exact": bool(_frames_equal(a, b)["equal"]),
                            "numeric_equals_published": bool(_frames_equal(c, b)["equal"])})
    pm = pd.DataFrame(per_meeting)
    return {
        "n_meetings": len(meeting_ids),
        "n_speeches": int(len(sp)),
        "n_published_dyads": int(len(pub)),
        "n_legacy_dyads": int(len(lex)),
        "n_numeric_dyads": int(len(num)),
        "all_rows_equal_in_file_order": bool(cmp_lex["equal"]),
        "mismatch_by_column": cmp_lex["mismatch_by_column"],
        "meetings_exact": int(pm["exact"].sum()),
        "meetings_where_numeric_equals_published": int(pm["numeric_equals_published"].sum()),
        "published_rows_contiguous_per_meeting": contiguous,
        "published_meetings_in_sorted_id_order": sorted_order,
        "meeting_ids": list(meeting_ids),
        "per_meeting": per_meeting,
    }


# =============================================================================
# Hand-check sample for leg_is_procedural precision
# =============================================================================

def v9_xlsx_turns_for_sample(n_meetings: int = 300, seed: int = SEED,
                             exclude_meeting_ids: Sequence[str] = ()) -> pd.DataFrame:
    """Enriched-turn stand-in built from v9 XLSX-era meetings (numeric order, v9 roles), used
    only to measure the procedural regex on real chair language before v10 turns exist."""
    con = _connect()
    ids = con.execute(f"""SELECT meeting_id FROM (SELECT DISTINCT meeting_id FROM read_parquet({_sql_str(str(V9_SPEECHES))})
        WHERE hearing_type IN ('상임위원회','국정감사','국정조사','예산결산특별위원회','국회본회의','인사청문특별위원회'))
        ORDER BY meeting_id""").fetchdf()
    ids = ids[~ids["meeting_id"].isin(set(exclude_meeting_ids))].reset_index(drop=True)
    pick = ids.sample(n=min(n_meetings, len(ids)), random_state=seed)["meeting_id"].tolist()
    con.execute("CREATE OR REPLACE TEMP TABLE _ids AS SELECT unnest($1) AS meeting_id", [pick])
    sp = con.execute(f"""SELECT meeting_id, try_cast(speech_order AS INTEGER) AS so, speaker, role, speech_text
        FROM read_parquet({_sql_str(str(V9_SPEECHES))}) WHERE meeting_id IN (SELECT meeting_id FROM _ids)""").fetchdf()
    con.close()
    sp = sp.sort_values(["meeting_id", "so"]).reset_index(drop=True)
    codes = {m: i + 1 for i, m in enumerate(sorted(sp["meeting_id"].unique()))}
    side = _v9_side_codes(sp["role"])
    t = pd.DataFrame({
        "conf_num": sp["meeting_id"].map(codes).astype("int64"),
        "turn_seq": sp.groupby("meeting_id").cumcount().astype("int32") + 1,
        "speaker_label_raw": sp["speaker"],
        "speaker_pos": sp["speaker"].str.split(" ").str[0],
        "role": sp["role"],
        "role_group": np.where(side == 1, "legislator", np.where(side == 2, "nonlegislator", "excluded")),
        "text": sp["speech_text"],
        "text_raw": sp["speech_text"],
        "v9_meeting_id": sp["meeting_id"],
    })
    return t


def make_procedural_sample(n: int = 200, seed: int = SEED, out_csv: Optional[Path] = None,
                           round_no: int = 1) -> pd.DataFrame:
    """n flagged dyads for hand-checking, from a frame of 300 v9 XLSX-era meetings. Round k draws
    its frame (seed 8374) from meetings outside the frames of rounds 1..k-1, so a final
    precision figure is never measured on meetings used to tune the patterns."""
    excl: list = []
    for _ in range(round_no - 1):
        prev = v9_xlsx_turns_for_sample(seed=seed, exclude_meeting_ids=excl)
        excl += prev["v9_meeting_id"].unique().tolist()
    t = v9_xlsx_turns_for_sample(seed=seed, exclude_meeting_ids=excl)
    d, st = build_dyads(t, meeting_cols=("v9_meeting_id",), return_stats=True, extra_turn_cols=("speaker_label_raw",))
    flagged = d[d["leg_is_procedural"]]
    s = flagged.sample(n=min(n, len(flagged)), random_state=seed).reset_index(drop=True)
    s = s[["v9_meeting_id", "leg_turn_seq", "wit_turn_seq", "direction", "leg_is_chair",
           "leg_speaker_label_raw", "leg_text", "wit_speaker_label_raw"]].copy()
    s["rules"] = [";".join(r) for r in procedural_rules(s["leg_text"].tolist())]
    s["hand_label"] = ""
    s["hand_note"] = ""
    info = {"frame_meetings": int(t["v9_meeting_id"].nunique()), "excluded_meetings": len(excl),
            "sample_frame_dyads": int(len(d)), "flagged": int(len(flagged)),
            "flagged_share": round(len(flagged) / max(len(d), 1), 4),
            "flagged_chair": int(flagged["leg_is_chair"].sum()),
            "chair_dyads": int(d["leg_is_chair"].sum())}
    if out_csv:
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
        s.to_csv(out_csv, index=False)
        Path(str(out_csv) + ".frame.json").write_text(json.dumps(info, indent=1))
    return s


def _main(argv: Sequence[str]) -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="build dyads from enriched turns parquet")
    b.add_argument("turns", nargs="+")
    b.add_argument("--out", default=str(OUT_DIR / "dyads.parquet"))
    b.add_argument("--meetings", default=str(V10 / "interim" / "pipeline" / "meetings" / "meetings.parquet"),
                   help="meetings table for the meeting-level columns")
    b.add_argument("--no-meetings", action="store_true", help="carry only meeting-level columns present in the turns")
    b.add_argument("--chunk-turns", type=int, default=300_000)
    b.add_argument("--exclude-after-end-marker", action="store_true",
                   help="turns printed after a meeting-end marker break adjacency (default: kept, flagged)")
    b.add_argument("--memory-limit", default="6GB")
    lg = sub.add_parser("legacy", help="write the legacy v9 string-sort dyads")
    lg.add_argument("--out", default=str(OUT_DIR / "dyads_16_22_v9_legacy_rebuild.parquet"))
    vf = sub.add_parser("verify-legacy", help="compare legacy rebuild with published v9 dyads")
    vf.add_argument("--n", type=int, default=100)
    vf.add_argument("--out", default=str(OUT_DIR / "verify_legacy_v9.json"))
    ps = sub.add_parser("procedural-sample")
    ps.add_argument("--out", default=str(OUT_DIR / "procedural_precision_sample.csv"))
    ps.add_argument("--round", type=int, default=1)
    a = ap.parse_args(argv)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if a.cmd == "build":
        mp = None if a.no_meetings else a.meetings
        if mp is not None and not Path(mp).exists():
            raise SystemExit(f"meetings table not found: {mp} (pass --meetings PATH or --no-meetings)")
        st = build_dyads_file(a.turns, a.out, meetings_path=mp, chunk_turns=a.chunk_turns,
                              exclude_after_end_marker=a.exclude_after_end_marker, memory_limit=a.memory_limit)
        print(json.dumps(st, indent=1, ensure_ascii=False))
        if st["meeting_cols_missing"]:
            print("WARNING: meeting-level columns missing from the output:", st["meeting_cols_missing"], file=sys.stderr)
    elif a.cmd == "legacy":
        print(json.dumps(build_legacy_v9_dyads_file(a.out), indent=1))
    elif a.cmd == "verify-legacy":
        r = verify_legacy_v9(a.n)
        Path(a.out).write_text(json.dumps(r, indent=1, ensure_ascii=False, default=str))
        print(json.dumps({k: v for k, v in r.items() if k not in ("per_meeting", "meeting_ids")}, indent=1))
    elif a.cmd == "procedural-sample":
        s = make_procedural_sample(out_csv=Path(a.out), round_no=a.round)
        print(len(s), "rows ->", a.out)
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
