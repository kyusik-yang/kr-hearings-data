"""party_timeline.py - date-indexed party affiliation and ruling status (v10 pipeline component).

Public interface (columns in docs/CODEBOOK.md)
    enrich(turns, meetings) -> turns + party, party_lineage, party_camp, is_satellite,
        ruling_status, presidency_state, president, president_party, party_method
        ('person_spell' | 'label_lineage' | null), plus president_last_party, acting_president, is_speaker_nonpartisan,
        party_before_speaker, ruling_null_reason, party_spell_id, party_basis, party_uncertain,
        party_uncertain_reason, party_unconfirmed_after_gap (the date lies in an uncertainty window
        of the member, see party_uncertainty.parquet: party_uncertain when the minutes bound a party
        change by two dates but do not date it or a later record contradicts an undocumented
        stretch; party_unconfirmed_after_gap when no record of the member follows a plenary whose
        report items cannot be read).
    build(write=True)       -> harvest events, build person-date spells, write interim tables.
    evaluate(write=True)    -> validation tables (seat counts, end-of-term agreement, coverage,
        ruling distribution, spot check).

Sources (all local; no request to record.assembly.go.kr)
    Plenary minutes 【보고사항】 items (교섭단체 소속의원 명부 제출 / 가입 / 제적, 의원 당적 변경,
    교섭단체 구성 / 해체 / 명칭 변경, 통지, 의원 등록 / 의석 승계 / 사직 / 퇴직 / 자격상실 / 사망) from
      - crawled viewer pages  v10/raw/viewer/view/{bucket}/{id}.html.gz (16, 17, 19-22대),
        falling back to the task-05 polite-fetch cache (v10/raw/05_members/cache) when a page
        is not crawled yet;
      - crawled HWP files     v10/raw/hwp/{bucket}/{id}.hwp (18대; any meeting without XML),
        read with hwp_parser.extract_paragraphs (pipeline component; minimal fallback below).
    Committee-assignment tables of the same reports carry each member's 교섭단체 on a date; they
    are kept as observations (validation, not events).
    Member-term seat spans and election party: Open API 의원이력 (nfzegpkvaclgtscxt, former
    members; nexgtxtmaamffofof, sitting members) as downloaded by the legislators component to
    v10/interim/pipeline/legislators/api (read only), ALLNAMEMBER, members_term_16_22.parquet.
    President calendar and party lineage: v10/interim/president_calendar.csv, party_lineage.csv
    (re-verified against the saved sources by verify_calendar / verify_lineage).

Rules (researcher decisions of 2026-09-25 plus task specification)
    party            party on speech_date from the member's person spell (formal label, renames
                     and mergers applied), else the election party carried through the lineage.
    ruling_status    'ruling' if the party's camp (satellite -> its main party) equals the
                     president's party on that date, 'opposition' for any other party,
                     'independent' for 무소속, NULL when presidency_state is 'acting' (and when
                     the member's party is unknown). A suspended president's party stays ruling.
                     Partyless president (researcher decision 6, 2026-09-26; windows 김대중
                     2002-05-06..2003-02-24, 노무현 2003-09-29..2004-05-19 and 2007-02-28..
                     2008-02-24): the president's most recent party, carried through its lineage
                     renames / mergers to the speech date, counts as ruling (e.g. 열린우리당 ->
                     대통합민주신당 from 2007-08-20 -> 통합민주당 from 2008-02-17).
    presidency_state 'normal' (president in office with a party, exercising powers),
                     'partyless' (president in office without a party: president_party NULL,
                     president_last_party = his most recent party), 'suspended' (impeachment
                     suspension, acting president; takes precedence over 'partyless', so the
                     2004-03-12..05-14 suspension of the partyless 노무현 is 'suspended' with
                     president_last_party set), 'acting' (office vacant after a removal, with an
                     acting president). There is no 'vacant' value: every calendar row after a
                     removal names an acting president (load_calendar refuses a row without one);
                     dates outside the calendar get NULL.
    president_last_party  the president's party on the date, or in a partyless window his most
                     recent party (calendar pres_last_party); NULL in acting windows.
    Speaker          the legally required 무소속 status (국회법 제20조의2) is kept (party=무소속,
                     ruling_status='independent') with is_speaker_nonpartisan=True and the party
                     held before in party_before_speaker.
"""
from __future__ import annotations

import bisect
import datetime as dt
import functools
import gzip
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

V10 = Path(__file__).resolve().parents[2]
REPO = V10.parent
INTERIM = V10 / "interim"
OUT = INTERIM / "pipeline" / "party_timeline"
RAW_VIEW = V10 / "raw" / "viewer" / "view"
RAW_HWP = V10 / "raw" / "hwp"
CACHE05 = V10 / "raw" / "05_members" / "cache"
SOURCES05 = V10 / "raw" / "05_members" / "sources"
LEG_API = INTERIM / "pipeline" / "legislators" / "api"
CRAWL_DB = INTERIM / "crawl_state.sqlite"
SEED = 8374

TERM_START = {16: "2000-05-30", 17: "2004-05-30", 18: "2008-05-30", 19: "2012-05-30",
              20: "2016-05-30", 21: "2020-05-30", 22: "2024-05-30"}
TERM_END = {16: "2004-05-29", 17: "2008-05-29", 18: "2012-05-29", 19: "2016-05-29",
            20: "2020-05-29", 21: "2024-05-29", 22: "2028-05-29"}
TERM_SEATS = {16: 273, 17: 299, 18: 299, 19: 300, 20: 300, 21: 300, 22: 300}
INDEP = "무소속"
NON_GROUP = "비교섭"   # '어느 교섭단체에도 속하지 아니하는 의원'
SATELLITES = {"더불어시민당", "미래한국당", "국민의미래", "더불어민주연합"}

# ----------------------------------------------------------------------------- text helpers

WS = re.compile(r"\s+")
HANJA_CHARS = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")


def nfkc(s):
    return unicodedata.normalize("NFKC", s or "")


def nows(s):
    return WS.sub("", nfkc(s).replace("\xa0", " "))


# hanja characters that occur in 16-17대 report captions and table headers -> hangul
_HJ = ("交교涉섭團단體체所소屬속議의員원名명簿부提제出출加가入입除제籍적構구成성解해變변更경稱칭代대表표"
       "選선任임席석承승繼계登등錄록辭사職직書서退퇴資자格격喪상失실死사亡망黨당國국會회常상特특別별補보"
       "幹간事사改개委위長장通통知지報보告고項항決결定정當당無무效효擧거區구政정由유年연月월日일事사件건")
HANJA_KO = {_HJ[i]: _HJ[i + 1] for i in range(0, len(_HJ), 2)}


def hangulize_caption(s):
    return "".join(HANJA_KO.get(ch, ch) for ch in nfkc(s))


def norm_caption(s):
    """'【報告事項】○交涉團體 加入' -> '교섭단체가입'."""
    t = nows(hangulize_caption(s))
    t = re.sub(r"【[^】]*】", "", t)
    t = re.sub(r"^[^】]{0,8}】", "", t)          # '報告事項】常任委員辭任및補任' (opening bracket missing)
    t = t.lstrip("◯○◦•·-")
    return t


@functools.lru_cache(maxsize=None)
def norm_party(s):
    """Party / 교섭단체 label as printed -> canonical key text (no spaces, no abbreviation list)."""
    t = nfkc(s).replace("\xa0", " ")
    t = re.sub(r"\((?:약칭|민주당,|통합당)[^)]*\)", "", t)       # '미래통합당(약칭:통합당)'
    t = re.sub(r"\([^)]*(?:더민주|약칭)[^)]*\)", "", t)          # '더불어민주당(민주당, 더민주)'
    t = nows(t)
    if not t:
        return ""
    t = re.sub(r"\(\d+[인명人]\)$", "", t)                   # '새누리당(7인)'
    h0 = nows(hangulize_caption(t))
    if re.fullmatch(r"(어느교섭단체에도속하지아니하는의원|비교섭단체|어느교섭단체에도속하지않는의원)(\([^)]*\))?", h0):
        return NON_GROUP
    t = re.sub(r"\((?:약칭:?)?[가-힣]{1,6}\)$", "", t)       # '미래통합당(통합)', '(전진당)'
    h = len(t) // 2
    if len(t) % 2 == 0 and h >= 2 and t[:h] == t[h:]:      # '더불어민주당더불어민주당' (merged cells)
        t = t[:h]
    return t


# ----------------------------------------------------------------------------- dates

DATE_FULL = re.compile(r"(\d{4})\s*(?:[.년]\s*|\s+)(\d{1,2})\s*[.월]\s*(\d{1,2})")   # '2024\n5. 8.' too
DATE_2Y = re.compile(r"[’'‘]\s*(\d{2})\s*\.\s*(\d{1,2})\s*\.\s*(\d{1,2})")
DATE_MD = re.compile(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일")


def _mk(y, m, d):
    try:
        return dt.date(int(y), int(m), int(d))
    except ValueError:
        return None


def parse_date(s, ref: dt.date | None = None, fwd_limit: dt.date | None = None):
    """Parse '2024. 3. 17.', '2000년12월28일자', '(10월 13일)' (year from ref), "'05. 9. 29".
    A month-day date takes the year of ref unless that puts it after the forward limit, in which
    case it is put in the previous year. The forward limit is `fwd_limit` when given (the next
    plenary of the term + 3 days, or the term end for the last plenary: minutes append report items
    received after the sitting, e.g. conf 27127 of 2004-03-12 lists items of 5월3일..5월27일 2004),
    else ref + 3 days."""
    if s is None:
        return None
    t = nfkc(str(s))
    m = DATE_FULL.search(t)
    if m:
        return _mk(*m.groups())
    m = DATE_2Y.search(t)
    if m:
        return _mk(2000 + int(m.group(1)), m.group(2), m.group(3))
    m = DATE_MD.search(t)
    if m and ref is not None:
        lim = max(fwd_limit, ref + dt.timedelta(days=3)) if fwd_limit is not None else ref + dt.timedelta(days=3)
        d = _mk(ref.year, m.group(1), m.group(2))
        if d is not None and d > lim:
            d = _mk(ref.year - 1, m.group(1), m.group(2))
        return d
    return None


def is_date_line(s):
    t = nows(s)
    if not t or len(t) > 24:
        return False
    t = t.strip("()").replace("자", "").replace("현재", "")
    return bool(re.fullmatch(r"(\d{4}[.년]?\d{1,2}[.월]\d{1,2}[.일]?|[’'‘]\d{2}\.\d{1,2}\.\d{1,2}\.?|\d{1,2}월\d{1,2}일)", t))


def iso(d):
    return d.isoformat() if isinstance(d, dt.date) else (d if d else None)


def day_before(s):
    return (dt.date.fromisoformat(s) - dt.timedelta(days=1)).isoformat()


def day_after(s):
    return (dt.date.fromisoformat(s) + dt.timedelta(days=1)).isoformat()


# ============================================================================= calendar

PRESIDENCY_STATES = ("normal", "partyless", "suspended", "acting")


def load_calendar(path=INTERIM / "president_calendar.csv"):
    """Interval rows (start, end, presidency_state, president, acting_president, president_party,
    president_last_party, last_party_since, president_elected_from).

    president_last_party: the formal party when the president has one; in a partyless row his most
    recent party, derived from the earlier rows of the same president and checked against the
    CSV column pres_last_party when that column is present (ValueError on a mismatch).
    last_party_since: the last day on which the president held president_last_party (the lineage
    successor on a later date is taken from that day on). A 'vacant_after_removal' row must name
    an acting president (ValueError otherwise), so presidency_state is never 'vacant'."""
    cal = pd.read_csv(path, dtype=str).fillna("")
    if "pres_last_party" not in cal.columns:
        cal["pres_last_party"] = ""
    cal = cal.sort_values("start").reset_index(drop=True)
    rows = []
    last = {}                           # president -> (party, last day held)
    for r in cal.to_dict("records"):
        st = r["status"]
        pres, party = r["president"] or None, r["pres_party_formal"] or None
        if st == "vacant_after_removal":
            if not r["acting_president"]:
                raise ValueError(f"calendar row {r['start']}: office vacant without an acting president")
            state = "acting"
        elif st == "suspended_impeachment":
            state = "suspended"
        elif st == "in_office":
            state = "normal" if party else "partyless"
        else:
            raise ValueError(f"unknown calendar status {st!r}")
        last_party, since = None, None
        if state != "acting":
            if pres is None:
                raise ValueError(f"calendar row {r['start']}: status {st!r} without a president")
            if party:
                last_party, since = party, None
                last[pres] = (party, r["end"] or "9999-12-31")
            else:
                if pres not in last:
                    raise ValueError(f"calendar row {r['start']}: partyless {pres} has no earlier party row")
                last_party, since = last[pres]
                if r["pres_last_party"] and r["pres_last_party"] != last_party:
                    raise ValueError(f"calendar row {r['start']}: pres_last_party {r['pres_last_party']!r} "
                                     f"differs from the derived {last_party!r}")
        elif r["pres_last_party"]:
            raise ValueError(f"calendar row {r['start']}: pres_last_party set in an acting row")
        rows.append({"start": r["start"], "end": r["end"] or "9999-12-31", "presidency_state": state,
                     "president": pres if state != "acting" else None,
                     "acting_president": r["acting_president"] or None,
                     "president_party": party,
                     "president_last_party": last_party,
                     "last_party_since": since,
                     "president_elected_from": r["pres_party_elected_from"] or None})
    out = pd.DataFrame(rows).sort_values("start").reset_index(drop=True)
    # contiguity: every row starts the day after the previous row ends
    for a, b in zip(out.itertuples(), out.iloc[1:].itertuples()):
        if day_after(a.end) != b.start:
            raise ValueError(f"calendar gap/overlap between {a.start}..{a.end} and {b.start}")
    return out


def presidency_on(date, cal=None):
    """dict(presidency_state, president, president_party, president_last_party, last_party_since,
    acting_president) for 'YYYY-MM-DD'; every value None for a missing date or a date outside the
    calendar."""
    cal = load_calendar() if cal is None else cal
    none = {"presidency_state": None, "president": None, "president_party": None, "president_last_party": None,
            "last_party_since": None, "acting_president": None}
    if not date:
        return none
    hit = cal[(cal.start <= date) & (cal.end >= date)]
    if hit.empty:
        return none
    r = hit.iloc[0]
    return {"presidency_state": r.presidency_state, "president": r.president,
            "president_party": r.president_party, "president_last_party": r.president_last_party,
            "last_party_since": r.last_party_since, "acting_president": r.acting_president}


OFFICIAL_SOURCE_PREFIXES = ("pa_", "korea_kr", "ktv_")   # saved official government pages
_WIKI_PATH = re.compile(r"raw wikitext saved (v10/raw/05_members/sources/wiki/.+?\.wiki)\)")


def _source_files(src):
    """Saved local copies for the URLs cited in a calendar / lineage 'source' cell, official
    government pages (pa.go.kr, korea.kr, ktv.go.kr) first."""
    files = []
    for m in re.finditer(r"pa\.go\.kr/online_contents/inauguration/president(\d+)\.jsp", src or ""):
        files.append(SOURCES05 / f"pa_inaug{m.group(1)}.html")
    for m in re.finditer(r"pa\.go\.kr/online_contents/president/history(\d+)\.jsp", src or ""):
        files.append(SOURCES05 / f"pa_history{m.group(1)}.html")
    for m in re.finditer(r"korea\.kr/news/policyNewsView\.do\?newsId=(\d+)", src or ""):
        files.append(SOURCES05 / f"korea_kr_{m.group(1)}.html")
    for m in re.finditer(r"ktv\.go\.kr/program/again/view\?content_id=(\d+)", src or ""):
        # KTV (한국정책방송원, government broadcaster); saved by this component, v10/raw is read-only
        files.append(OUT / "sources" / f"ktv_{m.group(1)}.html")
    for m in _WIKI_PATH.finditer(src or ""):
        files.append(REPO / m.group(1))
    return files


def _source_text(f: Path):
    t = f.read_text(encoding="utf-8", errors="replace")
    if f.suffix == ".html":
        from lxml import html as LH
        t = LH.fromstring(t).text_content()
    else:
        t = re.sub(r"<ref[^>]*>.*?</ref>|<ref[^>]*/>", "", t, flags=re.S)
        t = re.sub(r"\[\[(?:[^]|]*\|)?([^]]*)\]\]", r"\1", t)
    return WS.sub(" ", t)


def _date_patterns(d):
    y, m, dd = d.split("-")
    mi, di = int(m), int(dd)
    return [rf"{y}\s*년\s*0?{mi}\s*월\s*0?{di}\s*일", rf"{y}\s*\.\s*0?{mi}\s*\.\s*0?{di}(?!\d)",
            rf"{y}-{m}-{dd}"]


def _find_date(text, d, allow_md=False):
    pats = _date_patterns(d)
    if allow_md:  # '5월 20일' with the year given by the surrounding section
        _, m, dd = d.split("-")
        pats.append(rf"(?<!\d){int(m)}\s*월\s*{int(dd)}\s*일")
    for p in pats:
        mm = re.search(p, text)
        if mm:
            return text[max(0, mm.start() - 60): mm.end() + 60].strip()
    return None


def verify_calendar(cal_path=INTERIM / "president_calendar.csv"):
    """Re-check every calendar row against the saved copies of the sources it cites.
    start: the date (or the day before, for a start that follows an attested event such as the
      2004-05-15 row after the 2004-05-14 decision) must appear in a cited file; official pages
      are searched first. A bare month-day match ('5월 20일') is accepted only in wikitext and
      reported as match='month_day'.
    end: rows are contiguous, so an end is derived from the next row's start (match='derived');
      an end that is also printed in a source is reported as found.
    president / party / last_party: the president's name, the formal party and (partyless rows)
      the most recent party must occur in a cited file.
    Returns one row per check."""
    cal = pd.read_csv(cal_path, dtype=str).fillna("")
    out = []
    rows = cal.to_dict("records")
    for i, r in enumerate(rows):
        files = _source_files(r["source"])
        texts = {}
        for f in files:
            try:
                texts[f.name] = _source_text(f)
            except FileNotFoundError:
                texts[f.name] = None
        miss = ";".join(k for k, v in texts.items() if v is None)
        for which, d in (("start", r["start"]), ("end", r["end"])):
            if not d:
                continue
            hit, how, where = None, None, None
            cands = [(d, "exact")] + ([(day_before(d), "day_before_start")] if which == "start" else
                                      [(day_after(d), "day_after_end")])
            for strict in (True, False):
                for dd, lab in cands:
                    for fn, tx in texts.items():
                        if tx is None:
                            continue
                        if strict:
                            p = _find_date(tx, dd)
                        elif fn.endswith(".wiki"):
                            p = _find_date(tx, dd, allow_md=True)
                        else:
                            p = None
                        if p:
                            hit, how, where = p, lab if strict else lab + "_month_day", fn
                            break
                    if hit:
                        break
                if hit:
                    break
            derived = which == "end" and i + 1 < len(rows) and day_after(d) == rows[i + 1]["start"]
            if derived and how and how.endswith("month_day"):
                hit, how, where = None, None, None      # contiguity is the stronger evidence
            out.append({"row_start": r["start"], "president": r["president"], "status": r["status"],
                        "check": which, "value": d, "found": bool(hit) or derived,
                        "match": how or ("derived_next_row_start" if derived else None),
                        "source_file": where, "passage": hit,
                        "official_source": bool(where) and where.startswith(OFFICIAL_SOURCE_PREFIXES),
                        "files_cited": ";".join(texts), "files_missing": miss})
        for which, val in (("president", r["president"]), ("acting_president", r["acting_president"]),
                           ("party", r["pres_party_formal"]), ("last_party", r.get("pres_last_party", ""))):
            if not val:
                continue
            where = next((fn for fn, tx in texts.items() if tx and val in tx), None)
            if where is None and which in ("party", "last_party"):   # '국민회의' is printed for 새정치국민회의
                alias = {v: k for k, v in Lineage.ALIASES.items()}.get(val)
                where = next((fn for fn, tx in texts.items() if tx and alias and alias in tx), None)
            out.append({"row_start": r["start"], "president": r["president"], "status": r["status"],
                        "check": which, "value": val, "found": where is not None,
                        "match": "name_in_source" if where else None, "source_file": where, "passage": None,
                        "official_source": bool(where) and where.startswith(OFFICIAL_SOURCE_PREFIXES),
                        "files_cited": ";".join(texts), "files_missing": miss})
    return pd.DataFrame(out)


# ============================================================================= lineage

def _base_label(label):
    return re.sub(r"\(\d{4}\)$", "", label or "")


def _shift(d, days):
    return (dt.date.fromisoformat(d) + dt.timedelta(days=days)).isoformat()


class Lineage:
    """party_lineage.csv with date-aware label resolution.

    key(label, date, historic)  -> lineage row label ('민주당' on 2006-01-01 -> '민주당(2005)') or None.
        A label printed on `date` names a lineage row only when `date` lies in the row's interval
        widened by PRE_TOL days before label_from (a 교섭단체 renamed before the party's registration)
        and STALE_TOL days after label_to (an old label printed shortly after a rename / merger).
        historic=True drops the upper bound: the label was recorded earlier than `date` (election
        party, the list party of a 의석승계 notice). Without a date there is no bound. So '미래한국당'
        in 2008 and '새누리당' / '민주당' used for new parties in 2016-2017 are not the lineage rows.
    formal_on(label, date, since) -> the formal label on `date` after renames / mergers (successor
        chain) of the row the label named on `since` (default `date`), INDEP when the party ended
        without successor; also the chain used
    label_at(label, date)   -> a label recorded in another period (election party, notice party) as
        the formal label on `date`: forward through successors, or back through renames when the
        label was adopted after `date` ('자유한국당' for a 2016-05-30 seat start -> 새누리당)
    family(label, date)     -> terminal successor of the chain (organisational family)
    register_notice_aliases(ntr) -> a rename notified to the Assembly whose new label is no lineage
        row ('중도통합민주당' -> '민주당' 2007-08-20) makes the new label name the old row from the
        notice date to the row's label_to + STALE_TOL
    """

    ALIASES = {"국민회의": "새정치국민회의", "한국신당": "희망의한국신당", "민국당": "민주국민당",
               "자민련": "자유민주연합", "열린우리당주비위원회": "열린우리당",
               "국민참여통합신당주비위원회": "열린우리당", "통합당": "미래통합당",
               "진보정의당": "정의당", "녹색정의당": "정의당", "새진보연합": "기본소득당",
               "민중당": "진보당", "새미래민주당": "새로운미래"}
    PRE_TOL = 45
    STALE_TOL = 30

    def __init__(self, path=INTERIM / "party_lineage.csv"):
        self.df = pd.read_csv(path, dtype=str).fillna("")
        self.rows = {r["label"]: r for r in self.df.to_dict("records")}
        self.by_base = defaultdict(list)
        for r in self.df.to_dict("records"):
            self.by_base[_base_label(r["label"])].append(r)
        self.dated_alias = defaultdict(list)       # label -> [(from, to, key)]
        self._kc = {}

    def register_notice_aliases(self, ntr):
        """ntr: {old_norm: [(date, new_norm, kind, conf_num)]} (notice_transitions). Returns the
        aliases added as (new_label, from, to, key)."""
        added = []
        for old, v in (ntr or {}).items():
            for d, new, kind, _cn in v:
                if kind not in ("party_rename", "group_rename") or not new or new in (INDEP, NON_GROUP):
                    continue
                k = self.key(old, d)
                if k is None or self.key(new, d) is not None:
                    continue
                if self.rows[k]["successor_from"] and self.rows[k]["successor_from"] <= d:
                    continue
                lt = self.rows[k]["label_to"]
                to = _shift(lt, self.STALE_TOL) if lt else "9999-12-31"
                if (d, to, k) not in self.dated_alias[new]:
                    self.dated_alias[new].append((d, to, k))
                    added.append((new, d, to, k))
        if added:
            self._kc = {}
        return added

    def _row_ok(self, r, date, historic):
        a, b = r["label_from"], r["label_to"]
        if a and date < _shift(a, -self.PRE_TOL):
            return False
        if not historic and b and date > _shift(b, self.STALE_TOL):
            return False
        return True

    def key(self, label, date=None, historic=False):
        lab = norm_party(label)
        if not lab or lab in (NON_GROUP, INDEP):
            return None
        ck = (lab, date, historic)
        if ck in self._kc:
            return self._kc[ck]
        self._kc[ck] = v = self._key(lab, date, historic)
        return v

    def _key(self, lab, date, historic):
        if date:
            for a, b, k in self.dated_alias.get(lab, ()):
                if a <= date <= b:
                    return k
        if lab in self.rows and _base_label(lab) != lab:
            return lab                                   # explicit lineage key, e.g. '민주당(2005)'
        cands = list(self.by_base.get(lab, []))
        if not cands and lab in self.ALIASES:
            cands = self.by_base.get(self.ALIASES[lab], [])
            if date:
                # an alias names the target only within two years of the target's active interval
                # ('국민회의' 2016 is not 새정치국민회의 1995-2000)
                def near(r):
                    a = r["label_from"] or "0000-01-01"
                    b = r["label_to"] or "9999-12-31"
                    lo = (dt.date.fromisoformat(a) - dt.timedelta(days=730)).isoformat() if a > "0001" else a
                    hi = (dt.date.fromisoformat(b) + dt.timedelta(days=730)).isoformat() if b < "9999" else b
                    return lo <= date <= hi
                cands = [r for r in cands if near(r)]
            if not cands:
                return None
            if len(cands) == 1 or not date:
                return cands[0]["label"]
        if not cands:
            return None
        if not date:
            return cands[0]["label"]
        cands = [r for r in cands if self._row_ok(r, date, historic)]
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]["label"]

        def dist(r):
            a, b = r["label_from"] or "0000-01-01", r["label_to"] or "9999-12-31"
            if a <= date <= b:
                return 0
            return min(abs((dt.date.fromisoformat(date) - dt.date.fromisoformat(x)).days)
                       for x in (a, b) if x not in ("0000-01-01", "9999-12-31"))
        return min(cands, key=dist)["label"]

    def formal_on(self, label, date, since=None, historic=False):
        """Follow renames / mergers that took effect on or before `date`, starting from the row
        the label named on `since` (default `date`).
        Returns (formal_label, chain, ended): the printed label when no transition applies, the
        successor's label after a transition, INDEP (ended=True) when the party ended without a
        successor. chain = [(from_key, to_label, from_date, kind), ...]."""
        lab = norm_party(label)
        if lab in ("", NON_GROUP, INDEP):
            return lab, [], False
        k = self.key(lab, since or date, historic=historic)
        if k is None:
            return lab, [], False
        chain, seen = [], set()
        while True:
            r = self.rows[k]
            if r["successor_from"] and date and date >= r["successor_from"]:
                if r["successor"] and r["successor"] in self.rows and r["successor"] not in seen:
                    chain.append((k, r["successor"], r["successor_from"], r["kind"]))
                    seen.add(k)
                    k = r["successor"]
                    continue
                if not r["successor"]:
                    chain.append((k, INDEP, r["successor_from"], r["kind"]))
                    return INDEP, chain, True
            break
        return (_base_label(k) if chain else lab), chain, False

    def label_at(self, label, date):
        """(formal label on `date`, how) for a label recorded in another period.
        how: 'same' | 'successor' (renames / mergers applied) | 'backdated_rename' (the label was
        adopted after `date` by renaming the returned party) | 'postdated_unresolved' (adopted
        after `date` by a merger or founding: the label is returned unchanged) | 'unknown' (no
        lineage row)."""
        lab = norm_party(label)
        if lab in ("", NON_GROUP, INDEP):
            return lab, "same"
        if self.key(lab, date, historic=True) is not None:
            f, chain, _ = self.formal_on(lab, date, historic=True)
            return f, ("successor" if chain else "same")
        later = [r for r in self.by_base.get(lab, []) if r["label_from"] and r["label_from"] > date]
        if not later:
            return lab, "unknown"
        cur = min(later, key=lambda r: r["label_from"])
        for _ in range(10):
            preds = [p for p in self.rows.values()
                     if p["successor"] == cur["label"] and p["successor_from"] == cur["label_from"]]
            if len(preds) != 1 or preds[0]["kind"] != "rename":
                return lab, "postdated_unresolved"
            p = preds[0]
            if not p["label_from"] or p["label_from"] <= date:
                return _base_label(p["label"]), "backdated_rename"
            cur = p
        return lab, "postdated_unresolved"

    def exceptions(self, label, date=None, historic=False):
        """{member name: party} from the row's individual_exceptions text, e.g. '... 정혜경, 전종덕
        (진보당), 용혜인 (기본소득당)': names without their own parenthesis take the next one."""
        k = self.key(label, date, historic=historic)
        if k is None:
            return {}
        txt = self.rows[k]["individual_exceptions"] or ""
        body = txt.split(":", 1)[1] if ":" in txt else ""
        out, pending = {}, []
        for m in re.finditer(r"([가-힣]{2,4})\s*(?:\(([^)]+)\))?", body):
            pending.append(m.group(1))
            if m.group(2):
                for nm in pending:
                    out[nm] = m.group(2).strip()
                pending = []
        return out

    def org(self, label, date, since=None):
        """Organisation id on `date`: lineage key after renames / mergers, else the label."""
        f, _, _ = self.formal_on(label, date, since=since)
        if f in ("", NON_GROUP, INDEP):
            return f or None
        return self.key(f, date) or f

    def family(self, label, date=None):
        lab = norm_party(label)
        if lab in ("", NON_GROUP):
            return None
        if lab == INDEP:
            return INDEP
        k = self.key(lab, date)
        if k is None:
            return lab
        seen = set()
        while self.rows[k]["successor"] and self.rows[k]["successor"] in self.rows and k not in seen:
            seen.add(k)
            k = self.rows[k]["successor"]
        return _base_label(k)

    def satellite_of(self, label, date=None):
        k = self.key(label, date)
        if k is None:
            return None
        return self.rows[k]["satellite_of"] or None

    def camp(self, label, date):
        """Main party for a satellite (before its merger), else the formal label itself."""
        f, _, _ = self.formal_on(label, date)
        if f in ("", NON_GROUP, INDEP):
            return f or None
        sat = self.satellite_of(f, date)
        if sat:
            return self.formal_on(sat, date)[0]
        return f

    def same_party(self, a, b, date):
        """True when labels a and b, both as printed on `date`, denote the same organisation."""
        if not a or not b:
            return False
        return self.org(a, date) == self.org(b, date)


def verify_lineage(path=INTERIM / "party_lineage.csv", notices: pd.DataFrame | None = None):
    """Re-check label_from / label_to / successor_from of every lineage row in the saved sources it
    cites (a label_to that is the day before successor_from counts as derived). When `notices`
    (party rename / merger notices harvested from plenary 보고사항 ◯통지 and ◯교섭단체 명칭 변경)
    is given, the official notice date is listed next to the lineage date."""
    lin = pd.read_csv(path, dtype=str).fillna("")
    out = []
    for r in lin.to_dict("records"):
        files = _source_files(r["source"])
        texts = {}
        for f in files:
            try:
                texts[f.name] = _source_text(f)
            except FileNotFoundError:
                texts[f.name] = None
        for col in ("label_from", "label_to", "successor_from"):
            d = r[col]
            if not d:
                continue
            hit, where, how = None, None, None
            for fn, tx in texts.items():
                if tx:
                    p = _find_date(tx, d)
                    if p:
                        hit, where, how = p, fn, "exact"
                        break
            if not hit and col == "label_to" and r["successor_from"] and day_after(d) == r["successor_from"]:
                how = "derived_day_before_successor_from"
            out.append({"label": r["label"], "field": col, "date": d, "found": bool(hit),
                        "match": how, "source_file": where, "passage": hit,
                        "files_missing": ";".join(k for k, v in texts.items() if v is None)})
    df = pd.DataFrame(out)
    if notices is not None and len(notices):
        nt = notices.copy()
        rows = []
        for r in lin.to_dict("records"):
            if not r["successor"]:
                continue
            a, b = norm_party(_base_label(r["label"])), norm_party(r["successor"])
            m = nt[(nt.old_party.map(norm_party) == a) & (nt.new_party.map(norm_party) == norm_party(_base_label(b)))]
            for x in m.to_dict("records"):
                rows.append({"label": r["label"], "field": "successor_from(notice)", "date": x["date"],
                             "found": True, "match": "plenary_notice",
                             "source_file": f"conf_num {x['conf_num']}", "passage": x["raw"],
                             "lineage_date": r["successor_from"],
                             "differs_days": (dt.date.fromisoformat(x["date"]) - dt.date.fromisoformat(r["successor_from"])).days
                             if r["successor_from"] else None})
        if rows:
            df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    return df


# ============================================================================= report items
# A plenary page is turned into a flat token stream in document order:
#   ('head', text)        p.tit (XML) / a short paragraph starting with ◯ or ○ (HWP)
#   ('label', text)       p.tit_sm with text (XML)
#   ('line', raw_text)    any other text block, raw spacing kept (names are split on 2+ spaces)
#   ('table', caption, header[list], rows[list[list]])  rowspan / colspan expanded
# Report items are cut from the stream at heads / captions whose normalised caption matches
# ITEM_KINDS; the item keeps every following token up to the next head.

ITEM_KINDS = [  # (kind, regex on norm_caption) - checked in order
    ("roster", r"^교섭단체소속의원명부제출"),
    ("join", r"^교섭단체가입"),
    ("leave", r"^교섭단체소속의원제적|^교섭단체소속의원탈퇴"),
    ("switch", r"^(국회)?의원당적변경"),
    ("group_form", r"^교섭단체구성"),
    ("group_dissolve", r"^교섭단체해체"),
    ("group_rename", r"^교섭단체(명칭|명)변경"),
    ("notice", r"^통지"),
    ("enter", r"^(의원등록|의석승계)"),
    ("exit", r"^(의원사직(?!서)|의원퇴직|의원자격상실|의원사망|의원직상실|의원당선무효)"),
    ("committee", r"^(상임위원|특별위원|위원)(사임및보임|사임|보임|선임|개선)|^간사(선임|개선|사임)|"
                  r"^(상임|특별)?위원장(선임|사임)|^소위원장(선임|개선)|^안건조정위원장선임"),
]
ITEM_RE = [(k, re.compile(p)) for k, p in ITEM_KINDS]


def item_kind(caption):
    c = norm_caption(caption)
    for k, rx in ITEM_RE:
        if rx.search(c):
            return k
    return None


def _cell_text(el):
    """Text of a DOM cell with <br> as newline and nbsp as space (raw spacing kept)."""
    parts = []
    for node in el.iter():
        if node.tag == "br":
            parts.append("\n")
        if node.text and node.tag not in ("br",):
            parts.append(node.text)
        if node is not el and node.tail:
            parts.append(node.tail)
    return "".join(parts).replace("\xa0", " ").strip()


def expand_table(tbl):
    """lxml <table> -> (caption, header, rows) with rowspan/colspan expanded."""
    cap = tbl.xpath("./caption")
    caption = WS.sub(" ", _cell_text(cap[0])).strip() if cap else ""
    trs = tbl.xpath("./tr|./thead/tr|./tbody/tr")
    header, body = [], []
    for tr in trs:
        cells = tr.xpath("./th|./td")
        if cells and all(c.tag == "th" for c in cells) and not body:
            header = []
            for c in cells:
                try:
                    cs = max(1, int(c.get("colspan") or 1))
                except ValueError:
                    cs = 1
                header.extend([WS.sub(" ", _cell_text(c)).strip()] * cs)
        else:
            body.append(cells)
    grid, nrow = {}, len(body)
    ncol = len(header)
    for r, cells in enumerate(body):
        c = 0
        for td in cells:
            while (r, c) in grid:
                c += 1
            try:
                rs = max(1, int(td.get("rowspan") or 1))
                cs = max(1, int(td.get("colspan") or 1))
            except ValueError:
                rs, cs = 1, 1
            v = _cell_text(td)
            for dr in range(rs):
                for dc in range(cs):
                    grid[(r + dr, c + dc)] = v
            c += cs
        ncol = max(ncol, c)
    rows = [[grid.get((r, c)) for c in range(ncol)] for r in range(nrow)]
    return caption, header, rows


def _block_children(el):
    return el.xpath("./p|./div|./ul|./table|./ol|./section|./span[@class='spk_sub']")


def xml_tokens(page):
    """Token stream of the minutes footer (and of any table in the body) of a viewer page."""
    from lxml import html as LH
    if isinstance(page, (bytes, bytearray)):
        if page[:2] == b"\x1f\x8b":
            page = gzip.decompress(page)
        t = LH.fromstring(page)
    else:
        t = LH.fromstring(page.encode("utf-8"))
    toks = []

    def walk(el):
        for c in el:
            if not isinstance(c.tag, str):
                continue
            cls = (c.get("class") or "").split()
            if c.tag == "table":
                cap, head, rows = expand_table(c)
                toks.append(("table", cap, head, rows))
            elif c.tag == "p" and "tit" in cls:
                toks.append(("head", WS.sub(" ", _cell_text(c)).strip()))
            elif c.tag == "p" and "tit_sm" in cls:
                tx = _cell_text(c)
                if tx.strip():
                    toks.append(("label", tx))
            elif _block_children(c):
                if c.text and c.text.strip():
                    toks.append(("line", c.text.replace("\xa0", " ")))
                walk(c)
            else:
                tx = _cell_text(c)
                if tx.strip():
                    toks.append(("line", tx))
            if c.tail and c.tail.strip():
                toks.append(("line", c.tail.replace("\xa0", " ")))

    ft = t.xpath('//div[contains(concat(" ",normalize-space(@class)," ")," minutes_footer ")]')
    body_tables = t.xpath('//div[contains(concat(" ",normalize-space(@class)," ")," minutes_body ")]//table')
    for tb in body_tables:
        cap, head, rows = expand_table(tb)
        if item_kind(cap):
            toks.append(("head", "[body] " + cap))
            toks.append(("table", cap, head, rows))
    for f in ft:
        walk(f)
    return toks


# ------------------------------------------------------------------ HWP

def _hwp_paragraphs(data):
    """hwp_parser.extract_paragraphs when the pipeline component is importable, else a minimal
    reader (body text only, no table structure). Returns (paragraphs, reader_name)."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import hwp_parser  # noqa: WPS433 (pipeline component, may be edited concurrently)
        return hwp_parser.extract_paragraphs(data), "hwp_parser"
    except ImportError:
        return _minimal_hwp_paragraphs(data), "minimal"


def _minimal_hwp_paragraphs(data):
    import io
    import struct
    import zlib
    import olefile
    ole = olefile.OleFileIO(io.BytesIO(data))
    hdr = ole.openstream("FileHeader").read()
    compressed = bool(struct.unpack_from("<I", hdr, 36)[0] & 1)
    out = []
    secs = sorted([s for s in ole.listdir() if s[0] == "BodyText"], key=lambda s: int(s[1][7:]))
    for s in secs:
        buf = ole.openstream(s).read()
        if compressed:
            buf = zlib.decompress(buf, -15)
        i = 0
        while i + 4 <= len(buf):
            h = struct.unpack_from("<I", buf, i)[0]
            tag, size = h & 0x3FF, (h >> 20) & 0xFFF
            i += 4
            if size == 0xFFF:
                size = struct.unpack_from("<I", buf, i)[0]
                i += 4
            if tag == 67:
                ws = struct.unpack_from(f"<{size // 2}H", buf, i)
                txt, j = [], 0
                while j < len(ws):
                    ch = ws[j]
                    if ch < 32:
                        if ch in (1, 2, 3, 11, 12, 14, 15, 16, 17, 18, 21, 22, 23, 4, 5, 6, 7, 8, 9, 19, 20):
                            j += 8
                            if ch == 9:
                                txt.append("\t")
                            continue
                        if ch == 10:
                            txt.append("\n")
                        j += 1
                        continue
                    txt.append(chr(ch))
                    j += 1
                out.append({"text": "".join(txt), "in_table": False, "table_id": None, "row": None, "col": None})
            i += size
    return out


def hwp_tokens(data):
    """Token stream of an HWP minutes file (whole document; tables rebuilt from cell addresses,
    cells missing from a row are filled from the row above = rowspan)."""
    paras, reader = _hwp_paragraphs(data)
    toks = []
    cur_tid, cells = None, None

    def flush():
        if cur_tid is None or not cells:
            return
        nrow = max(r for r, _ in cells) + 1
        ncol = max(c for _, c in cells) + 1
        grid = []
        for r in range(nrow):
            row = []
            for c in range(ncol):
                if (r, c) in cells:
                    row.append("\n".join(cells[(r, c)]).strip())
                elif r > 0 and grid:
                    row.append(grid[-1][c])       # rowspan: value of the cell above
                else:
                    row.append(None)
            grid.append(row)
        header = [WS.sub(" ", x or "").strip() for x in grid[0]]
        toks.append(("table", "", header, grid[1:]))

    for p in paras:
        if p.get("in_table") and p.get("row") is not None:
            tid = p.get("outer_table_id", p.get("table_id"))
            if tid != cur_tid:
                flush()
                cur_tid, cells = tid, {}
            cells.setdefault((p["row"], p["col"]), []).append(p["text"])
            continue
        if cur_tid is not None:
            flush()
            cur_tid, cells = None, None
        tx = (p.get("text") or "").replace("\xa0", " ")
        if not tx.strip():
            continue
        st = tx.strip()
        if st[:1] in "◯○" and len(nows(st)) <= 40:
            toks.append(("head", st))
        elif st.startswith("【") and len(nows(st)) <= 40:
            toks.append(("head", st))
        else:
            toks.append(("line", tx))
    flush()
    return toks, reader


# ------------------------------------------------------------------ items

COLMAP = [  # header text (normalised, hangulized) -> field
    ("name", r"^(의원명|위원명|의원|성명|이름)$"),
    ("group", r"^(교섭단체|교섭단체명)$"),
    ("reason", r"^(사유|제적사유)$"),
    ("date", r"^(\S{0,4}연월일|일자|년월일|변경일|날짜)$"),         # '합당 연월일' (conf 36088)
    ("district", r"^(선거구|지역구)$"),
    ("party", r"^(소속정당|정당|정당명)$"),
    ("from", r"^변경전"),
    ("to", r"^변경후"),
    ("members", r"^(소속의원|소속의원명)$"),
    ("committee", r"^(위원회|위원회명)$"),
    ("resign", r"^(사임위원|사임의원)$"),
    ("appoint", r"^(보임위원|보임의원|선임위원|선임의원)$"),
    ("resign_cmt", r"^사임위원회$"),
    ("appoint_cmt", r"^보임위원회$"),
    ("n_label", r"^(명칭|정당명칭)$"),
    ("rep", r"^대표자$"),
    ("old_parties", r"^합당(한|된)구?정당"),
    ("survivor", r"^존속하는정당$"),
    ("absorbed", r"^흡수되는정당$"),
    ("change_item", r"^변경사항$"),
    ("old_reg", r"^기등록내용$"),
    ("new_reg", r"^변경등록내용$"),
]
COLMAP_RE = [(f, re.compile(p)) for f, p in COLMAP]


def header_fields(header):
    out = []
    for h in header:
        t = nows(hangulize_caption(h or ""))
        f = next((f for f, rx in COLMAP_RE if rx.search(t)), None)
        out.append(f)
    return out


def _as_head(tok):
    """A short line / label that starts with ◯ or ○ is an item heading when it names an item
    kind ('○常任委員辭任및補任' under a bare 【報告事項】 heading, p.tit_sm '◯特別委員長選任'),
    or any short ◯ heading once an item is open (it closes the item)."""
    if tok[0] in ("line", "label"):
        st = (tok[1] or "").strip()
        if st[:1] in "◯○" and len(nows(st)) <= 40:
            return ("head", st)
    return tok


def segment_items(tokens):
    """Cut the token stream into report items: list of dict(kind, caption, tokens)."""
    items, cur = [], None
    for tok in tokens:
        h = _as_head(tok)
        if h is not tok and (item_kind(h[1]) or cur is not None):
            tok = h
        if tok[0] == "head":
            k = item_kind(tok[1])
            if k:
                cur = {"kind": k, "caption": tok[1], "tokens": []}
                items.append(cur)
            else:
                cur = None
            continue
        if tok[0] == "table":
            k = item_kind(tok[1]) if tok[1] else None
            if k and (cur is None or cur["kind"] != k):
                cur = {"kind": k, "caption": tok[1], "tokens": []}
                items.append(cur)
            if cur is not None:
                cur["tokens"].append(tok)
            continue
        if cur is not None:
            cur["tokens"].append(tok)
    return items


NAME_SPLIT = re.compile(r"\s{2,}|\n|[,，、;․·ㆍ‧]")
# a standalone hangul syllable, spaces, another standalone syllable: one 2-syllable name ('김  현')
SPREAD2 = re.compile(r"(?<![가-힣])([가-힣])[ \u3000\xa0]{1,4}([가-힣])(?![가-힣])")
REASON_PAREN = re.compile(r"\(([^()]*(?:탈당|제명|이탈|제20조의2|사직|퇴직|사망|당선무효|상실|의장|합당|해산|당적)[^()]*)\)")
COUNT_PAREN = re.compile(r"\(\s*이상\s*(\d+)\s*[인명]\s*\)")
GROUP_TAIL = re.compile(r"(당|연합|모임|위원회|신당|정당|21|연대|선택|미래|전환|혁신당|의원|정의당|진보|同盟|黨)$")


def looks_like_group(line, known_groups):
    """A text line naming a party / 교섭단체 (vs a line of names)."""
    t = norm_party(line)
    if not t:
        return False
    if t == NON_GROUP or t in known_groups:
        return True
    if HANJA_CHARS.search(t) and not t.endswith(("黨", "聯合")):
        return False                       # hanja names
    return bool(GROUP_TAIL.search(t)) and len(t) >= 3 and " " not in line.strip()


def split_names(line):
    """Candidate names from a raw line: split on 2+ spaces / newlines / commas; a chunk made of
    single-syllable tokens is one name ('유 승 민'); in other chunks a single-syllable token is
    joined to the next one ('박 정' -> '박정'). Returns (names, inline_reasons, declared_count)."""
    reasons = {}
    cnt = COUNT_PAREN.search(line)
    declared = int(cnt.group(1)) if cnt else None
    t = COUNT_PAREN.sub(" ", line)
    t = SPREAD2.sub(r"\1\2", t)
    names = []
    chunks, run = [], []
    for chunk in NAME_SPLIT.split(t):        # '설  훈' / '문  희': 2-syllable names spread by spaces
        if len(nows(chunk)) == 1 and not REASON_PAREN.search(chunk):
            run.append(nows(chunk))
            continue
        if run:
            chunks.append("".join(run))
            run = []
        chunks.append(chunk)
    if run:
        chunks.append("".join(run))
    for chunk in chunks:
        # reason in parentheses attaches to the name(s) just before it
        rm = REASON_PAREN.search(chunk)
        rs = rm.group(1).strip() if rm else None
        chunk = REASON_PAREN.sub(" ", chunk)
        chunk = re.sub(r"\([^()]*\)", " ", chunk)       # other parentheticals (e.g. committee codes)
        toks = [x for x in chunk.split() if x]
        if not toks:
            continue
        if all(len(x) == 1 for x in toks):
            got = ["".join(toks)]
        else:
            got, i = [], 0
            while i < len(toks):
                x = toks[i]
                if len(x) == 1 and i + 1 < len(toks):
                    x, i = x + toks[i + 1], i + 1
                got.append(x)
                i += 1
        names.extend(got)
        if rs:
            reasons[got[-1]] = rs
    return names, reasons, declared


def _row_get(fields, row, f):
    for i, x in enumerate(fields):
        if x == f and i < len(row):
            return row[i]
    return None


def parse_item(item, meeting_date: dt.date, known_groups=frozenset(), fwd_limit: dt.date | None = None):
    """Report item -> list of raw records. Every record carries kind, date (ISO or None),
    date_src ('cell' / 'line' / 'meeting'), the raw text it came from and its position.
    fwd_limit: latest date a month-day date may take in the meeting's year (see parse_date).
    Committee-table rows without a date cell or date line take the meeting date as an upper bound
    (date_src 'meeting'), as the text-form items do."""
    kind, cap = item["kind"], item["caption"]
    recs = []
    toks = item["tokens"]
    # date line following each table (16-17대 put the date under the table)
    for ti, tok in enumerate(toks):
        if tok[0] != "table":
            continue
        _, tcap, header, rows = tok
        after = None
        for nxt in toks[ti + 1:]:
            if nxt[0] == "table":
                break
            if nxt[0] in ("line", "label") and is_date_line(nxt[1]):
                after = parse_date(nxt[1], meeting_date, fwd_limit)
                break
        fields = header_fields(header)
        for ri, row in enumerate(rows):
            base = {"item_kind": kind, "caption": cap, "table_idx": ti, "row_idx": ri,
                    "raw": " | ".join(x if x is not None else "" for x in row), "header": " | ".join(header)}
            d = _row_get(fields, row, "date")
            dd = parse_date(d, meeting_date, fwd_limit) if d else None
            if dd is None and after is not None:
                dd, dsrc = after, "line"
            else:
                dsrc = "cell" if dd is not None else None
            base.update(date=iso(dd), date_src=dsrc)
            if kind == "notice":
                recs.extend(_notice_rows(fields, row, base, meeting_date))
                continue
            if kind == "committee":
                grp = _row_get(fields, row, "group")
                ob = base if base["date"] else {**base, "date": iso(meeting_date), "date_src": "meeting"}
                for f in ("name", "resign", "appoint"):
                    v = _row_get(fields, row, f)
                    if v:
                        nms, _, _ = split_names(v)
                        for n in nms:
                            recs.append({**ob, "kind": "observation", "name_raw": n, "group": grp,
                                         "role_in_row": f})
                continue
            if kind == "group_rename":
                recs.append({**base, "kind": "group_rename", "from_party": _row_get(fields, row, "from"),
                             "to_party": _row_get(fields, row, "to")})
                continue
            if kind in ("group_form", "group_dissolve"):
                recs.append({**base, "kind": kind, "group": _row_get(fields, row, "group") or (row[0] if row else None)})
                continue
            names_cell = _row_get(fields, row, "members") if kind == "roster" else None
            if names_cell is None:
                names_cell = _row_get(fields, row, "name")
            if names_cell is None:
                recs.append({**base, "kind": "unparsed_row"})
                continue
            nms, rsn, declared = split_names(names_cell)
            for n in nms:
                r = {**base, "kind": kind, "name_raw": n,
                     "group": _row_get(fields, row, "group"),
                     "reason": rsn.get(n) or _row_get(fields, row, "reason"),
                     "district": _row_get(fields, row, "district"),
                     "party": _row_get(fields, row, "party"),
                     "from_party": _row_get(fields, row, "from"),
                     "to_party": _row_get(fields, row, "to"),
                     "declared_count": declared, "n_names_in_cell": len(nms)}
                recs.append(r)
    if any(t[0] == "table" for t in toks):
        return recs
    # ---------------- text items (no table): [group] names... (date) repeated
    if kind == "notice":
        lines = [t[1] for t in toks if t[0] in ("line", "label")]
        for li, ln in enumerate(lines):
            base = {"item_kind": kind, "caption": cap, "table_idx": None, "row_idx": li,
                    "kind": "notice_text", "raw": WS.sub(" ", ln).strip(),
                    "date": iso(parse_date(ln, meeting_date, fwd_limit)), "date_src": "line"}
            recs.extend(notice_text_records(base))
        return recs
    group, pending, committee = None, [], None
    li = 0
    if any(re.match(r"\s*(議員名|의원명|委員名|위원명|성\s*명)\s", t[1]) for t in toks if t[0] in ("line", "label")):
        return [{"item_kind": kind, "caption": cap, "table_idx": None, "row_idx": i, "kind": "unparsed_text",
                 "raw": WS.sub(" ", t[1]).strip(), "date": iso(meeting_date), "date_src": "meeting"}
                for i, t in enumerate(toks) if t[0] in ("line", "label")]
    for tok in toks:
        if tok[0] not in ("line", "label"):
            continue
        ln = tok[1]
        li += 1
        if is_date_line(ln):
            d = iso(parse_date(ln, meeting_date, fwd_limit))
            for r in pending:
                if r["date"] is None:
                    r["date"], r["date_src"] = d, "line"
            recs.extend(pending)
            pending = []
            continue
        if kind == "committee" and re.search(r"(委員會|위원회|特別|특별)$", nows(ln)) and not looks_like_group(ln, known_groups - {""}):
            committee = nows(ln)
            continue
        if looks_like_group(ln, known_groups):
            group = ln.strip()
            if kind in ("group_form", "group_dissolve"):
                pending.append({"item_kind": kind, "caption": cap, "table_idx": None, "row_idx": li,
                                "kind": kind, "group": group, "raw": WS.sub(" ", ln).strip(),
                                "date": None, "date_src": None})
            continue
        if kind in ("group_form", "group_dissolve", "group_rename"):
            pending.append({"item_kind": kind, "caption": cap, "table_idx": None, "row_idx": li,
                            "kind": "unparsed_text", "raw": WS.sub(" ", ln).strip(), "date": None, "date_src": None})
            continue
        line = re.sub(r"^\s*위\s*원\s*장\s{2,}", "", ln)
        nms, rsn, declared = split_names(line)
        for n in nms:
            r = {"item_kind": kind, "caption": cap, "table_idx": None, "row_idx": li,
                 "kind": "observation" if kind == "committee" else kind, "name_raw": n, "group": group,
                 "reason": rsn.get(n), "raw": WS.sub(" ", ln).strip(), "date": None, "date_src": None,
                 "declared_count": declared, "n_names_in_cell": len(nms)}
            if kind == "committee":
                r["role_in_row"] = "text"
            pending.append(r)
    for r in pending:          # no date line after the names: meeting date is an upper bound
        r["date"], r["date_src"] = iso(meeting_date), "meeting"
    recs.extend(pending)
    return recs


_Q = "‘’'\"“”"
NT_RENAME = [re.compile(r"(?<!\S)(\S+?)(?:이|가)\s+(\S+?)(?:으로|로)\s*명칭\s*(?:을\s*)?변경"),
             re.compile(rf"명칭이\s*[{_Q}]([^{_Q}]+)[{_Q}]\s*(?:이|가)\s*[{_Q}]([^{_Q}]+)[{_Q}]\s*(?:으로|로)\s*변경")]
NT_ABSORBED_INTO = re.compile(r"(?<!\S)(\S+?)(?:이|가)\s+(\S+?)에\s*흡수\s*합당")          # A merged into B
NT_ABSORB = re.compile(r"(?<!\S)(\S+?)(?:과|와)\s+(\S+?)의\s*흡수\s*합당")                  # B merged into A
NT_NEWMERGE = re.compile(r"(?<!\S)(\S+?)(?:과|와)\s+(\S+?)(?:이|가)\s+(\S+?)(?:으로|로)\s*신설\s*합당")
NT_REGISTER = re.compile(r"(?<!\S)(\S+?)(?:\([^)]*\))?(?:의)?\s*중앙당\s*(?:이\s*)?등록")
NT_JOIN = re.compile(r"([가-힣]{2,4})\s*의원으로부터\s*동\s*정당에\s*입당")


def notice_text_records(base):
    """A ◯통지 text line -> structured records when it states a party rename, merger or central-party
    registration (and a member's entry into the registered party). The line is kept as the first
    record; its kind becomes the structured kind, further facts on the line are extra records.
    Lines without such a statement stay kind 'notice_text'."""
    t = nfkc(base["raw"])
    out = []
    for rx in NT_RENAME:
        m = rx.search(t)
        if m:
            out.append({**base, "kind": "party_rename", "old_party": m.group(1), "new_party": m.group(2)})
            break
    if not out:
        m = NT_ABSORBED_INTO.search(t)
        if m:
            out.append({**base, "kind": "party_merge", "old_party": m.group(1), "new_party": m.group(2)})
    if not out:
        m = NT_ABSORB.search(t)
        if m:
            out.append({**base, "kind": "party_merge", "old_party": m.group(2), "new_party": m.group(1)})
    if not out:
        m = NT_NEWMERGE.search(t)
        if m:
            out.append({**base, "kind": "party_merge", "old_party": m.group(1), "new_party": m.group(3)})
            out.append({**base, "kind": "party_merge", "old_party": m.group(2), "new_party": m.group(3)})
    if not out:
        m = NT_REGISTER.search(t)
        if m and m.group(1) not in ("정당", "동"):
            out.append({**base, "kind": "party_register", "new_party": m.group(1), "reps": None})
            j = NT_JOIN.search(t)
            if j:
                out.append({**base, "kind": "join", "name_raw": j.group(1), "group": m.group(1),
                            "reason": "notice_text_entry", "declared_count": None, "n_names_in_cell": 1})
    if not out or not base.get("date"):       # undated lines are sub-captions ('중도통합민주당 중앙당 등록')
        return [base]
    for r in out:
        r["notice_text_parsed"] = True
    return out


def _notice_rows(fields, row, base, meeting_date):
    g = lambda f: _row_get(fields, row, f)  # noqa: E731
    out = []
    if g("survivor") or g("absorbed"):
        for ab in re.split(r"[,，、]\s*", g("absorbed") or ""):
            if ab.strip():
                out.append({**base, "kind": "party_merge", "old_party": ab.strip(), "new_party": g("survivor")})
    elif g("old_parties"):
        for ab in re.split(r"[,，、․·ㆍ\n]\s*", g("old_parties") or ""):
            if ab.strip():
                out.append({**base, "kind": "party_merge", "old_party": ab.strip(), "new_party": g("n_label")})
    elif g("change_item"):
        if nows(g("change_item")) == "명칭":
            out.append({**base, "kind": "party_rename", "old_party": g("old_reg"), "new_party": g("new_reg")})
        else:
            out.append({**base, "kind": "notice_other"})
    elif g("n_label") and g("rep"):
        out.append({**base, "kind": "party_register", "new_party": g("n_label"), "reps": g("rep")})
    elif g("from") and g("to"):
        k = "notice_rename_other" if re.search(r"위원회$", nows(g("to"))) else "party_rename"
        out.append({**base, "kind": k, "old_party": g("from"), "new_party": g("to")})
    else:
        out.append({**base, "kind": "notice_other"})
    return out


# ============================================================================= members

def _read_api_rows(pattern):
    rows = []
    for f in sorted(LEG_API.glob(pattern)):
        js = json.loads(f.read_text())
        for r in js.get("rows") or []:
            r = dict(r)
            r["_file"] = f.name
            r["_accessed_utc"] = js.get("accessed_utc")
            rows.append(r)
    return rows


def fetch_member_history(out_dir=OUT / "api"):
    """Keyed download of 의원이력 (only used when the legislators component's copy is missing).
    <= 1 request / 1.1 s; the key is read at runtime and never written or printed."""
    import time
    import requests
    from apikey import get_assembly_api_key      # env ASSEMBLY_API_KEY or ASSEMBLY_API_KEY_FILE
    key = get_assembly_api_key()
    out_dir.mkdir(parents=True, exist_ok=True)
    log = out_dir / "request_log.jsonl"
    base = "https://open.assembly.go.kr/portal/openapi/"
    calls = [("nfzegpkvaclgtscxt", {"PROFILE_UNIT_CD": f"1000{t}"}) for t in range(16, 23)]
    calls.append(("nexgtxtmaamffofof", {}))
    for svc, extra in calls:
        page = 1
        while True:
            params = {"Type": "json", "pIndex": page, "pSize": 1000, **extra}
            time.sleep(1.1)
            r = requests.get(base + svc, params={**params, "KEY": key}, timeout=60)
            with log.open("a") as fh:
                fh.write(json.dumps({"ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                                     "url": base + svc, "params": params, "status": r.status_code},
                                    ensure_ascii=False) + "\n")
            js = r.json()
            body = js.get(svc, [{}, {}])
            head = body[0].get("head", [{}, {}])
            total = head[0].get("list_total_count", 0)
            rows = body[1].get("row", []) if len(body) > 1 else []
            suffix = f"_{extra['PROFILE_UNIT_CD']}" if extra else ""
            (out_dir / f"{svc}{suffix}_p{page}.json").write_text(json.dumps(
                {"service": svc, "params": extra, "page": page, "total": total, "rows": rows,
                 "accessed_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")},
                ensure_ascii=False))
            if page * 1000 >= (total or 0):
                break
            page += 1


PROFILE_RE = re.compile(r"^제\s*(\d+)\s*대\s+(\S+)\s*(.*)$")


def load_seat_spans():
    """One row per member-term stint: naas_cd, term, stint, seat_start, seat_end, party_at_entry,
    district, name, name_hanja, source (의원이력 service and file)."""
    global LEG_API
    rows = _read_api_rows("nfzegpkvaclgtscxt_*_p*.json") + _read_api_rows("nexgtxtmaamffofof_p*.json")
    if not rows:
        fetch_member_history()
        LEG_API = OUT / "api"
        rows = _read_api_rows("nfzegpkvaclgtscxt_*_p*.json") + _read_api_rows("nexgtxtmaamffofof_p*.json")
    out = []
    for r in rows:
        m = PROFILE_RE.match(nfkc(r.get("PROFILE_SJ") or "").strip())
        if not m:
            continue
        term = int(m.group(1))
        if not 16 <= term <= 22:
            continue
        fr, _, to = (r.get("FRTO_DATE") or "").partition("~")
        conv = lambda s: "-".join(f"{int(x):02d}" if i else x for i, x in enumerate(re.findall(r"\d+", s)[:3])) if re.search(r"\d{4}", s or "") else None  # noqa: E731
        out.append({"naas_cd": r.get("MONA_CD"), "term": term, "seat_start": conv(fr), "seat_end": conv(to),
                    "party_at_entry": m.group(2), "district_hist": m.group(3).strip(),
                    "name": r.get("HG_NM"), "name_hanja": nfkc(r.get("HJ_NM")),
                    "source": r["_file"], "accessed_utc": r.get("_accessed_utc")})
    df = pd.DataFrame(out).drop_duplicates(["naas_cd", "term", "seat_start"])
    df = df.sort_values(["naas_cd", "term", "seat_start"]).reset_index(drop=True)
    df["stint"] = df.groupby(["naas_cd", "term"]).cumcount() + 1
    df["seat_end_raw"] = df.seat_end
    df["seat_end"] = df.seat_end.fillna(df.term.map(TERM_END))
    return df


def load_member_terms():
    """members_term_16_22 joined with the 의원이력 seat spans (by naas_cd, term, stint)."""
    mt = pd.read_parquet(INTERIM / "members_term_16_22.parquet")
    ss = load_seat_spans()
    m = mt.merge(ss[["naas_cd", "term", "stint", "seat_start", "seat_end", "seat_end_raw", "party_at_entry",
                     "district_hist", "source"]], on=["naas_cd", "term", "stint"], how="outer", indicator=True)
    m["name_hanja"] = m.name_hanja.map(lambda s: nfkc(s) if isinstance(s, str) else None)
    return m


HANJA_VARIANTS = {"雋": "儁"}


class NameIndex:
    """Per-term name lookup over member-term stints (hangul name, hanja name, hangulized hanja)."""

    def __init__(self, members: pd.DataFrame):
        self.m = members.copy()
        cnt = defaultdict(Counter)
        for r in self.m.itertuples():
            n, h = nows(r.name), nows(r.name_hanja or "")
            if n and h and len(n) == len(h):
                for a, b in zip(h, n):
                    if HANJA_CHARS.match(a):
                        cnt[a][b] += 1
        self.hj = {a: c.most_common(1)[0][0] for a, c in cnt.items()}
        self.idx = defaultdict(lambda: defaultdict(set))       # term -> key -> {naas_cd}
        for r in self.m.itertuples():
            for k in {nows(r.name), nows(r.name_hanja or ""), self.hangulize(r.name_hanja or "")}:
                if k:
                    self.idx[int(r.term)][k].add(r.naas_cd)
        self.info = {(r.naas_cd, int(r.term)): r for r in self.m.itertuples()}
        self.maxlen = 6

    def hangulize(self, s):
        return "".join(self.hj.get(ch, ch) for ch in nows(s))

    def _hanja_conflict(self, naas, term, k):
        r = self.info.get((naas, int(term)))
        h = nows(getattr(r, "name_hanja", "") or "") if r is not None else ""
        if not h or re.search("[가-힣]", h) or len(h) != len(k):
            return False
        vk = "".join(HANJA_VARIANTS.get(ch, ch) for ch in k)
        # one differing character is a variant form (鍾/鐘, 姃/政); two or more is another person
        return sum(a != b for a, b in zip(vk, h)) >= 2

    def lookup(self, raw, term):
        k = nows(raw)
        k = re.sub(r"\((?:비|비례)\)$|의원$|위원$", "", k)
        if not k:
            return set()
        d = self.idx[int(term)]
        if k in d:
            return set(d[k])
        hk = self.hangulize(k)
        if hk in d:
            hit = set(d[hk])
            if HANJA_CHARS.search(k):
                # a hanja name read as hangul must not pick a member whose known hanja differs
                hit = {x for x in hit if not self._hanja_conflict(x, term, k)}
            if hit:
                return hit
        if HANJA_CHARS.search(k):
            vk = "".join(HANJA_VARIANTS.get(ch, ch) for ch in k)
            if vk in d:
                return set(d[vk])
            # member hanja names store unencodable characters as hangul ('설松雄' for 偰松雄):
            # such a position matches any hanja, the other positions must agree
            hits = set()
            for key, ids in d.items():
                if len(key) == len(k) and re.search("[가-힣]", key) and HANJA_CHARS.search(key):
                    if all(a == b or (re.match("[가-힣]", b) and HANJA_CHARS.match(a)) for a, b in zip(vk, key)):
                        hits |= ids
            if len(hits) == 1:
                return hits
        return set()

    def segment(self, raw, term):
        """DP segmentation of a de-spaced run of names into member names of the term.
        Returns list of names or None when no full segmentation exists."""
        s = nows(raw)
        s = COUNT_PAREN.sub("", s)
        s = re.sub(r"\([^()]*\)", "", s)
        n = len(s)
        if n < 2:
            return None
        d = self.idx[int(term)]
        best = [None] * (n + 1)
        best[0] = []
        for i in range(n):
            if best[i] is None:
                continue
            for L in range(2, self.maxlen + 1):
                j = i + L
                if j > n:
                    break
                piece = s[i:j]
                if piece in d or self.hangulize(piece) in d or (HANJA_CHARS.search(piece) and self.lookup(piece, term)):
                    cand = best[i] + [piece]
                    if best[j] is None or len(cand) < len(best[j]):
                        best[j] = cand
        return best[n]


# ============================================================================= harvest

def plenary_meetings():
    u = pd.read_parquet(INTERIM / "meeting_universe_api.parquet",
                        columns=["CONFER_NUM", "CONF_ID", "DAE_NUM", "CLASS_NAME_unified", "CONF_DATE", "TITLE"])
    u = u[u.CLASS_NAME_unified == "국회본회의"].copy()
    u["conf_num"] = u.CONFER_NUM.astype("int64")
    u["term"] = u.DAE_NUM.astype(int)
    u["date"] = u.CONF_DATE.astype(str).str[:10]
    return u[["conf_num", "CONF_ID", "term", "date", "TITLE"]].sort_values(["date", "conf_num"]).reset_index(drop=True)


_CACHE_IDX = None


def _cache05_index():
    global _CACHE_IDX
    if _CACHE_IDX is None:
        _CACHE_IDX = {}
        for f in CACHE05.glob("*.meta.json"):
            try:
                m = json.loads(f.read_text())
            except Exception:
                continue
            u = m.get("url", "")
            if "xml.do" in u and "type=view" in u and m.get("status") == 200:
                mm = re.search(r"[?&]id=(\d+)", u)
                if mm:
                    _CACHE_IDX[int(mm.group(1))] = f.with_name(f.name.replace(".meta.json", ".body"))
    return _CACHE_IDX


def crawl_status():
    import sqlite3
    if not CRAWL_DB.exists():
        return pd.DataFrame(columns=["conf_num", "kind", "status"])
    con = sqlite3.connect(f"file:{CRAWL_DB}?mode=ro", uri=True, timeout=30)
    try:
        return pd.read_sql("select conf_num, kind, status from fetch", con)
    finally:
        con.close()


def load_page(conf_num):
    """('xml', bytes, path) from the crawl or the task-05 cache, ('hwp', bytes, path), or (None,...)."""
    b = f"{conf_num // 1000:03d}"
    p = RAW_VIEW / b / f"{conf_num}.html.gz"
    if p.exists():
        return "xml", p.read_bytes(), str(p.relative_to(V10))
    c = _cache05_index().get(conf_num)
    if c is not None and c.exists():
        return "xml", c.read_bytes(), str(c.relative_to(V10))
    h = RAW_HWP / b / f"{conf_num}.hwp"
    if h.exists():
        return "hwp", h.read_bytes(), str(h.relative_to(V10))
    return None, None, None


def known_group_labels(lineage: Lineage | None = None, members: pd.DataFrame | None = None):
    lab = set()
    if lineage is not None:
        lab |= {norm_party(_base_label(x)) for x in lineage.df.label}
        lab |= {norm_party(x) for x in lineage.ALIASES}
    if members is not None:
        lab |= {norm_party(x) for x in members.party_at_entry.dropna()}
        lab |= {norm_party(x) for x in members.party_elected.dropna()}
    lab |= {"열린우리당주비위원회", "국민참여통합신당주비위원회", "평화와정의의의원모임", "선진과창조의모임",
            "교섭단체", INDEP, NON_GROUP}
    return frozenset(x for x in lab if x)


PARSER_VERSION = "pt-2026-09-26b"
PARTY_ITEM_KINDS = ("roster", "join", "leave", "switch", "group_form", "group_dissolve", "group_rename", "enter", "exit")


def _hwp_parser_digest():
    """sha1 of hwp_parser.py (a pipeline component edited concurrently): HWP parses are cached
    under it, so an edit of the parser invalidates them."""
    import hashlib
    f = Path(__file__).resolve().parent / "hwp_parser.py"
    return hashlib.sha1(f.read_bytes()).hexdigest()[:12] if f.exists() else "minimal"


def _groups_digest(kg):
    import hashlib
    return hashlib.sha1("|".join(sorted(kg)).encode()).hexdigest()[:12]


def _harvest_one(args):
    """Parse one plenary page. Cached on disk under a key of PARSER_VERSION, page path, size and
    mtime, the known-group set (party_lineage.csv + member parties), the forward date limit and,
    for HWP pages, the hwp_parser.py digest."""
    conf_num, term, date, kg, fwd, kg_digest, hwp_digest = args
    import hashlib
    import pickle
    kind, data, path = load_page(conf_num)
    row = {"conf_num": conf_num, "term": term, "date": date, "source": kind, "path": path,
           "n_items": 0, "n_records": 0, "reader": None, "error": None}
    if kind is None:
        return row, []
    fp = V10 / path
    st = fp.stat()
    parts = [PARSER_VERSION, path, str(st.st_size), str(int(st.st_mtime)), kg_digest, fwd or ""]
    if kind == "hwp":
        parts.append(hwp_digest)
    key = hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]
    cf = OUT / "_cache" / f"{conf_num}_{key}.pkl"
    if cf.exists():
        return pickle.loads(cf.read_bytes())
    md = dt.date.fromisoformat(date)
    fl = dt.date.fromisoformat(fwd) if fwd else None
    recs = []
    try:
        if kind == "xml":
            toks, reader = xml_tokens(data), "lxml"
        else:
            toks, reader = hwp_tokens(data)
        row["reader"] = reader
        items = segment_items(toks)
        row["n_items"] = len(items)
        row["n_empty_party_items"] = sum(
            1 for it in items if it["kind"] in PARTY_ITEM_KINDS
            and not any(t[0] == "table" or (t[0] in ("line", "label") and t[1].strip()) for t in it["tokens"]))
        for k, it in enumerate(items):
            for r in parse_item(it, md, kg, fl):
                r.update(conf_num=conf_num, term=term, meeting_date=date, source=kind, item_seq=k)
                recs.append(r)
        row["n_records"] = len(recs)
        row.update({f"items_{k}": v for k, v in Counter(it["kind"] for it in items).items()})
    except Exception as e:  # keep going; every failure is counted in the inventory
        row["error"] = f"{type(e).__name__}: {e}"[:300]
    cf.parent.mkdir(parents=True, exist_ok=True)
    cf.write_bytes(pickle.dumps((row, recs)))
    return row, recs


def forward_limits(pm: pd.DataFrame):
    """conf_num -> latest date a month-day report date may take: the next plenary date of the
    same term + 3 days, or the term end for the term's last plenary."""
    out = {}
    for t, g in pm.sort_values(["date", "conf_num"]).groupby("term"):
        ds = g.date.tolist()
        for i, cn in enumerate(g.conf_num):
            nxt = next((x for x in ds[i + 1:] if x > ds[i]), None)
            lim = (dt.date.fromisoformat(nxt) + dt.timedelta(days=3)).isoformat() if nxt else TERM_END[int(t)]
            out[int(cn)] = lim
    return out


def harvest(meetings: pd.DataFrame | None = None, verbose=False, workers=4):
    """Parse the party / seat / committee report items of every available plenary page.
    Returns (records DataFrame, per-meeting inventory DataFrame)."""
    from concurrent.futures import ProcessPoolExecutor
    meetings = plenary_meetings() if meetings is None else meetings
    lin = Lineage()
    mem = load_member_terms()
    kg = known_group_labels(lin, mem)
    cs = crawl_status()
    view_status = dict(zip(cs[cs.kind == "view"].conf_num, cs[cs.kind == "view"].status))
    hwp_status = dict(zip(cs[cs.kind == "hwp"].conf_num, cs[cs.kind == "hwp"].status))
    _cache05_index()
    fwd = forward_limits(plenary_meetings())
    kgd, hwd = _groups_digest(kg), _hwp_parser_digest()
    args = [(int(m.conf_num), int(m.term), m.date, kg, fwd.get(int(m.conf_num)), kgd, hwd) for m in meetings.itertuples()]
    recs, inv = [], []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for i, (row, rr) in enumerate(ex.map(_harvest_one, args, chunksize=8)):
            row["crawl_view_status"] = view_status.get(row["conf_num"])
            row["crawl_hwp_status"] = hwp_status.get(row["conf_num"])
            inv.append(row)
            recs.extend(rr)
            if verbose and i % 200 == 0:
                print(i, len(args), len(recs), flush=True)
    rec = pd.DataFrame(recs)
    for c in ("reason", "district", "party", "from_party", "to_party", "old_party", "new_party", "reps",
              "group", "name_raw", "role_in_row", "raw", "header"):
        if c not in rec:
            rec[c] = None
    return rec, pd.DataFrame(inv)


# ============================================================================= events -> spells

PERSON_KINDS = ("roster", "join", "leave", "switch", "enter", "exit", "observation")
EVENT_ORDER = {"seat_start": -1, "enter": 0, "leave": 1, "switch": 2, "roster": 3, "join": 4,
               "group_rename": 5, "party_rename": 5, "party_merge": 6, "party_register": 7, "obs": 8.5, "exit": 9}
SEAT_REASON = re.compile(r"사직|퇴직|당선무효|사망|상실|의원직|궐원")
SPEAKER_REASON = re.compile(r"제\s*20\s*조\s*의\s*2|당적\s*이탈|의장")
# 교섭단체 names that are caucuses of several parties / independents, not parties
CAUCUS_ONLY = {"평화와정의의의원모임", "선진과창조의모임", "민주통합의원모임", "중도개혁통합신당추진모임",
               "열린우리당주비위원회", "국민참여통합신당주비위원회", "교섭단체"}
UNKNOWN_TO = {"", "․", "·", "ㆍ", "〃", "-", "―", "\"", "”", ".", "．", "‧"}
# 교섭단체 names of a party in formation (members have left their parties) and of joint caucuses
FORMING_RE = re.compile(r"(주비위원회|준비위원회|창당추진|신당추진모임|신당모임)$")
JOINT_RE = re.compile(r"(의원모임|의원 모임|의모임|모임)$")


def is_caucus_name(g):
    g = norm_party(g or "")
    return bool(g) and (g in CAUCUS_ONLY or bool(FORMING_RE.search(g)) or bool(JOINT_RE.search(g)))
PROV = [("서울특별시", "서울"), ("부산광역시", "부산"), ("대구광역시", "대구"), ("인천광역시", "인천"),
        ("광주광역시", "광주"), ("대전광역시", "대전"), ("울산광역시", "울산"), ("세종특별자치시", "세종"),
        ("경기도", "경기"), ("강원특별자치도", "강원"), ("강원도", "강원"), ("충청북도", "충북"),
        ("충청남도", "충남"), ("전북특별자치도", "전북"), ("전라북도", "전북"), ("전라남도", "전남"),
        ("경상북도", "경북"), ("경상남도", "경남"), ("제주특별자치도", "제주")]


def dnorm(s):
    t = nows(s)
    for a, b in PROV:
        t = t.replace(a, b)
    t = re.sub(r"[․·ㆍ‧,]", "", t)
    return t


def resolve_names(rec: pd.DataFrame, idx: NameIndex, spans: pd.DataFrame):
    """Add cands (tuple of naas_cd seated at the event date, tolerance 15 days), cands_all (any
    member of the term), name_match ('direct', 'dp_split', 'unmatched'). Names of one cell / line
    that fail as split are re-segmented against the term's member names (DP)."""
    rec = rec.copy()
    person = rec.kind.isin(PERSON_KINDS)
    other = rec[~person].copy()
    pr = rec[person].copy()
    pr["cands_all"] = [tuple(sorted(idx.lookup(n, t))) for n, t in zip(pr.name_raw, pr.term)]
    pr["name_match"] = ["direct" if c else "unmatched" for c in pr.cands_all]
    gkey = ["conf_num", "item_seq", "table_idx", "row_idx", "raw"]
    pr["_g"] = pr[gkey].astype(str).agg("|".join, axis=1)
    bad_groups = set(pr.loc[pr.name_match == "unmatched", "_g"])
    out = [pr[~pr._g.isin(bad_groups)]]
    n_dp, n_left = 0, 0
    for g, grp in pr[pr._g.isin(bad_groups)].groupby("_g", sort=False):
        seg = idx.segment("".join(grp.name_raw.astype(str)), int(grp.term.iloc[0]))
        if seg:
            base = grp.iloc[0].to_dict()
            rows = []
            for piece in seg:
                r = dict(base)
                r["name_raw"] = piece
                r["cands_all"] = tuple(sorted(idx.lookup(piece, r["term"])))
                r["name_match"] = "dp_split"
                rows.append(r)
            out.append(pd.DataFrame(rows))
            n_dp += 1
        else:
            out.append(grp)
            n_left += 1
    pr = pd.concat(out, ignore_index=True).drop(columns="_g")
    # seat filter
    sp = defaultdict(list)
    for r in spans.itertuples():
        sp[(r.naas_cd, int(r.term))].append((r.seat_start, r.seat_end))

    def seated(n, t, d):
        if not d:
            return True
        lo = (dt.date.fromisoformat(d) - dt.timedelta(days=15)).isoformat()
        hi = (dt.date.fromisoformat(d) + dt.timedelta(days=15)).isoformat()
        return any(a <= hi and b >= lo for a, b in sp.get((n, int(t)), []))
    pr["cands"] = [tuple(c for c in ca if seated(c, t, d)) for ca, t, d in zip(pr.cands_all, pr.term, pr.date)]
    other["cands_all"] = [()] * len(other)
    other["cands"] = [()] * len(other)
    other["name_match"] = None
    res = pd.concat([pr, other], ignore_index=True)
    return res, {"dp_resegmented_groups": n_dp, "unresolved_groups": n_left}


class TermBuilder:
    """Apply one term's events to its member-term stints and return person-date spells.

    Besides the spells it keeps uncertainty windows (self.windows): day ranges in which a member's
    party is not documented because a change is only bounded by two dates - a change inferred from
    a committee table, a switch whose 'from' party the minutes never recorded as joined, an exit
    without a dated record, an election label adopted after the seat start. A window's bounds are
    the last documented day of the earlier state (last membership event, or a later committee-table
    row showing the member in that party) and the first documented day of the later one."""

    def __init__(self, term, spans_t, events_t, lin: Lineage, idx: NameIndex, gap_dates=(), next_start=None):
        self.term = term
        self.next_start = next_start or {}     # naas -> 교섭단체 on the next term's start roster
        self.gap_dates = sorted(set(gap_dates))    # plenary dates whose report items are unreadable
        self.last_event = {}
        self.confirmed = {}    # naas -> last date a committee table showed the member in the current party
        self.since = {}        # naas -> date the current state label was set
        self.windows = []
        self.docs = defaultdict(list)   # naas -> dates on which a record documents the member's party
        self.stats = Counter()
        self.lin = lin
        self.idx = idx
        self.spans = spans_t.sort_values(["naas_cd", "seat_start"])
        self.ev = events_t
        self.info = {r.naas_cd: r for r in spans_t.itertuples()}
        self.iv = defaultdict(list)
        self.ends = defaultdict(list)
        for r in spans_t.itertuples():
            self.iv[r.naas_cd].append((r.seat_start, r.seat_end))
            if isinstance(r.seat_end_raw, str):
                self.ends[r.naas_cd].append(r.seat_end_raw)
        self.state = {}        # naas -> party label (formal on self.since[naas]) or None
        self.caucus = {}       # naas -> caucus-only 교섭단체 or None
        self.last_party = {}   # naas -> (last non-INDEP party label, date the label was set, date it was left)
        self.tl = defaultdict(list)
        self.log = []
        self.obs = defaultdict(list)
        self.tobs = defaultdict(list)       # table-based observations: (date, group, conf_num)
        for r in events_t[events_t.kind == "observation"].itertuples():
            if len(r.cands) == 1 and r.date:
                self.obs[r.cands[0]].append((r.date, norm_party(r.group or "")))
                if (getattr(r, "role_in_row", None) or "text") != "text":
                    self.tobs[r.cands[0]].append((r.date, norm_party(r.group or ""), r.conf_num))
        for k in self.tobs:
            self.tobs[k].sort()
        # 교섭단체 / party names attested in this term's event tables (trusted observation groups)
        self.known = set()
        for r in events_t[events_t.kind.isin(["roster", "join", "leave", "group_form", "group_dissolve"])].itertuples():
            g = norm_party(r.group or "")
            if g and g != NON_GROUP:
                self.known.add(g)
        for r in events_t[events_t.kind.isin(["switch", "group_rename"])].itertuples():
            for g in (norm_party(r.from_party or ""), norm_party(r.to_party or "")):
                if g and g not in (NON_GROUP, INDEP):
                    self.known.add(g)
        self.known |= {norm_party(_base_label(x)) for x in lin.df.label if x and x != INDEP}
        self.grename = defaultdict(set)
        for r in events_t[events_t.kind.isin(["group_rename", "party_rename", "party_merge"])].itertuples():
            a = norm_party((r.from_party if r.kind == "group_rename" else r.old_party) or "")
            b = norm_party((r.to_party if r.kind == "group_rename" else r.new_party) or "")
            if a and b and a != b:
                self.grename[a].add(b)
        self.group_form = defaultdict(list)
        for r in events_t[(events_t.kind == "group_form") & events_t.date.notna()].itertuples():
            self.group_form[norm_party(r.group or "")].append(r.date)
        # founding evidence of a label in the minutes: 교섭단체 구성, central-party registration
        self.founded = defaultdict(set)
        for r in events_t[events_t.kind.isin(["group_form", "party_register"]) & events_t.date.notna()].itertuples():
            g = norm_party((r.group if r.kind == "group_form" else getattr(r, "new_party", None)) or "")
            if g:
                self.founded[g].add(r.date)
        # future explicit party events per member (to leave observations to them)
        self.future = defaultdict(list)
        for r in events_t[events_t.kind.isin(["join", "roster", "switch", "leave"]) & events_t.date.notna()].itertuples():
            if len(r.cands) == 1:
                tgt = norm_party((r.to_party if r.kind == "switch" else r.group) or "")
                src = norm_party((r.from_party if r.kind == "switch" else r.group) or "") if r.kind in ("switch", "leave") else ""
                self.future[r.cands[0]].append((r.date, r.kind, INDEP if r.kind == "leave" else tgt, src))

    # -------------------------------------------------------------- helpers
    def same(self, a, b, date):
        a, b = norm_party(a or ""), norm_party(b or "")
        if not a or not b:
            return False
        if a == b:
            return True
        if INDEP in (a, b) or NON_GROUP in (a, b):
            return {a, b} <= {INDEP, NON_GROUP}
        return self.lin.same_party(a, b, date)

    def same_family(self, a, b, date):
        """Same organisation on `date`, or same lineage family."""
        if self.same(a, b, date):
            return True
        a, b = norm_party(a or ""), norm_party(b or "")
        if not a or not b or INDEP in (a, b) or NON_GROUP in (a, b):
            return False
        return self.lin.family(a, date) == self.lin.family(b, date)

    def cur(self, n, date):
        """The member's state label carried to `date` through lineage renames / mergers (INDEP when
        the party ended without successor)."""
        p = self.state.get(n)
        if p is None or p == INDEP or not date:
            return p
        return self.lin.formal_on(p, date, since=self.since.get(n) or date)[0]

    def elected(self, n, date):
        """The member's election party (as recorded by 의원이력) as the formal label on `date`."""
        s = self.info[n]
        f, how = self.lin.label_at(s.party_at_entry, s.seat_start)
        if how == "postdated_unresolved" or not date or date < s.seat_start:
            return f
        return self.lin.formal_on(f, date, since=s.seat_start)[0]

    def known_since(self, n):
        """Last day the member's current state is documented: the last membership event, or a
        later committee-table row showing the member in that party."""
        return max(self.last_event.get(n, "0000-01-01"), self.confirmed.get(n, "0000-01-01"))

    def add_window(self, n, lo, hi, reason, ev=None, **kw):
        """Uncertain days strictly between lo and hi (both documented days). lo '0000-01-01' (no
        documented state) starts at the member's first seat day."""
        if lo > "0001":
            a = day_after(lo)
        else:
            a = min((x for x, _ in self.iv.get(n, [])), default=hi)
        b = day_before(hi)
        if a > b:
            return None
        w = {"naas_cd": n, "term": self.term, "lo": a, "hi": b, "reason": reason,
             "conf_num": getattr(ev, "conf_num", None),
             "gaps_in_window": ";".join(x for x in self.gap_dates if a <= x <= hi), **kw}
        self.windows.append(w)
        return w

    def _drop_relabels(self, n, a, b):
        """Remove organisation-level relabel entries (touch=False) dated strictly between a and b:
        they relabelled a state that an inferred change starting on `a` replaces."""
        self.tl[n] = [x for x in self.tl[n] if not (a < x["date"] < b and not x.get("touch", True))]

    def set_party(self, n, date, party, basis, ev=None, extra=None, touch=True):
        """touch=False for organisation-level changes (renames, mergers) that say nothing about the
        member's own membership, so they do not narrow inference windows."""
        party = norm_party(party)
        if party == NON_GROUP:
            party = INDEP
        cur = self.state.get(n)
        if cur is not None and cur != INDEP and party == INDEP:
            self.last_party[n] = (cur, self.since.get(n) or date, date)   # label, its date, exit date
        if party != cur:
            self.confirmed.pop(n, None)
        self.state[n] = party
        self.since[n] = date
        if touch:
            self.last_event[n] = max(self.last_event.get(n, date), date)
            if not str(basis).startswith(("inferred_", "election_party")):
                self.docs[n].append(date)
        e = {"date": date, "party": party, "basis": basis, "touch": touch}
        if ev is not None:
            e.update(conf_num=getattr(ev, "conf_num", None), event_kind=getattr(ev, "kind", None),
                     reason=getattr(ev, "reason", None), raw=getattr(ev, "raw", None))
        if extra:
            e.update(extra)
        self.tl[n].append(e)

    def note(self, ev, status, n=None, **kw):
        self.log.append({"term": self.term, "date": ev.date, "kind": ev.kind, "conf_num": ev.conf_num,
                         "item_seq": ev.item_seq, "row_idx": ev.row_idx, "name_raw": getattr(ev, "name_raw", None),
                         "naas_cd": n, "status": status, "raw": getattr(ev, "raw", None), **kw})

    def seated(self, n, date):
        return any(a <= date <= b for a, b in self.iv.get(n, ()))

    def pick(self, ev):
        c = list(ev.cands)
        if not c:
            return None, "unmatched" if not ev.cands_all else "not_seated"
        if len(c) == 1:
            return c[0], "unique"
        d = getattr(ev, "district", None)
        if isinstance(d, str) and d.strip():
            c2 = [x for x in c if dnorm(self.info[x].district_hist) == dnorm(d)
                  or dnorm(self.info[x].district_hist).startswith(dnorm(d)) or dnorm(d).startswith(dnorm(self.info[x].district_hist))]
            if len(c2) == 1:
                return c2[0], "district"
            if "비례" in nows(d):
                c2 = [x for x in c if "비례" in nows(self.info[x].district_hist)]
                if len(c2) == 1:
                    return c2[0], "district_pr"
        frm = getattr(ev, "from_party", None) if ev.kind == "switch" else getattr(ev, "group", None)
        if ev.kind in ("leave", "switch") and isinstance(frm, str):
            c2 = [x for x in c if self.same(self.cur(x, ev.date), frm, ev.date)]
            if len(c2) == 1:
                return c2[0], "current_party"
        if ev.kind in ("join", "roster") and isinstance(ev.group, str):
            # a member seated within the last 60 days (after the term start) whose party is the
            # group: the join is his, not a sitting namesake's (18대 김선동 2011-04-27, 이영애 2011-10-04)
            t3, lo60 = _shift(TERM_START[self.term], 3), _shift(ev.date, -60)
            c2 = [x for x in c if any(t3 < a <= ev.date and a >= lo60 for a, _b in self.iv.get(x, ()))]
            if len(c2) == 1 and (self.cur(c2[0], ev.date) in (None, INDEP)
                                 or self.same(self.cur(c2[0], ev.date), ev.group, ev.date)
                                 or self.same_family(self.elected(c2[0], ev.date), ev.group, ev.date)):
                return c2[0], "newly_seated"
            c2 = [x for x in c if not self.same(self.cur(x, ev.date), ev.group, ev.date)]
            if len(c2) == 1:
                return c2[0], "not_yet_member"
        if ev.kind in ("join", "roster") and isinstance(ev.group, str) and not is_caucus_name(ev.group):
            c2 = [x for x in c if self.same(self.elected(x, ev.date), ev.group, ev.date)]
            if not c2:
                c2 = [x for x in c if self.same_family(self.elected(x, ev.date), ev.group, ev.date)]
            if len(c2) == 1:
                return c2[0], "election_party"
        raw = str(ev.name_raw or "")
        pr = [x for x in c if "비례" in nows(self.info[x].district_hist)]
        if "(비)" in raw and len(pr) == 1:
            return pr[0], "suffix_bi"
        if "(비)" not in raw and len(c) - len(pr) == 1 and len(pr) >= 1 and ev.kind != "observation":
            return [x for x in c if x not in pr][0], "no_suffix_district_member"
        return None, "ambiguous"

    # -------------------------------------------------------------- build
    def _dedupe(self, ev):
        """The same member event printed in two plenaries' reports (e.g. 19대 진보정의당 switches
        of 2012-10-31 in conf 36516 and 36598) is applied once; later copies are logged."""
        def f(x):
            p = norm_party(x or "") if isinstance(x, str) else ""
            return INDEP if p in (NON_GROUP, INDEP) else p
        seen, drop = {}, []
        for r in ev.sort_values(["date", "conf_num", "item_seq", "row_idx"]).itertuples():
            if r.kind not in ("roster", "join", "leave", "switch", "enter", "exit"):
                continue
            k = (r.kind, r.date, tuple(r.cands), re.sub(r"\((?:비|비례)\)$", "", nows(r.name_raw or "")),
                 f(r.group), f(r.from_party), f(r.to_party))
            if k in seen:
                if seen[k] != r.conf_num:
                    drop.append(r.Index)
                    self.note(r, "duplicate_report", r.cands[0] if len(r.cands) == 1 else None, dup_of_conf=seen[k])
                continue
            seen[k] = r.conf_num
        self.stats["duplicate_report"] += len(drop)
        return ev.drop(index=drop)

    def run(self):
        t0 = TERM_START[self.term]
        ev = self.ev[self.ev.kind != "observation"].copy()
        for r in ev[ev.date.isna()].itertuples():
            self.stats[f"no_date_{r.kind}"] += 1
            if r.kind in ("notice_text", "notice_other", "unparsed_text", "unparsed_row"):
                continue            # text lines, not events (counted only)
            self.note(r, "no_date", r.cands[0] if len(r.cands) == 1 else None)
        ev = ev[ev.date.notna()]
        ev = self._dedupe(ev)
        # start rosters (term start + 45 days): initial party of members seated at term start
        lim = (dt.date.fromisoformat(t0) + dt.timedelta(days=45)).isoformat()
        start_roster, roster_orgs, roster_fams = {}, set(), set()
        sr = ev[(ev.kind == "roster") & (ev.date <= lim)]
        # a name printed k times in one roster cell with exactly k seated namesakes: all of them
        dup_assigned = set()
        if len(sr):
            cell = sr.conf_num.astype(str) + "|" + sr.item_seq.astype(str) + "|" + sr.table_idx.astype(str) + "|" + sr.row_idx.astype(str)
            key = cell + "|" + sr.name_raw.map(lambda x: re.sub(r"\((?:비|비례)\)$", "", nows(x or "")))
            for k, grp in sr.groupby(key):
                c = sorted(set().union(*[set(x) for x in grp.cands]))
                if len(grp) > 1 and len(c) == len(grp) and all(len(x) == len(c) for x in grp.cands):
                    g = norm_party(grp.group.iloc[0] or "")
                    for n, r in zip(c, grp.itertuples()):
                        start_roster[n] = (g, r.conf_num, r.date)
                        self.note(r, "start_roster", n, how="duplicate_name_all_listed")
                    dup_assigned |= set(grp.index)
        for r in sr.itertuples():
            g = norm_party(r.group or "")
            if g:
                roster_orgs.add(self.lin.org(g, t0) or g)
                roster_fams.add(self.lin.family(g, t0) or g)
            if r.Index in dup_assigned:
                continue
            n, how = self.pick(r)
            if n is not None:
                start_roster[n] = (g, r.conf_num, r.date)
                self.note(r, "start_roster", n, how=how)
            else:
                self.note(r, "start_roster_" + how)
        enter = defaultdict(list)
        for r in ev[ev.kind == "enter"].itertuples():
            n, how = self.pick(r)
            if n is not None:
                enter[n].append(r)
        first_party_event = {}
        for r in ev[ev.kind.isin(["join", "roster", "switch"])].sort_values("date").itertuples():
            for c in r.cands:
                first_party_event.setdefault(c, r)
        # seat starts as events
        seat_rows = []
        for s in self.spans.itertuples():
            seat_rows.append({"kind": "seat_start", "date": s.seat_start, "naas_cd": s.naas_cd, "stint": s.stint,
                              "conf_num": None, "item_seq": -1, "row_idx": -1})
        body = ev[(ev.kind != "roster") | (ev.date > lim)]
        ob = self.ev[(self.ev.kind == "observation") & self.ev.date.notna()].copy()
        if len(ob):
            ob = ob[(ob.cands.map(len) == 1) & (ob.role_in_row.fillna("text") != "text")]
            ob["kind"] = "obs"
        allev = pd.concat([pd.DataFrame(seat_rows), body, ob], ignore_index=True)
        allev["_o"] = allev.kind.map(EVENT_ORDER).fillna(8)
        allev = allev.sort_values(["date", "_o", "conf_num", "item_seq", "row_idx"], na_position="first")
        for e in allev.itertuples():
            if e.kind == "seat_start":
                self._seat_start(e, start_roster, enter, (roster_orgs, roster_fams), first_party_event, t0)
            elif e.kind in ("roster", "join"):
                self._join(e)
            elif e.kind == "leave":
                self._leave(e)
            elif e.kind == "switch":
                self._switch(e)
            elif e.kind in ("group_rename", "party_rename", "party_merge"):
                self._rename(e)
            elif e.kind == "party_register":
                self._register(e)
            elif e.kind == "exit":
                self._exit(e)
            elif e.kind == "enter":
                pass    # used at seat start
            elif e.kind == "obs":
                self._obs(e)
            else:
                self.note(e, "ignored_kind")
        return self._spells()

    def _seat_start(self, e, start_roster, enter, roster_orgs, first_party_event, t0):
        n = e.naas_cd
        s = next(x for x in self.spans.itertuples() if x.naas_cd == n and x.stint == e.stint)
        d = s.seat_start
        near_start = d <= (dt.date.fromisoformat(t0) + dt.timedelta(days=3)).isoformat()
        if near_start and n in start_roster:
            g, cf, rd = start_roster[n]
            if is_caucus_name(g) or g == NON_GROUP:
                self.caucus[n] = g
                self.set_party(n, d, self.lin.label_at(s.party_at_entry, d)[0], "election_party_caucus_roster",
                               extra={"conf_num": cf})
            else:
                self.set_party(n, d, g, "start_roster", extra={"conf_num": cf, "event_kind": "roster"})
            return
        ent = [r for r in enter.get(n, []) if abs((dt.date.fromisoformat(r.date) - dt.date.fromisoformat(d)).days) <= 60]
        if ent:
            r = ent[0]
            p = norm_party(r.party or "")
            if p and p != NON_GROUP and p not in UNKNOWN_TO:
                # the notice prints the list / election party, possibly under a label that was
                # renamed or merged before the seat start (19대 황인자: '자유선진당' in 2013 -> 새누리당)
                f, how = self.lin.label_at(p, d)
                self.set_party(n, d, f, "enter_notice", r,
                               extra={"notice_party": p, "notice_party_resolution": how})
                if how == "postdated_unresolved":
                    self._postdated_window(n, p, d)
                return
        p0 = s.party_at_entry
        f, how = self.lin.label_at(p0, d)
        basis = {"successor": "election_party_lineage", "backdated_rename": "election_party_backdated"}.get(how, "election_party")
        exc = self.lin.exceptions(p0, d, historic=True).get(nows(s.name or ""))
        if how == "successor" and exc and not (near_start and n in start_roster):
            # documented pre-opening exit from the election party (party_lineage individual_exceptions)
            self.set_party(n, d, exc, "election_party_individual_exception",
                           extra={"election_party": p0, "evidence": self.lin.rows[self.lin.key(p0, d, historic=True)]["individual_exceptions"]})
            return
        roster_orgs, roster_fams = roster_orgs
        # the election party is the roster caucus's party on the start date, or a label that did
        # not exist yet on the start date (recorded under a later name) whose family is a roster's
        in_roster_party = (self.lin.org(f, d) or norm_party(f)) in roster_orgs or \
            (how == "postdated_unresolved" and (self.lin.family(f) or norm_party(f)) in roster_fams)
        if near_start and roster_orgs:
            if in_roster_party:
                fe = first_party_event.get(n)
                if fe is not None and fe.kind in ("join", "roster") and \
                        self.same_family(fe.group, self.lin.formal_on(f, fe.date, since=d)[0], fe.date):
                    self.set_party(n, d, INDEP, "inferred_independent_absent_from_roster",
                                   extra={"election_party": p0})
                    return
                basis += "_absent_from_start_roster"
        if how == "postdated_unresolved":
            basis += "_label_postdates_start"
        self.set_party(n, d, f, basis, extra={"election_party": p0})
        if how == "postdated_unresolved":
            self._postdated_window(n, p0, d)

    def _postdated_window(self, n, label, d):
        """The recorded label was adopted after the seat start by a merger or founding: the party
        held from the seat start to the label's first day is not documented."""
        lf = min((r["label_from"] for r in self.lin.by_base.get(norm_party(label), []) if r["label_from"] > d),
                 default=None)
        if lf:
            self.add_window(n, day_before(d), lf, "election_label_postdates_start",
                            evidence=f"{label} adopted {lf}")

    def _obs(self, e):
        """A committee-table row (member, 교섭단체, date) that contradicts the state and is not
        explained by an event of this member within 30 days after it: the change happened on an
        unreported date. It is dated at the earliest of (a) the formation of the observed
        교섭단체, (b) a plenary whose report items are unreadable, (c) the observation date, taken
        within (last documented day of the member's state, observation date]. Every inference is
        logged with its window. A row that agrees with the state documents it on that date."""
        n = e.cands[0]
        g = norm_party(e.group or "")
        if not g or g == NON_GROUP or not self.seated(n, e.date) or n not in self.state:
            return
        caucus = is_caucus_name(g)
        if not caucus and g not in self.known:
            return self.note(e, "obs_untrusted_group", n, group=g)
        cur = self.cur(n, e.date)
        if caucus:
            if self.caucus.get(n) == g or not FORMING_RE.search(g) or cur in (None, INDEP):
                self.caucus[n] = g
                return
            target = INDEP
        else:
            if self.same(cur, g, e.date):
                self.confirmed[n] = max(self.confirmed.get(n, e.date), e.date)
                self.docs[n].append(e.date)
                return
            target = g
        d = e.date
        for fd, fk, ft, _src in self.future.get(n, []):
            if d <= fd <= (dt.date.fromisoformat(d) + dt.timedelta(days=30)).isoformat() and \
                    (ft == target or (target != INDEP and ft and self.same(ft, target, fd))):
                return self.note(e, "obs_left_to_later_event", n, group=g, event_date=fd)
        last = self.known_since(n)
        prev = [x for x in self.tl[n] if x["date"] <= d]
        if len(prev) >= 2 and self.same(self.lin.formal_on(prev[-2]["party"], d, since=prev[-2]["date"])[0], g, d) and \
                (dt.date.fromisoformat(d) - dt.date.fromisoformat(prev[-1]["date"])).days <= 30:
            return self.note(e, "obs_stale_previous_party", n, group=g)
        if not self._corroborated(n, g, d):
            return self.note(e, "obs_uncorroborated", n, group=g, prev=cur)
        gaps = [x for x in self.gap_dates if last < x <= d]
        formed = [x for x in self.group_form.get(g, []) if last < x <= d]
        if formed and cur == INDEP:
            when, rule = min(formed), "group_formed"
        elif len(gaps) == 1:
            when, rule = gaps[0], "only_unreadable_plenary_in_window"
        else:
            when, rule = d, "observation_date"
        if caucus:
            self.caucus[n] = g
        ev_info = {"evidence": f"{e.date} committee table {g} (conf_num {e.conf_num})",
                   "inferred_rule": rule, "party_before": cur,
                   "window": f"({last}, {d}]", "window_gaps": ";".join(gaps)}
        self._drop_relabels(n, when, d)
        self.add_window(n, last, d, "inferred_change_window", e, evidence=ev_info["evidence"], rule=rule)
        # a party cannot be joined before it exists: 무소속 until the party's first day (the
        # Assembly's rename of a caucus into it, else the lineage founding date)
        k = self.lin.key(target, d) if target != INDEP else None
        born = self.lin.rows[k]["label_from"] if k else ""
        if born and when < born <= d:
            ren = [x.date for x in self.ev[(self.ev.kind == "group_rename") & self.ev.date.notna()].itertuples()
                   if norm_party(x.to_party or "") == target and when < x.date <= d]
            born2 = min(ren) if ren else born
            self.set_party(n, when, INDEP, "inferred_from_committee_table", e,
                           extra={**ev_info, "evidence": ev_info["evidence"] + f"; {target} not yet founded"})
            self.set_party(n, born2, target, "inferred_from_committee_table", e,
                           extra={**ev_info, "inferred_rule": "party_founded"})
            return self.note(e, "obs_inferred_change", n, group=g, prev=cur, when=when, rule=rule + "+party_founded",
                             window=ev_info["window"], window_gaps=ev_info["window_gaps"])
        self.set_party(n, when, target, "inferred_from_committee_table", e, extra=ev_info)
        self.note(e, "obs_inferred_change", n, group=g, prev=cur, when=when, rule=rule,
                  window=ev_info["window"], window_gaps=ev_info["window_gaps"])

    def _corroborated(self, n, g, d):
        """g is supported by a second committee-table row on another date before any row that shows
        another group, or the member's next explicit event leaves g / switches from g / joins g's
        successor."""
        dates = {d}
        succ, todo = {g}, [g]
        while todo:                                  # the group under its later names
            x = todo.pop()
            for y in self.grename.get(x, ()):
                if y not in succ:
                    succ.add(y)
                    todo.append(y)
        gf = lambda x: self.lin.formal_on(g, x, since=d)[0]  # noqa: E731  (g carried to a later date)
        for od, og, _ in self.tobs.get(n, []):
            if od <= d or not og or og == NON_GROUP:
                continue
            if og in succ or (not is_caucus_name(g) and self.same(og, gf(od), od)):
                dates.add(od)
                if len(dates) >= 2:
                    return True
            elif is_caucus_name(g) and FORMING_RE.search(g) and self.lin.key(og, od) is not None:
                continue      # the forming caucus became a party under a new name
            else:
                break
        nxt = self.next_start.get(n)
        if nxt and (nxt in succ or norm_party(self.lin.formal_on(g, TERM_START[self.term + 1], since=d)[0]) == nxt) \
                and not any(od > d and og and og != NON_GROUP and og not in succ and not self.same(og, gf(od), od)
                            for od, og, _ in self.tobs.get(n, [])):
            return True
        for fd, fk, ft, src in sorted(self.future.get(n, [])):
            if fd <= d:
                continue
            if fk in ("leave", "switch") and src:
                return self.same(src, gf(fd), fd) or norm_party(gf(fd)) == src
            if fk in ("join", "roster") and ft and not is_caucus_name(g):
                return norm_party(self.lin.formal_on(g, fd, since=d)[0]) == ft and ft != g
            break
        return False

    def _join(self, e):
        n, how = self.pick(e)
        if n is None:
            return self.note(e, how)
        if not self.seated(n, e.date):
            return self.note(e, "not_seated", n)
        g = norm_party(e.group or "")
        if not g:
            return self.note(e, "no_group", n)
        cur = self.cur(n, e.date)
        if is_caucus_name(g):
            self.caucus[n] = g
            if FORMING_RE.search(g) and cur not in (None, INDEP):
                self.set_party(n, e.date, INDEP, "join_forming_caucus", e,
                               extra={"evidence": f"{e.kind} {g}", "party_before": cur})
                return self.note(e, "caucus_forming_party_exit_inferred", n, how=how, prev=cur)
            # a joint caucus (평화와정의의의원모임, 선진과창조의모임) says nothing about the member's
            # party: it does not document the party state (no narrowing of inference windows)
            return self.note(e, "caucus_only", n, how=how)
        if self.same(cur, g, e.date):
            self.last_event[n] = max(self.last_event.get(n, e.date), e.date)
            self.docs[n].append(e.date)
            return self.note(e, "noop_same", n, how=how)
        self.set_party(n, e.date, g, e.kind, e)
        self.note(e, "applied", n, how=how, prev=cur)

    def _leave(self, e):
        n, how = self.pick(e)
        if n is None:
            return self.note(e, how)
        rs = nows(e.reason) if isinstance(e.reason, str) else ""
        g = norm_party(e.group or "")
        if rs and SEAT_REASON.search(rs) and not re.search(r"탈당|제명", rs.split("(")[0]):
            return self.note(e, "seat_reason", n, how=how)
        if rs.startswith("퇴직") or rs.startswith("사직"):
            return self.note(e, "seat_reason", n, how=how)
        ends = self.ends.get(n, [])
        if not rs and any(abs((dt.date.fromisoformat(x) - dt.date.fromisoformat(e.date)).days) <= 10 for x in ends):
            return self.note(e, "seat_near_end_no_reason", n, how=how)
        if is_caucus_name(g):
            if self.caucus.get(n) == g:
                self.caucus[n] = None
            return self.note(e, "caucus_only", n, how=how)
        if not self.seated(n, e.date):
            return self.note(e, "not_seated", n)
        if any(isinstance(x, str) and abs((dt.date.fromisoformat(x) - dt.date.fromisoformat(e.date)).days) <= 1 for x in ends):
            return self.note(e, "leave_at_seat_end", n, how=how)
        cur = self.cur(n, e.date)
        if cur == INDEP:
            self.docs[n].append(e.date)
            return self.note(e, "noop_already_independent", n, how=how)
        status = "applied"
        if g and not self.same(cur, g, e.date):
            status = "applied_conflict_state_not_group"
        speaker = bool(rs and SPEAKER_REASON.search(rs))
        self.set_party(n, e.date, INDEP, "leave", e,
                       extra={"speaker_exit": speaker, "party_before": cur})
        self.note(e, status, n, how=how, prev=cur, speaker=speaker)

    def _backfill_start(self, n, a, last, d):
        """First day of an unrecorded membership in `a` that the member held on day d: the
        party's founding / registration / 교섭단체 formation when it falls in (last, d], else the
        first committee-table row showing the member in `a`, else the day after `last`."""
        cands = []
        k = self.lin.key(a, d)
        lf = self.lin.rows[k]["label_from"] if k else ""
        if lf and last < lf <= d:
            cands.append((lf, "party_founded"))
        for x in self.founded.get(norm_party(a), ()):
            if last < x <= d:
                cands.append((x, "party_registered_or_group_formed"))
        if cands:
            return min(cands)
        obs = [od for od, og, _ in self.tobs.get(n, []) if last < od < d and og and self.same(og, a, od)]
        if obs:
            return min(obs), "first_observation"
        if last > "0001":
            return day_after(last), "window_start"
        return min((x for x, _ in self.iv.get(n, [])), default=d), "window_start"

    def _switch(self, e):
        n, how = self.pick(e)
        if n is None:
            return self.note(e, how)
        if not self.seated(n, e.date):
            return self.note(e, "not_seated", n)
        a = norm_party(e.from_party or "")
        b = norm_party(e.to_party or "")
        a = INDEP if a in (NON_GROUP, INDEP) else a
        if b in UNKNOWN_TO or not b:
            return self.note(e, "to_party_unreadable", n, how=how)
        b = INDEP if b in (NON_GROUP, INDEP) else b
        cur = self.cur(n, e.date)
        status = "applied"
        if a and not self.same(cur, a, e.date):
            lp = self.last_party.get(n)
            last = self.known_since(n)
            if cur == INDEP and lp and self.same(self.lin.formal_on(lp[0], e.date, since=lp[1])[0], a, e.date) and \
                    (dt.date.fromisoformat(e.date) - dt.date.fromisoformat(lp[2])).days <= 10:
                status = "applied_from_is_party_before_recent_exit"
            elif a == INDEP and self.tl[n] and self.tl[n][-1]["basis"] == "registration_notice_representative" \
                    and self.same(cur, b, e.date) and \
                    (dt.date.fromisoformat(e.date) - dt.date.fromisoformat(self.tl[n][-1]["date"])).days <= 10:
                # 20대 홍문종: 친박신당 registered with him as representative on 2020-03-09 (state set
                # from the notice), '무소속 -> 친박신당' row on 2020-03-10: the same move
                status = "applied_registration_then_switch"
            elif a == INDEP and cur not in (None, INDEP):
                # the member had left `cur` on an unreported date: inferred exit at the first
                # committee-table row that shows him in another 교섭단체 (a '비교섭' row is consistent
                # with a party that is no 교섭단체, so it is no evidence), else undated
                obs = sorted(x for x in self.obs.get(n, []) if last < x[0] < e.date and x[1]
                             and x[1] != NON_GROUP and not is_caucus_name(x[1]) and x[1] in self.known
                             and not self.same(x[1], self.cur(n, x[0]), x[0]))
                if obs:
                    self._drop_relabels(n, obs[0][0], e.date)
                    self.set_party(n, obs[0][0], INDEP, "inferred_exit_before_switch", e,
                                   extra={"evidence": f"observation {obs[0][1]} on {obs[0][0]}",
                                          "window": f"({last}, {obs[0][0]}]", "party_before": cur})
                    self.add_window(n, last, obs[0][0], "inferred_exit_window", e,
                                    evidence=f"observation {obs[0][1]} on {obs[0][0]}")
                    status = "applied_inferred_exit_from_observation"
                else:
                    status = "applied_conflict_from_independent_state_party"
                    if self.tl[n]:
                        self.tl[n][-1]["end_undated_exit"] = True
                    self.add_window(n, last, e.date, "undated_exit_before_switch", e,
                                    evidence=f"{e.date} 당적 변경 row: from 무소속 (state {cur})")
            elif a != INDEP:
                # the member held `a` on the switch date, but no record shows him joining it (a
                # party that never formed a 교섭단체, a rename the minutes do not report): the
                # membership is backfilled from its earliest documented possible day and the
                # window between the last documented day of the state and the switch is flagged
                start, rule = self._backfill_start(n, a, last, e.date)
                self._drop_relabels(n, start, e.date)
                self.set_party(n, start, a, "inferred_from_switch_from_party", e,
                               extra={"evidence": f"{e.date} 당적 변경 row: from {a} (state {cur})",
                                      "inferred_rule": rule, "party_before": cur, "window": f"({last}, {e.date})"})
                self.add_window(n, last, e.date, "switch_from_party_unrecorded", e,
                                evidence=f"{e.date} 당적 변경 row: from {a} (state {cur})", rule=rule)
                status = "applied_backfilled_from_party"
            else:
                status = "applied_conflict_from_mismatch"
        if self.same(cur, b, e.date) and status == "applied":
            self.last_event[n] = max(self.last_event.get(n, e.date), e.date)
            self.docs[n].append(e.date)
            return self.note(e, "noop_same", n, how=how)
        self.set_party(n, e.date, b, "switch", e, extra={"from_party": a})
        self.note(e, status, n, how=how, prev=cur, from_party=a, to_party=b)

    def _rename(self, e):
        if e.kind == "group_rename":
            old, new = norm_party(e.from_party or ""), norm_party(e.to_party or "")
        else:
            old, new = norm_party(e.old_party or ""), norm_party(e.new_party or "")
        if not old or not new:
            return self.note(e, "rename_unreadable")
        if old == new:
            return self.note(e, "rename_same_label")
        moved = 0
        d0 = day_before(e.date)
        for n, raw in list(self.state.items()):
            if not self.seated(n, e.date):
                continue
            if self.caucus.get(n) == old:
                if is_caucus_name(new):
                    self.caucus[n] = new
                else:
                    self.caucus[n] = None
                    self.set_party(n, e.date, new, e.kind + "_caucus_to_party", e, touch=False)
                    moved += 1
                continue
            cur = self.cur(n, d0)
            if cur and cur != INDEP and (raw == old or cur == old or self.same(cur, old, d0)) and norm_party(cur) != new:
                if is_caucus_name(new):
                    self.caucus[n] = new
                    continue
                self.set_party(n, e.date, new, e.kind, e, touch=False)
                moved += 1
        self.note(e, "applied", None, moved=moved, old=old, new=new)

    def _register(self, e):
        reps = [x for x in re.split(r"[․·ㆍ‧,，\s]+", str(e.reps or "")) if x and x != "None"]
        p = norm_party(e.new_party or "")
        k = 0
        for nm in reps:
            c = [x for x in self.idx.lookup(nm, self.term) if self.seated(x, e.date)]
            if len(c) == 1 and self.cur(c[0], e.date) == INDEP:
                self.set_party(c[0], e.date, p, "registration_notice_representative", e)
                k += 1
        self.note(e, "applied" if k else "no_member_representative", None, moved=k, new=p)

    def _exit(self, e):
        n, how = self.pick(e)
        if n is None:
            return self.note(e, how)
        ends = self.ends.get(n, [])
        ok = any(isinstance(x, str) and abs((dt.date.fromisoformat(x) - dt.date.fromisoformat(e.date)).days) <= 2 for x in ends)
        self.note(e, "exit_matches_seat_end" if ok else "exit_differs_from_seat_end", n, how=how,
                  seat_ends=";".join(str(x) for x in ends))

    # -------------------------------------------------------------- spells
    def _spells(self):
        out = []
        for s in self.spans.itertuples():
            tl = sorted([x for x in self.tl.get(s.naas_cd, []) if s.seat_start <= x["date"] <= s.seat_end],
                        key=lambda x: x["date"])
            # same-day entries: the last one wins (events are processed in EVENT_ORDER)
            dedup = []
            for x in tl:
                if dedup and dedup[-1]["date"] == x["date"]:
                    x = {**x, "same_day_replaced": dedup[-1]["basis"]}
                    dedup[-1] = x
                else:
                    dedup.append(x)
            merged = []
            for x in dedup:
                if merged and merged[-1]["party"] == x["party"]:
                    continue
                merged.append(x)
            if not merged:
                out.append({"naas_cd": s.naas_cd, "term": self.term, "stint": s.stint, "party": None,
                            "start": s.seat_start, "end": s.seat_end, "basis": "no_state"})
                continue
            for i, x in enumerate(merged):
                end = day_before(merged[i + 1]["date"]) if i + 1 < len(merged) else s.seat_end
                out.append({"naas_cd": s.naas_cd, "term": self.term, "stint": s.stint, "party": x["party"],
                            "start": x["date"], "end": end, "basis": x["basis"],
                            "event_kind": x.get("event_kind"), "conf_num": x.get("conf_num"),
                            "reason": x.get("reason"), "speaker_exit": bool(x.get("speaker_exit")),
                            "party_before": x.get("party_before"), "election_party": x.get("election_party"),
                            "evidence": x.get("evidence"), "raw": x.get("raw"),
                            "inferred_rule": x.get("inferred_rule"),
                            "start_window": x.get("window"), "start_window_gaps": x.get("window_gaps"),
                            "notice_party": x.get("notice_party"),
                            "end_undated_exit": bool(x.get("end_undated_exit"))})
        return pd.DataFrame(out)


def notice_transitions(rec: pd.DataFrame):
    """Party renames / mergers notified to the Assembly (◯통지, ◯교섭단체 명칭 변경 tables):
    {old_label_norm: [(date, new_label_norm, kind, conf_num), ...]} (dated rows only)."""
    out = defaultdict(list)
    if rec is None or not len(rec):
        return out
    for r in rec[rec.kind.isin(["party_rename", "party_merge", "group_rename"])].itertuples():
        if r.kind == "group_rename":
            a, b = norm_party(r.from_party or ""), norm_party(r.to_party or "")
        else:
            a, b = norm_party(r.old_party or ""), norm_party(r.new_party or "")
        if not a or not b or a == b or not r.date or is_caucus_name(a) or is_caucus_name(b):
            continue
        out[a].append((r.date, b, r.kind, r.conf_num))
    for k in out:
        out[k] = sorted(set(out[k]))
    return out


def next_transition(label, start, end, lin: Lineage, ntr):
    """Earliest rename / merger of `label` taking effect in (start, end]: lineage successor_from
    (verified registration dates) or the Assembly notice date, whichever is earlier.
    Returns (date, new_label, source, other_date) or None; new_label INDEP = party ended."""
    lab = norm_party(label)
    cands = []
    k = lin.key(lab, start)
    if k is not None:
        row = lin.rows[k]
        sf = row["successor_from"]
        if sf and start < sf <= end:
            succ = _base_label(row["successor"]) if row["successor"] else INDEP
            cands.append((sf, norm_party(succ), "lineage"))
    for d, b, kind, cn in ntr.get(lab, []):
        # a notice about the label applies only when the label names the same lineage row (or no
        # row) on the notice date as on `start`: the 2020 notice '미래한국당 -> 미래통합당' is not
        # about the 2008 미래한국당
        if start < d <= end and lin.key(lab, d) == (k if k is not None else None):
            cands.append((d, b, f"notice_{kind}:{cn}"))
    if not cands:
        return None
    cands.sort()
    d, b, src = cands[0]
    other = [x for x in cands[1:] if x[1] == b or lin.same_party(x[1], b, x[0])]
    return d, b, src, (other[0][0] if other else None)


def lineage_relabel(spells: pd.DataFrame, lin: Lineage, ntr=None):
    """Split spells at the rename / merger dates of their party (lineage + Assembly notices) and
    relabel each piece with the formal label on its first day; then merge consecutive pieces of a
    member-term stint that carry the same label. Returns (spells, n_split)."""
    ntr = ntr or {}
    out, n_split = [], 0
    for r in spells.to_dict("records"):
        p, s0, e = r["party"], r["start"], r["end"]
        if not p or p == INDEP:
            out.append(r)
            continue
        pieces = [(s0, norm_party(p), None, None)]
        cur, a, seen = norm_party(p), s0, 0
        while seen < 20:
            seen += 1
            t = next_transition(cur, a, e, lin, ntr)
            if t is None:
                break
            d, b, src, other = t
            pieces.append((d, b, src, other))
            if b == INDEP:
                break
            cur, a = b, d
        for i, (a, lab, src, other) in enumerate(pieces):
            b = day_before(pieces[i + 1][0]) if i + 1 < len(pieces) else e
            q = dict(r)
            q.update(start=a, end=b, party=lab)
            if i > 0:
                q.update(basis="lineage_relabel" if src == "lineage" else "notice_relabel",
                         relabel_from=pieces[i - 1][1], relabel_source=src, relabel_other_date=other,
                         party_ended=(lab == INDEP), event_kind=None, conf_num=None, reason=None,
                         speaker_exit=False, raw=None, end_undated_exit=False, start_window=None,
                         start_window_gaps=None, notice_party=None)
                n_split += 1
            if i + 1 < len(pieces):
                q["end_undated_exit"] = False
            out.append(q)
    df = pd.DataFrame(out).sort_values(["naas_cd", "term", "stint", "start"]).reset_index(drop=True)
    # merge consecutive same-label pieces (a relabel at the lineage date followed by the notice)
    keep, merged_n = [], 0
    for r in df.to_dict("records"):
        if keep and keep[-1]["naas_cd"] == r["naas_cd"] and keep[-1]["term"] == r["term"] \
                and keep[-1]["stint"] == r["stint"] and keep[-1]["party"] == r["party"] \
                and day_after(keep[-1]["end"]) == r["start"]:
            keep[-1]["end"] = r["end"]
            keep[-1]["merged_bases"] = ";".join(x for x in (keep[-1].get("merged_bases"), r["basis"]) if isinstance(x, str))
            keep[-1]["end_undated_exit"] = bool(r.get("end_undated_exit"))
            merged_n += 1
            continue
        keep.append(dict(r))
    return pd.DataFrame(keep), n_split


def speaker_flags(spells: pd.DataFrame):
    """is_speaker_nonpartisan on 무소속 spells opened by a 국회법 제20조의2 exit, and the party held
    before in party_before_speaker."""
    spells = spells.sort_values(["naas_cd", "term", "start"]).copy()
    spells["is_speaker_nonpartisan"] = spells.speaker_exit.fillna(False).astype(bool) & (spells.party == INDEP)
    spells["party_before_speaker"] = spells.party_before.where(spells.is_speaker_nonpartisan)
    return spells


# ============================================================================= Speaker

SPEAKER_LAW_FROM = "2002-03-07"     # 국회법 제20조의2 (의장의 당적 보유 금지) in force
_POS_NAME = re.compile(r'data-name="([^"]*)"\s+data-pos="([^"]*)"|data-pos="([^"]*)"[^>]*data-name="([^"]*)"')


def _chair_names_one(args):
    conf_num, date = args
    kind, data, path = load_page(conf_num)
    names = Counter()
    if kind == "xml":
        t = gzip.decompress(data).decode("utf-8", "replace") if data[:2] == b"\x1f\x8b" else data.decode("utf-8", "replace")
        for m in _POS_NAME.finditer(t):
            nm, pos = (m.group(1), m.group(2)) if m.group(1) is not None else (m.group(4), m.group(3))
            if nows(hangulize_caption(pos)) == "의장":
                names[nows(nm)] += 1
    elif kind == "hwp":
        paras, _ = _hwp_paragraphs(data)
        for p in paras:
            m = re.match(r"^\s*[◯○]\s*(議長|의장)\s+([가-힣\u3400-\u9fff]{2,4})(?:\s|$)", (p.get("text") or "").split("\n")[0])
            if m:
                names[nows(m.group(2))] += 1
    return conf_num, date, kind, dict(names)


def speaker_tenures(idx: "NameIndex", workers=4):
    """Presiding Speakers from the plenary pages: (term, naas_cd, name, first_date, last_date,
    n_meetings). Only turns printed with the position 의장 (not 의장직무대행 / 부의장) count."""
    from concurrent.futures import ProcessPoolExecutor
    pm = plenary_meetings()
    args = [(int(a), d) for a, d in zip(pm.conf_num, pm.date)]
    rows = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for cn, d, kind, names in ex.map(_chair_names_one, args, chunksize=8):
            for nm, k in names.items():
                rows.append((cn, d, kind, nm, k))
    ch = pd.DataFrame(rows, columns=["conf_num", "date", "source", "name_raw", "n_turns"])
    ch = ch.merge(pm[["conf_num", "term"]], on="conf_num", how="left")
    ch["naas_cd"] = [(lambda c: next(iter(c)) if len(c) == 1 else None)(idx.lookup(n, t)) for n, t in zip(ch.name_raw, ch.term)]
    ten = (ch.dropna(subset=["naas_cd"]).groupby(["term", "naas_cd"])
           .agg(name_raw=("name_raw", "first"), first_date=("date", "min"), last_date=("date", "max"),
                n_meetings=("conf_num", "nunique")).reset_index())
    # a presiding officer counted as Speaker needs at least 5 plenary meetings as 의장
    ten = ten[ten.n_meetings >= 5].sort_values(["term", "first_date"]).reset_index(drop=True)
    return ten, ch


def apply_speaker_rule(spells: pd.DataFrame, ten: pd.DataFrame):
    """Mark the Speaker's 무소속 spell (is_speaker_nonpartisan, party_before_speaker). When the
    minutes carry no exit for a Speaker elected after the 국회법 제20조의2 came into force, the
    spell covering the day after the first presiding date is split and the rest set to 무소속
    (basis 'speaker_nonpartisan_by_law'). Returns (spells, log rows)."""
    sp = spells.sort_values(["naas_cd", "term", "start"]).reset_index(drop=True).copy()
    if "is_speaker_nonpartisan" not in sp:
        sp["is_speaker_nonpartisan"] = False
        sp["party_before_speaker"] = None
    log = []
    for t in ten.itertuples():
        mine = sp[(sp.naas_cd == t.naas_cd) & (sp.term == t.term)]
        d1 = day_after(t.first_date)
        hit = mine[(mine.party == INDEP) & (mine.start <= t.last_date) & (mine.end >= t.first_date)
                   & (mine.start >= (dt.date.fromisoformat(t.first_date) - dt.timedelta(days=30)).isoformat())]
        if len(hit):
            i = hit.index[0]
            prev = mine[mine.end < sp.at[i, "start"]]
            sp.at[i, "is_speaker_nonpartisan"] = True
            sp.at[i, "party_before_speaker"] = prev.party.iloc[-1] if len(prev) else sp.at[i, "party_before"]
            log.append({"term": t.term, "naas_cd": t.naas_cd, "name": t.name_raw, "first_date": t.first_date,
                        "status": "recorded_exit", "spell_start": sp.at[i, "start"]})
            continue
        if t.first_date < SPEAKER_LAW_FROM:
            log.append({"term": t.term, "naas_cd": t.naas_cd, "name": t.name_raw, "first_date": t.first_date,
                        "status": "before_law"})
            continue
        cov = mine[(mine.start <= d1) & (mine.end >= d1)]
        if cov.empty or cov.party.iloc[0] == INDEP:
            log.append({"term": t.term, "naas_cd": t.naas_cd, "name": t.name_raw, "first_date": t.first_date,
                        "status": "no_spell" if cov.empty else "already_independent"})
            if not cov.empty:
                i = cov.index[0]
                prev = mine[mine.end < sp.at[i, "start"]]
                sp.at[i, "is_speaker_nonpartisan"] = True
                sp.at[i, "party_before_speaker"] = prev.party.iloc[-1] if len(prev) else sp.at[i, "party_before"]
            continue
        i = cov.index[0]
        row = sp.loc[i].to_dict()
        before = dict(row, end=t.first_date)
        after = dict(row, start=d1, party=INDEP, basis="speaker_nonpartisan_by_law", event_kind=None,
                     conf_num=None, reason=None, raw=None, speaker_exit=True, is_speaker_nonpartisan=True,
                     start_window=None, start_window_gaps=None, end_undated_exit=False,
                     party_before_speaker=row["party"], party_before=row["party"],
                     evidence=f"presided as 의장 from {t.first_date} (국회법 제20조의2)")
        sp = pd.concat([sp.drop(index=i), pd.DataFrame([before, after])], ignore_index=True)
        log.append({"term": t.term, "naas_cd": t.naas_cd, "name": t.name_raw, "first_date": t.first_date,
                    "status": "imputed_by_law", "spell_start": d1})
    return sp.sort_values(["naas_cd", "term", "start"]).reset_index(drop=True), pd.DataFrame(log)


# ============================================================================= build

def check_observations(spells: pd.DataFrame, rec: pd.DataFrame, lin: Lineage):
    """Committee-table observations (member, 교섭단체, date) against the spells.
    consistent_same_party | consistent_nongroup (member 무소속 or in a party that is not one of
    the term's 교섭단체 names) | caucus_only | inconsistent | no_spell | ambiguous_name."""
    obs = rec[(rec.kind == "observation") & rec.date.notna()]
    groups_by_term = defaultdict(set)
    for r in rec[rec.kind.isin(["roster", "join", "group_form"])].itertuples():
        g = norm_party(r.group or "")
        if g:
            groups_by_term[int(r.term)].add(lin.org(g, r.date) or g)
    sp = spells.dropna(subset=["party"])
    by = defaultdict(list)
    for r in sp.itertuples():
        by[r.naas_cd].append((r.start, r.end, r.party))
    out = []
    for r in obs.itertuples():
        g = norm_party(r.group or "")
        if len(r.cands) != 1:
            out.append((r.term, "ambiguous_or_unmatched_name"))
            continue
        n = r.cands[0]
        hit = [p for a, b, p in by.get(n, []) if a <= r.date <= b]
        if not hit:
            out.append((r.term, "no_spell"))
            continue
        p = hit[0]
        if not g:
            out.append((r.term, "no_group_in_row"))
        elif is_caucus_name(g):
            out.append((r.term, "caucus_only"))
        elif g == NON_GROUP:
            org = lin.org(p, r.date) or p
            out.append((r.term, "consistent_nongroup" if (p == INDEP or org not in groups_by_term[int(r.term)])
                        else "inconsistent_nongroup_but_group_party"))
        elif lin.same_party(p, g, r.date) or norm_party(p) == g:
            out.append((r.term, "consistent_same_party"))
        else:
            out.append((r.term, "inconsistent"))
    return pd.DataFrame(out, columns=["term", "status"])


def load_term_party_records():
    """{(naas_cd, term): POLY_NM} from the Open API per-term member list (npffdutiapkzbfyvr, as
    downloaded by the legislators component to v10/interim/pipeline/legislators/api; read only)."""
    out = {}
    for r in _read_api_rows("npffdutiapkzbfyvr_*_p*.json"):
        m = re.search(r"(\d+)", str(r.get("UNIT_NM") or r.get("UNIT_CD") or ""))
        if not m or not r.get("POLY_NM"):
            continue
        t = int(m.group(1)) if len(m.group(1)) <= 2 else int(m.group(1)[-2:])
        out[(r.get("MONA_CD"), t)] = nfkc(r["POLY_NM"]).strip()
    return out


def _clip_speaker(spells, n, t, a, b):
    """Window [a, b] without the member's Speaker spells at its start or end (무소속 by 국회법
    제20조의2 is documented, not an unknown party state)."""
    g = spells[(spells.naas_cd == n) & (spells.term == t)]
    if "is_speaker_nonpartisan" in g:
        for r in g[g.is_speaker_nonpartisan.fillna(False).astype(bool)].sort_values("start").itertuples():
            if r.start <= a <= r.end:
                a = day_after(r.end)
            elif a < r.start <= b <= r.end:
                b = day_before(r.start)
    return a, b


def structural_windows(spells: pd.DataFrame, spans: pd.DataFrame, tbs: dict, lin: Lineage, term_party=None):
    """Uncertainty windows found on the finished spells (after renames / mergers / Speaker rule):
    possible_unrecorded_membership - a 무소속 spell (not the Speaker's) followed by a party formed by
        the merger of parties founded during that spell (17대 통합민주당 <- 대통합민주신당 /
        중도통합민주당; 20대 민생당 <- 민주평화당): no member-level record says who joined the
        predecessor; the window runs from the earliest such founding to the spell end.
    contradicted_by_next_term_election_party - a member who served to the term end and was elected
        at the next general election from another organisation than his last spell's party: the
        change is not recorded (16대: conf 26536 report items are empty); the window runs from the
        last documented day of his final state to the term end.
    contradicted_by_term_party_record - the Open API per-term record (POLY_NM) names a party
        founded after the member's last documented day that none of his later spells holds (16대
        members recorded as 열린우리당): the window runs from that day to his last seat day."""
    out = []
    preds = defaultdict(list)
    for r in lin.rows.values():
        if r["successor"] and r["kind"] == "merger_into" and r["label_from"]:
            preds[r["successor"]].append(r)
    for (n, t, st), g in spells.sort_values("start").groupby(["naas_cd", "term", "stint"], sort=False):
        rows = g.to_dict("records")
        for i, s in enumerate(rows[:-1]):
            if s["party"] != INDEP or bool(s.get("is_speaker_nonpartisan")):
                continue
            nx = rows[i + 1]
            k = lin.key(nx["party"], nx["start"]) if nx["party"] not in (None, INDEP) else None
            if not k:
                continue
            lo = _shift(s["start"], -30)
            q = [r for r in preds.get(k, []) if lo <= r["label_from"] <= s["end"]]
            if q:
                f = max(s["start"], min(r["label_from"] for r in q))
                out.append({"naas_cd": n, "term": int(t), "lo": f, "hi": s["end"],
                            "reason": "possible_unrecorded_membership",
                            "evidence": f"{nx['party']} ({nx['start']}) formed by merger of "
                                        + ", ".join(f"{_base_label(r['label'])} (label from {r['label_from']})" for r in q),
                            "conf_num": None, "gaps_in_window": None, "rule": None})
    nxt = spans.copy()
    nxt["label"] = nxt.party_elected.where(nxt.party_elected.notna() & (nxt.party_elected.astype(str) != ""), nxt.party_at_entry)
    nxt = nxt[nxt.apply(lambda r: r.seat_start == TERM_START.get(int(r.term)), axis=1)]
    nxt = dict(zip(zip(nxt.naas_cd, nxt.term.astype(int)), nxt.label))
    last = spells.sort_values("start").groupby(["naas_cd", "term"]).tail(1)
    for s in last.itertuples():
        t = int(s.term)
        end = TERM_END[t]
        lab = nxt.get((s.naas_cd, t + 1))
        if s.end != end or not isinstance(lab, str) or not lab.strip() or t not in tbs:
            continue
        q, how = lin.label_at(lab, end)
        ours = norm_party(s.party or "")
        theirs = norm_party(q)
        if {ours, theirs} <= {INDEP, NON_GROUP} or ours == theirs:
            continue
        if ours not in (INDEP,) and theirs not in (INDEP,) and lin.same_party(ours, theirs, end):
            continue
        tb = tbs[t]
        lo = tb.known_since(s.naas_cd)
        a, hi = _clip_speaker(spells, s.naas_cd, t, day_after(lo) if lo > "0001" else s.start, end)
        if a > hi:
            continue
        gaps = ";".join(x for x in tb.gap_dates if a <= x <= end)
        out.append({"naas_cd": s.naas_cd, "term": t, "lo": a, "hi": hi,
                    "reason": "contradicted_by_next_term_election_party",
                    "evidence": f"elected {TERM_START[t + 1][:4]} from {lab} (-> {q} on {end}); last spell {s.party}",
                    "conf_num": None, "gaps_in_window": gaps, "rule": how})
    by_member = {k: g.sort_values("start") for k, g in spells.groupby(["naas_cd", "term"])}
    for (n, t), lab in (term_party or {}).items():
        g = by_member.get((n, t))
        if g is None or t not in tbs:
            continue
        if len(lin.by_base.get(norm_party(lab), [])) > 1:
            continue                    # '국민의당' (2016 / 2020), '민주당' (2005 / 2008 / 2013): ambiguous
        k = lin.key(lab, g.end.max(), historic=True)
        lf = lin.rows[k]["label_from"] if k else ""
        lo = tbs[t].known_since(n)
        end = g.end.max()
        if not lf or not (lo < lf <= end):
            continue
        later = g[g.end >= lf]
        if any(r.party not in (None, INDEP) and lin.same_party(r.party, lin.formal_on(lab, max(r.start, lf), historic=True)[0],
                                                                 max(r.start, lf)) for r in later.itertuples()):
            continue
        a, hi = _clip_speaker(spells, n, t, day_after(lo) if lo > "0001" else g.start.min(), end)
        if a > hi:
            continue
        out.append({"naas_cd": n, "term": int(t), "lo": a, "hi": hi, "reason": "contradicted_by_term_party_record",
                    "evidence": f"Open API per-term record (npffdutiapkzbfyvr POLY_NM) {lab}, label from {lf}; "
                                f"spells after it: {', '.join(later.party.astype(str))}",
                    "conf_num": None, "gaps_in_window": ";".join(x for x in tbs[t].gap_dates if a <= x <= end), "rule": None})
    return out


def gap_periods_from_inventory(inv: pd.DataFrame):
    """term -> [(report period start, plenary date, conf_num, why)] for plenaries whose report items
    cannot be read: no page ('no_page') or party items printed empty ('empty_party_items'). The
    report period starts the day after the term's previous plenary."""
    out = defaultdict(list)
    inv = inv.sort_values(["term", "date", "conf_num"])
    for t, g in inv.groupby("term"):
        prev = None
        for r in g.itertuples():
            empty = getattr(r, "n_empty_party_items", 0)
            nopage = r.source is None or (isinstance(r.source, float) and pd.isna(r.source))
            if nopage or (empty and empty > 0):
                ps = day_after(prev) if prev and prev < r.date else TERM_START[int(t)]
                out[int(t)].append((ps, r.date, int(r.conf_num), "no_page" if nopage else "empty_party_items"))
            prev = r.date
    return out


def unreadable_report_windows(spells: pd.DataFrame, tbs: dict, periods: dict):
    """no_record_after_unreadable_report: a member seated on the date of a plenary whose report
    items cannot be read, and of whom no later record of the term (membership event, or committee
    row agreeing with his party) exists: a change reported in that plenary would be invisible. The
    window runs from the report period start (or the member's last record inside it) to his seat end."""
    out = []
    for t, tb in tbs.items():
        for ps, g, cn, why in periods.get(t, []):
            for n, ivs in tb.iv.items():
                docs = sorted(tb.docs.get(n, []))
                for a, b in ivs:
                    if not (a <= g <= b) or any(g < d <= b for d in docs):
                        continue
                    inside = [d for d in docs if ps <= d <= g]
                    lo = max(ps, a, day_after(inside[-1]) if inside else ps)
                    lo, hi = _clip_speaker(spells, n, t, lo, b)
                    if lo > hi:
                        continue
                    out.append({"naas_cd": n, "term": int(t), "lo": lo, "hi": hi,
                                "reason": "no_record_after_unreadable_report",
                                "evidence": f"conf_num {cn} ({g}, {why}): no record of the member's party after it",
                                "conf_num": cn, "gaps_in_window": g, "rule": None})
    return out


WEAK_REASONS = ("no_record_after_unreadable_report",)


def mark_uncertain(spells: pd.DataFrame, win: pd.DataFrame):
    """uncertain (the spell overlaps an uncertainty window of the member in the term that rests on
    evidence of an undocumented change), uncertain_reason (reasons of those windows), uncertain_days
    (days of the spell in them); unconfirmed_after_gap (the spell overlaps a window where only the
    confirmation is missing: no record of the member after a plenary whose report items cannot be
    read, WEAK_REASONS)."""
    sp = spells.copy()
    by = defaultdict(list)
    for w in win.itertuples():
        by[(w.naas_cd, int(w.term))].append((w.lo, w.hi, w.reason))
    unc, why, days, weak = [], [], [], []
    for r in sp.itertuples():
        ws = [w for w in by.get((r.naas_cd, int(r.term)), []) if w[0] <= r.end and w[1] >= r.start]
        strong = [w for w in ws if w[2] not in WEAK_REASONS]
        unc.append(bool(strong))
        weak.append(any(w[2] in WEAK_REASONS for w in ws))
        why.append(";".join(sorted({w[2] for w in strong})) or None)
        cover = set()
        for a, b, _ in strong:
            x, y = max(a, r.start), min(b, r.end)
            d0 = dt.date.fromisoformat(x)
            for i in range((dt.date.fromisoformat(y) - d0).days + 1):
                cover.add(d0 + dt.timedelta(days=i))
        days.append(len(cover))
    sp["uncertain"] = unc
    sp["uncertain_reason"] = why
    sp["uncertain_days"] = days
    sp["unconfirmed_after_gap"] = weak
    return sp


def gap_dates_from_inventory(inv: pd.DataFrame):
    """term -> plenary dates whose report items cannot be read (no page, or party items empty)."""
    out = defaultdict(list)
    for r in inv.itertuples():
        empty = getattr(r, "n_empty_party_items", 0)
        if r.source is None or (isinstance(r.source, float) and pd.isna(r.source)) or (empty and empty > 0):
            out[int(r.term)].append(r.date)
    return out


def start_roster_map(rec: pd.DataFrame):
    """term -> {naas_cd: 교섭단체} from start rosters (within 45 days of the term start; names that
    resolve to one seated member only)."""
    out = defaultdict(dict)
    r = rec[(rec.kind == "roster") & rec.date.notna()]
    for x in r.itertuples():
        t = int(x.term)
        lim = (dt.date.fromisoformat(TERM_START[t]) + dt.timedelta(days=45)).isoformat()
        if x.date <= lim and len(x.cands) == 1:
            out[t][x.cands[0]] = norm_party(x.group or "")
    return out


def build(write=True, verbose=True, meetings=None, harvested=None, terms=range(16, 23)):
    """Harvest -> resolve -> spells for all terms. Writes to v10/interim/pipeline/party_timeline/.
    harvested=(rec, inv) reuses a harvest."""
    OUT.mkdir(parents=True, exist_ok=True)
    lin = Lineage()
    mem = load_member_terms()
    spans = mem[["naas_cd", "term", "stint", "name", "name_hanja", "seat_start", "seat_end", "seat_end_raw",
                 "party_at_entry", "party_elected", "district", "district_hist", "source"]].copy()
    idx = NameIndex(mem)
    rec, inv = harvest(meetings=meetings, verbose=verbose) if harvested is None else harvested
    rec, rstats = resolve_names(rec, idx, spans)
    ntr = notice_transitions(rec)
    aliases = lin.register_notice_aliases(ntr)
    gaps = gap_dates_from_inventory(inv)
    starts = start_roster_map(rec)
    spells, logs, tbs, windows, tstats = [], [], {}, [], Counter()
    for term in terms:
        tb = TermBuilder(term, spans[spans.term == term], rec[rec.term == term], lin, idx,
                         gap_dates=gaps.get(term, []), next_start=starts.get(term + 1, {}))
        spells.append(tb.run())
        logs.extend(tb.log)
        windows.extend(tb.windows)
        tstats.update(tb.stats)
        tbs[term] = tb
    spells = pd.concat(spells, ignore_index=True)
    spells, n_relabel = lineage_relabel(spells, lin, ntr)
    spells = speaker_flags(spells)
    ten, _chairs = speaker_tenures(idx)
    spells, spk_log = apply_speaker_rule(spells, ten)
    spells = spells.merge(spans[["naas_cd", "term", "stint", "name"]], on=["naas_cd", "term", "stint"], how="left")
    spells = spells.sort_values(["term", "naas_cd", "start"]).reset_index(drop=True)
    windows.extend(structural_windows(spells, spans, tbs, lin, load_term_party_records()))
    windows.extend(unreadable_report_windows(spells, tbs, gap_periods_from_inventory(inv)))
    win = pd.DataFrame(windows, columns=["naas_cd", "term", "lo", "hi", "reason", "conf_num", "gaps_in_window",
                                         "evidence", "rule"])
    spells = mark_uncertain(spells, win)
    spells["spell_id"] = (spells.term.astype(str) + "-" + spells.naas_cd + "-"
                          + (spells.groupby(["term", "naas_cd"]).cumcount() + 1).astype(str))
    log = pd.DataFrame(logs)
    obs = check_observations(spells, rec, lin)
    null = rec[rec.date.isna()].kind.value_counts().to_dict()
    stats = {"resolve": rstats, "n_records": len(rec), "n_spells": len(spells), "lineage_relabel_pieces": n_relabel,
             "meetings": len(inv), "meetings_with_page": int(inv.source.notna().sum()),
             "meetings_with_page_but_no_items": int(((inv.source.notna()) & (inv.n_items.fillna(0) == 0)).sum()),
             "records_null_date_by_kind": {k: int(v) for k, v in null.items()},
             "records_meeting_date_upper_bound_by_kind": {k: int(v) for k, v in
                                                          rec[rec.date_src == "meeting"].kind.value_counts().items()},
             "termbuilder": dict(tstats),
             "notice_aliases": [list(x) for x in aliases],
             "uncertainty_windows_by_reason": {k: int(v) for k, v in win.reason.value_counts().items()},
             "uncertain_spells": int(spells.uncertain.sum()),
             "unconfirmed_after_gap_spells": int(spells.unconfirmed_after_gap.sum())}
    if write:
        keep = [c for c in rec.columns if c not in ("header",)]
        r2 = rec[keep].copy()
        for c in ("cands", "cands_all"):
            r2[c] = r2[c].map(lambda x: ";".join(x) if isinstance(x, tuple) else None)
        for c in r2.columns:
            if r2[c].dtype == object:
                r2[c] = r2[c].map(lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
        r2.to_parquet(OUT / "report_records.parquet", index=False)
        inv.to_parquet(OUT / "plenary_inventory.parquet", index=False)
        lg = log.copy()
        for c in lg.columns:
            if lg[c].dtype == object:
                lg[c] = lg[c].map(lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
        lg.to_parquet(OUT / "event_log.parquet", index=False)
        sp2 = spells.copy()
        for c in sp2.columns:
            if sp2[c].dtype == object:
                sp2[c] = sp2[c].map(lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
        sp2.to_parquet(OUT / "party_spells.parquet", index=False)
        spans.to_parquet(OUT / "seat_spans.parquet", index=False)
        obs.groupby(["term", "status"]).size().rename("n").reset_index().to_csv(OUT / "observation_check.csv", index=False)
        ten.to_csv(OUT / "speaker_tenures.csv", index=False)
        pd.DataFrame([{"old": a, "date": d, "new": b, "kind": k, "conf_num": int(c)}
                      for a, v in ntr.items() for d, b, k, c in v]).to_parquet(OUT / "party_transitions.parquet", index=False)
        spk_log.to_csv(OUT / "speaker_rule_log.csv", index=False)
        w2 = win.copy()
        for c in w2.columns:
            if w2[c].dtype == object:
                w2[c] = w2[c].map(lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
        w2.to_parquet(OUT / "party_uncertainty.parquet", index=False)
        (OUT / "build_stats.json").write_text(json.dumps(stats, ensure_ascii=False, indent=1, default=str))
    return {"records": rec, "inventory": inv, "log": log, "spells": spells, "spans": spans,
            "observations": obs, "stats": stats, "speakers": ten, "speaker_log": spk_log, "windows": win}


# ============================================================================= enrich

ENRICH_COLS = ["party", "party_lineage", "party_camp", "is_satellite", "ruling_status", "presidency_state",
               "president", "president_party", "president_last_party", "party_method", "acting_president",
               "is_speaker_nonpartisan",
               "party_before_speaker", "ruling_null_reason", "party_spell_id", "party_basis",
               "party_uncertain", "party_uncertain_reason", "party_unconfirmed_after_gap"]
_DATE_RX = re.compile(r"^\s*(\d{4})[-./](\d{1,2})[-./](\d{1,2})")


def norm_date(x):
    """'2022-07-10', '2022/7/10', '2022.07.10', Timestamp -> 'YYYY-MM-DD'; invalid or missing -> None."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or x is pd.NaT:
        return None
    if isinstance(x, (dt.date, pd.Timestamp)):
        try:
            return pd.Timestamp(x).date().isoformat()
        except (ValueError, TypeError):
            return None
    m = _DATE_RX.match(str(x))
    if not m:
        return None
    d = _mk(*m.groups())
    return d.isoformat() if d else None


class Resolver:
    """Date-indexed party lookups built from the written interim tables (or given frames).

    formal_on(label, since, date) -> label after the renames / mergers effective in (since, date]
    family(label, date)    -> last label of the rename / merger chain from `date` on
    camp(label, date)      -> main party for a satellite (before its merger), else the label
    ruling(party, date)    -> (ruling_status, null_reason, presidency dict)
    ruling_reference(date) -> the label that counts as the president's party on `date`: his formal
                              party, or in a partyless window his most recent party carried through
                              renames / mergers to `date`; None in acting windows
    spell_lookup(pairs)    -> spell covering (naas_cd, date)
    nearest_spell(naas, term, date) -> the member's closest spell in the term (no spell covers date)
    uncertain(pairs)       -> party_uncertain / party_uncertain_reason from party_uncertainty.parquet"""

    def __init__(self, spells=None, spans=None, ntr=None, lin=None, cal=None, windows=None):
        self.lin = lin or Lineage()
        self.cal = load_calendar() if cal is None else cal
        if spells is None:
            spells = pd.read_parquet(OUT / "party_spells.parquet")
        if spans is None:
            spans = pd.read_parquet(OUT / "seat_spans.parquet")
        if ntr is None:
            f = OUT / "party_transitions.parquet"
            ntr = defaultdict(list)
            if f.exists():
                for r in pd.read_parquet(f).itertuples():
                    ntr[r.old].append((r.date, r.new, r.kind, r.conf_num))
        if windows is None:
            f = OUT / "party_uncertainty.parquet"
            windows = pd.read_parquet(f) if f.exists() else pd.DataFrame(columns=["naas_cd", "term", "lo", "hi", "reason"])
        self.ntr = ntr
        self.lin.register_notice_aliases(ntr)
        self.spells = spells.copy()
        self.spells["term"] = self.spells.term.astype(int)
        self.spans = spans.copy()
        self.spans["term"] = self.spans.term.astype(int)
        self.windows = windows
        self._fam, self._camp, self._pres, self._ref = {}, {}, {}, {}
        self._by_member = None

    # ------------------------------------------------------------------ labels
    def formal_on(self, label, since, date):
        cur, a = norm_party(label), since
        for _ in range(20):
            if not cur or cur == INDEP:
                return cur
            t = next_transition(cur, a, date, self.lin, self.ntr)
            if t is None:
                return cur
            cur, a = t[1], t[0]
        return cur

    def family(self, label, date):
        key = (label, date)
        if key not in self._fam:
            cur, a, last = norm_party(label), date, norm_party(label)
            for _ in range(30):
                if not cur or cur == INDEP:
                    break
                last = cur
                t = next_transition(cur, a, "9999-12-31", self.lin, self.ntr)
                if t is None:
                    break
                cur, a = t[1], t[0]
            self._fam[key] = last or None
        return self._fam[key]

    def satellite_parent(self, label, date):
        k = self.lin.key(label, date)
        return (self.lin.rows[k]["satellite_of"] or None) if k else None

    def camp(self, label, date):
        key = (label, date)
        if key not in self._camp:
            p = norm_party(label)
            if p and p != INDEP and self.lin.key(p, date) is None:
                q, how = self.lin.label_at(p, date)
                if how == "backdated_rename":        # '자유한국당' on 2016-07-01 = 새누리당
                    p = q
            sat = self.satellite_parent(p, date) if p and p != INDEP else None
            if sat:
                k = self.lin.key(sat, date)
                since = (self.lin.rows[k]["label_from"] or date) if k else date
                self._camp[key] = self.formal_on(sat, min(since, date), date)
            else:
                self._camp[key] = p or None
        return self._camp[key]

    def presidency(self, date):
        """Calendar row for `date`; dates before the calendar's first row (1998-02-25) are outside
        the calendar (presidency_state None), not 'vacant'."""
        if date not in self._pres:
            if not hasattr(self, "_cal_rows"):
                self._cal_rows = self.cal.to_dict("records")
                self._cal_starts = [r["start"] for r in self._cal_rows]
            i = bisect.bisect_right(self._cal_starts, date) - 1 if date else -1
            r = self._cal_rows[i] if i >= 0 else None
            if not date or r is None or date > r["end"]:
                self._pres[date] = {"presidency_state": None, "president": None, "president_party": None,
                                    "president_last_party": None, "last_party_since": None,
                                    "acting_president": None, "outside": bool(date)}
            else:
                self._pres[date] = {"presidency_state": r["presidency_state"], "president": r["president"],
                                    "president_party": r["president_party"],
                                    "president_last_party": r["president_last_party"],
                                    "last_party_since": r["last_party_since"],
                                    "acting_president": r["acting_president"], "outside": False}
        return self._pres[date]

    def ruling_reference(self, date):
        """Formal label that counts as the president's party on `date` (see the class docstring)."""
        pr = self.presidency(date)
        if pr["president_party"]:
            return pr["president_party"]
        last = pr.get("president_last_party")
        if not last or pr["presidency_state"] == "acting":
            return None
        key = (last, pr["last_party_since"], date)
        if key not in self._ref:
            # decision 6: the most recent party and its lineage successors (renames / mergers after
            # the last day the president held it, up to `date`)
            self._ref[key] = self.formal_on(last, pr["last_party_since"] or date, date)
        return self._ref[key]

    def ruling(self, party, date):
        pr = self.presidency(date)
        st = pr["presidency_state"]
        if pr.get("outside"):
            return None, "outside_calendar", pr
        if st == "acting":
            return None, st, pr
        if not party:
            return None, "no_party", pr
        if party == INDEP:
            return "independent", None, pr
        ref = self.ruling_reference(date)
        if not ref:                           # impossible with a calendar that load_calendar accepts
            return None, "no_president_party", pr
        c = self.camp(party, date)
        same = norm_party(c) == norm_party(ref) or self.lin.same_party(c, ref, date)
        return ("ruling" if same else "opposition"), None, pr

    # ------------------------------------------------------------------ people
    SPELL_COLS = ["party", "spell_id", "basis", "is_speaker_nonpartisan", "party_before_speaker", "spell_term", "start"]

    def spell_lookup(self, pairs: pd.DataFrame):
        """pairs(naas_cd, date 'YYYY-MM-DD') -> same rows + spell columns (None when no spell covers
        the date). Dates are parsed with one fixed format, so a row's result never depends on the
        other rows."""
        sp = self.spells.dropna(subset=["start"]).sort_values("start")
        left = pairs.reset_index(drop=True).copy()
        left["_k"] = pd.to_datetime(left["date"], format="%Y-%m-%d", errors="coerce")
        left["_i"] = range(len(left))
        l2 = left.dropna(subset=["_k"]).sort_values("_k")
        sp = sp.assign(_k=pd.to_datetime(sp.start, format="%Y-%m-%d"))
        m = pd.merge_asof(l2, sp[["_k", "naas_cd", "end", "party", "spell_id", "basis", "term", "start",
                                  "is_speaker_nonpartisan", "party_before_speaker"]].rename(columns={"term": "spell_term"}),
                          on="_k", by="naas_cd", direction="backward")
        ok = m["end"].notna() & (m["date"] <= m["end"])
        for c in self.SPELL_COLS:
            m[c] = m[c].astype(object).where(ok, None)
        m = m.set_index("_i").reindex(range(len(left)))
        return pd.concat([left.drop(columns=["_k", "_i"]), m[self.SPELL_COLS].reset_index(drop=True)], axis=1)

    def nearest_spell(self, naas, term, date):
        """The member's spell closest to `date` in `term` (the last one ending before it, else the
        first one starting after it); any term when term is unknown. None when the member has no
        spell there."""
        if self._by_member is None:
            self._by_member = {k: g.sort_values("start").to_dict("records")
                               for k, g in self.spells.dropna(subset=["start"]).groupby("naas_cd")}
        rows = self._by_member.get(naas, [])
        if term is not None:
            rows = [r for r in rows if int(r["term"]) == int(term)]
        if not rows:
            return None, None
        before = [r for r in rows if r["end"] < date]
        if before:
            return before[-1], "before"
        after = [r for r in rows if r["start"] > date]
        return (after[0], "after") if after else (None, None)

    def uncertain(self, pairs: pd.DataFrame):
        """pairs(naas_cd, date) -> (party_uncertain, reason, unconfirmed_after_gap) arrays from the
        uncertainty windows (any term of the member). party_uncertain covers the windows that rest
        on evidence of an undocumented change; the reason lists every window's reason, including
        the weak 'no_record_after_unreadable_report', which alone sets only unconfirmed_after_gap."""
        n = len(pairs)
        unc = np.zeros(n, dtype=bool)
        weak = np.zeros(n, dtype=bool)
        why = np.full(n, None, dtype=object)
        if self.windows is None or not len(self.windows) or not n:
            return unc, why, weak
        by = defaultdict(list)
        for w in self.windows.itertuples():
            by[w.naas_cd].append((w.lo, w.hi, w.reason))
        for i, (a, d) in enumerate(zip(pairs.naas_cd, pairs.date)):
            ws = by.get(a)
            if not ws or not isinstance(d, str):
                continue
            hit = sorted({r for lo, hi, r in ws if lo <= d <= hi})
            if hit:
                unc[i] = any(r not in WEAK_REASONS for r in hit)
                weak[i] = any(r in WEAK_REASONS for r in hit)
                why[i] = ";".join(hit)
        return unc, why, weak

    def election_party(self, naas, term, date):
        s = self.spans[self.spans.naas_cd == naas]
        if s.empty:
            return None, None
        t = s[s.term == int(term)] if term is not None and not pd.isna(term) else s.iloc[0:0]
        if t.empty:
            s = s.assign(_d=(pd.to_datetime(s.seat_start) - pd.Timestamp(date)).abs())
            t = s.sort_values("_d").head(1)
        r = t.sort_values("seat_start").iloc[0]
        return r.party_at_entry, r.seat_start


def enrich(turns: pd.DataFrame, meetings: pd.DataFrame, resolver: Resolver | None = None):
    """CONTRACT enrichment: adds ENRICH_COLS to `turns` without reordering rows.
    Legislator-group turns (role_group == 'legislator'; when role_group is absent, any turn with a
    naas_cd) get party columns; every turn with a date gets the presidency columns. The date is
    speech_date, else the meeting date, normalised to YYYY-MM-DD (an invalid date counts as none).
    party_method: 'person_spell' (a spell of the member covers the date; when none does, his
    closest spell in the meeting's term carried to the date through renames / mergers, party_basis
    'nearest_spell_before:...' / 'nearest_spell_after:...'), 'label_lineage' (no spell of the member
    in the term: election party through the lineage), or null (ruling_null_reason says why:
    no_date, no_naas_cd, no_party). Lookups run on unique (naas_cd, date) and (party, date) keys."""
    rs = resolver or Resolver()
    out = turns.copy()
    n = len(out)
    df = out.reset_index(drop=True)
    need = [c for c in ("term", "date") if c not in df.columns]
    if need and meetings is not None and len(meetings):
        m = meetings[["conf_num"] + need].drop_duplicates("conf_num")
        df = df.merge(m, on="conf_num", how="left")
    if len(df) != n:
        raise ValueError("meetings join changed the row count")
    sd = df["speech_date"] if "speech_date" in df.columns else pd.Series([None] * n, dtype=object)
    md = df["date"] if "date" in df.columns else pd.Series([None] * n, dtype=object)
    def _norm(col):             # per unique value (dates repeat across turns)
        codes, uniq = pd.factorize(col.astype(object), use_na_sentinel=True)
        lut = np.array([norm_date(x) for x in uniq] + [None], dtype=object)
        return pd.Series(lut[codes], dtype=object)
    s1, s2 = _norm(sd), _norm(md)
    d = s1.where(s1.notna(), s2).astype(object)
    dn = d.notna()
    if "role_group" in df.columns:
        is_leg = df.role_group.eq("legislator").to_numpy()
    else:
        is_leg = df.get("naas_cd", pd.Series([None] * n)).notna().to_numpy()
    naas = df["naas_cd"].astype(object) if "naas_cd" in df.columns else pd.Series([None] * n, dtype=object)
    term = df["term"] if "term" in df.columns else pd.Series([None] * n, dtype=object)
    res = {c: np.full(n, None, dtype=object) for c in ENRICH_COLS}
    # presidency (all rows with a date)
    pres = {x: rs.presidency(x) for x in d.dropna().unique()}
    dcodes, duniq = pd.factorize(d, use_na_sentinel=True)
    for c in ("presidency_state", "president", "president_party", "president_last_party", "acting_president"):
        lut = np.array([pres[x][c] for x in duniq] + [None], dtype=object)
        res[c] = lut[dcodes]                         # code -1 (no date) -> last entry (None)
    has_date = dn.to_numpy()
    linked = is_leg & has_date & (naas.notna() & (naas.astype(str) != "")).to_numpy()
    res["ruling_null_reason"][is_leg & ~has_date] = "no_date"
    unl = is_leg & has_date & ~linked
    res["ruling_null_reason"][unl] = "no_naas_cd"
    if linked.any():
        rows = np.flatnonzero(linked)
        ln = naas.to_numpy(dtype=object)[rows]
        ld = d.to_numpy(dtype=object)[rows]
        lt = term.to_numpy(dtype=object)[rows]
        # unique (naas_cd, date, term) keys; everything below runs per key
        key = pd.MultiIndex.from_arrays([pd.Series(ln).astype(str), pd.Series(ld).astype(str), pd.Series(lt).astype(str)])
        kc, ku = pd.factorize(key)
        first = np.full(len(ku), -1)
        first[kc[::-1]] = np.arange(len(kc))[::-1]
        U = pd.DataFrame({"naas_cd": ln[first], "date": ld[first], "term": lt[first]})
        hit = rs.spell_lookup(U[["naas_cd", "date"]])
        U["party"] = hit["party"].to_numpy(dtype=object)
        has_spell = U.party.notna().to_numpy()
        U["party_method"] = np.where(has_spell, "person_spell", None)
        U["party_basis"] = np.where(has_spell, hit["basis"].to_numpy(dtype=object), None)
        U["party_spell_id"] = np.where(has_spell, hit["spell_id"].to_numpy(dtype=object), None)
        spk = hit.is_speaker_nonpartisan.map(lambda x: x is True or x == "True" or x == 1).to_numpy(dtype=bool)
        U["is_speaker_nonpartisan"] = (spk & has_spell).astype(object)
        pbs = hit.party_before_speaker.to_numpy(dtype=object)
        U["party_before_speaker"] = np.where(has_spell & ~pd.isna(pbs), pbs, None)
        cache = {}
        for i in np.flatnonzero(~has_spell):
            a_, t_, dd = U.at[i, "naas_cd"], U.at[i, "term"], U.at[i, "date"]
            tt = None if t_ is None or pd.isna(t_) or str(t_) in ("", "None", "nan") else int(float(t_))
            k2 = (a_, tt, dd)
            if k2 not in cache:
                sp_, side = rs.nearest_spell(a_, tt, dd)
                if sp_ is not None:
                    lab = sp_["party"]
                    v = lab if lab in (None, INDEP) or side == "after" else rs.formal_on(lab, sp_["start"], dd)
                    spk_ = str(sp_.get("is_speaker_nonpartisan")) in ("True", "1")
                    cache[k2] = (v, "person_spell", f"nearest_spell_{side}:{sp_['basis']}", sp_["spell_id"], spk_,
                                 sp_.get("party_before_speaker") if spk_ else None)
                else:
                    ep, since = rs.election_party(a_, tt, dd)
                    v = None
                    if ep:
                        # the recorded label as the formal label on the seat start (renames and
                        # mergers forward, a rename adopted later backward), then carried to the date
                        f0, _how = rs.lin.label_at(ep, since)
                        k = rs.lin.key(f0, since)
                        born = (rs.lin.rows[k]["label_from"] or "0000-01-01") if k else "0000-01-01"
                        v = rs.formal_on(f0, min(born, since, dd), dd)
                    cache[k2] = (v, "label_lineage" if v else None,
                                 "election_party_lineage" if v else "no_member_term", None, False, None)
            v, meth, basis, sid, spk_, pb = cache[k2]
            U.at[i, "party"] = v
            U.at[i, "party_method"] = meth
            U.at[i, "party_basis"] = basis
            U.at[i, "party_spell_id"] = sid
            U.at[i, "is_speaker_nonpartisan"] = bool(spk_) if meth == "person_spell" else False
            U.at[i, "party_before_speaker"] = pb
        unc, why, weak = rs.uncertain(U[["naas_cd", "date"]])
        U["party_uncertain"] = unc.astype(object)
        U["party_uncertain_reason"] = why
        U["party_unconfirmed_after_gap"] = weak.astype(object)
        # party-level columns on unique (party, date)
        pk = pd.MultiIndex.from_arrays([U.party.astype(str), U.date.astype(str)])
        pc, pu = pd.factorize(pk)
        pfirst = np.full(len(pu), -1)
        pfirst[pc[::-1]] = np.arange(len(pc))[::-1]
        cols = {c: np.full(len(pu), None, dtype=object) for c in
                ("party_lineage", "party_camp", "is_satellite", "ruling_status", "ruling_null_reason")}
        Up, Ud = U.party.to_numpy(dtype=object), U.date.to_numpy(dtype=object)
        for j, i in enumerate(pfirst):
            pa, dd = Up[i], Ud[i]
            if isinstance(pa, str):
                cols["party_lineage"][j] = INDEP if pa == INDEP else rs.family(pa, dd)
                cols["party_camp"][j] = INDEP if pa == INDEP else rs.camp(pa, dd)
                cols["is_satellite"][j] = False if pa == INDEP else bool(rs.satellite_parent(pa, dd))
            s_, w_, _ = rs.ruling(pa if isinstance(pa, str) else None, dd)
            cols["ruling_status"][j], cols["ruling_null_reason"][j] = s_, w_
        for c, v in cols.items():
            U[c] = v[pc]
        for c in ("party", "party_lineage", "party_camp", "is_satellite", "ruling_status", "party_method",
                  "is_speaker_nonpartisan", "party_before_speaker", "ruling_null_reason", "party_spell_id",
                  "party_basis", "party_uncertain", "party_uncertain_reason", "party_unconfirmed_after_gap"):
            v = U[c].to_numpy(dtype=object)[kc]
            v[pd.isna(v)] = None
            res[c][rows] = v
    # rows with a date but outside the calendar
    outside = np.array([bool(pres[x].get("outside")) if isinstance(x, str) else False for x in d.to_numpy(dtype=object)])
    fill = outside & is_leg & (res["ruling_null_reason"] == None)  # noqa: E711
    res["ruling_null_reason"][fill] = "outside_calendar"
    for c in ENRICH_COLS:
        out[c] = res[c]
    out["is_satellite"] = out["is_satellite"].astype("boolean")
    out["is_speaker_nonpartisan"] = out["is_speaker_nonpartisan"].astype("boolean")
    out["party_uncertain"] = out["party_uncertain"].astype("boolean")
    out["party_unconfirmed_after_gap"] = out["party_unconfirmed_after_gap"].astype("boolean")
    return out
