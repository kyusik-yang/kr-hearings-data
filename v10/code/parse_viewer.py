"""Parser for record.assembly.go.kr minutes viewer pages (국회회의록시스템, 2025 relaunch).

    parse_view(html)    -> dict(status, meeting, agenda_header, agenda, speeches, events, footer, stats)
    parse_summary(html) -> dict(status, meeting_info, files, attendance, agenda, charts)

Input is the raw page (bytes or str) of
    https://record.assembly.go.kr/assembly/viewer/minutes/xml.do?id={id}&type=view
    https://record.assembly.go.kr/assembly/viewer/minutes/xml.do?id={id}&type=summary

Design notes:
- Speeches are div.speaker blocks inside div.minutes_body, in document order. Each carries
  data-mem_id / data-name / data-pos; sentences are span.spk_sub inside div.talk > div.txt.
- Agenda anchors are p.tit_sm.angun (a.tit with id="itemN", optional likms bill link).
- Time markers are p.tit_sm.taR, e.g. "(15시07분 개의)", "(15시08분)", "(12시05분 정회)".
- Every text-bearing node in minutes_body is accounted for: speeches, agenda anchors,
  time markers, or `events` of kind "other" (so text loss can be tested).
- Off-mic remarks of other people recorded inside a turn ('(◯정양석 의원 발언대 옆에서 ― ...)')
  are stage sentences of kind "interjection" with {who, where, text} in `interjections`.
- The speaker div class 'itemK' is the ordinal of the K-th body anchor (p.angun), not the
  anchor's own id; `agenda_ordinal` / `item_class_ordinal` carry both views and agree.
- Stage directions are kept in the text. A sentence is flagged `is_stage` (with a
  `stage_kind`) when the whole sentence is parenthesized or wraps a div.taR note;
  `text_spoken` drops stage sentences and oath-signature lines; inline parentheticals
  that match the stage lexicon are listed in `inline_stage_parens`.
- A turn (one spk_N id) can be split into several div.speaker fragments by a time marker
  such as '(3월19일 24시 경과)' or '(14시38분 투표종료)'; fragments are merged into one
  speech (`n_fragments`), and the marker events get `within_turn: true`.

Pure functions, lxml only. CLI:  python parse_viewer.py FILE[.gz] [--summary] [--indent N]
"""
import datetime as _dt
import gzip
import json
import re
import sys

from lxml import html as LH

__all__ = ["parse_view", "parse_summary", "norm", "parse_title"]

WS_RE = re.compile(r"\s+")
HANJA_RE = re.compile(r"[㐀-䶿一-鿿豈-﫿]")
TITLE_RE = re.compile(
    r"^제?(?P<term>\d+|헌)대?(?:국회)?\s*"
    r"(?:제(?P<session>\d+)회\s*)?"
    r"(?:\((?P<session_type>[^)]*)\)\s*)?"
    r"(?P<sitting>제\s*\d+\s*차|개회식|[^\s]*차)?\s*"
    r"(?P<body>.*?)\s*$"
)
DATE_RE = re.compile(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})")
TIME_RE = re.compile(r"(\d{1,2})\s*시\s*(\d{1,2})\s*분")
MD_RE = re.compile(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일")
ROLLOVER_RE = re.compile(r"24\s*시\s*경과")
AUDIT_RE = re.compile(r"^(?P<year>\d{4})년도\s*국정감사\s*(?P<body>.*)$")
PAREN_RE = re.compile(r"\([^()]{1,80}\)")
TIME_ACTIONS = ("비공개회의개시", "비공개회의종료", "계속개의", "개의", "정회", "속개", "산회", "폐회", "회의중지", "회의계속",
                "감사개시", "감사중지", "감사계속", "감사종료", "조사개시", "조사중지", "조사계속", "조사종료",
                "투표개시", "투표종료", "투표중단", "투표재개", "기록중지", "기록계속")
# stage-direction kinds for whole-sentence parentheticals (checked in order)
STAGE_KINDS = [
    ("mic_cut", re.compile(r"마이크\s*중단|마이크중단")),
    ("chair_change", re.compile(r"사회\s*교대")),
    ("collective_response", re.compile(r"[｢「『\"“].*[｣」』\"”]\s*하는\s*(?:위원|의원|이|사람)")),
    ("appendix_note", re.compile(r"끝에\s*실음|부록으로\s*보존|참\s*조")),
    ("vote_procedure", re.compile(r"투표|표결|명패|계표|개함|폐함")),
    ("visual_aid", re.compile(r"영상|자료|패널|사진|책자|책을|화면|PPT|피켓|들어\s*보이며")),
    ("noise", re.compile(r"웃음|소란|박수|고성|장내|소음|야유")),
    ("movement", re.compile(r"퇴장|입장|퇴석|착석|기립|인사|이석|복귀")),
    ("gesture", re.compile(r"고개|손을|끄덕")),
]
OATH_RE = re.compile(r"선서합니다|맹서합니다|선서함|서약합니다")
# off-mic remark of ANOTHER person recorded inside a turn, e.g.
#   '(◯정양석 의원 발언대 옆에서 ― 왜 그거를 ...)', '(◯방청인 윤방부 - 예.)',
#   '(◯의사국장 권영진 단상에서 ― 합의를 하셨다고……)'
INTERJ_RE = re.compile(r"^\(\s*[◯○]\s*(?P<who>[^―–\-]{1,40}?)\s*[―–\-]\s*(?P<txt>.*)\)$", re.S)
INTERJ_WHERE_RE = re.compile(r"\s*(의석에서|발언대\s*옆에서|단상에서|위원석에서|방청석에서|국무위원석에서|[가-힣]+석에서|[가-힣]+\s*옆에서)$")
BILL_ID_RE = re.compile(r"billId=([A-Z0-9_]+)")
BILL_NO_RE = re.compile(r"의안번호\s*(\d+)")
SLUG_RE = re.compile(r"/members/(\d+)(?:st|nd|rd|th)/([^/?#]+)")


def norm(s):
    """Collapse whitespace (incl. nbsp) and strip."""
    if s is None:
        return ""
    return WS_RE.sub(" ", s.replace("\xa0", " ")).strip()


def _cls(el):
    return (el.get("class") or "").split()


def _to_tree(page):
    if isinstance(page, (bytes, bytearray)):
        if page[:2] == b"\x1f\x8b":
            page = gzip.decompress(page)
        return LH.fromstring(page), len(page)
    return LH.fromstring(page.encode("utf-8")), len(page)


def parse_title(title):
    """Parse '제16대국회 제243회 (정기회) 제2차 문화관광위원회(법안심사소위원회)'."""
    out = {"title": title, "term": None, "session": None, "session_type": None,
           "sitting": None, "committee_full": None, "committee": None, "subcommittee": None,
           "is_audit": False, "audit_year": None}
    if not title:
        return out
    m = TITLE_RE.match(title)
    if not m:
        return out
    term = m.group("term")
    out["term"] = 0 if term == "헌" else int(term)
    out["session"] = int(m.group("session")) if m.group("session") else None
    out["session_type"] = m.group("session_type")
    sit = m.group("sitting")
    out["sitting"] = WS_RE.sub("", sit) if sit else None
    body = m.group("body") or ""
    am = AUDIT_RE.match(body)
    if am:  # '제22대국회 2025년도 국정감사 국회운영위원회'
        out["is_audit"] = True
        out["audit_year"] = int(am.group("year"))
        body = am.group("body")
    out["committee_full"] = body or None
    # trailing "(xxx소위원회)" style suffix = subcommittee
    sm = re.match(r"^(?P<c>.+)\((?P<s>[^()]*(?:소위|위원회)[^()]*)\)$", body)
    if sm:
        out["committee"] = sm.group("c")
        out["subcommittee"] = sm.group("s")
    else:
        out["committee"] = body or None
    return out


def _header_title(t):
    h2 = t.xpath('//div[@id="header"]//div[@class="tit"]/h2')
    if not h2:
        h2 = t.xpath("//div[contains(@class,'summary_wrap')]/h3[1]")
    if not h2:
        return None, None
    strong = h2[0].xpath("./strong")
    date = h2[0].xpath('./span[@class="date"]')
    title = norm(strong[0].text_content()) if strong else norm(h2[0].text_content())
    d = None
    if date:
        dm = DATE_RE.search(date[0].text_content())
        if dm:
            d = f"{int(dm.group(1)):04d}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
    return title, d


# ---------------------------------------------------------------- view page

def _parse_minutes_header(h):
    out = {"doc_title": None, "turn": None, "doc_no": None, "author": None, "fields": [],
           "agenda_header": []}
    x = h.xpath(".//h1")
    out["doc_title"] = norm(" ".join(e.text_content() for e in x)) or None
    x = h.xpath('.//p[@class="turn"]')
    out["turn"] = norm(" ".join(e.text_content() for e in x)) or None
    x = h.xpath('.//p[@class="num"]')
    out["doc_no"] = norm(" ".join(e.text_content() for e in x)) or None
    x = h.xpath('.//p[@class="author"]')
    out["author"] = norm(" ".join(e.text_content() for e in x)) or None
    for li in h.xpath('.//div[@class="place"]/ul/li'):
        sbj = li.xpath('./div[contains(@class,"sbj")]')
        key = norm(sbj[0].text_content()) if sbj else None
        items = []
        for sub in li.xpath("./ul/li | ./ul/*/li"):
            num = sub.xpath('.//span[@class="num"]')
            a = sub.xpath("./a")
            items.append({
                "head_id": sub.get("id"),
                "level": next((c for c in _cls(sub) if c.startswith("pl")), None),
                "num": norm(num[0].text_content()) if num else None,
                "text": norm(sub.text_content()),
                "target": a[0].get("data-target") if a else None,
            })
        con = li.xpath('./p[@class="con"]')
        val = norm(" ".join(e.text_content() for e in con)) or None
        out["fields"].append({"key": key, "value": val, "items": items})
        if key and items and any(i["head_id"] for i in items):
            out["agenda_header"].extend(dict(i, section=key) for i in items)
    return out


def _speaker_block(div):
    man = div.xpath('./div[@class="man"]')
    man = man[0] if man else None
    prof = man.xpath("./a[@href]") if man is not None else []
    href = prof[0].get("href") if prof else None
    slug_term = slug = None
    if href:
        sm = SLUG_RE.search(href)
        if sm:
            slug_term, slug = int(sm.group(1)), sm.group(2)
    def _t(xp):
        if man is None:
            return None
        e = man.xpath(xp)
        return norm(e[0].text_content()) if e else None
    area = _t('.//span[@class="area"]')
    img = man.xpath(".//img/@src") if man is not None else []
    sents = []
    other = []
    for tx in div.xpath('./div[@class="talk"]/div[@class="txt"]'):
        pos = 0
        if norm(tx.text):
            other.append({"kind": "textnode", "text": norm(tx.text), "pos": pos})
        for c in tx:
            if not isinstance(c.tag, str):
                continue
            ccls = _cls(c)
            if c.tag == "span" and "spk_sub" in ccls:
                raw_txt = norm(c.text_content())
                has_note = bool(c.xpath('.//div[contains(@class,"taR")]'))
                sents.append({
                    "sub_id": c.get("id"),
                    "text": raw_txt,
                    "is_note": has_note,
                })
                pos += 1
            elif c.tag in ("br",) or (c.tag == "div" and ("line_dot" in ccls or "line_solid" in ccls)):
                if c.tag == "div":
                    sents.append({"sub_id": None, "text": "", "is_note": False, "separator": "line_dot"})
            else:
                txt = norm(c.text_content())
                kind = "table" if c.xpath("self::table|.//table") else ("img" if c.xpath("self::img|.//img") else c.tag)
                if txt or kind == "img":
                    other.append({"kind": kind, "text": txt, "pos": pos,
                                  "cls": " ".join(ccls) or None})
                    sents.append({"sub_id": None, "text": txt, "is_note": False, "embedded": kind})
            if norm(c.tail):
                other.append({"kind": "tailtext", "text": norm(c.tail), "pos": pos})
                sents.append({"sub_id": None, "text": norm(c.tail), "is_note": False, "embedded": "tailtext"})
    mem_id = div.get("data-mem_id")
    return {
        "spk_id": div.get("id"),
        "item_class": next((c for c in _cls(div) if re.fullmatch(r"item\d+", c)), None),
        "classes": [c for c in _cls(div) if not re.fullmatch(r"item\d+", c)],
        "mem_id": mem_id,
        "is_member_linked": bool(mem_id and mem_id != "0"),
        "name": div.get("data-name"),
        "pos": div.get("data-pos"),
        "name_disp": _t('.//strong[@class="name"]'),
        "pos_disp": _t('.//span[@class="position"]'),
        "area": area.strip("()") if area else None,
        "profile_url": href,
        "profile_term": slug_term,
        "profile_slug": slug,
        "photo": img[0] if img else None,
        "name_has_hanja": bool(HANJA_RE.search(div.get("data-name") or "")),
        "n_fragments": 1,
        "sentences": sents,
        "embedded_other": other,
    }


_STAGE_LEX = re.compile("|".join(rx.pattern for k, rx in STAGE_KINDS if k != "appendix_note"))


NAME_ROLE_RE = re.compile("^(?P<name>[\uac00-\ud7a3\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]{2,4})\\s*(?P<role>委員長|委員|議員|위원장|위원|의원)$")
NAME_ONLY_RE = re.compile(r"^[가-힣]{2,4}$")


def _speaker_norm(sp):
    """name_norm / pos_norm. Some early pages (16대 2000-2001, a few 17대 turns) leave
    data-name empty and put the whole label in data-pos: '宋榮珍議員', '南景弼委員 ',
    '김성곤위원', '副總理兼財政經濟部長官陳稔', or a bare name '정의화'."""
    name, pos = (sp["name"] or "").strip(), (sp["pos"] or "").strip()
    sp["name_norm"], sp["pos_norm"], sp["label_split"] = name or None, pos or None, "attr"
    if name:
        return
    m = NAME_ROLE_RE.match(pos)
    if m:
        sp["name_norm"], sp["pos_norm"], sp["label_split"] = m.group("name"), m.group("role"), "pos=name+role"
    elif NAME_ONLY_RE.match(pos):
        sp["name_norm"], sp["pos_norm"], sp["label_split"] = pos, None, "pos=name_only"
    else:
        sp["label_split"] = "unsplit"  # e.g. role+name fused without a delimiter


def _finalize_speech(sp):
    """Derived per-turn fields, computed after fragments of one turn are merged."""
    _speaker_norm(sp)
    sents = sp["sentences"]
    sp["n_separators"] = sum(1 for s in sents if s.get("separator"))
    sents = [s for s in sents if not s.get("separator")]
    sp["sentences"] = sents
    after_oath = False
    for s in sents:
        st = s["text"]
        paren = st.startswith("(") and st.endswith(")") and st.count("(") == st.count(")")
        s["stage_kind"] = None
        im = INTERJ_RE.match(st) if paren else None
        if im:
            who = im.group("who").strip()
            wm = INTERJ_WHERE_RE.search(who)
            s["stage_kind"] = "interjection"
            s["interjection"] = {"who": who[:wm.start()].strip() if wm else who,
                                 "where": wm.group(1) if wm else None,
                                 "text": im.group("txt").strip()}
        elif paren:
            s["stage_kind"] = next((k for k, rx in STAGE_KINDS if rx.search(st)), "other")
        elif s["is_note"]:
            s["stage_kind"] = "note_block"
        s["is_stage"] = s["stage_kind"] is not None
        # oath signature block: short non-sentence lines right after a sworn oath
        s["is_oath_signature"] = bool(after_oath and not paren and len(st) <= 40
                                      and not re.search(r"[다요까죠]\s*[.?!]?\s*$", st))
        if OATH_RE.search(st):
            after_oath = True
        elif not s["is_oath_signature"]:
            after_oath = False
    full = "\n".join(s["text"] for s in sents if s["text"])
    sp["n_sentences"] = sum(1 for s in sents if s["sub_id"])
    sp["text"] = full
    sp["n_chars"] = len(full)
    sp["has_stage"] = any(s["is_stage"] for s in sents)
    sp["stage_texts"] = [s["text"] for s in sents if s["is_stage"]]
    sp["stage_kinds"] = sorted({s["stage_kind"] for s in sents if s["stage_kind"]})
    sp["n_oath_signature"] = sum(1 for s in sents if s["is_oath_signature"])
    sp["interjections"] = [s["interjection"] for s in sents if s.get("interjection")]
    sp["text_spoken"] = "\n".join(s["text"] for s in sents
                                  if s["text"] and not s["is_stage"] and not s["is_oath_signature"])
    sp["inline_stage_parens"] = [p for s in sents if not s["is_stage"]
                                 for p in PAREN_RE.findall(s["text"]) if _STAGE_LEX.search(p)]
    return sp


def _md_to_date(md, ref):
    """Month/day match -> date in the year of `ref` (next year if it would jump back > 180 days)."""
    if ref is None:
        return None
    try:
        d = _dt.date(ref.year, int(md.group(1)), int(md.group(2)))
    except ValueError:
        return None
    if (ref - d).days > 180:
        d = d.replace(year=d.year + 1)
    return d


def _walk_body(body, state, speeches, agenda, events):
    """Walk minutes_body children in document order; descend into unknown wrappers."""
    if norm(body.text):
        events.append({"kind": "other", "tag": "textnode", "text": norm(body.text),
                       "after_speech_seq": state["seq"]})
    for c in body:
        if not isinstance(c.tag, str):
            continue
        cl = _cls(c)
        if c.tag == "div" and "speaker" in cl:
            sp = _speaker_block(c)
            prev = speeches[-1] if speeches else None
            if prev is not None and sp["spk_id"] and prev["spk_id"] == sp["spk_id"] \
                    and state["last_block"] == "speaker_or_marker":
                # same turn split by a time marker (e.g. '(3월19일 24시 경과)', '(14시38분 투표종료)')
                prev["sentences"].extend(sp["sentences"])
                prev["embedded_other"].extend(sp["embedded_other"])
                prev["n_fragments"] += 1
                prev["time_hhmm_end"] = state["hhmm"]
                prev["speech_date_end"] = state["date"].isoformat() if state["date"] else None
                for ev in events[state["n_events_at_speech"]:]:
                    ev["within_turn"] = True
                state["n_events_at_speech"] = len(events)
                if norm(c.tail):  # e.g. 36455: '!CENTER<2012년도 국정감사 일반증인 명단>CENTER!'
                    events.append({"kind": "other", "tag": "tailtext", "text": norm(c.tail),
                                   "after_speech_seq": state["seq"]})
                continue
            state["last_block"] = "speaker_or_marker"
            state["n_events_at_speech"] = len(events)
            state["seq"] += 1
            sp["speech_seq"] = state["seq"]
            sp["agenda_item"] = state["item"]
            sp["agenda_ordinal"] = len(agenda)   # 1-based index of the body anchor in force (0 = none)
            sp["agenda_block"] = list(state["block"])
            sp["agenda_text"] = state["item_text"]
            sp["agenda_top_item"] = state["top_item"]
            sp["agenda_top_text"] = state["top_text"]
            sp["time_marker"] = state["time"]
            sp["time_hhmm"] = state["hhmm"]
            sp["speech_date"] = state["date"].isoformat() if state["date"] else None
            speeches.append(sp)
        elif c.tag == "p" and "angun" in cl:
            a = c.xpath('./a[contains(@class,"tit")]')
            a = a[0] if a else None
            href = a.get("href") if a is not None else None
            txt = norm(a.text_content()) if a is not None else norm(c.text_content())
            level = next((x for x in cl if x.startswith("pl")), None)
            bm = BILL_ID_RE.search(href or "")
            nm = BILL_NO_RE.search(txt)
            rec = {"ordinal": len(agenda) + 1, "anchor": a.get("id") if a is not None else None,
                   "level": level, "text": txt,
                   "bill_url": href, "bill_id": bm.group(1) if bm else None,
                   "bill_no": nm.group(1) if nm else None,
                   "is_continued": "(계속)" in txt, "after_speech_seq": state["seq"]}
            agenda.append(rec)
            if state["last_block"] == "anchor":
                state["block"].append(rec["anchor"])      # consecutive anchors = one bundle
            else:
                state["block"] = [rec["anchor"]]
            state["last_block"] = "anchor"
            state["item"], state["item_text"] = rec["anchor"], txt
            if level in (None, "pl10", "pl0"):
                state["top_item"], state["top_text"] = rec["anchor"], txt
            # text of the anchor paragraph other than a.tit/btn_move (should be none)
            rest = norm("".join(x.text_content() for x in c if isinstance(x.tag, str)
                                and "tit" not in _cls(x) and "btn_move" not in _cls(x)))
            if rest:
                events.append({"kind": "other", "tag": "angun_rest", "text": rest,
                               "after_speech_seq": state["seq"]})
        elif c.tag == "p" and "taR" in cl:
            txt = norm(c.text_content())
            tm = TIME_RE.search(txt)
            md = MD_RE.search(txt)
            ev = {"kind": "note", "text": txt, "after_speech_seq": state["seq"]}
            if ROLLOVER_RE.search(txt):
                # '(3월19일 24시 경과)' or '(24시 경과)': the following speech is on the next day
                ev["kind"] = "day_rollover"
                base = _md_to_date(md, state["date"]) if md else state["date"]
                if base is not None:
                    state["date"] = base + _dt.timedelta(days=1)
                    ev["new_date"] = state["date"].isoformat()
            elif tm:
                ev["kind"] = "time"
                ev["hhmm"] = f"{int(tm.group(1)):02d}:{int(tm.group(2)):02d}"
                ev["action"] = next((w for w in TIME_ACTIONS if w in txt), None)
                if md:  # '(9월26일 01시15분 산회)'
                    d = _md_to_date(md, state["date"])
                    if d is not None:
                        state["date"] = d
                        ev["new_date"] = d.isoformat()
                state["time"], state["hhmm"] = txt, ev["hhmm"]
            events.append(ev)
        elif c.tag == "br" or (c.tag == "div" and ("line_solid" in cl or "line_dot" in cl)):
            pass
        elif c.tag in ("div", "section", "article") and c.xpath(
                './/div[contains(concat(" ",normalize-space(@class)," ")," speaker ")]'
                '|.//p[contains(@class,"angun")]'):
            _walk_body(c, state, speeches, agenda, events)  # wrapper containing speeches
        else:
            txt = norm(c.text_content())
            kind = "table" if c.xpath("self::table|.//table") else "other"
            if txt or c.xpath("self::img|.//img"):
                events.append({"kind": kind, "tag": c.tag, "cls": " ".join(cl) or None,
                               "text": txt, "after_speech_seq": state["seq"]})
        if norm(c.tail):
            events.append({"kind": "other", "tag": "tailtext", "text": norm(c.tail),
                           "after_speech_seq": state["seq"]})


def _footer_list(ul, group):
    """ul.list_nm children: li = person (span.pos + span.name, optional profile link);
    p = either an organisation heading for the following li items (e.g. '문화관광부') or a
    free-text person line (e.g. witnesses '金永旭 한국언론재단책임연구위원')."""
    org = None
    kids = [c for c in ul if isinstance(c.tag, str)]
    for i, c in enumerate(kids):
        if c.tag == "li":
            pos = c.xpath('.//span[@class="pos"]')
            nm = c.xpath('.//span[contains(@class,"name")]')
            a = c.xpath(".//a[@href]")
            rec = {"pos": norm(pos[0].text_content()) if pos else None,
                   "name": norm(nm[0].text_content()) if nm else norm(c.text_content()),
                   "org": org}
            rest = norm(c.text_content())
            used = norm((rec["pos"] or "") + (rec["name"] or ""))
            if nows_(rest) != nows_(used):
                rec["extra"] = rest
            if a:
                rec["profile_url"] = a[0].get("href")
                sm = SLUG_RE.search(rec["profile_url"])
                if sm:
                    rec["profile_term"], rec["profile_slug"] = int(sm.group(1)), sm.group(2)
            group["names"].append(rec)
        else:
            t = norm(c.text_content())
            if not t:
                continue
            nxt = kids[i + 1] if i + 1 < len(kids) else None
            if nxt is not None and nxt.tag == "li":
                org = t
                group.setdefault("orgs", []).append(t)
            else:
                group["lines"].append(t)
        if norm(c.tail):
            group["lines"].append(norm(c.tail))


def nows_(s):
    return WS_RE.sub("", (s or "").replace("\xa0", " "))


def _parse_footer(f):
    """Footer (부록: 표결 명단, 출석/청가 의원, 참석자, 보고사항, 서면질의 등).

    Grammar: p.tit starts a section; div.con holds groups where each p.tit_sm labels the
    following ul.list_nm (names) and plain nodes are text lines. p.tit / div.con can be
    nested inside div.list wrappers, so descendants are visited in document order.
    Returns [{title, groups:[{label, names:[{pos,name}], lines:[...]}]}].
    """
    sections = []
    st = {"cur": {"title": None, "groups": []}, "g": None}
    sections.append(st["cur"])

    def new_section(title):
        st["cur"] = {"title": title, "groups": []}
        st["g"] = None
        sections.append(st["cur"])

    def new_group(label=None):
        st["g"] = {"label": label, "names": [], "lines": []}
        st["cur"]["groups"].append(st["g"])
        return st["g"]

    def group():
        return st["g"] or new_group()

    BLOCK = "./*[self::p or self::div or self::ul or self::table or self::span[@class='spk_sub']]"

    def walk(el, in_con):
        if norm(el.text) and el.tag not in ("ul",):
            group()["lines"].append(norm(el.text))
        for c in el:
            if not isinstance(c.tag, str):
                continue
            cl = _cls(c)
            if c.tag == "p" and ("tit" in cl or "tit_sm" in cl) and not in_con:
                new_section(norm(c.text_content()))           # p.tit (or top-level p.tit_sm)
            elif c.tag == "p" and "tit_sm" in cl:
                new_group(norm(c.text_content()) or None)     # label of the following list
            elif c.tag == "div" and "con" in cl:
                st["g"] = None
                walk(c, True)
            elif c.tag == "ul":
                _footer_list(c, group())
            elif c.tag == "table":
                rows = [[norm(td.text_content()) for td in tr.xpath("./th|./td")] for tr in c.xpath(".//tr")]
                cap = c.xpath("./caption")
                group().setdefault("tables", []).append(
                    {"caption": norm(cap[0].text_content()) if cap else None, "rows": rows})
            elif c.xpath(BLOCK):
                walk(c, in_con)                                # wrapper (div.list, div.pl10 ...)
            else:
                t = norm(c.text_content())
                if t:
                    group()["lines"].append(t)
                if c.get("data-id"):
                    group()["data_id"] = c.get("data-id")
            if norm(c.tail):
                group()["lines"].append(norm(c.tail))

    walk(f, False)
    return [s for s in sections if s["title"] or s["groups"]]


def parse_view(page):
    t, nbytes = _to_tree(page)
    res = {"status": None, "bytes": nbytes, "meeting": {}, "agenda_header": [], "agenda": [],
           "speeches": [], "events": [], "footer": [], "stats": {}}
    raw_text = norm(t.text_content())
    if nbytes < 2000 and "Bad Request" in raw_text:
        res["status"] = "bad_request"
        return res
    title, date = _header_title(t)
    meeting = parse_title(title)
    meeting["date"] = date
    res["meeting"] = meeting
    mins = t.xpath('//div[@id="minutes"]')
    if not mins:
        res["status"] = "no_minutes_div"
        return res
    m = mins[0]
    hdr = m.find_class("minutes_header")
    if hdr:
        h = _parse_minutes_header(hdr[0])
        res["agenda_header"] = h.pop("agenda_header")
        meeting.update(h)
    body = m.find_class("minutes_body")
    if not body:
        imgs = m.xpath(".//img/@src | .//canvas")
        res["status"] = "no_body_images" if imgs else ("no_body_empty" if not norm(m.text_content()) else "no_body_text")
        return res
    d0 = _dt.date.fromisoformat(date) if date else None
    state = {"seq": 0, "item": None, "item_text": None, "top_item": None, "top_text": None,
             "time": None, "hhmm": None, "date": d0, "last_block": None, "n_events_at_speech": 0, "block": []}
    # audits: audited agencies from header field '피감사기관' (no agenda anchors in audits)
    for f in meeting.get("fields", []):
        if f["key"] and f["key"].replace(" ", "") == "피감사기관" and f["value"]:
            meeting["audited_agencies"] = [x.strip() for x in re.split(r"[|․·,，]", f["value"]) if x.strip()]
    _walk_body(body[0], state, res["speeches"], res["agenda"], res["events"])
    for s in res["speeches"]:
        _finalize_speech(s)
        # The source's own agenda assignment. The speaker div class 'itemK' is the ORDINAL of
        # the K-th p.angun anchor in the body (item0 = before any anchor). It is NOT the anchor's
        # own id: a.tit ids ('itemN') index the header list li#head_itemN / sidebar #itemList,
        # which can hold items never anchored in the body (42338: 591 header items, 21 anchors).
        k = int(s["item_class"][4:]) if s["item_class"] else None
        s["item_class_ordinal"] = k
        s["item_class_text"] = res["agenda"][k - 1]["text"] if k and k <= len(res["agenda"]) else None
    ftr = m.find_class("minutes_footer")
    if ftr:
        res["footer"] = _parse_footer(ftr[0])
    sp = res["speeches"]
    res["status"] = "ok" if sp else "ok_no_speeches"
    res["stats"] = {
        "n_speeches": len(sp),
        "n_sentences": sum(s["n_sentences"] for s in sp),
        "n_chars": sum(s["n_chars"] for s in sp),
        "n_unique_speakers": len({(s["name"], s["pos"]) for s in sp}),
        "n_member_linked": sum(1 for s in sp if s["is_member_linked"]),
        "n_hanja_names": sum(1 for s in sp if s["name_has_hanja"]),
        "n_empty_data_name": sum(1 for s in sp if not (s["name"] or "").strip()),
        "n_label_unsplit": sum(1 for s in sp if s["label_split"] == "unsplit"),
        "n_agenda_anchors": len(res["agenda"]),
        "n_time_markers": sum(1 for e in res["events"] if e["kind"] == "time"),
        "n_other_events": sum(1 for e in res["events"] if e["kind"] in ("other", "table", "note")),
        "n_stage_sentences": sum(len(s["stage_texts"]) for s in sp),
        "n_interjections": sum(len(s["interjections"]) for s in sp),
        "n_oath_signature_lines": sum(s["n_oath_signature"] for s in sp),
        "n_day_rollovers": sum(1 for e in res["events"] if e["kind"] == "day_rollover"),
        "n_embedded_other": sum(len(s["embedded_other"]) for s in sp),
        "n_footer_sections": len(res["footer"]),
    }
    return res


# ------------------------------------------------------------- summary page

def _names_from_ul(ul):
    out = []
    for li in ul.xpath("./li"):
        a = li.xpath(".//a")
        href = a[0].get("href") if a else None
        pos = li.xpath('.//span[@class="pos"]')
        nm = li.xpath('.//span[contains(@class,"name")]')
        rec = {"name": norm(nm[0].text_content()) if nm else norm(li.text_content()),
               "pos": norm(pos[0].text_content()) if pos else None}
        if href and href.startswith("http"):
            rec["profile_url"] = href
            sm = SLUG_RE.search(href)
            if sm:
                rec["profile_term"], rec["profile_slug"] = int(sm.group(1)), sm.group(2)
        out.append(rec)
    return out


def _js_const(text, name):
    m = re.search(r"const\s+" + name + r"\s*=\s*(.+?);?\s*$", text, flags=re.S)
    if not m:
        return None
    s = m.group(1).strip().rstrip(";")
    try:
        return json.loads(s)
    except Exception:
        # attendrate style: [{attendee:"x", number :9},] -> quote keys, drop trailing comma
        s2 = re.sub(r"([{,]\s*)([A-Za-z_]+)\s*:", r'\1"\2":', s)
        s2 = re.sub(r",\s*]", "]", s2)
        try:
            return json.loads(s2)
        except Exception:
            return s


def parse_summary(page):
    t, nbytes = _to_tree(page)
    res = {"status": None, "bytes": nbytes, "template": None, "meeting_info": {}, "files": [],
           "vod_url": None, "attendance": {}, "attendance_tables": [], "agenda": [],
           "appendix": [], "charts": {}}
    if nbytes < 2000 and "Bad Request" in norm(t.text_content()):
        res["status"] = "bad_request"  # e.g. 28770: 12-byte 'Bad Request.' for view and summary
        return res
    wrap = t.xpath('//div[contains(@class,"summary_wrap")]')
    if not wrap:
        res["status"] = "no_summary"
        return res
    w = wrap[0]
    res["template"] = "full" if t.xpath('//div[@id="container_summary"]') else "basic"
    title, date = _header_title(t)
    info = parse_title(title)
    info["date"] = date
    for li in w.xpath('.//div[@class="list_info"]/ul/li'):
        k = li.xpath('./strong[contains(@class,"sbj")]')
        v = li.xpath('./p[@class="con"]')
        if k:
            info.setdefault("fields", {})[norm(k[0].text_content())] = norm(v[0].text_content()) if v else None
    res["meeting_info"] = info
    for a in w.xpath('.//div[@class="list_file"]//a[@href]'):
        href = a.get("href")
        if "player.do" in href:
            res["vod_url"] = href
        else:
            res["files"].append({"href": href, "title": a.get("title")})
    # attendance: h3 "출석 의원 (N인)" followed by div.list_box.list_member / list_attend
    for h3 in w.xpath("./h3"):
        head = norm(h3.text_content())
        nxt = h3.getnext()
        while nxt is not None and not isinstance(nxt.tag, str):
            nxt = nxt.getnext()
        if nxt is None:
            continue
        ncls = _cls(nxt)
        if "list_member" in ncls:
            key = re.sub(r"\s*\(.*$", "", head)
            names = []
            for ul in nxt.xpath("./ul"):
                names.extend(_names_from_ul(ul))
            cm = re.search(r"\((\d+)\s*인\)", head)
            res["attendance"][key] = {"n_reported": int(cm.group(1)) if cm else None, "members": names}
        elif "list_attend" in ncls:
            for tr in nxt.xpath(".//tr"):
                th = tr.xpath("./th")
                grp = norm(th[0].text_content()) if th else None
                names = []
                for ul in tr.xpath("./td/ul"):
                    names.extend(_names_from_ul(ul))
                res["attendance_tables"].append({"block": re.sub(r"\s*\(.*$", "", head), "group": grp,
                                                 "is_subgroup": "bg_gray" in (th[0].get("class") or "") if th else False,
                                                 "people": names})
        elif "list_angunct" in ncls:
            if head.startswith("보존부록"):
                for a in nxt.xpath('.//a[contains(@class,"tit")]'):
                    res["appendix"].append({"title": norm(a.text_content()), "href": a.get("href")})
            else:
                for li in nxt.xpath(".//li"):
                    res["agenda"].append({"text": norm(li.text_content()),
                                          "level": next((c for c in _cls(li) if c.startswith("pl")), None)})
    for b in w.xpath('.//div[contains(@class,"list_angun")]//button[@data-bill_id]'):
        res["agenda"].append({"text": norm(b.text_content()), "bill_id": b.get("data-bill_id"),
                              "bill_no": b.get("datra-bill_no") or b.get("data-bill_no"), "top5": True})
    for sc in t.xpath("//script[not(@src)]"):
        txt = sc.text or ""
        for name in ("keyword", "attendrate", "speakerrate"):
            if re.search(r"const\s+" + name + r"\s*=", txt):
                res["charts"][name] = _js_const(txt, name)
    res["status"] = "ok"
    return res


# ------------------------------------------------------------------ CLI

def _main(argv):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("file")
    ap.add_argument("--summary", action="store_true", help="parse as type=summary page")
    ap.add_argument("--indent", type=int, default=1)
    ap.add_argument("--brief", action="store_true", help="drop per-sentence detail")
    a = ap.parse_args(argv)
    with open(a.file, "rb") as fh:
        page = fh.read()
    out = parse_summary(page) if a.summary else parse_view(page)
    if a.brief and not a.summary:
        for s in out["speeches"]:
            s.pop("sentences", None)
    json.dump(out, sys.stdout, ensure_ascii=False, indent=a.indent)
    sys.stdout.write("\n")


if __name__ == "__main__":
    _main(sys.argv[1:])
