"""Validate hwp_parser against the v9 XLSX rows (ground truth) for 18대 상임위원회 / 국정감사
meetings whose HWP file has been crawled, and parse every crawled 18대 plenary HWP (and, with
--all, every crawled HWP file) for status and grammar statistics.

    python validate_hwp_parser.py [--limit N] [--workers 4] [--all] [--out DIR]
    python validate_hwp_parser.py --make-golden 31926,32196,...   (adds / replaces these entries)
    python validate_hwp_parser.py --anomalies [--tag _x]    anomaly scan over every crawled HWP
    python validate_hwp_parser.py --audit                   stratified audit sample and sheets
    python validate_hwp_parser.py --audit-rates             error rates from audit/verdicts.json

Pairs: v9 meetings with v9_source == 'xlsx' and term 18 (v10/interim/v9_to_api_crosswalk.parquet
gives api_CONFER_NUM for each v9 meeting_id) x HWP files with status ok in
v10/interim/crawl_state.sqlite (kind = 'hwp'). The v9 rows of the 2,433 meetings are extracted
once (duckdb, projection only) to interim/pipeline/hwp_parser/v9_xlsx18_rows.parquet.

Speaker labels are compared as keys at three tiers:
  raw    NFKC, whitespace removed;
  norm   + middle-dot variants unified, Hanja -> Hangul (a Hanja name through the 16-22대 roster
         name_hanja -> name when unique, else the hanja 0.15.1 character table with 두음법칙 on
         the first syllable; positions through the character table);
  dueum  + 두음법칙 canonicalisation of the first syllable of every label token on both sides
         (the XLSX prints some Hangul names in the non-initial reading, '류선호' vs '유선호').
Per meeting: positional agreement (label i == label i over max(n_hwp, n_xlsx)), aligned agreement
(difflib.SequenceMatcher matching blocks, same denominator); text on the dueum-aligned pairs:
normalised equality share (NFKC, dots unified, whitespace removed) and rapidfuzz Indel
similarity; 10-char shingle containment in both directions over the whole meeting.
Every non-equal alignment block and every aligned pair with different text is written with a
residual class (see classify_*).

Outputs (v10/interim/pipeline/hwp_parser/): validation_per_meeting.parquet,
validation_residuals.parquet, plenary_stats.parquet, [all_hwp_stats.parquet],
validation_summary.json.
"""
import argparse
import collections
import difflib
import functools
import json
import re
import sqlite3
import sys
import time
import unicodedata
from multiprocessing import Pool
from pathlib import Path

import duckdb
import pandas as pd
from rapidfuzz.distance import Indel, Levenshtein

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
import hwp_parser as H  # noqa: E402

V10 = HERE.parent.parent
REPO = V10.parent
V9 = REPO / "data" / "all_speeches_16_22_v9.parquet"
XWALK = V10 / "interim" / "v9_to_api_crosswalk.parquet"
UNIVERSE = V10 / "interim" / "meeting_universe_api.parquet"
CRAWL = V10 / "interim" / "crawl_state.sqlite"
ROSTER = V10 / "interim" / "members_term_16_22.parquet"
HANJA_TABLE = V10 / "raw" / "third_party" / "hanja_table_0.15.1.yml"
OUT = V10 / "interim" / "pipeline" / "hwp_parser"
ROWS = OUT / "v9_xlsx18_rows.parquet"

# ----------------------------------------------------------------------------- normalisation

DOTS = str.maketrans({c: "·" for c in "․‧ㆍ・∙⋅•"})
LOOSE = str.maketrans({"｢": "「", "｣": "」", "『": "「", "』": "」", "“": '"', "”": '"', "‘": "'",
                       "’": "'", "‥": "…", "～": "~", "〜": "~", "―": "-", "—": "-", "–": "-",
                       "─": "-", "〈": "<", "〉": ">", "《": "<", "》": ">"})
REDACT = str.maketrans({c: "O" for c in "◯○〇ＯO"})   # redacted names: '◯◯◯' vs 'OOO'
HAN_RE = re.compile("[㐀-䶿一-鿿豈-﫿]")
DUEUM = dict(zip(
    "녀뇨뉴니랴려례료류리라래로뢰루르락란람랑래략량렬렴렵령록론롱룡륜률륭륵름릉린림립력련녁년념녕뉵",
    "여요유이야여예요유이나내노뇌누느낙난남낭내약양열염엽영녹논농용윤율융늑늠능인임입역연역연염영육"))


def ntext(s):
    # dots first: NFKC folds U+2024 to '.' and U+318D to a jungseong
    return "".join(unicodedata.normalize("NFKC", (s or "").translate(DOTS)).split())


def ntext_loose(s):
    t = ntext(s).translate(LOOSE)
    return t.replace("...", "…").replace("·", "")


@functools.lru_cache(maxsize=1)
def hanja_map():
    m = {}
    if not HANJA_TABLE.exists():
        return m
    rx = re.compile(r'^"(.+?)":\s*"(.+?)"\s*$')
    with open(HANJA_TABLE, encoding="utf-8") as fh:
        for line in fh:
            mm = rx.match(line.strip())
            if mm:
                k, v = json.loads('"' + mm.group(1) + '"'), json.loads('"' + mm.group(2) + '"')
                if len(k) == 1 and len(v) == 1:
                    m[k] = v
    return m


@functools.lru_cache(maxsize=1)
def roster_map():
    r = pd.read_parquet(ROSTER, columns=["name", "name_hanja"]).dropna()
    m = {}
    for a, b in zip(r.name_hanja, r.name):
        m.setdefault("".join(unicodedata.normalize("NFKC", a).split()), set()).add(b)
    return {k: next(iter(v)) for k, v in m.items() if len(v) == 1}


def tok_hangul(t):
    if not HAN_RE.search(t):
        return t
    hm = hanja_map()
    out = []
    for ch in t:
        c = unicodedata.normalize("NFKC", ch) if "豈" <= ch <= "﫿" else ch
        out.append(hm.get(c, c) if HAN_RE.match(c) else c)
    s = "".join(out)
    if HAN_RE.match(t[0]) and s and s[0] in DUEUM:
        s = DUEUM[s[0]] + s[1:]
    return s


def name_hangul(t):
    t = unicodedata.normalize("NFKC", t)
    if HAN_RE.search(t):
        r = roster_map().get("".join(t.split()))
        if r:
            return r
    return tok_hangul(t)


TIERS = ("raw", "norm", "dueum")


@functools.lru_cache(maxsize=None)
def label_key(label, tier):
    if tier == "raw":
        return "".join(unicodedata.normalize("NFKC", label or "").split())
    pos_, name, _ = H.split_label(label or "")
    toks = [tok_hangul(t) for t in (pos_ or "").split(" ") if t] + ([name_hangul(name)] if name else [])
    toks = [unicodedata.normalize("NFKC", t.translate(DOTS)).translate(REDACT) for t in toks]
    if tier == "dueum":
        toks = [(DUEUM.get(t[0], t[0]) + t[1:]) if t else t for t in toks]
    return "".join("".join(toks).split())


# ----------------------------------------------------------------------------- data

def hwp_path(n):
    return V10 / "raw" / "hwp" / f"{n // 1000:03d}" / f"{n}.hwp"


def crawled_hwp():
    con = sqlite3.connect(f"file:{CRAWL}?mode=ro", uri=True)
    rows = con.execute("SELECT conf_num FROM fetch WHERE kind='hwp' AND status='ok'").fetchall()
    con.close()
    return {int(r[0]) for r in rows if hwp_path(int(r[0])).exists()}


def xlsx18_meetings():
    xw = pd.read_parquet(XWALK, columns=["meeting_id", "term", "hearing_type", "committee", "date",
                                          "v9_source", "api_CONFER_NUM", "api_CONF_ID"])
    xw = xw[(xw.term == 18) & (xw.v9_source == "xlsx") & xw.api_CONFER_NUM.notna()].copy()
    xw["conf_num"] = xw.api_CONFER_NUM.astype(int)
    return xw


def ensure_rows(xw):
    if ROWS.exists():
        return
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET threads=4")
    con.register("ids", pd.DataFrame({"meeting_id": xw.meeting_id.tolist()}))
    con.execute(f"""COPY (SELECT v.meeting_id, TRY_CAST(v.speech_order AS INT) AS o, v.speech_order,
                    v.speaker, v.role, v.person_name, v.speech_text
                  FROM read_parquet('{V9}') v JOIN ids USING (meeting_id) WHERE v.term = 18
                  ORDER BY v.meeting_id, o) TO '{ROWS}' (FORMAT parquet, COMPRESSION zstd)""")


def load_rows(ids, con):
    q = con.execute(f"SELECT meeting_id, o, speaker, speech_text FROM read_parquet('{ROWS}') "
                    "WHERE meeting_id IN (SELECT UNNEST(?)) ORDER BY meeting_id, o", [ids]).fetchall()
    by = collections.defaultdict(list)
    for mid, o, spk, txt in q:
        by[mid].append((o, spk or "", txt or ""))
    return by


def shingles(s, k=10):
    return {s[i:i + k] for i in range(max(0, len(s) - k + 1))}


# ----------------------------------------------------------------------------- residual classes

def classify_label_pair(hl, xl, htext, xtext, x_repeats_prev=False):
    """Aligned-by-position label mismatch inside a replace block of equal size."""
    kh, kx = label_key(hl, "dueum"), label_key(xl, "dueum")
    nh, nx = ntext(hl), ntext(xtext)
    if nh and nx.startswith(nh):
        return "xlsx_label_left_in_text"      # XLSX kept the printed label in the text, previous speaker
    if x_repeats_prev:
        return "xlsx_repeats_previous_row"    # the XLSX row copies the row before it
    hpos, hname, _ = H.split_label(hl)
    xpos, xname, _ = H.split_label(xl)
    if kh.startswith(kx) and len(kh) > len(kx) and nx.startswith(kh[len(kx):]):
        return "xlsx_label_truncated"         # the label tail ('에드워즈') moved into the XLSX text
    if hname is None and kx.startswith(kh):
        return "name_absent_hwp"
    if xname is None and kh.startswith(kx):
        return "name_absent_xlsx"
    if hname and xname and label_key(hname, "dueum") == label_key(xname, "dueum"):
        return "position_text_diff"           # same name, position printed differently
    if kh.startswith(kx):
        return "hwp_label_longer"
    if kx.startswith(kh):
        return "xlsx_label_longer"
    if Levenshtein.distance(kh, kx) <= 2 or Levenshtein.distance(label_key(hl, "raw"), label_key(xl, "raw")) <= 2:
        return "label_char_diff"              # a typo in one source ('정진섭 위워', '위원장 고홍길')
    if sorted(tok_hangul(x) for x in H.norm(hl).split(" ")) == sorted(H.norm(xl).split(" ")):
        return "label_token_order"            # '정병국 위원장' vs '위원장 정병국'
    return "different_speaker"


def classify_unaligned(side, k, turns_h, rows_x, i1, i2, j1, j2, xkeys=None, xpos=None):
    """An HWP turn (side='hwp') or XLSX row (side='xlsx') without a partner."""
    if side == "xlsx":
        x = ntext(rows_x[k][2])
        kx = label_key(rows_x[k][1], "dueum")
        if xkeys is not None and x:
            # the XLSX repeats a block of rows: this row and a neighbour equal another pair
            for p in xpos.get(xkeys[k], ()):
                if p != k and any(0 <= k + d < len(xkeys) and 0 <= p + d < len(xkeys)
                                  and xkeys[k + d] == xkeys[p + d] for d in (-1, 1)):
                    return "xlsx_duplicate_block"
        for i in (i1 - 1, i1, i2 - 1, i2):
            if 0 <= i < len(turns_h) and x and x in ntext(turns_h[i]["text_raw"]):
                t = turns_h[i]
                if label_key(t["speaker_label_raw"], "dueum") == kx:
                    return "xlsx_split_same_speaker"   # XLSX cut one HWP turn in two rows
                nl = ntext(rows_x[k][1])
                ht = ntext(t["text_raw"])
                if nl and ("◯" + nl in ht or "○" + nl in ht):
                    return "hwp_inline_marker_kept"    # a marker inside the HWP turn (quoted speech)
                return "merged_in_hwp"
        return "xlsx_only_other"
    h = ntext(turns_h[k]["text_raw"])
    for j in (j1 - 1, j1, j2 - 1, j2):
        if 0 <= j < len(rows_x) and h and h in ntext(rows_x[j][2]):
            return "merged_in_xlsx"
    if 0 < j1 < len(rows_x) and rows_x[j1][0] is not None and rows_x[j1 - 1][0] is not None \
            and rows_x[j1][0] - rows_x[j1 - 1][0] > 1:
        return "xlsx_speech_order_gap"          # the v9 row numbering skips here
    if not h:
        return "hwp_empty_turn"
    if turns_h[k].get("after_end_marker"):
        return "hwp_turn_after_end_marker"      # e.g. a transcript appended after 감사종료
    if turns_h[k].get("label_how") != "sep":
        return "xlsx_lacks_single_space_label_turn"   # label printed without the double space
    return "hwp_only_other"


TIME_PAREN_RE = re.compile(r"\((?:\d{1,2}월\d{1,2}일)?\d{1,2}시(?:\d{1,2}분)?[^()]{0,20}\)")
PAREN_SEG_RE = re.compile(r"\([^()]*\)")


def block_xlsx_missplit(turns, rows, i1, i2, j1, j2):
    """True when an HWP turn in or next to the block has a label printed without the double
    space (label_how != 'sep') and that printed label appears inside an XLSX text in or next
    to the block: the XLSX producer did not split the label off (it kept it in the text of
    the previous speaker's row or merged the turn)."""
    for i in range(max(0, i1 - 1), min(len(turns), i2 + 1)):
        t = turns[i]
        if t.get("label_how") == "sep":
            continue
        nl = ntext(t["speaker_label_raw"])
        for j in range(max(0, j1 - 1), min(len(rows), j2 + 1)):
            if nl and nl in ntext(rows[j][2]):
                return True
    return False


PAREN_ONLY_RE = re.compile(r"^(?:\([^()]*\))+$")


def _context(res):
    """Loose-normalised text of the HWP appendix (footer) and of agenda / event lines, used to
    attribute XLSX-only text."""
    parts = []
    for sec in res.get("footer") or []:
        parts.append(sec.get("title") or "")
        parts.extend(sec.get("lines") or [])
        for tb in sec.get("tables") or []:
            parts.extend(c.get("text") or "" for c in tb.get("cells") or [])
    ev = [a.get("text") or "" for a in res.get("agenda") or []] + \
         [e.get("text") or "" for e in res.get("events") or []]
    footer, events = ntext_loose("".join(parts)), ntext_loose("".join(ev))
    doc = footer + events + "".join(ntext_loose(t["text_raw"]) + ntext_loose("".join(t.get("embedded_tables") or []))
                                    for t in res.get("turns") or [])
    return {"footer": footer, "events": events, "doc_shingles": shingles(doc)}


def _coverage(chunk, sh):
    c = shingles(chunk)
    return len(c & sh) / len(c) if c else 0.0


def classify_text(t, xtext, ctx=None):
    """Class of an aligned pair whose normalised texts differ, plus explained kinds and the
    number of unexplained characters. Chunks of the loose-normalised Indel alignment are
    attributed: XLSX-only time markers, XLSX-only text found in the HWP appendix or in
    agenda/event lines, HWP-only parenthetical lines, text present on both sides at another
    position (moved, e.g. a table placed elsewhere). Everything else is unexplained."""
    ctx = ctx or {"footer": "", "events": "", "doc_shingles": set()}
    if ntext_loose(t["text_raw"]) == ntext_loose(xtext):
        return "punctuation_variant", "", 0
    if ntext(t["text"]) == ntext(xtext) or ntext_loose(t["text"]) == ntext_loose(xtext):
        return "hwp_stage_or_oath_lines", "", 0   # XLSX lacks lines the parser flags as stage/oath
    Hs, Xs = ntext_loose(t["text_raw"]), ntext_loose(xtext)
    t_tab = shingles(ntext_loose("".join(ln["text"] for ln in t.get("lines") or [] if ln.get("table"))
                                 + "".join(t.get("embedded_tables") or [])))
    kinds, unexplained = set(), 0
    h_left, x_left = collections.Counter(), collections.Counter()   # unexplained characters
    for op in Indel.opcodes(Hs, Xs):
        if op.tag == "equal":
            continue
        hs, xs = Hs[op.src_start:op.src_end], Xs[op.dest_start:op.dest_end]
        if xs and not hs:
            if TIME_PAREN_RE.fullmatch(xs) or all(TIME_PAREN_RE.fullmatch(m) for m in PAREN_SEG_RE.findall(xs)) \
                    and PAREN_SEG_RE.sub("", xs) == "" and xs:
                kinds.add("xlsx_time_marker")
            elif len(xs) >= 6 and xs in ctx["footer"]:
                kinds.add("xlsx_appendix_text")
            elif len(xs) >= 6 and xs in ctx["events"]:
                kinds.add("xlsx_agenda_or_event_text")
            elif len(xs) >= 6 and xs in Hs:
                kinds.add("moved_text")
            elif len(xs) >= 12 and _coverage(xs, ctx["doc_shingles"]) >= 0.8:
                kinds.add("xlsx_text_elsewhere_in_hwp")   # e.g. a table placed / ordered differently
            else:
                x_left.update(xs)
        elif hs and not xs:
            if PAREN_ONLY_RE.match(hs):
                kinds.add("hwp_parenthetical")
            elif len(hs) >= 6 and hs in Xs:
                kinds.add("moved_text")
            elif len(hs) >= 12 and _coverage(hs, shingles(Xs)) >= 0.8:
                kinds.add("moved_text")
            elif len(hs) >= 12 and t_tab and _coverage(hs, t_tab) >= 0.8:
                kinds.add("hwp_table_text")   # a table inside the HWP turn that this XLSX row lacks
            else:
                h_left.update(hs)
        else:
            h_left.update(hs)
            x_left.update(xs)
    # the same characters on both sides in another order (a table read row-wise in one source
    # and column-wise in the other): only the multiset difference stays unexplained
    n_left = sum(h_left.values()) + sum(x_left.values())
    diff = sum(((h_left - x_left) + (x_left - h_left)).values())
    if n_left >= 40 and diff <= 0.1 * n_left:
        kinds.add("same_chars_other_order")
        unexplained = diff
    else:
        unexplained = max(sum(h_left.values()), sum(x_left.values()))
    if unexplained == 0 and kinds:
        return "+".join(sorted(kinds)), "+".join(sorted(kinds)), 0
    if unexplained <= max(4, 0.02 * max(len(Hs), len(Xs))):
        return "minor_wording", "+".join(sorted(kinds)), unexplained
    return "major_text_diff", "+".join(sorted(kinds)), unexplained


# ----------------------------------------------------------------------------- compare

def compare(res, rows):
    """res: parse_hwp output; rows: list of (speech_order, speaker, speech_text)."""
    turns = res["turns"]
    ctx = _context(res)
    n_h, n_x = len(turns), len(rows)
    den = max(n_h, n_x, 1)
    out = {"n_hwp": n_h, "n_xlsx": n_x, "n_diff": n_h - n_x, "den": den}
    keys = {}
    for tier in TIERS:
        a = [label_key(t["speaker_label_raw"], tier) for t in turns]
        b = [label_key(r[1], tier) for r in rows]
        keys[tier] = (a, b)
        out[f"pos_agree_{tier}_n"] = sum(1 for u, v in zip(a, b) if u == v)
        sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
        out[f"aligned_agree_{tier}_n"] = sum(s for _, _, s in sm.get_matching_blocks())
        out[f"pos_agree_{tier}"] = round(out[f"pos_agree_{tier}_n"] / den, 6)
        out[f"aligned_agree_{tier}"] = round(out[f"aligned_agree_{tier}_n"] / den, 6)
        if tier == "dueum":
            opcodes = sm.get_opcodes()
    residuals = []
    xkeys = [(label_key(r[1], "dueum"), ntext(r[2])) for r in rows]
    xpos = {}
    for p, kk in enumerate(xkeys):
        xpos.setdefault(kk, []).append(p)
    exact = 0
    sims = []
    for op, i1, i2, j1, j2 in opcodes:
        if op == "equal":
            for q in range(i2 - i1):
                t, r = turns[i1 + q], rows[j1 + q]
                h, x = ntext(t["text_raw"]), ntext(r[2])
                if h == x:
                    exact += 1
                    sims.append(1.0)
                    continue
                sim = Indel.normalized_similarity(h, x)
                sims.append(sim)
                cls, kinds, unexpl = classify_text(t, r[2], ctx)
                residuals.append({"kind": "text_diff", "cls": cls, "explained": kinds,
                                  "unexplained_chars": unexpl, "hwp_turn": i1 + q + 1,
                                  "xlsx_row": j1 + q + 1, "hwp_label": t["speaker_label_raw"],
                                  "hwp_label_how": t.get("label_how"),
                                  "xlsx_label": r[1], "sim": round(sim, 4), "len_hwp": len(h),
                                  "len_xlsx": len(x), "hwp_text": t["text_raw"][:800],
                                  "xlsx_text": r[2][:800]})
            continue
        missplit = block_xlsx_missplit(turns, rows, i1, i2, j1, j2)
        if op == "replace" and i2 - i1 == j2 - j1:
            for q in range(i2 - i1):
                t, r = turns[i1 + q], rows[j1 + q]
                residuals.append({"kind": "label_diff",
                                  "cls": "xlsx_missplit_single_space_label" if missplit else
                                  classify_label_pair(t["speaker_label_raw"], r[1], t["text_raw"], r[2],
                                                      j1 + q > 0 and xkeys[j1 + q] == xkeys[j1 + q - 1]),
                                  "hwp_turn": i1 + q + 1, "xlsx_row": j1 + q + 1,
                                  "hwp_label": t["speaker_label_raw"], "hwp_label_how": t.get("label_how"),
                                  "xlsx_label": r[1], "sim": None,
                                  "len_hwp": len(ntext(t["text_raw"])), "len_xlsx": len(ntext(r[2])),
                                  "hwp_text": t["text_raw"][:800], "xlsx_text": r[2][:800]})
            continue
        for i in range(i1, i2):
            t = turns[i]
            residuals.append({"kind": "hwp_only", "cls": "xlsx_missplit_single_space_label" if missplit else
                              classify_unaligned("hwp", i, turns, rows, i1, i2, j1, j2),
                              "hwp_turn": i + 1, "xlsx_row": None, "hwp_label": t["speaker_label_raw"],
                              "hwp_label_how": t.get("label_how"),
                              "xlsx_label": None, "sim": None, "len_hwp": len(ntext(t["text_raw"])),
                              "len_xlsx": None, "hwp_text": t["text_raw"][:800], "xlsx_text": None})
        for j in range(j1, j2):
            r = rows[j]
            residuals.append({"kind": "xlsx_only", "cls": "xlsx_missplit_single_space_label" if missplit else
                              classify_unaligned("xlsx", j, turns, rows, i1, i2, j1, j2, xkeys, xpos),
                              "hwp_turn": None, "xlsx_row": j + 1, "hwp_label": None, "xlsx_label": r[1],
                              "sim": None, "len_hwp": None, "len_xlsx": len(ntext(r[2])),
                              "hwp_text": None, "xlsx_text": r[2][:800]})
    n_al = len(sims)
    H_all = "".join(ntext(t["text_raw"]) for t in turns)
    X_all = "".join(ntext(r[2]) for r in rows)
    sh_h, sh_x = shingles(H_all), shingles(X_all)
    a, b = keys["dueum"]
    out.update({
        "n_aligned": n_al, "n_text_exact": exact,
        "text_exact_share": round(exact / max(1, n_al), 6),
        "text_sim_mean": round(sum(sims) / max(1, n_al), 6),
        "text_sim_min": round(min(sims), 6) if sims else None,
        "chars_hwp": len(H_all), "chars_xlsx": len(X_all),
        "shingle_x_in_h": round(len(sh_x & sh_h) / max(1, len(sh_x)), 6),
        "shingle_h_in_x": round(len(sh_h & sh_x) / max(1, len(sh_h)), 6),
        "hwp_same_label_adjacent": sum(1 for u, v in zip(a, a[1:]) if u == v),
        "xlsx_same_label_adjacent": sum(1 for u, v in zip(b, b[1:]) if u == v),
        "n_residual_label": sum(1 for r in residuals if r["kind"] == "label_diff"),
        "n_residual_hwp_only": sum(1 for r in residuals if r["kind"] == "hwp_only"),
        "n_residual_xlsx_only": sum(1 for r in residuals if r["kind"] == "xlsx_only"),
        "n_residual_text": sum(1 for r in residuals if r["kind"] == "text_diff"),
    })
    return out, residuals


def _stats_row(res):
    s = res["stats"]
    c = s.get("counters", {})
    return {"status": res["status"], "hwp_version": res["reader"].get("version"),
            "reader_errors": len(res["reader"].get("errors") or []),
            "hwp_date": res["meeting"].get("date"), "hwp_committee": res["meeting"].get("committee_raw"),
            "session_no": res["meeting"].get("session_no"), "doc_no": res["meeting"].get("doc_no"),
            "n_turns": s.get("n_turns"), "n_chars": s.get("n_chars"),
            "n_agenda": s.get("n_agenda_anchors"), "n_agenda_header": s.get("n_agenda_header"),
            "n_time": s.get("n_time_markers"), "n_rollover": s.get("n_day_rollovers"),
            "n_footer": s.get("n_footer_sections"), "appendix_how": s.get("appendix_how"),
            "n_unassigned": s.get("n_unassigned"),
            "chars_check": s.get("chars_turns") == s.get("chars_turn_items") if s else None,
            "unit_char_mismatch": c.get("unit_char_mismatch", 0),
            "orphan_other": c.get("orphan_other", 0), "orphan_note": c.get("orphan_note", 0),
            "n_unique_speakers": s.get("n_unique_speakers"), "n_hanja_names": s.get("n_hanja_names"),
            "n_label_unsplit": s.get("n_label_unsplit"),
            "para_line_breaks": c.get("para_line_breaks", 0),
            "midline_marker_split": c.get("midline_marker_split", 0),
            "turn_head_not_paragraph_start": c.get("turn_head_not_paragraph_start", 0),
            "n_stage_lines": s.get("n_stage_lines"), "n_oath_signature_lines": s.get("n_oath_signature_lines"),
            "n_embedded_tables": s.get("n_embedded_tables"),
            "n_turns_after_end_marker": s.get("n_turns_after_end_marker"),
            "end_markers_before_last_turn": s.get("end_markers_before_last_turn"),
            "appendix_inner_lines": c.get("appendix_inner_lines", 0),
            "table_rows_interleaved": c.get("table_rows_interleaved", 0),
            # appendix after the meeting-end marker (2026-09-28): sittings, lines kept as events between an
            # end marker and a new sitting, characters moved from the body to the appendix, rejected starts
            "n_sittings": s.get("n_sittings"), "after_end_lines": c.get("after_end_lines", 0),
            "appendix_moved_chars": (s.get("appendix_moved") or {}).get("chars", 0),
            "sitting_start_rejected": sum(v for k, v in c.items() if k.startswith("sitting_start_rejected_")),
            "label_counts": json.dumps({k[6:]: v for k, v in c.items() if k.startswith("label_")},
                                       ensure_ascii=False)}


def _work_pair(task):
    conf_num, meta, rows = task
    try:
        res = H.parse_hwp(hwp_path(conf_num).read_bytes(), conf_num=conf_num)
    except Exception as e:  # counted, never silent
        return dict(meta, conf_num=conf_num, status="exception", error=repr(e)[:300]), []
    base = dict(meta, conf_num=conf_num, **_stats_row(res))
    if res["status"] not in ("ok", "ok_no_turns"):
        return base, []
    m, resid = compare(res, rows)
    base.update(m)
    for r in resid:
        r.update(conf_num=conf_num, v9_meeting_id=meta["v9_meeting_id"])
    return base, resid


def _work_stats(task):
    conf_num, meta = task
    p = hwp_path(conf_num)
    if not p.exists():
        return dict(meta, conf_num=conf_num, status="not_crawled")
    try:
        res = H.parse_hwp(p.read_bytes(), conf_num=conf_num)
    except Exception as e:
        return dict(meta, conf_num=conf_num, status="exception", error=repr(e)[:300])
    return dict(meta, conf_num=conf_num, file_bytes=p.stat().st_size, **_stats_row(res))


# ----------------------------------------------------------------------------- summary

def summarize(per, resid, plen, allst=None):
    ok = per[per.status.isin(["ok", "ok_no_turns"]) & per.n_hwp.notna()]
    den = ok.den.sum()
    s = {"n_pairs": int(len(per)), "status_counts": per.status.value_counts().to_dict(),
         "n_compared": int(len(ok)), "n_xlsx_rows": int(ok.n_xlsx.sum()), "n_hwp_turns": int(ok.n_hwp.sum()),
         "turn_count_equal_share": round(float((ok.n_diff == 0).mean()), 6),
         "n_diff_distribution": {str(k): int(v) for k, v in ok.n_diff.clip(-10, 10).value_counts().sort_index().items()}}
    for tier in TIERS:
        s[f"pooled_pos_agree_{tier}"] = round(float(ok[f"pos_agree_{tier}_n"].sum() / den), 6)
        s[f"pooled_aligned_agree_{tier}"] = round(float(ok[f"aligned_agree_{tier}_n"].sum() / den), 6)
        s[f"share_meetings_pos_ge_099_{tier}"] = round(float((ok[f"pos_agree_{tier}"] >= 0.99).mean()), 6)
        s[f"pos_agree_{tier}_quantiles"] = {str(q): round(float(ok[f"pos_agree_{tier}"].quantile(q)), 6)
                                            for q in (0.01, 0.05, 0.1, 0.25, 0.5)}
    s["pooled_text_exact_share"] = round(float(ok.n_text_exact.sum() / max(1, ok.n_aligned.sum())), 6)
    s["mean_text_sim"] = round(float((ok.text_sim_mean * ok.n_aligned).sum() / max(1, ok.n_aligned.sum())), 6)
    s["shingle_x_in_h_quantiles"] = {str(q): round(float(ok.shingle_x_in_h.quantile(q)), 6) for q in (0.01, 0.05, 0.5)}
    s["shingle_h_in_x_quantiles"] = {str(q): round(float(ok.shingle_h_in_x.quantile(q)), 6) for q in (0.01, 0.05, 0.5)}
    if len(resid):
        s["residuals_by_kind_class"] = {f"{k}|{c}": int(v) for (k, c), v in
                                       resid.groupby(["kind", "cls"]).size().sort_values(ascending=False).items()}
    s["parser_counters_pairs"] = {c: int(ok[c].sum()) for c in (
        "para_line_breaks", "midline_marker_split", "turn_head_not_paragraph_start", "orphan_other",
        "orphan_note", "n_unassigned", "unit_char_mismatch", "n_label_unsplit", "n_hanja_names",
        "appendix_inner_lines", "table_rows_interleaved", "n_turns_after_end_marker",
        "end_markers_before_last_turn")}
    s["chars_check_false"] = int((ok.chars_check == False).sum())  # noqa: E712
    lc = collections.Counter()
    for x in ok.label_counts.dropna():
        lc.update(json.loads(x))
    s["label_how_pairs"] = dict(lc.most_common())
    s["plenary"] = {"n": int(len(plen)), "status_counts": plen.status.value_counts().to_dict()}
    pk = plen[plen.status == "ok"]
    if len(pk):
        s["plenary"].update({
            "n_turns_total": int(pk.n_turns.sum()), "n_turns_quantiles": {str(q): float(pk.n_turns.quantile(q)) for q in (0, .25, .5, .75, 1)},
            "date_match_api": int((pk.hwp_date == pk.api_date).sum()),
            "appendix_how": pk.appendix_how.value_counts().to_dict(),
            "n_unassigned": int(pk.n_unassigned.sum()), "chars_check_false": int((pk.chars_check == False).sum()),  # noqa: E712
            "orphan_other": int(pk.orphan_other.sum()), "orphan_note": int(pk.orphan_note.sum()),
            "n_label_unsplit": int(pk.n_label_unsplit.sum()), "n_hanja_names": int(pk.n_hanja_names.sum()),
            "para_line_breaks": int(pk.para_line_breaks.sum()), "midline_marker_split": int(pk.midline_marker_split.sum()),
            "n_agenda_total": int(pk.n_agenda.sum()), "n_time_total": int(pk.n_time.sum()),
            "n_footer_total": int(pk.n_footer.sum()), "n_stage_lines": int(pk.n_stage_lines.sum()),
            "appendix_inner_lines": int(pk.appendix_inner_lines.sum()),
            "n_turns_after_end_marker": int(pk.n_turns_after_end_marker.sum()),
        })
        lc = collections.Counter()
        for x in pk.label_counts.dropna():
            lc.update(json.loads(x))
        s["plenary"]["label_how"] = dict(lc.most_common())
    if allst is not None:
        s["all_hwp"] = {"n": int(len(allst)), "status_counts": allst.status.value_counts().to_dict()}
        ak = allst[allst.status.isin(["ok", "ok_no_turns"])]
        s["all_hwp"].update({"n_turns_total": int(ak.n_turns.sum()),
                             "status_by_class": {f"{a}|{b}": int(v) for (a, b), v in
                                                 allst.groupby(["class_name", "status"]).size().items()},
                             "n_unassigned": int(ak.n_unassigned.sum()),
                             "chars_check_false": int((ak.chars_check == False).sum()),  # noqa: E712
                             "orphan_other": int(ak.orphan_other.sum()),
                             "orphan_note": int(ak.orphan_note.sum()),
                             "appendix_inner_lines": int(ak.appendix_inner_lines.sum()),
                             "n_turns_after_end_marker": int(ak.n_turns_after_end_marker.sum()),
                             "files_with_turns_after_end_marker": int((ak.n_turns_after_end_marker > 0).sum()),
                             "files_several_sittings": int((ak.n_sittings > 1).sum()) if "n_sittings" in ak else None,
                             "after_end_lines": int(ak.after_end_lines.sum()) if "after_end_lines" in ak else None,
                             "appendix_moved_chars": int(ak.appendix_moved_chars.sum()) if "appendix_moved_chars" in ak else None,
                             "files_appendix_moved": int((ak.appendix_moved_chars > 0).sum()) if "appendix_moved_chars" in ak else None,
                             "sitting_start_rejected": int(ak.sitting_start_rejected.sum()) if "sitting_start_rejected" in ak else None,
                             "appendix_how": ak.appendix_how.value_counts().to_dict(),
                             "label_how": dict(sum((collections.Counter(json.loads(x)) for x in ak.label_counts.dropna()),
                                                   collections.Counter()).most_common()),
                             "n_label_unsplit": int(ak.n_label_unsplit.sum()),
                             "zero_turn_files": int((ak.n_turns == 0).sum()),
                             "date_match_api": int((ak.hwp_date == ak.api_date).sum()),
                             "date_missing": int(ak.hwp_date.isna().sum())})
    return s


# ----------------------------------------------------------------------------- anomaly scan
# The XLSX comparison covers only the 2,433 18대 meetings whose production source is the XLSX.
# The anomaly scan runs over every crawled HWP file and tags each with its role:
#   pair           18대 XLSX meeting (HWP parsed for validation only)
#   production_18  18대 meeting whose production source is the HWP
#   production_other  non-18대 meeting whose viewer page returned HTTP 400 (HWP fallback)
# It lists turns and events that a parser defect would produce. The definitions use only the
# CONTRACT turn fields plus label_how / after_end_marker, so the scan also runs on an older
# parser version (sitting_seq and the parser's own flags are used when present).

STRONG_HOW = frozenset({"sep", "sep_trimmed_by_lexicon", "sep_joined_pos_name", "lexicon_prefix"})
ANOMALY_KINDS = ("weak_label", "label_not_in_lexicon", "post_end_turn", "time_regress",
                 "name_list_text", "long_turn", "orphan_event", "sitting_boundary")
NAME_LIST_MIN_TOKENS = 20
NAME_LIST_SHARE = 0.5
LONG_TURN_CHARS = 20000


@functools.lru_cache(maxsize=1)
def roster_names():
    """Hangul and Hanja (NFKC) names of every 16-22대 member: the name-list token set."""
    r = pd.read_parquet(ROSTER, columns=["name", "name_hanja"])
    out = set()
    for a, b in zip(r.name, r.name_hanja):
        for x in (a, b):
            if isinstance(x, str) and x.strip():
                out.add(unicodedata.normalize("NFKC", "".join(x.split())))
    return frozenset(out)


def name_list_share(text, names=None):
    """(number of whitespace tokens, share of tokens that are member names)."""
    names = names if names is not None else roster_names()
    toks = [unicodedata.normalize("NFKC", t) for t in (text or "").split()]
    if not toks:
        return 0, 0.0
    return len(toks), sum(1 for t in toks if t in names) / len(toks)


def _minutes(hhmm):
    try:
        return int(hhmm[:2]) * 60 + int(hhmm[3:5])
    except (TypeError, ValueError):
        return None


def anomaly_rows(res, conf_num=None, names=None):
    """Anomalies of one parse_hwp result: list of dicts (conf_num, kind, turn_seq, event_i,
    label, label_how, detail, text_head)."""
    turns = res.get("turns") or []
    out = []

    def add(kind, t=None, ev_i=None, detail=None, text=None):
        out.append({"conf_num": conf_num, "kind": kind,
                    "turn_seq": None if t is None else int(t["turn_seq"]), "event_i": ev_i,
                    "label": None if t is None else t.get("speaker_label_raw"),
                    "label_how": None if t is None else t.get("label_how"),
                    "detail": detail, "text_head": (text if text is not None else
                                                    (t["text_raw"] if t is not None else ""))[:160]})
    lexicon = {t.get("speaker_label_raw") for t in turns if t.get("label_how") in STRONG_HOW}
    prev = None
    for t in turns:
        how = t.get("label_how")
        if how not in STRONG_HOW:
            add("weak_label", t, detail=how)
        if t.get("speaker_label_raw") not in lexicon:
            add("label_not_in_lexicon", t, detail=how)
        if t.get("after_end_marker"):
            add("post_end_turn", t, detail=f"sitting {t.get('sitting_seq')}")
        m = _minutes(t.get("time_hhmm"))
        if prev is not None and m is not None and prev[0] is not None and prev[1] == t.get("speech_date") \
                and prev[2] == t.get("sitting_seq") and m < prev[0]:
            add("time_regress", t, detail=f"{prev[3]} -> {t.get('time_hhmm')}")
        if m is not None:
            prev = (m, t.get("speech_date"), t.get("sitting_seq"), t.get("time_hhmm"))
        n_tok, share = name_list_share(t.get("text"), names)
        if n_tok >= NAME_LIST_MIN_TOKENS and share > NAME_LIST_SHARE:
            add("name_list_text", t, detail=f"{n_tok} tokens, {share:.2f} names")
        if len(t.get("text") or "") > LONG_TURN_CHARS:
            add("long_turn", t, detail=f"{len(t['text'])} chars")
    for i, e in enumerate(res.get("events") or []):
        if e.get("kind") in ("other", "note"):
            add("orphan_event", ev_i=i, detail=f"{e['kind']} after turn {e.get('after_turn_seq')}",
                text=e.get("text") or "")
    for sg in (res.get("sittings") or [])[1:]:
        add("sitting_boundary", detail=f"sitting {sg['sitting_seq']} {sg['how']} at turn {sg['start_turn_seq']}"
            f" date {sg['date']} ({sg['date_how']}) n_turns {sg.get('n_turns')}",
            text=sg.get("start_text") or "")
    return out


def production_roles():
    """conf_num -> (role, term, class_name, is_subcommittee_name) for every crawled HWP file."""
    have = crawled_hwp()
    pairs = set(xlsx18_meetings().conf_num)
    u = pd.read_parquet(UNIVERSE, columns=["CONFER_NUM", "DAE_NUM", "CLASS_NAME_unified", "COMM_NAME",
                                           "is_subcommittee_name", "CONF_DATE"])
    u["conf_num"] = u.CONFER_NUM.astype(int)
    u = u.set_index("conf_num")
    out = {}
    for n in sorted(have):
        r = u.loc[n] if n in u.index else None
        term = None if r is None else int(r.DAE_NUM)
        role = "pair" if n in pairs else ("production_18" if term == 18 else "production_other")
        out[n] = {"role": role, "term": term, "class_name": None if r is None else r.CLASS_NAME_unified,
                  "comm_name": None if r is None else r.COMM_NAME,
                  "is_sub": None if r is None else bool(r.is_subcommittee_name),
                  "api_date": None if r is None else r.CONF_DATE}
    return out


def _work_anomaly(task):
    conf_num, meta = task
    try:
        res = H.parse_hwp(hwp_path(conf_num).read_bytes(), conf_num=conf_num)
    except Exception as e:  # counted, never silent
        return dict(meta, conf_num=conf_num, status="exception", error=repr(e)[:300]), []
    rows = anomaly_rows(res, conf_num)
    s = res["stats"]
    f = dict(meta, conf_num=conf_num, status=res["status"], hwp_version=res["reader"].get("version"),
             n_turns=s.get("n_turns"), n_chars=s.get("n_chars"), n_sittings=s.get("n_sittings", 1),
             appendix_how=s.get("appendix_how"), n_footer=len(res["footer"]),
             hwp_date=res["meeting"].get("date"), sitting=res["meeting"].get("sitting"),
             chars_check=s.get("chars_turns") == s.get("chars_turn_items"),
             n_unassigned=s.get("n_unassigned"),
             reader_errors=len(res["reader"].get("errors") or []),
             ctrl_unrendered=json.dumps({k: v for k, v in (res["reader"].get("reader_stats") or {}).items()
                                         if k.startswith("ctrl_")}, ensure_ascii=False))
    for k in ANOMALY_KINDS:
        f["n_" + k] = sum(1 for r in rows if r["kind"] == k)
    return f, [dict(r, role=meta["role"]) for r in rows]


def run_anomaly_scan(out_dir=OUT, workers=4, tag=""):
    """Scan every crawled HWP file. Writes anomaly_files{tag}.parquet, anomaly_rows{tag}.parquet
    and anomaly_summary{tag}.json."""
    t0 = time.time()
    roles = production_roles()
    roster_names()   # warm the cache before forking
    tasks = sorted(roles.items())
    F, R = [], []
    with Pool(workers) as pool:
        for f, rows in pool.imap_unordered(_work_anomaly, tasks, chunksize=4):
            F.append(f)
            R.extend(rows)
    F = pd.DataFrame(F).sort_values("conf_num")
    R = pd.DataFrame(R, columns=["conf_num", "kind", "turn_seq", "event_i", "label", "label_how", "detail",
                                 "text_head", "role"])
    R = R.sort_values(["conf_num", "kind", "turn_seq"], na_position="last")
    F.to_parquet(out_dir / f"anomaly_files{tag}.parquet", index=False)
    R.to_parquet(out_dir / f"anomaly_rows{tag}.parquet", index=False)
    summ = {"n_files": int(len(F)), "elapsed_s": round(time.time() - t0, 1)}
    for role, g in F.groupby("role"):
        ok = g[g.status.isin(["ok", "ok_no_turns"])]
        d = {"n_files": int(len(g)), "status": g.status.value_counts().to_dict(),
             "n_turns": int(ok.n_turns.sum()), "files_with_sittings_gt1": int((ok.n_sittings > 1).sum())}
        for k in ANOMALY_KINDS:
            d["n_" + k] = int(ok["n_" + k].sum())
            d["files_" + k] = int((ok["n_" + k] > 0).sum())
        rr = R[R.role == role]
        d["weak_label_how"] = rr[rr.kind == "weak_label"].label_how.value_counts().to_dict()
        summ[role] = d
    (out_dir / f"anomaly_summary{tag}.json").write_text(json.dumps(summ, ensure_ascii=False, indent=1, default=str),
                                                       encoding="utf-8")
    return F, R, summ


# ----------------------------------------------------------------------------- hand audit
# A stratified random sample of production files (seed 8374). For each file the parsed turns
# are set against the raw paragraph stream (extract_paragraphs): every body line that opens
# with a marker and a label set off by a double space or tab is a reference turn head. The
# audit covers the first 30 and the last 30 turns and every anomaly turn. For each audited turn
# the sheet shows the paragraph lines from its head to the next turn head, what the parser made
# of them, and automatic checks (head_found: a reference head sits where the turn starts;
# heads_inside: reference heads inside the turn's span that the parser did not split on). The
# verdicts of the hand review are kept in audit/verdicts.json and turned into error rates.

AUDIT_STRATA = {"plenary": 7, "subcommittee": 8, "special": 7, "budget": 6, "investigation": 5, "other_term": 7}
AUDIT_SEED = 8374
REF_HEAD_RE = re.compile(r"^[ \t　\xa0]*[◯○](?![◯○])[ 　]?(?P<label>[^\t]+?)(?:[ 　\xa0]{2,}|\t)(?P<text>\S.*)?$")


def audit_stratum(meta):
    if meta["role"] == "production_other":
        return "other_term"
    c = meta["class_name"]
    if c == "국회본회의":
        return "plenary"
    if c == "예산결산특별위원회":
        return "budget"
    if c == "국정조사":
        return "investigation"
    if c == "특별위원회":
        return "special"
    if c in ("상임위원회", "국정감사") and meta.get("is_sub"):
        return "subcommittee"
    return "committee_other"


def draw_audit_sample(roles=None, strata=AUDIT_STRATA, seed=AUDIT_SEED):
    import random
    roles = roles or production_roles()
    by = collections.defaultdict(list)
    for n, m in sorted(roles.items()):
        if m["role"] == "pair":
            continue
        by[audit_stratum(m)].append(n)
    rng = random.Random(seed)
    out = []
    for s, k in strata.items():
        pool_ = sorted(by.get(s, []))
        out.extend((s, n) for n in sorted(rng.sample(pool_, min(k, len(pool_)))))
    return out, {s: len(v) for s, v in by.items()}


def _ref_heads(paras):
    """(line index, label, text) of reference turn heads in the body line stream."""
    lines = []
    for p in paras:
        if p["zone"] != "body" or p["in_table"]:
            continue
        for ln in p["text"].split("\n"):
            lines.append(ln)
    heads = []
    for i, ln in enumerate(lines):
        m = REF_HEAD_RE.match(ln)
        if m and len(m.group("label")) <= 80 and len(m.group("label").split()) <= 4:
            heads.append((i, H.norm(m.group("label")), H.norm(m.group("text") or "")))
    return lines, heads


def audit_file(conf_num, n_edge=30):
    """Audit records for one file: the audited turns with automatic checks and a text sheet."""
    data = hwp_path(conf_num).read_bytes()
    res = H.parse_hwp(data, conf_num=conf_num)
    paras = H.extract_paragraphs(data)
    lines, heads = _ref_heads(paras)
    turns = res["turns"]
    anomalies = anomaly_rows(res, conf_num)
    an_turns = {r["turn_seq"] for r in anomalies if r["turn_seq"] is not None}
    sel = sorted(set(range(1, min(n_edge, len(turns)) + 1)) | set(range(max(1, len(turns) - n_edge + 1), len(turns) + 1))
                 | an_turns)
    nl = [H.norm(x) for x in lines]
    # locate each turn head in the line stream, in order: the first line at or after the previous
    # head that carries the turn's label (as printed) and the start of its text
    pos, cursor = {}, 0
    for t in turns:
        lab = H.nows(t["speaker_label_raw"] or "")
        first = H.nows((t["text_raw"] or "").split("\n")[0])[:12]
        for i in range(cursor, len(nl)):
            s = H.nows(nl[i])
            if s[:1] in "◯○" and lab in s[:len(lab) + 12] and (not first or first in s):
                pos[t["turn_seq"]] = i
                cursor = i + 1
                break
    head_lines = {i for i, _, _ in heads}
    recs, sheet = [], []
    seqs = [t["turn_seq"] for t in turns]
    for t in turns:
        if t["turn_seq"] not in sel:
            continue
        i0 = pos.get(t["turn_seq"])
        nxt = [pos[s] for s in seqs if s > t["turn_seq"] and s in pos]
        i1 = nxt[0] if nxt else None
        inside = [] if i0 is None else [i for i in head_lines if i0 < i < (i1 if i1 is not None else i0 + 1 + len(t["text_raw"].split("\n")))]
        rec = {"conf_num": conf_num, "turn_seq": t["turn_seq"], "label": t["speaker_label_raw"],
               "label_how": t.get("label_how"), "anomaly": t["turn_seq"] in an_turns,
               "head_line": i0, "head_found": i0 is not None and i0 in head_lines,
               "heads_inside": len(inside), "n_lines": t.get("n_lines"),
               "sitting_seq": t.get("sitting_seq"), "after_end": t.get("after_end_marker")}
        recs.append(rec)
        sheet.append(f"## turn {t['turn_seq']} [{t.get('label_how')}] {t['speaker_label_raw']!r} "
                     f"time={t.get('time_hhmm')} date={t.get('speech_date')} sit={t.get('sitting_seq')} "
                     f"aem={t.get('after_end_marker')} head_found={rec['head_found']} heads_inside={len(inside)}"
                     + (" ANOMALY" if rec["anomaly"] else ""))
        sheet.append("   TEXT: " + " | ".join(x[:70] for x in t["text_raw"].split("\n")[:3])
                     + (f" ... [{len(t['text_raw'].split(chr(10)))} lines] ... " + t["text_raw"].split("\n")[-1][-60:]
                        if len(t["text_raw"].split("\n")) > 3 else ""))
        if i0 is not None:
            end = i1 if i1 is not None else min(len(lines), i0 + 8)
            raw = [f"{j}:{lines[j][:90]}" for j in range(i0, min(end, i0 + 6))]
            if end - i0 > 6:
                raw.append(f"... {end - i0 - 6} more lines ...")
                raw.extend(f"{j}:{lines[j][:90]}" for j in range(max(i0 + 6, end - 3), end))
            sheet.append("   RAW:  " + "\n         ".join(raw))
        else:
            sheet.append("   RAW:  head not located in the line stream")
    return res, recs, sheet, anomalies


def run_audit(out_dir=OUT, n_edge=30):
    roles = production_roles()
    sample, sizes = draw_audit_sample(roles)
    ad = out_dir / "audit"
    ad.mkdir(parents=True, exist_ok=True)
    R, files = [], []
    for stratum, n in sample:
        res, recs, sheet, anomalies = audit_file(n, n_edge)
        for r in recs:
            r["stratum"] = stratum
        R.extend(recs)
        m = roles[n]
        files.append({"conf_num": n, "stratum": stratum, "term": m["term"], "class_name": m["class_name"],
                      "comm_name": m["comm_name"], "n_turns": len(res["turns"]), "n_audited": len(recs),
                      "n_anomalies": len(anomalies), "n_sittings": res["stats"].get("n_sittings", 1)})
        head = [f"# {n} {m['class_name']} {m['comm_name']} term {m['term']} date {m['api_date']} "
                f"turns {len(res['turns'])} audited {len(recs)} anomalies {len(anomalies)}"]
        head += [f"#   anomaly {a['kind']} turn {a['turn_seq']} {a['detail']} :: {a['text_head'][:80]}" for a in anomalies]
        (ad / f"{n}.txt").write_text("\n".join(head + sheet) + "\n", encoding="utf-8")
    R = pd.DataFrame(R)
    F = pd.DataFrame(files)
    R.to_parquet(ad / "audit_turns.parquet", index=False)
    F.to_parquet(ad / "audit_files.parquet", index=False)
    (ad / "audit_sample.json").write_text(json.dumps({"seed": AUDIT_SEED, "strata": AUDIT_STRATA,
                                                      "population": sizes, "sample": sample},
                                                     ensure_ascii=False, indent=1), encoding="utf-8")
    return F, R


def audit_rates(out_dir=OUT):
    """Errors per 1,000 audited turns from audit/verdicts.json ({conf_num: {turn_seq: [error
    classes]}}; turns not listed were reviewed and found correct)."""
    ad = out_dir / "audit"
    R = pd.read_parquet(ad / "audit_turns.parquet")
    v = json.loads((ad / "verdicts.json").read_text(encoding="utf-8")) if (ad / "verdicts.json").exists() else {}
    errs = []
    for n, d in v.get("errors", {}).items():
        for ts, classes in d.items():
            for c in classes:
                errs.append({"conf_num": int(n), "turn_seq": int(ts), "cls": c})
    E = pd.DataFrame(errs, columns=["conf_num", "turn_seq", "cls"])
    audited = R[["conf_num", "turn_seq", "stratum"]]
    E = E.merge(audited, on=["conf_num", "turn_seq"], how="left")
    out = {"n_files": int(R.conf_num.nunique()), "n_audited_turns": int(len(R)),
           "n_error_turns": int(E[["conf_num", "turn_seq"]].drop_duplicates().shape[0]),
           "n_errors": int(len(E)),
           "errors_per_1000_turns": round(1000 * len(E) / max(1, len(R)), 2),
           "by_class": E.cls.value_counts().to_dict(), "by_stratum": {}}
    for s, g in R.groupby("stratum"):
        e = E[E.stratum == s]
        out["by_stratum"][s] = {"files": int(g.conf_num.nunique()), "turns": int(len(g)), "errors": int(len(e)),
                                "per_1000": round(1000 * len(e) / max(1, len(g)), 2)}
    return out


# ----------------------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--all", action="store_true", help="also parse every crawled HWP (all_hwp_stats.parquet)")
    ap.add_argument("--anomalies", action="store_true",
                    help="only the anomaly scan over every crawled HWP (anomaly_*.parquet / .json)")
    ap.add_argument("--audit", action="store_true", help="only draw the audit sample and write audit sheets")
    ap.add_argument("--audit-rates", action="store_true", help="only compute error rates from audit/verdicts.json")
    ap.add_argument("--tag", default="", help="suffix for anomaly output files")
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    if a.anomalies:
        _, _, summ = run_anomaly_scan(a.out, a.workers, a.tag)
        print(json.dumps(summ, ensure_ascii=False, indent=1, default=str))
        return
    if a.audit:
        F, R = run_audit(a.out)
        print(F.to_string())
        return
    if a.audit_rates:
        r = audit_rates(a.out)
        (a.out / "audit" / "audit_rates.json").write_text(json.dumps(r, ensure_ascii=False, indent=1), encoding="utf-8")
        print(json.dumps(r, ensure_ascii=False, indent=1))
        return
    t0 = time.time()
    xw = xlsx18_meetings()
    ensure_rows(xw)
    have = crawled_hwp()
    pairs = xw[xw.conf_num.isin(have)].sort_values("conf_num")
    if a.limit:
        pairs = pairs.head(a.limit)
    print(f"xlsx 18대 meetings {len(xw)}, crawled hwp {len(have)}, pairs {len(pairs)}", flush=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'; SET threads=2")
    per, resid = [], []
    recs = pairs.to_dict("records")
    with Pool(a.workers) as pool:
        for b in range(0, len(recs), a.batch):
            chunk = recs[b:b + a.batch]
            by = load_rows([r["meeting_id"] for r in chunk], con)
            tasks = [(r["conf_num"], {"v9_meeting_id": r["meeting_id"], "hearing_type": r["hearing_type"],
                                      "committee": r["committee"], "date": r["date"]},
                      by.get(r["meeting_id"], [])) for r in chunk]
            for m, rs in pool.imap_unordered(_work_pair, tasks, chunksize=2):
                per.append(m)
                resid.extend(rs)
            print(f"  pairs {min(b + a.batch, len(recs))}/{len(recs)}  {time.time() - t0:.0f}s", flush=True)
        per = pd.DataFrame(per).sort_values("conf_num")
        per.to_parquet(a.out / "validation_per_meeting.parquet", index=False)
        resid = pd.DataFrame(resid)
        resid.to_parquet(a.out / "validation_residuals.parquet", index=False)

        u = pd.read_parquet(UNIVERSE, columns=["CONFER_NUM", "DAE_NUM", "CLASS_NAME_unified", "COMM_NAME", "CONF_DATE"])
        u["conf_num"] = u.CONFER_NUM.astype(int)
        u18 = u[u.DAE_NUM.astype(int) == 18]
        pl = u18[u18.CLASS_NAME_unified == "국회본회의"]
        tasks = [(r.conf_num, {"api_date": r.CONF_DATE, "class_name": r.CLASS_NAME_unified, "comm_name": r.COMM_NAME})
                 for r in pl.itertuples()]
        plen = pd.DataFrame(pool.map(_work_stats, tasks, chunksize=2)).sort_values("conf_num")
        plen.to_parquet(a.out / "plenary_stats.parquet", index=False)
        print(f"  plenary {len(plen)}  {time.time() - t0:.0f}s", flush=True)
        allst = None
        if a.all:
            uu = u.set_index("conf_num")
            tasks = []
            for n in sorted(have):
                r = uu.loc[n] if n in uu.index else None
                tasks.append((n, {"api_date": None if r is None else r.CONF_DATE,
                                  "class_name": None if r is None else r.CLASS_NAME_unified,
                                  "comm_name": None if r is None else r.COMM_NAME,
                                  "term": None if r is None else int(r.DAE_NUM)}))
            allst = pd.DataFrame(pool.map(_work_stats, tasks, chunksize=4)).sort_values("conf_num")
            allst.to_parquet(a.out / "all_hwp_stats.parquet", index=False)
            print(f"  all hwp {len(allst)}  {time.time() - t0:.0f}s", flush=True)
    summ = summarize(per, resid, plen, allst)
    summ["elapsed_s"] = round(time.time() - t0, 1)
    (a.out / "validation_summary.json").write_text(json.dumps(summ, ensure_ascii=False, indent=1, default=str),
                                                  encoding="utf-8")
    print(json.dumps({k: v for k, v in summ.items() if not isinstance(v, dict)}, ensure_ascii=False, indent=1))


def make_golden(conf_nums, per_meeting=None):
    """Write test_hwp_parser_golden.json for the given conf_nums (expected values = current
    parser output; XLSX agreement recomputed from the extracted v9 rows when the meeting is an
    XLSX pair)."""
    xw = xlsx18_meetings().set_index("conf_num")
    con = duckdb.connect()
    gpath = HERE / "test_hwp_parser_golden.json"
    gold = json.loads(gpath.read_text(encoding="utf-8")) if gpath.exists() else {}
    for n in conf_nums:
        p = hwp_path(n)
        if not p.exists():
            p = V10 / "raw" / "samples_hwp" / f"{n}.hwp"
        res = H.parse_hwp(p.read_bytes(), conf_num=n)
        t = res["turns"]
        st = res["stats"]
        g = {"status": res["status"], "n_turns": len(t),
             "first_labels": [x["speaker_label_raw"] for x in t[:3]],
             "last_labels": [x["speaker_label_raw"] for x in t[-2:]],
             "chars_text_raw": sum(len(H.nows(x["text_raw"])) for x in t),
             "chars_text": sum(len(H.nows(x["text"])) for x in t),
             "date": res["meeting"].get("date"),
             "committee_raw": res["meeting"].get("committee_raw"),
             "subcommittee": res["meeting"].get("subcommittee"),
             "appendix_how": st["appendix_how"],
             "n_agenda_anchors": st["n_agenda_anchors"],
             "n_time_markers": st["n_time_markers"],
             "n_footer_sections": len(res["footer"]),
             "orphan_other": st["counters"].get("orphan_other", 0),
             "n_sittings": st.get("n_sittings", 1),
             "n_turns_after_end_marker": st.get("n_turns_after_end_marker", 0),
             "n_weak_labels": sum(1 for x in t if x.get("label_how") not in STRONG_HOW),
             "n_name_list_turns": sum(1 for r in anomaly_rows(res, n) if r["kind"] == "name_list_text"),
             "sittings": [[sg["sitting_seq"], sg["start_turn_seq"], sg["date"], sg["date_how"]]
                          for sg in res.get("sittings") or []]}
        if n in xw.index and ROWS.exists():
            mid = xw.loc[n, "meeting_id"]
            rows = load_rows([mid], con).get(mid, [])
            m, _ = compare(res, rows)
            g.update({"v9_meeting_id": mid, "xlsx_n_rows": len(rows),
                      "pos_agree_dueum": m["pos_agree_dueum"], "aligned_agree_dueum": m["aligned_agree_dueum"]})
        else:
            # no XLSX reference (16대 / 17대 files checked against the viewer XML): pin the labels
            # themselves, label -> number of turns
            lc = collections.Counter(x["speaker_label_raw"] for x in t)
            g["label_counts"] = {k: lc[k] for k in sorted(lc, key=lambda k: (-lc[k], k or ""))}
        gold[str(n)] = g
    gold = {k: gold[k] for k in sorted(gold, key=int)}
    gpath.write_text(json.dumps(gold, ensure_ascii=False, indent=1), encoding="utf-8")
    return gold


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--make-golden":
        make_golden([int(x) for x in sys.argv[2].split(",")], OUT / "validation_per_meeting.parquet")
    else:
        main()
