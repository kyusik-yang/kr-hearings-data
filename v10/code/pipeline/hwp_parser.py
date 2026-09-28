"""HWP 5.x reader and 국회 minutes parser (v10 pipeline component).

    extract_paragraphs(data)  -> list[dict(text, in_table, table_id, row, col, ...)]
    parse_hwp(data)           -> dict(status, meeting, agenda, turns, events, footer, stats)

Layer 1 (binary, self-contained, olefile only):
- FileHeader: signature, version, property flags (bit0 compressed, bit1 password,
  bit2 distribution). Encrypted and distribution documents are reported by status, not read.
- BodyText/Section{N} streams, raw-deflate (wbits=-15) when compressed.
- Record header DWORD: tag = bits 0-9, level = bits 10-19, size = bits 20-31; size 0xFFF
  means the real size follows as a DWORD.
- Records are nested by level into a tree. A paragraph (HWPTAG_PARA_HEADER, 66) owns its
  PARA_TEXT (67) and CTRL_HEADER (71) children. Each extended control character in the text
  refers, in order, to the paragraph's next CTRL_HEADER child. Table cells (LIST_HEADER, 72,
  followed by their paragraphs) are rendered at the position of the table's control
  character, so cell paragraphs keep reading order; the paragraph text before and after the
  table becomes separate fragments of the same paragraph.
- PARA_TEXT is UTF-16LE. Control characters 0-31 are 1 WCHAR (0, 10, 13, 24-31) or 8 WCHARs
  (1-9, 11-12, 14-23: inline and extended controls). Tab (9) -> '\\t', line break (10) ->
  '\\n', paragraph end (13) dropped, bound/fixed-width space (30/31) -> ' ', hyphen (24)
  -> '-'. Field start/end (3/4) and every other control produce no text.

Layer 2 (minutes grammar): header table (제N회국회(...), 회의록 title, 제N호), 일시/장소,
agenda blocks (의사일정, 상정된 안건, 부의된 안건, 심사된 안건), speaker turns introduced by
◯ / ○, time markers, stage directions, oath signature blocks, and the appendix after the
body (출석 위원/의원, 전문위원, 정부측 참석자, 증인/참고인, 보고사항). Turns carry the
CONTRACT turn fields with source='hwp'.

Meeting end, appendix and sittings. A meeting-end marker ('(18시41분 산회)', 폐회, 감사종료, 조사종료,
散會, 閉會, 폐식, 유회, or '(계속개의되지 않았음)') ends the running turn: nothing printed after it is
turn text. After it the body is closed, and a new sitting starts only on evidence of one
(_sitting_starts): an opening marker ('(14시40분 감사계속)', 개의, 계속개의, 속개, 개회, 감사개시, 조사개시,
…), or a second cover (a 회의록 table, or 日時 + date as for an appended address), cover lines
('일  시 …' + '장  소 …'), a sitting heading ('【오전회의 내용】') or another time marker, each followed
by a speaker line with an attested label (a label of the document before the end marker, or a
separator-set label matching the title patterns). Everything else after the end marker (written
questions and answers, 제안설명서, review reports, budget tables, audit plans, attendee lists) is
appendix: kept verbatim in the footer (when no sitting follows) or as events (between sittings),
never turns. Turn flags (CONTRACT): after_end_marker = a meeting-end marker was printed earlier in
the turn's own sitting; a new sitting starts fresh (researcher decision 2026-09-26), and since the
body is closed after an end marker until a new sitting starts, no HWP turn carries it.
after_final_end_marker = the turn starts after the document's last end marker (the turns of a
later sitting that prints no end marker of its own). sitting_seq numbers the sittings.

The 16대 layout is read as well: Hanja titles and
names, labels set off by one ideographic space with justified position and name inside
('○委員長  李允洙\u3000…'), spaced two-syllable names ('委員長 李  協'), marker agenda headings
('○ 위원장(金德圭)인사') and speech that runs on inside a table frame.

Pure functions. CLI:  python hwp_parser.py FILE.hwp [--paragraphs] [--brief]
"""
from __future__ import annotations

import datetime as _dt
import io
import json
import re
import struct
import sys
import unicodedata
import zlib

import olefile

__all__ = [
    "read_hwp", "iter_records", "decode_para_text", "build_tree", "paragraphs_from_sections",
    "extract_paragraphs", "parse_hwp", "parse_minutes",
    "split_label", "HwpError",
]

# ----------------------------------------------------------------------------- constants

HWPTAG_BEGIN = 16
TAG_PARA_HEADER = HWPTAG_BEGIN + 50      # 66
TAG_PARA_TEXT = HWPTAG_BEGIN + 51        # 67
TAG_CTRL_HEADER = HWPTAG_BEGIN + 55      # 71
TAG_LIST_HEADER = HWPTAG_BEGIN + 56      # 72
TAG_SHAPE_COMPONENT = HWPTAG_BEGIN + 60  # 76
TAG_TABLE = HWPTAG_BEGIN + 61            # 77
TAG_EQEDIT = HWPTAG_BEGIN + 72           # 88

SIGNATURE = b"HWP Document File"
FLAG_COMPRESSED = 0x1
FLAG_PASSWORD = 0x2
FLAG_DISTRIBUTION = 0x4

# control character classes (HWP 5.0 spec, table "제어 문자")
CHAR_CTRLS = {0, 10, 13, 24, 25, 26, 27, 28, 29, 30, 31}            # 1 WCHAR
INLINE_CTRLS = {4, 5, 6, 7, 8, 9, 19, 20}                              # 8 WCHARs, no object
EXTENDED_CTRLS = {1, 2, 3, 11, 12, 14, 15, 16, 17, 18, 21, 22, 23}     # 8 WCHARs, CTRL_HEADER
# ctrl ids whose CTRL_HEADER subtree holds paragraphs, and the container label used for them
CONTAINER_OF = {"tbl ": "table", "head": "header", "foot": "footer", "fn  ": "footnote",
                "en  ": "endnote", "gso ": "textbox", "tcmt": "comment"}


class HwpError(Exception):
    pass


# ----------------------------------------------------------------------------- layer 1

def read_hwp(data: bytes) -> dict:
    """Open an HWP 5 compound file. Returns dict(status, version, flags, sections=[bytes]).

    status: ok | not_ole | not_hwp5 | password | distribution | stream_error
    """
    out = {"status": None, "version": None, "flags": None, "compressed": None, "sections": [],
           "prv_text": None, "errors": []}
    if data[:4] == b"PK\x03\x04":
        out["status"] = "hwpx_unsupported"
        return out
    if not olefile.isOleFile(io.BytesIO(data)):
        out["status"] = "hwp3" if data[:len(SIGNATURE)] == SIGNATURE else "not_ole"
        return out
    ole = olefile.OleFileIO(io.BytesIO(data))
    try:
        if not ole.exists("FileHeader"):
            out["status"] = "not_hwp5"
            return out
        fh = ole.openstream("FileHeader").read()
        if not fh.startswith(SIGNATURE):
            out["status"] = "not_hwp5"
            return out
        ver = struct.unpack_from("<I", fh, 32)[0]
        flags = struct.unpack_from("<I", fh, 36)[0]
        out["version"] = f"{ver >> 24 & 0xFF}.{ver >> 16 & 0xFF}.{ver >> 8 & 0xFF}.{ver & 0xFF}"
        out["flags"] = flags
        out["compressed"] = bool(flags & FLAG_COMPRESSED)
        if flags & FLAG_PASSWORD:
            out["status"] = "password"
            return out
        if flags & FLAG_DISTRIBUTION:
            out["status"] = "distribution"
            return out
        names = []
        for entry in ole.listdir():
            if len(entry) == 2 and entry[0] == "BodyText" and entry[1].startswith("Section"):
                try:
                    names.append((int(entry[1][7:]), "/".join(entry)))
                except ValueError:
                    out["errors"].append(f"odd section name {entry[1]}")
        for _, name in sorted(names):
            raw = ole.openstream(name).read()
            if out["compressed"]:
                try:
                    raw = zlib.decompress(raw, -15)
                except zlib.error as e:
                    # tolerate trailing garbage: decompressobj keeps what it could inflate
                    d = zlib.decompressobj(-15)
                    try:
                        raw = d.decompress(raw)
                        out["errors"].append(f"{name}: partial inflate ({e})")
                    except zlib.error as e2:
                        out["errors"].append(f"{name}: inflate failed ({e2})")
                        out["status"] = "stream_error"
                        return out
            out["sections"].append(raw)
        if ole.exists("PrvText"):
            try:
                out["prv_text"] = ole.openstream("PrvText").read().decode("utf-16le", "replace")
            except Exception:  # pragma: no cover
                pass
        out["status"] = "ok" if out["sections"] else "no_body"
        return out
    finally:
        ole.close()


def iter_records(buf: bytes):
    """Yield (tag, level, payload) from a decompressed record stream.

    Raises HwpError on a truncated header or payload (the caller counts it)."""
    pos, n = 0, len(buf)
    while pos < n:
        if pos + 4 > n:
            raise HwpError(f"truncated record header at {pos}")
        h = struct.unpack_from("<I", buf, pos)[0]
        pos += 4
        tag, level, size = h & 0x3FF, (h >> 10) & 0x3FF, (h >> 20) & 0xFFF
        if size == 0xFFF:
            if pos + 4 > n:
                raise HwpError(f"truncated extended size at {pos}")
            size = struct.unpack_from("<I", buf, pos)[0]
            pos += 4
        if pos + size > n:
            raise HwpError(f"truncated payload at {pos} (size {size}, have {n - pos})")
        yield tag, level, buf[pos:pos + size]
        pos += size


class Rec:
    __slots__ = ("tag", "level", "data", "children", "idx")

    def __init__(self, tag, level, data, idx):
        self.tag, self.level, self.data, self.idx = tag, level, data, idx
        self.children = []

    def ctrl_id(self):
        if self.tag != TAG_CTRL_HEADER or len(self.data) < 4:
            return None
        return self.data[:4][::-1].decode("latin-1")


def build_tree(records, stats=None):
    """Nest records by level. A record at level L is a child of the last record at level L-1.
    A level jump (> last level + 1) attaches to the deepest open record and is counted."""
    root = Rec(-1, -1, b"", -1)
    stack = [root]  # stack[i] = open record at level i-1 (stack[0] = root)
    for i, (tag, level, data) in enumerate(records):
        r = Rec(tag, level, data, i)
        if level + 1 > len(stack):
            if stats is not None:
                stats["level_jumps"] = stats.get("level_jumps", 0) + 1
            parent = stack[-1]
        else:
            del stack[level + 1:]
            parent = stack[level]
        parent.children.append(r)
        stack.append(r)
    return root


def decode_para_text(payload: bytes, stats=None):
    """Decode a PARA_TEXT payload. Returns a list of tokens:
    ('text', str) or ('ctrl', code, ctrl_id_or_None) for extended controls
    (the caller maps them, in order, to CTRL_HEADER children).

    Inline controls: tab (9) -> ('text','\\t'); field end (4) and others -> dropped.
    Char controls: 10 -> '\\n', 13 -> end (dropped), 24 -> '-', 30/31 -> ' ', others dropped."""
    n = len(payload) // 2
    if len(payload) % 2 and stats is not None:
        stats["odd_text_bytes"] = stats.get("odd_text_bytes", 0) + 1
    w = struct.unpack_from(f"<{n}H", payload, 0)
    # Text between control characters is copied as byte runs; the runs of one token are
    # decoded together, so surrogate pairs are recombined by the utf-16 codec.
    toks, buf = [], []
    i = 0
    for j in [j for j, c in enumerate(w) if c < 32]:
        if j < i:                      # inside the 8 WCHARs of the previous control
            continue
        if j > i:
            buf.append(payload[2 * i:2 * j])
        c = w[j]
        if c in CHAR_CTRLS:
            if c == 10:
                buf.append(b"\n\x00")
            elif c == 24:
                buf.append(b"-\x00")
            elif c in (30, 31):
                buf.append(b" \x00")
            elif c != 13 and stats is not None:
                stats["dropped_char_ctrl"] = stats.get("dropped_char_ctrl", 0) + 1
            i = j + 1
            continue
        # 8-WCHAR control: code, 6 WCHARs of info, code
        if j + 8 > n:
            if stats is not None:
                stats["truncated_ctrl"] = stats.get("truncated_ctrl", 0) + 1
            i = n
            break
        if w[j + 7] != c and stats is not None:
            stats["ctrl_trailer_mismatch"] = stats.get("ctrl_trailer_mismatch", 0) + 1
        i = j + 8
        if c in INLINE_CTRLS:
            if c == 9:
                buf.append(b"\t\x00")
            continue
        # extended control
        cid = struct.pack("<2H", w[j + 1], w[j + 2])[::-1].decode("latin-1")
        if buf:
            toks.append(("text", _b2s(buf)))
            buf = []
        toks.append(("ctrl", c, cid))
    if i < n:
        buf.append(payload[2 * i:2 * n])
    if buf:
        s = _b2s(buf)
        if s:
            toks.append(("text", s))
    return toks


def _b2s(parts):
    return b"".join(parts).decode("utf-16le", "replace")


def _cell_addr(lh: Rec):
    """LIST_HEADER of a table cell: UINT16 nparas, UINT16, UINT32 flags, then
    UINT16 col, row, colspan, rowspan."""
    d = lh.data
    if len(d) >= 16:
        col, row, cs, rs = struct.unpack_from("<4H", d, 8)
        return row, col, rs, cs
    return None, None, None, None


def _table_shape(t: Rec):
    d = t.data
    if len(d) >= 8:
        _, nrows, ncols = struct.unpack_from("<IHH", d, 0)
        return nrows, ncols
    return None, None


class _Emitter:
    """Collects paragraph fragments in reading order."""

    def __init__(self, stats):
        self.out = []
        self.stats = stats
        self.n_tables = 0
        self.n_paras = 0

    def emit(self, text, ctx, para_id, frag):
        d = {"text": text, "in_table": ctx["table_id"] is not None,
             "table_id": ctx["table_id"], "row": ctx["row"], "col": ctx["col"],
             "outer_table_id": ctx["outer_table_id"],
             "cell": ctx["cell"], "container": ctx["container"], "zone": ctx["zone"],
             "depth": ctx["depth"],
             "section": ctx["section"], "para_id": para_id, "frag": frag}
        self.out.append(d)


def _render_paragraph(p: Rec, ctx, em: _Emitter):
    """Render one PARA_HEADER node: text fragments and nested containers in order."""
    em.n_paras += 1
    para_id = em.n_paras
    ptext = [c for c in p.children if c.tag == TAG_PARA_TEXT]
    ctrls = [c for c in p.children if c.tag == TAG_CTRL_HEADER]
    toks = []
    for pt in ptext:
        toks.extend(decode_para_text(pt.data, em.stats))
    if len(p.data) >= 4:
        nchars = struct.unpack_from("<I", p.data, 0)[0] & 0x7FFFFFFF
        have = sum(len(pt.data) // 2 for pt in ptext)
        if ptext and nchars != have:
            em.stats["nchars_mismatch"] = em.stats.get("nchars_mismatch", 0) + 1
    if len(ptext) > 1:
        em.stats["multi_para_text"] = em.stats.get("multi_para_text", 0) + 1
    ci = 0
    frag = 0
    buf = []
    for tk in toks:
        if tk[0] == "text":
            buf.append(tk[1])
            continue
        _, code, cid = tk
        node = ctrls[ci] if ci < len(ctrls) else None
        ci += 1
        if node is None:
            em.stats["ctrl_without_header"] = em.stats.get("ctrl_without_header", 0) + 1
            continue
        real = node.ctrl_id()
        if real != cid:
            em.stats["ctrl_id_mismatch"] = em.stats.get("ctrl_id_mismatch", 0) + 1
        if _has_paragraphs(node):
            if buf:
                em.emit("".join(buf), ctx, para_id, frag)
                frag += 1
                buf = []
            _render_container(node, ctx, em)
        elif real in ("tdut", "tcps"):   # 덧말 / 글자 겹침: the main text is shown inline
            buf.append(_dutmal_main(node, em.stats))
            em.stats["ctrl_text_" + real] = em.stats.get("ctrl_text_" + real, 0) + 1
        elif real == "eqed":
            em.stats["equations"] = em.stats.get("equations", 0) + 1
        elif real == "atno":
            em.stats["auto_numbers"] = em.stats.get("auto_numbers", 0) + 1
        else:
            # every other extended control without paragraphs (section / column definitions,
            # page numbers, fields, drawing objects without text boxes, ...) produces no text
            # here; it is counted by id so that a text-bearing kind would not pass unseen
            k = "ctrl_unrendered_" + ((real or "?").strip() or "?")
            em.stats[k] = em.stats.get(k, 0) + 1
    for node in ctrls[ci:]:   # CTRL_HEADERs without a control char: render so nothing is lost
        em.stats["header_without_ctrl"] = em.stats.get("header_without_ctrl", 0) + 1
        if _has_paragraphs(node):
            if buf:
                em.emit("".join(buf), ctx, para_id, frag)
                frag += 1
                buf = []
            _render_container(node, ctx, em)
    if buf or frag == 0:
        em.emit("".join(buf), ctx, para_id, frag)


def _has_paragraphs(node: Rec):
    stack = list(node.children)
    while stack:
        c = stack.pop()
        if c.tag == TAG_PARA_HEADER:
            return True
        stack.extend(c.children)
    return False


def _dutmal_main(node: Rec, stats):
    d = node.data
    try:
        ln = struct.unpack_from("<H", d, 4)[0]
        return d[6:6 + 2 * ln].decode("utf-16le", "replace")
    except Exception:
        stats["dutmal_decode_fail"] = stats.get("dutmal_decode_fail", 0) + 1
        return ""


def _render_container(node: Rec, ctx, em: _Emitter):
    cid = node.ctrl_id()
    kind = CONTAINER_OF.get(cid, "other:" + (cid or "?"))
    sub = dict(ctx, depth=ctx["depth"] + 1, container=kind)
    if kind != "table" and ctx["zone"] == "body":
        sub["zone"] = kind          # outermost non-table container (header, footnote, ...)
    if cid == "tbl ":
        em.n_tables += 1
        sub.update(table_id=em.n_tables, row=None, col=None, cell=0,
                   outer_table_id=ctx["outer_table_id"] or em.n_tables)
        if not any(c.tag == TAG_TABLE for c in node.children):
            em.stats["table_without_record"] = em.stats.get("table_without_record", 0) + 1
    _walk_container(node, sub, em)


def _walk_container(node: Rec, ctx, em: _Emitter):
    """Visit descendants in order. LIST_HEADER sets the cell context of the paragraphs that
    follow it (tables); PARA_HEADER nodes are rendered; other records are descended into
    (drawing objects keep their text-box paragraphs under SHAPE_COMPONENT)."""
    cell_ctx = ctx
    for c in node.children:
        if c.tag == TAG_LIST_HEADER:
            if ctx["container"] == "table" and ctx["table_id"] is not None:
                row, col, rs, cs = _cell_addr(c)
                cell_ctx = dict(ctx, row=row, col=col, cell=(cell_ctx.get("cell") or 0) + 1)
            else:
                cell_ctx = ctx
        elif c.tag == TAG_PARA_HEADER:
            _render_paragraph(c, cell_ctx, em)
        elif c.children:
            _walk_container(c, cell_ctx, em)


def paragraphs_from_sections(sections, errors=None):
    """Render decompressed BodyText section streams into paragraphs (see extract_paragraphs).
    Returns (paragraphs, stats)."""
    stats = {}
    em = _Emitter(stats)
    for si, sec in enumerate(sections):
        recs = []
        try:
            for r in iter_records(sec):
                recs.append(r)
        except HwpError as e:   # keep the records read before the error, count it
            stats["record_errors"] = stats.get("record_errors", 0) + 1
            if errors is not None:
                errors.append(f"section {si}: {e}")
        stats["n_records"] = stats.get("n_records", 0) + len(recs)
        root = build_tree(recs, stats)
        ctx = {"table_id": None, "outer_table_id": None, "row": None, "col": None, "cell": None,
               "container": "body", "zone": "body", "depth": 0, "section": si}
        for c in root.children:
            if c.tag == TAG_PARA_HEADER:
                _render_paragraph(c, ctx, em)
            else:
                stats["top_level_non_para"] = stats.get("top_level_non_para", 0) + 1
    stats["n_tables"] = em.n_tables
    stats["n_paragraphs"] = em.n_paras
    return em.out, stats


def extract_paragraphs(data: bytes, with_status: bool = False):
    """Paragraphs of all BodyText sections in reading order.

    Each item: text, in_table, table_id, row, col (plus outer_table_id, cell, container, zone,
    depth, section, para_id, frag). Table cell paragraphs appear where the table sits in the
    text flow. container (innermost) is body | table | header | footer | footnote | endnote |
    textbox | comment; zone is the outermost non-table container (body for body text and
    body tables). With with_status=True returns (paragraphs, info) where info has the reader
    status (see read_hwp) and counters."""
    info = read_hwp(data)
    paras, stats = [], {}
    if info["status"] == "ok":
        paras, stats = paragraphs_from_sections(info["sections"], info["errors"])
    info = {k: v for k, v in info.items() if k != "sections"}
    info["reader_stats"] = stats
    return (paras, info) if with_status else paras


# ============================================================================= layer 2
# Minutes grammar. Stage-direction lexicon and oath / interjection rules are shared with
# the viewer parser (v10/code/parse_viewer.py) so XML and HWP turns are flagged alike.

try:  # pragma: no cover - import path depends on the caller's cwd
    from parse_viewer import (STAGE_KINDS, OATH_RE, INTERJ_RE, INTERJ_WHERE_RE, TIME_ACTIONS,
                              PAREN_RE, ROLLOVER_RE, _STAGE_LEX)
except ImportError:  # pragma: no cover
    from pathlib import Path as _Path
    sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
    from parse_viewer import (STAGE_KINDS, OATH_RE, INTERJ_RE, INTERJ_WHERE_RE, TIME_ACTIONS,
                              PAREN_RE, ROLLOVER_RE, _STAGE_LEX)

WS_RE = re.compile(r"\s+")
NOWS_RE = re.compile(r"\s+")
INDENT_CHARS = " \t　\xa0"
MARKERS = "◯○"
SEP_RE = re.compile(r"[ 　\xa0]{2,}|\t")
HAN = "\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff"   # CJK ideographs incl. compatibility block (ASCII escapes: editors NFC-fold U+F900)
HANJA_RE = re.compile("[" + HAN + "]")
# '(10시05분)', '(10시08분 개의)', '(9월26일 01시15분 산회)', '(3월19일 24시 경과)', '(10時05分)'
TIME_LINE_RE = re.compile(
    r"^\(\s*(?:(?P<mo>\d{1,2})\s*[월月]\s*(?P<d>\d{1,2})\s*[일日]\s*)?"
    r"(?P<h>\d{1,2})\s*[시時]\s*(?:(?P<mi>\d{1,2})\s*[분分])?\s*(?P<act>[^()]{0,20})\)$")
END_ACTIONS = ("산회", "폐회", "감사종료", "조사종료", "散會", "閉會", "폐식", "閉式", "유회", "流會")
SESSION_RE = re.compile(r"(?:第|제)\s*(\d+)\s*(?:回|회)\s*(?:國會|국회)?\s*(?:\(\s*(\S+?)\s*\))?")
DOCNO_RE = re.compile(r"^(?:第|제)\s*(\d+)\s*(?:號|호)$")
YMD_RE = re.compile(r"(\d{4})\s*[年년.]\s*(\d{1,2})\s*[月월.]\s*(\d{1,2})\s*[日일]?")
# running header: '제294회－제15차', '제284회-보건복지가족제10차', '제280회-행정안전소위1차'
# (the committee name may sit between the session and the sitting; '…제2반' is not a sitting)
RUNHEAD_RE = re.compile(r"(?:제|第)\s*(\d+)\s*(?:회|回)\s*[－\-–―]\s*(?P<body>[^()]*?)\s*(?:제|第)?\s*(\d+)\s*(?:차|次)")
# 16대 layout: a horizontal rule line ('ꠏꠏꠏ…') before an indented agenda heading
RULE_LINE_RE = re.compile(r"^[ꠏꠚ─━―═=_~]{8,}$")
# a cover line that holds the session, the title and the number together
# ('第227回國會 女性特別委員會會議錄 第 2 號', '文化觀光委員會會議錄\t第 1 號')
COVER_JOINED_RE = re.compile(r"^(?P<sess>(?:第|제)\s*\d+\s*(?:回|회)\s*(?:國會|국회))?\s*"
                             r"(?P<title>\S.*?(?:會議錄|회의록|調査錄|조사록))\s*(?P<no>(?:第|제)\s*\d+\s*(?:號|호))?$")
# a block heading that starts another sitting printed in the same file ('【오전회의 내용】',
# '【서울남부구치소 현장 조사단 회의록】'); checked before the appendix headings
SITTING_HEAD_RE = re.compile(r"^【[^】]*(?:회의\s*내용|회의록|會議錄|조사록|調査錄)[^】]*】$")
AGENDA_BLOCK_RE = re.compile(
    r"^(議事日程|의사일정|附議된\s*案件|부의된\s*안건|상정된\s*안건|심사된\s*안건|上程된\s*案件|審査된\s*案件"
    r"|보고된\s*안건|처리된\s*안건|의결된\s*안건|감사\s*일정|심사\s*안건|附議案件)")
AGENDA_NUM_RE = re.compile(r"^(?:\d{1,3}\s*[.．]|[가-하]\s*[.．]|[oOㅇ◦•]\s)\s*\S")
# Appendix section headings, matched on the whole whitespace-free line (heading words and an
# optional '(N인)'): attendance, staff and attendee lists (some printed with a double space,
# '◯정부측 및 기타  참석자') and 【보고사항】. Whole-line matching keeps speaker labels that start
# with the same words ('◯국회사무처입법차장 안병옥  …') out.
APPENDIX_HEAD_NOWS_RE = re.compile(
    r"^(?:【.*|[◯○](?:"
    r"(?:출석|청가|출장|결석|참석|出席|請暇|出張|缺席|參席)[가-힣\u4e00-\u9fff및·․ㆍ]{0,24}"
    r"|(?:개의시|산회시|속개시)재석[가-힣]{0,4}"
    r"|[가-힣]{0,4}아닌출석[가-힣]{0,4}"
    r"|[^◯○()]{0,30}(?:참석자|출석자)"
    r"|국회사무처|國會事務處|政府側[^()]{0,10}"
    r"|제\d+회국회(?:\([^()]*\))?(?:집회공고|집회요구)"
    r")(?:\(.*\))?)$")
# meeting-end time-marker actions (exact; '비공개감사종료', '투표종료', '회의중지' are not ends).
# '폐식' closes an opening ceremony (no speech follows it in the 18대 files); '유회' is a sitting
# that never opened ('(24시 유회)').
END_ACTIONS_EXACT = frozenset({"산회", "폐회", "감사종료", "조사종료", "散會", "閉會", "폐식", "閉式", "유회", "流會"})
# a recess that never resumed also ends the body: '(계속개의되지 않았음)'
END_NOTE_RE = re.compile(r"^\(\s*(?:계속\s*)?개의\s*되지\s*않았음\s*\)$|^\(\s*繼續開議되지않았음\s*\)$")
# (re)opening and continuation actions (exact, as build_turns.OPEN_ACTIONS): printed after a meeting-end
# marker, such a time marker starts a new sitting by itself
OPEN_ACTIONS_EXACT = frozenset({"개의", "계속개의", "속개", "개회", "감사개시", "조사개시", "감사계속", "조사계속",
                                "회의계속", "開議", "續開", "開會", "繼續開議"})
# an opening marker printed without a clock ('(개의)', '(계속개의)'; none in the crawled files so far)
OPEN_NOTE_RE = re.compile(r"^\(\s*(?:" + "|".join(sorted(OPEN_ACTIONS_EXACT, key=len, reverse=True)) + r")\s*\)$")
# a whole-line clock marker whose action _time_line does not take ('(13시53분 화상통화 개시)', 32689): after a
# meeting-end marker it may start a new sitting like a time marker (see _sitting_starts)
CLOCK_LINE_RE = re.compile(
    r"^\(\s*(?:(?P<mo>\d{1,2})\s*[월月]\s*(?P<d>\d{1,2})\s*[일日]\s*)?"
    r"(?P<h>\d{1,2})\s*[시時]\s*(?:(?P<mi>\d{1,2})\s*[분分])?\s*(?P<act>[^()]{0,30})\)$")
# speaker-line rules whose label is set off by the printed separator or known from the document
# (the strong rules of strong_speaker in parse_minutes)
STRONG_LABEL_HOW = frozenset({"sep", "sep_trimmed_by_lexicon", "sep_joined_pos_name", "lexicon_prefix",
                              "lexicon_prefix_punct", "lexicon_prefix_fused"})
NAME_POS_TAIL = ("위원", "의원", "委員", "議員", "義員")   # '義員' is a printed typo of 議員
FUSED_LABEL_RE = re.compile(r"^(?P<name>[가-힣" + HAN + r"]{2,4})(?P<pos>위원|의원|委員|議員|義員)$")


def _name_glued(label):
    """True for the NAME+委員 shape when the part before 委員 may be a name: two or three
    syllables ('金鎭載委員', '松雄委員') or a surname-initial name. A longer title ('首席專門委員',
    '수석전문위원') is a position and may take the name printed after the gap."""
    m = FUSED_LABEL_RE.match(label)
    return bool(m and (len(m.group("name")) <= 3 or _surname_name(m.group("name"))))


def _fused_match(tok):
    """NAME+委員 ('宋榮珍議員', '金鎭載委員') with a surname-initial name; '首席專門委員' and '전문위원' are
    positions, not a name glued to 委員 / 위원."""
    m = FUSED_LABEL_RE.match(tok)
    return m if m and _surname_name(m.group("name")) else None


# a Hanja chair title printed after the name ('劉容泰 委員長')
HANJA_NAME_POS_RE = re.compile(r"^[" + HAN + r"]{1,6}(?:長|代理)$")
SENT_END_RE = re.compile(r"[다요까죠오][.?!]?\s*$|[.?!]\s*$")


def norm(s):
    """Collapse whitespace runs to one space and strip (str.split() uses the same Unicode
    whitespace set as the regex \\s)."""
    if s is None:
        return ""
    return " ".join(s.split())


def nows(s):
    return "".join((s or "").split())


COMPOUND_SURNAMES = frozenset("南宮 皇甫 鮮于 諸葛 司空 西門 獨孤 남궁 황보 선우 제갈 사공 서문 독고".split())


def _glue_spaced_name(toks):
    """Re-join a two-syllable name that the 16대 layout prints justified to three-syllable width
    ('委員長 李  協', '薛  勳委員', '證人 權  証', '南宮  晳議員'): a one-syllable surname (or a
    two-syllable compound surname) token followed by a one-syllable token, or by a one-syllable
    name glued to 委員 / 議員 ('勳委員'), becomes one token ('李協', '薛勳委員', '南宮晳議員').
    Other tokens are kept."""
    out, i = [], 0
    while i < len(toks):
        a = toks[i]
        if i + 1 < len(toks) and ((len(a) == 1 and unicodedata.normalize("NFKC", a) in SURNAMES)
                                  or unicodedata.normalize("NFKC", a) in COMPOUND_SURNAMES):
            b = toks[i + 1]
            if ((len(b) == 1 and NAME_OR_SYLLABLE_RE.match(b)) or
                    (len(b) >= 3 and NAME_OR_SYLLABLE_RE.match(b[0]) and b[1:] in NAME_POS_TAIL)) \
                    and _surname_name(a + b[0]):     # not a spaced word ('2003 예 산')
                out.append(a + b)
                i += 2
                continue
        out.append(a)
        i += 1
    return out


def split_label(label):
    """'위원장 홍길동' -> ('위원장', '홍길동'); '권영진 위원' -> ('위원', '권영진');
    '국무총리 후보자 김태호' -> ('국무총리 후보자', '김태호'); '宋榮珍議員' -> ('議員', '宋榮珍');
    '委員長 李 協' -> ('委員長', '李協') (a spaced two-syllable name, see _glue_spaced_name).
    Returns (pos, name, how)."""
    toks = norm(label).split(" ")
    toks = _glue_spaced_name([t for t in toks if t])
    if not toks:
        return None, None, "empty"
    if len(toks) >= 2 and toks[-1] in NAME_POS_TAIL:
        return toks[-1], " ".join(toks[:-1]), "name_pos"
    if len(toks) == 2 and HANJA_NAME_POS_RE.match(toks[1]) and not _surname_name(toks[1]) \
            and HANJA_RE.match(toks[0]) and _surname_name(toks[0]) and not POSITION_TAIL_RE.search(toks[0]):
        # '劉容泰 委員長', '朴憲基 委員長' (16대): a Hanja name before a Hanja chair title
        return toks[1], toks[0], "name_pos"
    if len(toks) >= 2:
        return " ".join(toks[:-1]), toks[-1], "pos_name"
    m = _fused_match(toks[0])
    if m:
        return m.group("pos"), m.group("name"), "fused"
    return toks[0], None, "unsplit"


# A printed label has at most 4 tokens ('국무총리 후보자 김태호', '한국투자공사투자운용본부장 구안 옹'),
# at most 80 characters (special committee chairs run to 60+) and no sentence punctuation or quotes.
LABEL_BAD_RE = re.compile(r"[,?!…‥“”\"'‘’:;~「」『』：；，？！→←⇒□■※]|(?<![A-Z])(?<!Mr)(?<!Ms)(?<!Dr)(?<!Mrs)\.")
NAME_TOKEN_RE = re.compile(r"^[가-힣" + HAN + r"]{2,4}$")
LATIN_NAME_RE = re.compile(r"^[A-Z][A-Za-z.\-]+$")
# a 2-4 syllable token ending like a predicate is not a name ('좋습니다', '그렇죠'); names such as
# '김형오', '이재오', '김민서' must stay names, so the list is short
VERB_END_RE = re.compile(r"(?:다|요|죠|까)$")
# Evidence for a label that is not set off by a double space and not known from the document's
# own lexicon (steps 3 and 5 of _speaker_line): a position word ending ('위원장대리',
# '입법조사관', '문화체육관광방송통신위원장대리') and a name that starts with a surname and does not
# end in a case particle ('가족관계의 등록', '축산농장 출입', '위원님께서 요구하신' are not labels).
POSITION_TAIL_RE = re.compile(
    r"(?:장|관|대리|대행|위원|의원|委員|議員|총리|대통령|지사|교육감|후보자|증인|참고인|진술인|수석|검사|판사"
    r"|감사|이사|대표|長|官|代理"
    # Hanja titles of the 16대 minutes ('大法官候補者', '證人', '國務總理', '京畿道敎育監', '韓國銀行總裁')
    r"|代行|總理|大統領|知事|敎育監|教育監|候補者|證人|參考人|参考人|陳述人|首席|檢事|判事|監事|理事|代表"
    r"|總裁|大使|領事)$")
SURNAMES = frozenset(
    "김이박최정강조윤장임한오서신권황안송류유전홍고문양손배백허남심노하곽성차주우구민나진지엄채원천방공현함변염여"
    "추도소석선설마길연위표명기반왕금옥육인맹제모탁국어은편용예경봉사부가복태목형피두감음빈동온호범좌팽승간상시갈단"
    "견당화창종"
    "金李朴崔鄭姜趙尹張林任韓吳徐申權黃安宋柳劉全洪高文梁孫裵白許南沈盧河郭成車朱禹具閔羅陳池嚴蔡元千方孔玄咸卞廉呂"
    "秋都蘇石宣薛馬吉延魏表明奇潘王琴玉陸印孟諸牟卓鞠魚殷片龍芮慶奉史夫賈卜太睦邢皮杜甘陰賓董溫扈范左彭昇簡尙施葛段"
    "堅唐化昌宗"
    # Hanja surnames of the 16대 minutes' speakers missing above ('田溶鶴', '曺雄奎', '丁世均', '康奉均',
    # '辛基南', '邊在承', '兪成根', '偰松雄', '皇甫星', …)
    "田曺曹丁康辛邊兪偰房愼慎蔣楊周頓桂秦裴魯丘愈章皇司柴程異錢承粱余昔鮮獨邦景夏胡")
PARTICLE_END = frozenset("의에을를는와과께")
# frequent words that start with a surname syllable but are not names ('◯국토해양부장관 정부 입장은 …')
NOT_NAMES = frozenset("""우리 정부 이번 이제 이것 이거 이게 이런 이렇게 이미 이상 이후 이전 이유 이견 이와 이가 이어서
    지금 지난 지역 지원 지방 제가 제도 제안 제출 제일 국민 국가 국회 국정 한번 한편 한국 함께 전체 전부 전문 전국 정말
    정도 정리 정책 정상 조금 조정 조치 조사 주요 주민 최근 최종 최대 현재 현장 오늘 오전 오후 여기 여러분 방금 방안 방법
    하나 기본 기타 남북 우선 추가 사실 문제 안건 안전 장관 차관 공개 공정 민간 민생 신규 신청 연구 예산 예정 원래 원칙
    유지 인사 인정 임시 장기 진행 차원 채택 홍보 황당 모두 다음 계속 감사 마지막 선서 사회 위원 위원님 의원님
    한나라당 민주당 국민회의""".split())


def _parens_balanced(label):
    """A label never ends inside a parenthesis: '간사(黃祐呂‧薛  勳)인사' split at the spaced name
    gives '간사(黃祐呂‧薛', a heading fragment, not a label (a stray closing parenthesis is a
    printing typo, '아이엠픽쳐스(주))대표이사 최완')."""
    return label.count("(") <= label.count(")")


def _plausible_label(label):
    toks = label.split(" ")
    return (1 <= len(toks) <= 4 and 2 <= len(nows(label)) <= 80
            and not LABEL_BAD_RE.search(label) and _parens_balanced(label))


def _label_shape_ok(label):
    """A whole marker line taken as a label must look like one: a single word
    ('국토해양부장관', '진술인', '宋榮珍議員'), 'POS NAME' or 'NAME 위원' (not '처 우'); a spaced
    two-syllable name counts as one name ('委員長 李 協')."""
    toks = _glue_spaced_name(label.split(" "))
    if len(toks) == 1:
        return True
    return _name_like(toks[-1]) or toks[-1] in NAME_POS_TAIL or bool(LATIN_NAME_RE.match(toks[-1]))


def _name_like(tok):
    return bool(NAME_TOKEN_RE.match(tok)) and not VERB_END_RE.search(tok)


def _surname_name(tok):
    """A 2-4 syllable token that starts with a Korean surname, does not end in a particle and is
    not a frequent word (Hanja names often use CJK compatibility ideographs, '李' U+F9E1: NFKC
    before the lookup)."""
    return _name_like(tok) and unicodedata.normalize("NFKC", tok[0]) in SURNAMES \
        and tok[-1] not in PARTICLE_END and tok not in NOT_NAMES


def _name_attested(name, lex):
    return name in lex.names or _surname_name(name)


HANJA_WORD_RE = re.compile(r"^[" + HAN + r"]{2,}$")


def _has_name(label):
    """The label carries a surname-initial name ('위원장 김영선', '金鎭載委員'; not '首席專門委員',
    which FUSED_LABEL_RE would read as '首席專門' + '委員')."""
    return _surname_name(split_label(label)[1] or "")


def _name_piece(toks):
    """The trailing tokens of a label are (part of) a name: one surname-initial name, a
    one-syllable piece of a spaced name, a Latin name or 委員 / 議員."""
    if len(toks) != 1:
        return False
    x = toks[0]
    return bool(_surname_name(x) or (len(x) == 1 and NAME_OR_SYLLABLE_RE.match(x)) or LATIN_NAME_RE.match(x)
                or x in NAME_POS_TAIL or _fused_match(x) or (HANJA_WORD_RE.match(x) and _name_like(x)))


def _trim_label(label, lex):
    """A double-space label that is not a label of the document (count <= 1) but starts with one
    (count >= 1) or with a title label (_title_label), followed by words that are not a name:
    the longest such prefix. Also a known label (count >= 2) followed by one or two stray
    non-letter characters ('朴明煥 委員-', '반장 김성곤｣'; a stray syllable, '정해걸 위원걸', stays in
    the label because the XLSX reference drops it from the text). Returns the prefix or None."""
    toks = label.split(" ")
    for n in range(len(toks) - 1, 0, -1):
        p = " ".join(toks[:n])
        if (lex.get(p) or _title_label(p, lex)) and not _name_piece(toks[n:]) \
                and not any(LATIN_NAME_RE.match(x) for x in toks[n:]):   # '… 도널드 J. 트럼프'
            return p
    for k in lex.by_first.get(toks[0], ()):
        rem = label[len(k):]
        if lex.get(k) >= 2 and label.startswith(k) and 1 <= len(rem) <= 2 and " " not in rem \
                and not re.search(r"\w", rem):
            return k
    return None


def _pos_attested(pos, lex):
    """A position known from the document, ending like one, or written in Hanja only (16대 minutes
    print every title in Hanja, '國防部次官補', and a Hanja word before a Hanja name at the start of a
    marker line is a title)."""
    return pos in lex.positions or bool(POSITION_TAIL_RE.search(pos)) or bool(HANJA_WORD_RE.match(pos))


class _Lexicon:
    """Speaker labels of one document, counted from marker lines whose label is set off by a
    double space or tab and is plausible. Used to split labels printed with a single space,
    to trim sep labels that swallowed the first word of the speech, and to validate
    mid-line markers."""

    def __init__(self, counts=None):
        self.counts = dict(counts or {})
        self.by_first = {}      # first token -> labels, longest first
        self.positions, self.names = set(), set()   # parts of the document's labels
        for k in sorted(self.counts, key=lambda k: (-len(k), k)):
            self.by_first.setdefault(k.split(" ", 1)[0], []).append(k)
            pos_, name, _ = split_label(k)
            if name:
                self.names.add(name)
                if pos_:
                    self.positions.add(pos_)

    def get(self, label):
        return self.counts.get(label, 0)

    def prefix(self, rest_n, min_tokens=1, proper=False):
        """Longest known label that is a whole-token prefix of rest_n (a proper prefix when
        proper=True)."""
        for k in self.by_first.get(rest_n.split(" ", 1)[0], ()):
            if k.count(" ") + 1 < min_tokens:
                continue
            if (rest_n == k and not proper) or rest_n.startswith(k + " "):
                return k
        return None


IDEO_SPACE = "\u3000"


def _ideo_label(rest):
    """16대 layout: the label is set off by ONE ideographic space (U+3000), and position and
    name inside the label are justified with runs of ASCII spaces ('委員長  李允洙　…',
    '金文洙  委員　…', '委員長 李  協　…', '薛  勳委員　…'). The part before the first
    U+3000 is the label when it is plausible and label-shaped and every piece after a
    justification gap is a single name piece or 委員 / 議員 (so 'POS NAME  text　…', a label
    set off by a double space with a U+3000 later in the text, is left to _sep_label).
    Returns (label, first) or None."""
    i = rest.find(IDEO_SPACE)
    if i <= 0:
        return None
    head = rest[:i]
    if "\t" in head:
        return None
    label = norm(head)
    if not label or not _plausible_label(label) or not _ideo_label_ok(label):
        return None
    parts = [norm(x) for x in re.split(r"[ \xa0]{2,}", head) if norm(x)]
    if len(parts) > 1:
        t0 = parts[0].split(" ")
        if len(t0) >= 2 and _name_like(t0[-1]):
            return None          # the first piece is already 'POS NAME'
        for x in parts[1:]:
            g = _glue_spaced_name(x.split(" "))      # '韓國勞動敎育院長  李 銑\u3000'
            if len(g) != 1 or not (NAME_OR_SYLLABLE_RE.match(g[0]) or g[0] in NAME_POS_TAIL):
                return None
    first = rest[i + 1:]
    toks = label.split(" ")
    if len(toks) == 1 and not _name_glued(label):
        # '國民生活體育協議會長　嚴三鐸  국민생활체육협의회 회장 …': the position is set off by the
        # ideographic space and the name by a double space (or another ideographic space)
        m = re.match(r"^(?P<nm>[가-힣" + HAN + r"]{2,4})(?:[ \xa0]{2,}|　|\t|[ \xa0]*$)", first)
        if m and _surname_name(m.group("nm")):
            label, first = f"{label} {m.group('nm')}", first[m.end():]
    return label, first


def _one_word_label_ok(tok, lex=None):
    """A one-word label printed alone on a marker line is a speaker when it is a position
    ('진술인', '國土海洋部長官'), NAME+委員 ('宋榮珍議員') or a label the document sets off elsewhere;
    '의사진행의건', '5분자유발언', '위원장(金德圭)인사' are agenda headings."""
    return bool(_fused_match(tok) or POSITION_TAIL_RE.search(tok) or (lex is not None and lex.get(tok)))


LIST_SEP_CHARS = "‧·․ㆍ,，"


def _label_only_ok(label, lex=None):
    """Evidence that a whole marker line is a speaker label printed alone (its text follows on the
    next lines): a label of the document, a position word or NAME+委員, or a last token that is an
    attested name (or 委員 after one, or a Latin name). A list of names ('간사(鄭亨根‧張誠源) 인사')
    or a last word that is no name ('중국여객기 추락사고 보고', '細部事項 說明') is a heading."""
    if lex is not None and lex.get(label):
        return True
    if any(ch in label for ch in LIST_SEP_CHARS):
        return False
    toks = _glue_spaced_name(label.split(" "))
    if len(toks) == 1:
        return _one_word_label_ok(toks[0], lex)
    lx = lex or _Lexicon()
    if toks[-1] in NAME_POS_TAIL:
        return _name_attested(toks[-2], lx)
    return bool(_fused_match(toks[-1]) or LATIN_NAME_RE.match(toks[-1]) or _name_attested(toks[-1], lx))


def _ideo_label_ok(label):
    """Evidence that the part before an ideographic space is a label (the U+3000 split has no
    double-space support): one token that is NAME+委員 or ends like a position ('證人', '委員長',
    '大法官候補者'), or a last token that is a surname-initial name ('環境部長官 金明子', '委員長 李 協')
    or 委員 / 議員 after one ('金文洙 委員'). Appendix lines ('2003 예 산　199억 …', '현황　…')
    are not labels."""
    toks = _glue_spaced_name(label.split(" "))
    if len(toks) == 1:
        return _one_word_label_ok(toks[0])
    if toks[-1] in NAME_POS_TAIL or (len(toks) == 2 and POSITION_TAIL_RE.search(toks[-1])):
        return _surname_name(toks[-2])       # '金文洙 委員', '朴憲基 委員長'
    if _fused_match(toks[-1]):
        return True
    if LATIN_NAME_RE.match(toks[-1]) and POSITION_TAIL_RE.search(toks[0]):
        return True                          # '第一銀行長 Wilfred Y. Horie'
    if len(toks[-1]) == 3 and HANJA_WORD_RE.match(toks[-1]) and POSITION_TAIL_RE.search(" ".join(toks[:-1])):
        return True                          # a rare surname: '韓國冷藏株式會社社長 心基燮'
    return _surname_name(toks[-1])


def _sep_label(rest):
    """Label set off by 2+ spaces or a tab (or, 16대 layout, by one ideographic space, see
    _ideo_label), with the 'POS  NAME  text' join. Returns (label, first, how) or None."""
    r = _ideo_label(rest)
    if r is not None:
        return r[0], r[1], "sep"
    m = SEP_RE.search(rest)
    if not m:
        return None
    label, first = norm(rest[:m.start()]), rest[m.end():]
    if not label:
        return None
    how = "sep"
    last = label.rsplit(" ", 1)[-1]
    if (len(last) == 1 and unicodedata.normalize("NFKC", last) in SURNAMES) or \
            unicodedata.normalize("NFKC", last) in COMPOUND_SURNAMES:
        # '◯서울特別市長 高  建  “선서. …': a two-syllable name justified with the same double space
        # that sets the label off; the one-syllable given name belongs to the label
        m3 = re.match(r"^(?P<g>[가-힣" + HAN + r"])(?:[ 　\xa0]{2,}|\t)", first)
        if m3 and _surname_name(last + m3.group("g")) and norm(first[m3.end():]):
            label, first = f"{label} {m3.group('g')}", first[m3.end():]
    if " " not in label and not _name_glued(label):
        # '◯한국농촌공사감사실장  황승현  다른 부처라고…' : position, name and text all set off
        m2 = SEP_RE.search(first)
        if m2:
            nm = norm(first[:m2.start()])
            if _name_like(nm) and norm(first[m2.end():]):
                label, first, how = f"{label} {nm}", first[m2.end():], "sep_joined_pos_name"
        elif _surname_name(norm(first)) and " " not in norm(first):
            # '◯서울올림픽記念國民體育振興公團理事長  崔一鴻 ' : a justified label with the text on the
            # next line; the name alone after the gap belongs to the label
            label, first, how = f"{label} {norm(first)}", "", "sep_joined_pos_name"
    return label, first, how


def _collect_lexicon(lines):
    counts = {}
    for ln in lines:
        s = ln.lstrip(INDENT_CHARS)
        if not s or s[0] not in MARKERS:
            continue
        r = _sep_label(s[1:].lstrip(" 　"))
        if r and _plausible_label(r[0]):
            counts[r[0]] = counts.get(r[0], 0) + 1
    return _Lexicon(counts)


def _speaker_line(text, lex=None):
    """If `text` is a speaker line ('◯LABEL  TEXT'), return (label, first_text, how, marker).

    Order: (1) label set off by 2+ spaces / tab, or by one ideographic space in the 16대
    layout (_ideo_label), when plausible: trimmed to a more frequent known label that is its
    token prefix, or to a known / title label when it swallowed the first words of the speech
    (_trim_label); 'POS  NAME text' joins the name; (2) longest known document label that is a
    token prefix of the line; (2b) a known label glued to the text; (3) 'NAME 위원 text' when the
    name is attested; (4) the whole line when it is a plausible label; (5) 'POS NAME text' when
    both parts are attested (position known from the document, ending like a position or
    written in Hanja; name known from the document or starting with a surname, see
    _pos_attested / _name_attested); a Hanja two-syllable name may be printed spaced out in
    (3) and (5); (6) an implausible double-space split. Otherwise None: the marker line is text
    (a bullet in a quoted document, a self-introduction line, a redacted name '○○○ 씨가 …').

    A marker with no label at all ('◯`') returns label None, the stray characters as text and
    how 'label_missing'."""
    s = text.lstrip(INDENT_CHARS)
    if not s or s[0] not in MARKERS:
        return None
    marker = s[0]
    rest = s[1:].lstrip(" 　")
    lex = lex or _Lexicon()
    r = _sep_label(rest)
    if r and _plausible_label(r[0]):
        label, first, how = r
        c = lex.get(label)
        k = lex.prefix(label, proper=True)
        if k is not None and lex.get(k) >= 2 and lex.get(k) > c:
            extra = label[len(k):].strip()
            # never move a name out of a label without one ('국토해양부장관' + '정종환'); a label
            # that has its name already may shed a repeat ('金鎭載委員 金鎭載  위원입니다.')
            if _has_name(k) or not (_name_like(extra) or FUSED_LABEL_RE.match(extra) or extra in NAME_POS_TAIL):
                return k, extra + " " + first, "sep_trimmed_by_lexicon", marker
        if c <= 1:
            t = _trim_label(label, lex)
            if t is not None:
                # a label that swallowed the first word(s) of the speech ('國防部長官 趙成台 그것은
                # 장관책임입니다마는······  ', '朴明煥 委員-  따라서 …'): cut back to a known or title label
                return t, label[len(t):].strip() + " " + first, "sep_trimmed_by_lexicon", marker
        if " " not in label and not _name_glued(label):
            # '○環境管理公團專務理事  金德治 예, 청소하고 있습니다.': the name after the gap is followed
            # by a single space; it belongs to the label when the document knows 'POS NAME' or it is
            # a Hanja surname-initial name after a position word
            m = re.match(r"^(?P<nm>[가-힣" + HAN + r"]{2,4})[ \xa0\u3000](?=\S)", first)
            if m and (lex.get(f"{label} {m.group('nm')}") or
                      (HANJA_WORD_RE.match(m.group("nm")) and _surname_name(m.group("nm"))
                       and (label in lex.positions or POSITION_TAIL_RE.search(label)))):
                return f"{label} {m.group('nm')}", first[m.end():], "sep_joined_pos_name", marker
        return label, first, how, marker
    if rest[:1] in MARKERS or rest[:2] in ("OO", "ＯＯ"):
        # a redacted name at the start of a line ('○○○ 씨가 …'): text, not a speaker marker
        # (a redacted speaker label set off by a double space is taken by step 1 above)
        return None
    rest_n = norm(rest)
    if not re.search("[가-힣A-Za-z" + HAN + "]", rest_n):
        # a marker with no label at all ('◯`', '◯'): the turn boundary is kept, label unknown;
        # the stray characters stay in the text so that no character is lost
        return None, rest_n, "label_missing", marker
    k = lex.prefix(rest_n)
    if k is None:
        # a known label followed by a stray '.' or ':' ('◯전병헌 위원. 자, 잠깐만요.')
        m = re.match(r"^(.+?)[.:](?:\s+|$)", rest_n)
        if m and lex.get(m.group(1)) and " " in m.group(1):
            # the stray mark stays at the head of the text so no character is lost
            return m.group(1), rest_n[len(m.group(1)):].strip(), "lexicon_prefix_punct", marker
    if k is not None:
        nxt = rest_n[len(k):].strip()
        # a one-token known label followed by an attested name: prefer 'POS NAME' (step 5)
        nm = nxt.split(" ")[0] if nxt else ""
        if " " in k or _fused_match(k) or not nxt or not _name_like(nm) \
                or (" " in nxt and not _name_attested(nm, lex)):
            return k, nxt, "lexicon_prefix", marker
    # a known label with the text glued to it ('◯위원장 우윤근이어서 의사일정 …', '◯…팀長 崔德律105억 원을'),
    # checked before the whole-line label of step 4
    for k2 in lex.by_first.get(rest_n.split(" ", 1)[0], ()):
        if lex.get(k2) >= 2 and " " in k2 and _name_like(k2.split(" ")[-1]) and rest_n.startswith(k2) \
                and len(rest_n) > len(k2) and re.match("[가-힣0-9]", rest_n[len(k2)]):
            return k2, rest_n[len(k2):], "lexicon_prefix_fused", marker
    # (a Hanja two-syllable name may be printed spaced out, '○薛 勳委員 金周慶 증인 계십니까?')
    m = re.match(r"^(?P<name>[가-힣" + HAN + r"]{2,4}|[" + HAN + r"] [" + HAN + r"])\s?(?P<pos>위원|의원|委員|議員)"
                 r"(?:\s+(?P<t>.*))?$", rest_n)
    if m and _name_attested(m.group("name").replace(" ", ""), lex):
        return f"{m.group('name')} {m.group('pos')}", m.group("t") or "", "single_space_name_pos", marker
    if _plausible_label(rest_n) and len(rest_n.split(" ")) <= 3 and _label_shape_ok(rest_n):
        return rest_n, "", "label_only", marker
    m = re.match(r"^(?P<pos>\S+)\s(?P<name>[가-힣" + HAN + r"]{2,4}|[" + HAN + r"] [" + HAN + r"])\s(?P<t>.+)$", rest_n)
    if m and _plausible_label(f"{m.group('pos')} {m.group('name')}") and _name_like(m.group("name").replace(" ", "")) \
            and _pos_attested(m.group("pos"), lex) and _name_attested(m.group("name").replace(" ", ""), lex):
        return f"{m.group('pos')} {m.group('name')}", m.group("t"), "single_space_pos_name", marker
    if k is not None:        # the known one-token label, when 'POS NAME' is not attested
        return k, rest_n[len(k):].strip(), "lexicon_prefix", marker
    if r and len(nows(r[0])) >= 2 and not LABEL_BAD_RE.search(r[0]) and _parens_balanced(r[0]):
        return r[0], r[1], "sep_implausible", marker   # too long / too many tokens, counted
    return None   # a marker line that is not a speaker line (bullet, '◯ 통일교육원장입니다.')


MIDLINE_MARKER_RE = re.compile(r"(?<=[\s.?!…)」』”’])[◯○](?=\S)")
# any marker inside a line followed by a character (the conditions are checked in _split_midline)
MIDLINE_ANY_RE = re.compile(r"(?<=.)(?<![◯○])[◯○](?=[^\s◯○])")
STRAY_PREFIX_RE = re.compile(r"^[" + INDENT_CHARS + r"]*[^\s\w(（◯○]{1,3}$|^[" + INDENT_CHARS + r"]*\d$")
QUOTE_PAIRS = (("“", "”"), ("‘", "’"), ("「", "」"), ("『", "』"), ("｢", "｣"))


def _in_quote(pre):
    """True when `pre` leaves a quotation open (a quoted transcript: '… “○위원장 홍길동  …')."""
    return any(pre.count(a) > pre.count(b) for a, b in QUOTE_PAIRS) or pre.count('"') % 2 == 1


def _title_label(label, lex):
    """A label that matches the title patterns without being known from the document: 'POS NAME'
    with an attested position and name ('保健福祉部長官 金花中', '委員長 李 協'), 'NAME 委員' or
    NAME+委員 with a surname-initial name."""
    toks = _glue_spaced_name(label.split(" "))
    if len(toks) == 1:
        m = _fused_match(toks[0])
        return bool(m and _surname_name(m.group("name")))
    if toks[-1] in NAME_POS_TAIL:
        return len(toks) == 2 and _surname_name(toks[0])
    return _name_like(toks[-1]) and _name_attested(toks[-1], lex) and _pos_attested(" ".join(toks[:-1]), lex)


def _split_midline(line, lex):
    """Split a line at speaker markers inside it. A split needs (1) the marker after the end of a
    sentence ('한 1분만요. ◯위원장대리 이광재  예.'), after stray characters at the start of the line
    ('-◯保健福祉部長官 金花中  예.', a printing artefact) or glued to a label-only head
    ('○海洋水産部長官 柳三男○許泰烈 委員\u3000장관님, …'), and (2) the text after the marker to start
    with a known document label (2+ tokens or fused) followed by a space or end; after the end of
    a sentence a label that is not known from the document counts when it is set off by the
    printed separator and matches the title patterns (_title_label) and no quotation is open.
    Markers inside parentheses (interjections '(◯박지원 의원 의석에서 ― 뭐요?)') and redaction marks
    ('○○○') are never split. Returns list of lines."""
    out, start = [], 0
    for m in MIDLINE_ANY_RE.finditer(line):
        pre = line[start:m.start()]
        if not pre.strip():
            continue
        whole_pre = line[:m.start()]
        if whole_pre.count("(") > whole_pre.count(")") or whole_pre.count("（") > whole_pre.count("）"):
            continue
        after = line[m.end():]
        sent = bool(MIDLINE_MARKER_RE.match(line, m.start()))
        stray = start == 0 and bool(STRAY_PREFIX_RE.match(whole_pre))
        glued = False
        if not sent and not stray:
            head = _speaker_line(pre, lex)
            glued = bool(head and head[0] and not norm(head[1]) and lex.get(head[0]))
        if not (sent or stray or glued):
            continue
        k = lex.prefix(norm(after), min_tokens=1)
        ok = k is not None and (" " in k or bool(FUSED_LABEL_RE.match(k)))
        if not ok and sent and not _in_quote(whole_pre):
            r = _sep_label(after)
            ok = bool(r and _plausible_label(r[0]) and norm(r[1]) and _title_label(r[0], lex))
        if not ok:
            continue
        out.append(pre)
        start = m.start()
    out.append(line[start:])
    return out


def _table_holds_speech(it, lex):
    """A body table whose cell lines include a strong speaker line: a marker, a label of the
    document (count >= 2) that carries a name, set off by the printed separator, and text (a
    budget table with '○기본사업비  …' bullets is not speech)."""
    for c in it["cells"]:
        for ln in c["text"].split("\n"):
            if ln.lstrip(INDENT_CHARS)[:1] not in MARKERS:
                continue
            sl = _speaker_line(ln, lex)
            if sl and sl[0] and sl[2] in ("sep", "sep_joined_pos_name", "sep_trimmed_by_lexicon") \
                    and lex.get(sl[0]) >= 2 and norm(sl[1]) and _has_name(sl[0]):
                return True
    return False


def _time_line(s):
    """Parse a whole-line time marker. Returns dict or None."""
    m = TIME_LINE_RE.match(s)
    if not m:
        return None
    act = norm(m.group("act"))
    if act and not (act in TIME_ACTIONS or act in END_ACTIONS or act in ("경과", "開議", "停會", "續開")
                    or re.fullmatch(r"[가-힣]{1,8}", act)):
        return None
    return {"h": int(m.group("h")), "mi": int(m.group("mi") or 0), "mo": m.group("mo"),
            "d": m.group("d"), "action": act or None, "rollover": bool(ROLLOVER_RE.search(s))}


def _clock_line(s):
    """A whole-line clock marker that _time_line does not take ('(13시53분 화상통화 개시)'), in the
    _time_line dict shape; None for a time marker, an end marker or anything else."""
    if _time_line(s) is not None or END_NOTE_RE.match(s):
        return None
    m = CLOCK_LINE_RE.match(s)
    if not m:
        return None
    act = norm(m.group("act"))
    if nows(act) in END_ACTIONS_EXACT:
        return None
    return {"h": int(m.group("h")), "mi": int(m.group("mi") or 0), "mo": m.group("mo"),
            "d": m.group("d"), "action": act or None, "rollover": False}


def _end_line(s):
    """A meeting-end marker line: a time marker with an end action ('(18시41분 산회)') or a recess that
    never resumed ('(계속개의되지 않았음)'). s is normalized."""
    if not s.startswith("("):
        return False
    if END_NOTE_RE.match(s):
        return True
    tl = _time_line(s)
    return bool(tl and tl["action"] and nows(tl["action"]) in END_ACTIONS_EXACT)


DATE_LINE_RE = re.compile(r"^(?:일\s*시|日\s*時)\s+.*?\d{4}\s*[年년.]\s*\d{1,2}\s*[月월.]\s*\d{1,2}")
PLACE_LINE_RE = re.compile(r"^(?:장\s*소|場\s*所)\s")


def _next_text_matches(units, u, rx, window=3):
    """True when the next non-blank paragraph unit after units[u] matches rx."""
    for k2, kind2, raw2, _ in units[u + 1:u + 1 + window]:
        if kind2 != "para":
            return False
        t2 = norm(raw2)
        if t2:
            return bool(rx.match(t2))
    return False


CLOCK_TEXT_RE = re.compile(r"(午前|午後|오전|오후)?\s*(\d{1,2})\s*[時시]\s*(?:(\d{1,2})\s*[分분])?")


def _clock_text(s):
    """'午後 2時40分' -> '14:40' (the start time printed on a cover), else None."""
    m = CLOCK_TEXT_RE.search(s or "")
    if not m:
        return None
    h, mi = int(m.group(2)), int(m.group(3) or 0)
    if m.group(1) in ("午後", "오후") and h < 12:
        h += 12
    return f"{h:02d}:{mi:02d}" if h < 24 and mi < 60 else None


def _is_paren_line(s):
    return s.startswith("(") and s.endswith(")") and s.count("(") == s.count(")")


def _md_date(mo, d, ref):
    """Month/day printed in a time marker -> the valid date nearest to ref (the year before,
    of or after ref; '(2월29일 …)' only exists in leap years). None when no year fits."""
    if ref is None:
        return None
    best = None
    for y in (ref.year - 1, ref.year, ref.year + 1):
        try:
            x = _dt.date(y, int(mo), int(d))
        except ValueError:
            continue
        if best is None or abs((x - ref).days) < abs((best - ref).days):
            best = x
    return best


def _logical_items(paras):
    """Merge fragments of one paragraph, group table cells by outermost table, split zones.

    Returns (items, running_header) where each item is
      {'kind': 'para', 'text', 'idx': [para indices]}
      {'kind': 'table', 'table_id', 'cells': [{row, col, text}], 'idx': [...]}
      {'kind': 'aside', 'zone', 'text', 'idx': [...]}   (footnote, textbox, comment, ...)."""
    items, running = [], []
    by_para, by_table = {}, {}
    for i, p in enumerate(paras):
        z = p["zone"]
        if z in ("header", "footer"):
            running.append((i, p["text"]))
            continue
        if z != "body":
            items.append({"kind": "aside", "zone": z, "text": p["text"], "idx": [i]})
            continue
        if p["in_table"]:
            tid = p["outer_table_id"]
            it = by_table.get(tid)
            if it is None:
                it = {"kind": "table", "table_id": tid, "cells": [], "idx": []}
                by_table[tid] = it
                items.append(it)
            it["idx"].append(i)
            key = (p["table_id"], p["cell"])
            if it["cells"] and it["cells"][-1]["key"] == key:
                it["cells"][-1]["text"] += "\n" + p["text"]
            else:
                it["cells"].append({"key": key, "row": p["row"], "col": p["col"],
                                    "nested": p["table_id"] != tid, "text": p["text"]})
            continue
        key = (p["section"], p["para_id"])
        it = by_para.get(key)
        if it is not None:          # later fragment of a paragraph split by a table / text box
            it["text"] += p["text"]
            it["idx"].append(i)
            it["n_frags"] += 1
            continue
        it = {"kind": "para", "text": p["text"], "idx": [i], "n_frags": 1}
        by_para[key] = it
        items.append(it)
    for it in items:
        if it["kind"] == "table":
            for c in it["cells"]:
                c.pop("key", None)
            it["text"] = "\n".join(c["text"] for c in it["cells"])
    return items, running


# agency lines of a cover: '被調査機關' (국정조사), '被監査機關' / '報告機關' (국정감사, 報告機關 only on a
# cover that says 國政監査)
COVER_AGENCY_RE = re.compile(r"^(?P<k>被調査機關|피조사기관|被監査機關|피감사기관|報告機關|보고기관)\s*(?P<v>.+)$")
AUDIT_AGENCY_LABELS = frozenset({"被監査機關", "피감사기관", "報告機關", "보고기관"})
AGENCY_SEP_CHARS = "|․·,，"      # the separators parse_viewer splits the viewer's 피감사기관 field on


def split_agencies(value):
    """'中小企業廳․中小企業振興公團' -> ['中小企業廳', '中小企業振興公團']; separators inside parentheses
    do not split ('韓國銀行全北本部(光州全南․大田忠南 本部 포함)' stays one item)."""
    out, cur, depth = [], [], 0
    for ch in value or "":
        if ch in "(（":
            depth += 1
        elif ch in ")）" and depth:
            depth -= 1
        if ch in AGENCY_SEP_CHARS and depth == 0:
            out.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur))
    return [x.strip() for x in out if x.strip()]


def _parse_cover(lines):
    """Cover block (first table): session, session type, title, 號, date/time, place, agenda.

    lines: str, or (cell index, str) so that a title printed over several lines of one cell
    ('國際競技大會開催및誘致' / '支援特別委員會會議錄' / '(法律案審査小委員會)') is joined and the
    parenthesised line after it is kept as the subcommittee."""
    meet = {"session_no": None, "session_type": None, "doc_title": None, "committee_raw": None,
            "subcommittee": None, "doc_no": None, "date": None, "time_text": None, "place": None,
            "cover_notes": []}
    agenda, block = [], None
    note_cell = []            # cell index of each cover_notes entry
    title_cell = None
    expanded = []
    for li, entry in enumerate(lines):
        cell, raw = entry if isinstance(entry, tuple) else (li, entry)
        jm = COVER_JOINED_RE.match(norm(raw.replace("\t", " ")))
        if jm and (jm.group("sess") or jm.group("no")):
            expanded.extend((cell, x) for x in (jm.group("sess"), jm.group("title"), jm.group("no")) if x)
            meet["cover_lines_split"] = meet.get("cover_lines_split", 0) + 1
        else:
            expanded.append((cell, raw))
    for li, (cell, raw) in enumerate(expanded):
        s = norm(raw.replace("\t", " \t "))
        if not s:
            continue
        s1 = norm(raw)
        if re.fullmatch(r"\(\s*[^()]+\s*\)", s1):
            inner = unicodedata.normalize("NFKC", nows(s1.strip("()")))   # '臨' may be U+F9F6
            if inner in ("임시회의록", "臨時會議錄"):
                # '(임 시 회 의 록)': the provisional edition of the minutes, not a subcommittee
                meet["provisional"] = True
                continue
            if meet["session_type"] is None and re.match(r"^(?:臨時|定期|임시|정기|特別|특별)(?:會|회)", inner):
                meet["session_type"] = s1.strip("() ")
                continue
        if title_cell is not None and cell == title_cell and meet["subcommittee"] is None and \
                re.fullmatch(r"\(\s*[^()]+\s*\)", s1):
            meet["subcommittee"] = s1.strip("() ")
            continue
        if meet["session_no"] is None:
            m = SESSION_RE.match(s1)
            if m and ("회" in s1[:12] or "回" in s1[:12]) and len(s1) < 40:
                meet["session_no"] = int(m.group(1))
                if m.group(2):
                    meet["session_type"] = m.group(2)
                continue
        if meet["session_no"] is not None and meet["session_type"] is None and \
                re.fullmatch(r"\(\s*\S+?\s*\)", s1):
            meet["session_type"] = s1.strip("() ")
            continue
        if meet["doc_title"] is None and re.search(r"(會議錄|회의록|調査錄|조사록)\s*$", s1):
            # title lines printed above it in the same cell (kept as notes so far) belong to it
            pre = [i for i, c in enumerate(note_cell) if c == cell]
            prefix = "".join(meet["cover_notes"][i] for i in pre)
            for i in reversed(pre):
                del meet["cover_notes"][i]
                del note_cell[i]
            meet["doc_title"] = prefix + s1
            meet["title_lines_joined"] = len(pre)
            meet["committee_raw"] = re.sub(r"\s*(會議錄|회의록|調査錄|조사록)\s*$", "", prefix + s1).strip() or None
            title_cell = cell
            continue
        m = DOCNO_RE.match(nows(s1)) if len(s1) < 20 else None
        if m and meet["doc_no"] is None:
            meet["doc_no"] = int(m.group(1))
            continue
        if nows(s1) in ("國會事務處", "국회사무처"):
            continue
        am = COVER_AGENCY_RE.match(s1)
        if am and block is None and (am.group("k") != "報告機關"
                                     or any("監査" in x or "감사" in x for x in meet["cover_notes"])):
            val = am.group("v").strip()
            if am.group("k") in AUDIT_AGENCY_LABELS:
                # 국정감사 covers ('被監査機關  中小企業廳․中小企業振興公團', '報告機關  韓國銀行釜山本部
                # (蔚山․慶南 本部 포함)'): the audited bodies, split at the printed separators outside
                # parentheses (the printed value is kept in audited_agencies_raw)
                meet.setdefault("audited_agencies", []).extend(split_agencies(val))
                meet.setdefault("audited_agencies_raw", []).append(val)
                meet["audited_agencies_label"] = am.group("k")
            else:
                # 국정조사 covers ('被調査機關  釜山地方國稅廳'): the investigated body, not a note
                meet.setdefault("investigated_agencies", []).append(val)
            continue
        m = AGENDA_BLOCK_RE.match(nows(s1))
        if m and len(nows(s1)) <= 12:
            block = nows(s1)
            continue
        if block is None:
            dm = YMD_RE.search(s1)
            if dm and meet["date"] is None:
                try:
                    meet["date"] = _ymd(dm).isoformat()
                except ValueError:
                    pass
                meet["time_text"] = norm(s1[dm.end():]) or None
                continue
            pm = re.match(r"^(?:장\s*소|場\s*所)\s*(.*)$", s1)
            if pm:
                meet["place"] = pm.group(1).strip() or None
                continue
            im = re.match(r"^(?:일\s*시|日\s*時)\s*(.*)$", s1)
            if im:
                meet["time_text"] = im.group(1).strip() or None
                continue
            meet["cover_notes"].append(s1)
            note_cell.append(cell)
            continue
        # agenda item line inside a block; trailing '\tN' is the page number
        parts = raw.rsplit("\t", 1)
        page = None
        txt = raw
        if len(parts) == 2 and re.fullmatch(r"\s*\d{1,4}\s*", parts[1]):
            txt, page = parts[0], int(parts[1])
        agenda.append({"section": block, "text": norm(txt.replace("\t", " ")), "page": page})
    return meet, agenda


def _ymd(dm):
    """Date from a YMD_RE match; 단기 years (4200+, pre-1962 minutes) are converted to CE."""
    y = int(dm.group(1))
    if y >= 4200:
        y -= 2333
    return _dt.date(y, int(dm.group(2)), int(dm.group(3)))


def _agenda_key(s):
    """Normalization for matching body headings to cover agenda items."""
    s = nows(s)
    s = re.sub(r"^(?:\d{1,3}[.．]|[oOㅇ◦•◯○])", "", s)
    return s.replace("｢", "「").replace("｣", "」").replace("ㆍ", "·").replace("․", "·").replace("‧", "·")


def _table_lines(cells):
    """Lines of a table in reading order. Cells in one row that hold the same number (2+) of
    lines are read across, line by line ('이사장' | '정재정' over '사무총장' | '신연성' ->
    이사장, 정재정, 사무총장, 신연성), as the printed page is read; other rows cell by cell.
    Returns (lines, number of interleaved rows)."""
    rows, order = {}, []
    for c in cells:
        key = ("n", id(c)) if c.get("nested") or c.get("row") is None else ("r", c["row"])
        if key not in rows:
            rows[key] = []
            order.append(key)
        rows[key].append([norm(x) for x in c["text"].split("\n") if norm(x)])
    out, n_inter = [], 0
    for key in order:
        cl = rows[key]
        lens = {len(x) for x in cl}
        if len(cl) >= 2 and len(lens) == 1 and min(lens) >= 2:
            n_inter += 1
            for i in range(len(cl[0])):
                out.extend(x[i] for x in cl)
        else:
            for x in cl:
                out.extend(x)
    return out, n_inter


NAME_OR_SYLLABLE_RE = re.compile(r"^[가-힣" + HAN + r"]{1,4}$")


def _name_list_line(s):
    """A line of two or more whitespace-separated names and nothing else (a name printed
    spaced out, '박  진', gives one-syllable tokens: at least 80% of the tokens are 2-4 syllables)."""
    toks = s.split()
    if len(toks) < 2 or not all(NAME_OR_SYLLABLE_RE.match(x) and not VERB_END_RE.search(x) for x in toks):
        return False
    return sum(1 for x in toks if len(x) >= 2) >= 0.8 * len(toks)


def _finalize_turn(t):
    """Stage / oath flags per line and the CONTRACT text fields."""
    after_oath = False
    for ln in t["lines"]:
        st = ln["text"]
        if ln.get("table"):
            # cell text of a table inside the turn: never a stage direction; after a sworn
            # oath the signatories' table (positions and names) is an oath signature block
            ln["stage_kind"], ln["is_stage"] = None, False
            ln["is_oath_signature"] = bool(after_oath and len(st) <= 40
                                           and not re.search(r"[다요까죠]\s*[.?!]?\s*$", st))
            if not ln["is_oath_signature"]:
                after_oath = False
            continue
        paren = _is_paren_line(st)
        ln["stage_kind"] = None
        im = INTERJ_RE.match(st) if paren else None
        if im:
            who = im.group("who").strip()
            wm = INTERJ_WHERE_RE.search(who)
            ln["stage_kind"] = "interjection"
            ln["interjection"] = {"who": who[:wm.start()].strip() if wm else who,
                                  "where": wm.group(1) if wm else None,
                                  "text": im.group("txt").strip()}
        elif paren:
            ln["stage_kind"] = next((k for k, rx in STAGE_KINDS if rx.search(st)), "other")
        ln["is_stage"] = ln["stage_kind"] is not None
        # the signatories after a sworn oath: short lines (date, position, name) and lines that
        # are nothing but names ('강기정 강길부 강명순 …', the members at an opening ceremony)
        ln["is_oath_signature"] = bool(after_oath and not paren and (len(st) <= 40 or _name_list_line(st))
                                       and not re.search(r"[다요까죠]\s*[.?!]?\s*$", st))
        if OATH_RE.search(st):
            after_oath = True
        elif not ln["is_oath_signature"]:
            after_oath = False
    lines = [ln for ln in t["lines"] if ln["text"]]
    t["text_raw"] = "\n".join(ln["text"] for ln in lines)
    t["text"] = "\n".join(ln["text"] for ln in lines if not ln["is_stage"] and not ln["is_oath_signature"])
    t["has_stage"] = any(ln["is_stage"] for ln in lines)
    t["stage_kinds"] = sorted({ln["stage_kind"] for ln in lines if ln["stage_kind"]})
    t["stage_texts"] = [ln["text"] for ln in lines if ln["is_stage"]]
    t["n_oath_signature"] = sum(1 for ln in lines if ln["is_oath_signature"])
    t["interjections"] = [ln["interjection"] for ln in lines if ln.get("interjection")]
    t["inline_stage_parens"] = [p for ln in lines if not ln["is_stage"]
                                for p in PAREN_RE.findall(ln["text"]) if _STAGE_LEX.search(p)]
    t["n_lines"] = len(lines)
    t["n_table_lines"] = sum(1 for ln in lines if ln.get("table"))
    t["n_chars"] = len(t["text_raw"])
    return t


def _footer_names(lines):
    """Name tokens from attendance-style lines: split on 2+ spaces or tabs, re-joining names
    that the layout spaced out ('박  진' -> '박진')."""
    out = []
    for ln in lines:
        toks = [x for x in re.split(r"[ 　\xa0]{2,}|\t", ln.strip()) if x.strip()]
        merged, i = [], 0
        while i < len(toks):
            a = toks[i].strip()
            if len(a) == 1 and i + 1 < len(toks) and len(toks[i + 1].strip()) == 1:
                merged.append(a + toks[i + 1].strip())
                i += 2
                continue
            merged.extend(a.split(" ") if re.fullmatch(r"(?:[가-힣]{2,4} )+[가-힣]{2,4}", a) else [a])
            i += 1
        out.extend(merged)
    return out


FOOTER_NAME_TITLES = re.compile(r"출석|청가|출장|결석|재석|참석|위원\s*아닌|의원\s*아닌|찬성|반대|기권|투표|出席|請暇")


def _parse_footer(items):
    sections = []
    cur = {"title": None, "lines": [], "tables": []}
    sections.append(cur)
    for it in items:
        if it["kind"] == "table":
            cur["tables"].append({"table_id": it["table_id"],
                                  "cells": [{"row": c["row"], "col": c["col"], "text": c["text"]}
                                            for c in it["cells"]]})
            continue
        s = it["text"].strip()
        if not s:
            continue
        head = (s[0] in MARKERS and not SEP_RE.search(s.lstrip(MARKERS).strip())) or s.startswith("【") \
            or bool(APPENDIX_HEAD_NOWS_RE.match(nows(s)))
        if head:
            cur = {"title": norm(s), "lines": [], "tables": []}
            sections.append(cur)
        else:
            cur["lines"].append(it["text"].rstrip())
    for sec in sections:
        if sec["title"] and FOOTER_NAME_TITLES.search(sec["title"]):
            sec["names"] = _footer_names(sec["lines"])
    return [s for s in sections if s["title"] or s["lines"] or s["tables"]]


# ----------------------------------------------------------------------------- sittings after an end
# After a meeting-end marker the body is closed. A new sitting starts only on evidence of one:
#   after_end_time (opening action)  '(14시40분 감사계속)', '(16시37분 개의)': always
#   after_end_open                    '(개의)', '(계속개의)' (an opening marker without a clock): always
#   after_end_time (other action)     '(14시37분)', '(15시24분 간담회개시)'           } only when the first
#   after_end_clock                   '(13시53분 화상통화 개시)'                      } speaker line after it
#   sub_cover                         a second cover table (회의록 title, or 日時 + date) } carries an attested
#   sub_cover_line                    '일  시  2017년11월8일 …' + '장  소 …' lines       } label (_label_attested)
#   sitting_head                      '【오전회의 내용】'                                }
# Everything else printed after the end marker (written questions and answers, 제안설명서, review
# reports, budget tables, audit plans, attendee lists) is appendix: never a turn and never turn text.

COVER_TITLE_RE = re.compile(r"會議錄|회의록|調査錄|조사록")
DATE_HEAD_RE = re.compile(r"^(?:日\s*時|일\s*시)\s*(?P<v>.*)$")


def _sub_cover(it):
    """A body table that starts another document or sitting: a printed date and a 회의록 title
    ('第251回國會 (臨時會) 敎育委員會會議錄 第 1 號 … 日時 2004年12月14日'), or a 日時 / 일시 line whose
    value is a date, on the line or on the next line ('캐나다총리(스티븐 하퍼) 연설' / '日  時  2009年12月7日 …',
    '日  時' / '2001년3월2일 오후 2시'). A table cell that merely says '일시 철수' or an audit schedule
    headed '일 시 | 대 상 기 관 | …' is no cover. Returns the _parse_cover meeting dict or None."""
    lines_ = [(ci, ln) for ci, c in enumerate(it["cells"]) for ln in c["text"].split("\n")]
    if len(lines_) > 60:
        return None
    txt = "\n".join(ln for _, ln in lines_)
    if not YMD_RE.search(txt):
        return None
    ok = bool(COVER_TITLE_RE.search(txt))
    if not ok:
        nb = [norm(ln) for _, ln in lines_ if norm(ln)]
        for i, s in enumerate(nb):
            m = DATE_HEAD_RE.match(s)
            if m and (YMD_RE.match(m.group("v")) or (not m.group("v") and i + 1 < len(nb) and YMD_RE.match(nb[i + 1]))):
                ok = True
                break
    if not ok:
        return None
    m_, _ = _parse_cover(lines_)
    return m_ if m_.get("date") else None


def _label_attested(sl, lex_pre):
    """The speaker line after a candidate sitting start carries an attested label: a label of the
    document before the end marker (lex_pre), or a label set off by the printed separator (or known
    from the document) with text on the line that matches the title patterns (_title_label: an
    attested position and a surname-initial name, 'NAME 위원', NAME+委員). Budget items ('2001년도
    주요업무 추진현황'), written-answer headers printed alone ('중앙인사위원회 위원장 金光雄') and other
    appendix lines are not attested."""
    label, first, how = sl[0], sl[1], sl[2]
    if not label or not _label_shape_ok(label):
        return False
    known = lex_pre.get(label) >= 1
    if how in STRONG_LABEL_HOW and norm(first):
        return known or _title_label(label, lex_pre)
    return known and (bool(norm(first)) or how == "label_only")


def _first_speaker_after(units, u, lex, window=200):
    """The first speaker line after units[u] and before the next meeting-end marker (marker agenda
    headings that fail _label_only_ok are skipped), or None."""
    for v in range(u + 1, min(len(units), u + 1 + window)):
        _, kind, raw, _ = units[v]
        if kind != "para":
            continue
        s = norm(raw)
        if not s:
            continue
        if s.startswith("("):
            if _end_line(s):
                return None
            continue
        sl = _speaker_line(raw, lex)
        if not sl or sl[0] is None:
            continue
        if sl[2] == "label_only" and not _label_only_ok(sl[0], lex):
            continue
        return sl
    return None


def _sitting_starts(units, items, lex):
    """Sitting starts after meeting-end markers (see the table above), in the body units of
    parse_minutes. An end marker closes the body; while it is closed, candidate starts are tested and
    the first accepted one opens a new sitting. Returns (starts, cut, rejected):
      starts    {unit index: how} of the accepted starts;
      cut       the unit index of the end marker that closes the body for good (no accepted start
                after it: the rest of the body is appendix), or None;
      rejected  {how: n} candidates that failed the attested-speaker test."""
    starts, rejected = {}, {}
    closed, end_u, lex_pre = False, None, None
    for u, (k, kind, raw, _) in enumerate(units):
        if not closed:
            if kind == "para":
                s = norm(raw)
                if s and _end_line(s):
                    closed, end_u, lex_pre = True, u, None
            continue
        how, need = None, True
        if kind == "table":
            if _sub_cover(items[k]) is not None:
                how = "sub_cover"
        elif kind == "para":
            s = norm(raw)
            if not s:
                continue
            if SITTING_HEAD_RE.match(nows(s)):
                how = "sitting_head"
            elif DATE_LINE_RE.match(s) and _next_text_matches(units, u, PLACE_LINE_RE):
                how = "sub_cover_line"
            elif s.startswith("(") and not _end_line(s):
                tl = _time_line(s)
                if OPEN_NOTE_RE.match(nows(s)):
                    how, need = "after_end_open", False
                elif tl is not None:
                    how = "after_end_time"
                    need = not (tl["action"] and nows(tl["action"]) in OPEN_ACTIONS_EXACT)
                elif _clock_line(s) is not None:
                    how = "after_end_clock"
        if how is None:
            continue
        if need:
            if lex_pre is None:
                lex_pre = _collect_lexicon([units[v][2] for v in range(end_u) if units[v][1] == "para"])
            sl = _first_speaker_after(units, u, lex)
            if sl is None or not _label_attested(sl, lex_pre):
                rejected[how] = rejected.get(how, 0) + 1
                continue
        starts[u] = how
        closed = False
    return starts, (end_u if closed else None), rejected


def parse_hwp(data: bytes, conf_num=None, **opts) -> dict:
    """Parse one 국회 minutes HWP file.

    Returns dict(status, meeting, agenda_header, agenda, turns, events, footer, sittings, stats).
    status: ok | ok_no_turns | not_ole | hwp3 | hwpx_unsupported | not_hwp5 | password |
    distribution | stream_error | no_body. opts are passed to parse_minutes."""
    paras, info = extract_paragraphs(data, with_status=True)
    if info["status"] != "ok":
        res = {"status": info["status"], "meeting": {}, "agenda_header": [], "agenda": [],
               "turns": [], "events": [], "footer": [], "sittings": [], "stats": {}}
    else:
        res = parse_minutes(paras, conf_num=conf_num, **opts)
    res["reader"] = {k: info[k] for k in ("status", "version", "flags", "errors", "reader_stats")}
    return res


def parse_minutes(paras, conf_num=None, later_sitting_date="inherit", reattach_continuations=True) -> dict:
    """Minutes grammar over extract_paragraphs() output (see parse_hwp).

    Options (defaults keep the documented behaviour; the researcher rules on them):
      later_sitting_date      'inherit': a later sitting with no printed date keeps the date in
                              force (speech_date_how 'inherited'); 'null': its speech_date is None.
      reattach_continuations  True: indented lines after an agenda heading / time marker with no
                              new marker continue the turn the heading closed; False: orphan events."""
    if later_sitting_date not in ("inherit", "null"):
        raise ValueError(later_sitting_date)
    res = {"status": None, "meeting": {}, "agenda_header": [], "agenda": [], "turns": [],
           "events": [], "footer": [], "sittings": [], "stats": {}}
    items, running = _logical_items(paras)
    assign = {}   # item position -> category (accounting)

    # ---- cover: the leading table (and nothing else) when the document starts with one
    pos = 0
    while pos < len(items) and items[pos]["kind"] == "para" and not items[pos]["text"].strip():
        assign[pos] = "blank"
        pos += 1
    cover_lines = []
    if pos < len(items) and items[pos]["kind"] == "table":
        cover_lines = [(ci, ln) for ci, c in enumerate(items[pos]["cells"]) for ln in c["text"].split("\n")]
        assign[pos] = "cover"
        pos += 1
    meeting, agenda_header = _parse_cover(cover_lines)
    meeting["cover_source"] = "table" if cover_lines else None
    rh = [norm(t.replace("\t", " ")) for _, t in running if norm(t)]
    meeting["running_header"] = sorted(set(rh))
    for r in rh:
        m = RUNHEAD_RE.search(r)
        if m:
            meeting["sitting"] = f"제{int(m.group(3))}차"
            if meeting.get("session_no") is None:
                meeting["session_no"] = int(m.group(1))
            if meeting.get("date") is None:
                dm = YMD_RE.search(r)
                if dm:
                    try:
                        meeting["date"] = _ymd(dm).isoformat()
                    except ValueError:
                        pass
            break
    res["agenda_header"] = agenda_header
    cover_keys = {_agenda_key(a["text"]) for a in agenda_header if a["text"]}

    # ---- body end. The appendix (attendance, staff and attendee lists, 【보고사항】) starts at
    # the first appendix heading after the last real speaker line (label set off by a double
    # space, text on the line, label shaped like one); when a meeting-end time marker
    # ('(18시41분 산회)') lies between that speaker line and the heading, right after the marker.
    # A file can hold several sittings with their own end markers and attendance lists: only the
    # last ones count.
    body_start = pos
    # a paragraph may hold several lines separated by line-break controls (char 10); a new
    # speaker can start after one ('…주요?\n◯금융위원장 전광우  이 부분은…')
    nl_lines = {k: items[k]["text"].split("\n") for k in range(body_start, len(items))
                if items[k]["kind"] == "para"}
    ends, heads, last_spk = [], [], None
    for k in range(body_start, len(items)):
        if k not in nl_lines:
            continue
        for ln in nl_lines[k]:
            s = ln.strip()
            if not s:
                continue
            if SITTING_HEAD_RE.match(nows(s)):
                continue
            if APPENDIX_HEAD_NOWS_RE.match(nows(s)):
                heads.append(k)
                continue
            tl = _time_line(s) if s.startswith("(") else None
            if (tl and tl["action"] and nows(tl["action"]) in END_ACTIONS_EXACT) or END_NOTE_RE.match(s):
                ends.append(k)
                continue
            sl = _speaker_line(ln)
            if sl and sl[2] == "sep" and norm(sl[1]) and _label_shape_ok(sl[0]):
                last_spk = k
    heads_after = [k for k in heads if last_spk is None or k > last_spk]
    ends_after = [k for k in ends if last_spk is None or k > last_spk]
    appendix_start, appendix_how = len(items), "none"
    if heads_after:
        appendix_start, appendix_how = heads_after[0], "appendix_heading"
        if ends_after and ends_after[-1] < heads_after[0]:
            appendix_start, appendix_how = ends_after[-1] + 1, "after_end_marker"
    elif ends_after:
        appendix_start, appendix_how = ends_after[-1] + 1, "after_end_marker"
    res["stats"]["n_end_markers"] = len(ends)
    res["stats"]["end_markers_before_last_turn"] = len(ends) - len(ends_after)
    res["stats"]["appendix_heads_before_last_turn"] = len(heads) - len(heads_after)
    res["stats"]["appendix_how"] = appendix_how

    # ---- walk the body
    # Date and clock: st['date'] is the speech date in force, st['clock'] the minutes after
    # midnight of the last time marker (None at the start of a sitting). A marker earlier than
    # the clock is a time regression; from 20:00 or later to 06:00 or earlier with no printed
    # date it is an unprinted midnight ('(23시38분)' … '(00시01분 감사종료)') and advances the
    # date. '(24시 산회)' is 00:00 of the next day (hhmm_printed keeps '24:00').
    # Sittings: a meeting-end marker ('(18시41분 산회)', '(계속개의되지 않았음)') ends the running turn and
    # closes the body. A new sitting starts only at an accepted start (_sitting_starts): an opening
    # time marker ('(14시40분 감사계속)'), or a sub-cover, sub-cover lines, a sitting heading
    # ('【오전회의 내용】') or another time / clock marker followed by a speaker line with an attested label.
    # While the body is closed every unit is an event (kind 'appendix_inner', 'table', aside), never a
    # turn or turn text; when no start follows the last end marker the appendix begins right after it
    # (appendix_how 'after_end_marker'). A sitting heading printed with no end marker before it always
    # starts a sitting. A later sitting takes the date printed at its start (sub-cover, or a time
    # marker with month/day); without one it inherits the date in force
    # (later_sitting_date='inherit', speech_date_how 'inherited') or gets none
    # (later_sitting_date='null'). Turn flags: after_end_marker = a meeting-end marker was printed
    # earlier in the turn's own sitting (reset at every new sitting, researcher decision 2026-09-26;
    # since the body is closed after an end marker until a new sitting starts, no HWP turn carries it);
    # after_final_end_marker = the turn starts after the document's last meeting-end marker (the turns
    # of a later sitting that prints no end marker of its own).
    d0 = _dt.date.fromisoformat(meeting["date"]) if meeting.get("date") else None
    st = {"date": d0, "ref_date": d0, "date_how": "cover" if d0 else None,
          "hhmm": None, "time": None, "clock": None, "regress": False,
          "agenda_ordinal": None, "agenda_text": None,
          "last": None, "ended": False, "sit_ended": False, "sitting": 1}
    turns, events, agenda = res["turns"], res["events"], res["agenda"]
    sittings = res["sittings"] = [{"sitting_seq": 1, "how": "first", "start_turn_seq": 1,
                                   "date": meeting.get("date"), "date_how": st["date_how"],
                                   "start_text": None}]
    cur = None
    reattach = None       # (turn, events index) closed by an agenda heading, may continue
    counters = {}

    def bump(k, n=1):
        counters[k] = counters.get(k, 0) + n

    def close():
        nonlocal cur
        if cur is not None:
            turns.append(cur)
        cur = None

    def set_date(d, how):
        st["date"], st["date_how"] = d, how
        if d is not None:
            st["ref_date"] = d

    def new_sitting(how, text, date=None, hhmm=None):
        nonlocal reattach
        st["sitting"] += 1
        # after_end_marker means 'after an end marker of the turn's own sitting' (researcher decision
        # 2026-09-26): a new sitting resets it (after_final_end_marker keeps the document-level view)
        if st["ended"]:
            bump("after_end_reset_at_new_sitting")
        st["ended"] = False
        st["prev_clock"], st["check_boundary_regress"] = st["clock"], True
        st["sit_ended"], st["clock"], st["regress"] = False, None, False
        # the clock of the previous sitting does not carry over; a sub-cover may print the start
        st["hhmm"], st["time"] = hhmm, None
        if hhmm is not None:
            st["clock"] = int(hhmm[:2]) * 60 + int(hhmm[3:])
        reattach = None
        if date is not None:
            set_date(date, "sub_cover")
        elif later_sitting_date == "null":
            set_date(None, None)
        else:
            st["date_how"] = "inherited" if st["date"] is not None else None
        sittings.append({"sitting_seq": st["sitting"], "how": how,
                         "start_turn_seq": len(turns) + 1 + (1 if cur is not None else 0),
                         "date": st["date"].isoformat() if st["date"] else None,
                         "date_how": st["date_how"], "start_text": text[:200]})
        bump("sitting_boundary_" + how)

    # ---- document label lexicon, then body units (paragraph lines; mid-line markers split)
    def body_units(end):
        """Lexicon and units of the body items body_start..end-1, with the unit counters."""
        lex_ = _collect_lexicon([ln for k in range(body_start, end) if k in nl_lines for ln in nl_lines[k]])
        units_, cnt = [], {}   # units: (item index, kind, text, first line of its paragraph)
        for k in range(body_start, end):
            it = items[k]
            if it["kind"] == "table" and _table_holds_speech(it, lex_):
                # 16대 files where the body text runs on inside a table frame ('○金敬天委員  …' before
                # the table, '○委員長 李揆澤\u3000아주 진지한 토론을 했습니다.' in its cell): the cell lines are
                # read as body lines (never as agenda headings), so the turns inside are not merged into
                # the running turn as table text
                cnt["table_read_as_body"] = cnt.get("table_read_as_body", 0) + 1
                for c in it["cells"]:
                    for ln in c["text"].split("\n"):
                        for part in _split_midline(ln, lex_):
                            units_.append((k, "para", part, False))
                continue
            if it["kind"] != "para":
                units_.append((k, it["kind"], it["text"], True))
                continue
            lines = nl_lines[k]
            if len(lines) > 1:
                cnt["para_line_breaks"] = cnt.get("para_line_breaks", 0) + len(lines) - 1
            for li, ln in enumerate(lines):
                parts = _split_midline(ln, lex_)
                if len(parts) > 1:
                    cnt["midline_marker_split"] = cnt.get("midline_marker_split", 0) + len(parts) - 1
                for pi, part in enumerate(parts):
                    units_.append((k, "para", part, li == 0 and pi == 0))
        return lex_, units_, cnt

    lex, units, ucnt = body_units(appendix_start)
    # sitting starts after end markers; material after the last end marker with no accepted start
    # after it is appendix: the body ends at that marker (lexicon and units are rebuilt without it)
    starts, cut, rejected = _sitting_starts(units, items, lex)
    if cut is not None and units[cut][0] + 1 < appendix_start:
        k_cut = units[cut][0] + 1
        res["stats"]["appendix_moved"] = {
            "from_how": appendix_how, "items": appendix_start - k_cut,
            "chars": sum(len(nows(items[k]["text"])) for k in range(k_cut, appendix_start)),
            "marker": norm(units[cut][2])}
        appendix_start, appendix_how = k_cut, "after_end_marker"
        res["stats"]["appendix_how"] = appendix_how
        lex, units, ucnt = body_units(appendix_start)
        # (the rejected candidates are counted from the first pass: the rebuilt body ends at the marker)
        starts, cut, _ = _sitting_starts(units, items, lex)
    for key, n in ucnt.items():
        bump(key, n)
    for key, n in rejected.items():
        bump("sitting_start_rejected_" + key, n)
    uassign = {}

    def strong_speaker(sl):
        """A speaker line that may end an in-body appendix block: a label set off by a double
        space or known from the document, shaped like a label, with text on the line. Weak
        single-space labels (steps 3-5) and label-only lines never end the block (vote-name
        blocks '◯가족관계의 등록 등에 관한 법률 …', written answers '○위원님께서 요구하신 …')."""
        if not sl or sl[0] is None or not norm(sl[1]):
            return False
        if sl[2] not in ("sep", "sep_trimmed_by_lexicon", "sep_joined_pos_name", "lexicon_prefix",
                         "lexicon_prefix_punct", "lexicon_prefix_fused"):
            return False
        return _label_shape_ok(sl[0]) and (lex.get(sl[0]) >= 2 or " " in sl[0] or bool(FUSED_LABEL_RE.match(sl[0])))

    prev_blank = True
    inner_appendix = False
    skip = set()          # units consumed by the previous unit (a label broken over two lines)
    for u, (k, ukind, raw, first_line) in enumerate(units):
        if u in skip:
            continue
        it = items[k]
        if ukind == "table":
            # a sub-cover opens a new sitting only as an accepted start (_sitting_starts); any other
            # table after an end marker is an event (the running turn was ended at the marker)
            sub = _sub_cover(it) if starts.get(u) == "sub_cover" else None
            if sub is not None:
                close()
                inner_appendix = False
                dd = _dt.date.fromisoformat(sub["date"])
                new_sitting("sub_cover", norm(it["text"].replace("\n", " ")), date=dd,
                            hhmm=_clock_text(sub.get("time_text")))
                events.append({"kind": "sub_cover", "text": it["text"], "table_id": it["table_id"],
                               "after_turn_seq": len(turns), "within_turn": False, "new_date": sub["date"],
                               "sitting_seq": st["sitting"]})
                uassign[u] = "table_event"
                prev_blank = False
                continue
            if cur is not None:
                # a table inside a turn is part of the turn text, cell by cell in reading order
                # (as the viewer parser keeps embedded tables in the speaker's text)
                cur["embedded_tables"].append(it["text"])
                tlines, n_inter = _table_lines(it["cells"])
                if n_inter:
                    bump("table_rows_interleaved", n_inter)
                for ln in tlines:
                    cur["lines"].append({"text": ln, "table": True})
                uassign[u] = "table_in_turn"
            else:
                events.append({"kind": "table", "text": it["text"], "table_id": it["table_id"],
                               "after_turn_seq": len(turns), "within_turn": False, "sitting_seq": st["sitting"]})
                uassign[u] = "table_event"
                reattach = None
            continue
        if ukind == "aside":
            events.append({"kind": it["zone"], "text": it["text"], "within_turn": cur is not None,
                           "after_turn_seq": len(turns) + (1 if cur else 0), "sitting_seq": st["sitting"]})
            uassign[u] = "aside"
            continue
        s = norm(raw)
        if not s:
            uassign[u] = "blank"
            prev_blank = True
            continue
        if st["sit_ended"] and u not in starts:
            # after a meeting-end marker and before an accepted sitting start: appendix material
            # printed in the body (written questions and answers, 제안설명서, review reports, budget
            # tables, attendee lists, vote-name blocks), kept verbatim as events, never a turn
            events.append({"kind": "appendix_inner", "text": s, "after_turn_seq": len(turns),
                           "within_turn": False, "sitting_seq": st["sitting"], "after_end": True})
            uassign[u] = "after_end_event"
            bump("after_end_lines")
            prev_blank = False
            continue
        if u in starts:
            inner_appendix = False
        if starts.get(u) == "sub_cover_line":
            # a sub-cover printed as lines after the end of a sitting ('미합중국대통령(…) 연설' /
            # '일  시  2017년11월8일(수) 오전 11시' / '장  소  국회본회의장'): the next sitting's date
            close()
            inner_appendix = False
            dm = YMD_RE.search(s)
            try:
                dd = _ymd(dm)
            except ValueError:
                dd = None
            new_sitting("sub_cover_line", s, date=dd, hhmm=_clock_text(s[dm.end():]))
            events.append({"kind": "sub_cover", "text": s, "after_turn_seq": len(turns), "within_turn": False,
                           "new_date": dd.isoformat() if dd else None, "sitting_seq": st["sitting"]})
            uassign[u] = "event_sub_cover"
            prev_blank = False
            continue
        if cur is None and PLACE_LINE_RE.match(s) and events and events[-1]["kind"] == "sub_cover" \
                and uassign.get(u - 1) == "event_sub_cover":
            # the place line of a sub-cover printed as lines ('장  소  국회본회의장')
            events[-1]["text"] += "\n" + s
            uassign[u] = "event_sub_cover"
            prev_blank = False
            continue
        if RULE_LINE_RE.match(nows(s)) and not inner_appendix:
            # 16대 layout: a rule line separates the turn from the next agenda heading
            if cur is not None:
                reattach = (cur, len(events)) if not st["sit_ended"] else None
            close()
            events.append({"kind": "rule", "text": s, "after_turn_seq": len(turns), "within_turn": False,
                           "sitting_seq": st["sitting"]})
            uassign[u] = "event_rule"
            st["last"] = "rule"
            bump("rule_line")
            prev_blank = False
            continue
        if SITTING_HEAD_RE.match(nows(s)):
            # '【오전회의 내용】': another sitting of the meeting printed after this one
            close()
            inner_appendix = False
            new_sitting("sitting_head", s)
            events.append({"kind": "sitting_head", "text": s, "after_turn_seq": len(turns),
                           "within_turn": False, "sitting_seq": st["sitting"]})
            uassign[u] = "event_sitting_head"
            prev_blank = False
            continue
        # an attendance / attendee / vote-name list inside the body (a file holding several
        # sittings): its lines are events until the next strong speaker line or time marker
        if APPENDIX_HEAD_NOWS_RE.match(nows(s)):
            close()
            inner_appendix = True
            reattach = None
        elif inner_appendix:
            if strong_speaker(_speaker_line(raw, lex)) or (s.startswith("(") and (_time_line(s) or END_NOTE_RE.match(s))):
                inner_appendix = False
        if inner_appendix:
            events.append({"kind": "appendix_inner", "text": s, "after_turn_seq": len(turns),
                           "within_turn": False, "sitting_seq": st["sitting"]})
            uassign[u] = "appendix_inner"
            bump("appendix_inner_lines")
            prev_blank = False
            continue
        spk = _speaker_line(raw, lex)
        if spk is None and s[:1] in MARKERS:
            bump("marker_line_not_speaker")
            if s[1:2] in MARKERS:
                bump("redacted_name_line")
        if spk and spk[2] == "label_only" and " " not in spk[0] and u + 1 < len(units):
            # a long label broken over two lines ('◯한국보건산업진흥원GlobalHealthcareBusiness' /
            # 'Center장 장경원  외국인 환자에…'): the next unindented line carries the rest of the
            # label, a double space and the text
            k2, kind2, raw2, _ = units[u + 1]
            if kind2 == "para" and raw2[:1] not in INDENT_CHARS and raw2[:1] not in MARKERS \
                    and not raw2.startswith("("):
                r2 = _sep_label(raw2)
                if r2 and _plausible_label(spk[0] + r2[0]) and norm(r2[1]):
                    spk = (spk[0] + r2[0], r2[1], "label_joined_next_line", spk[3])
                    skip.add(u + 1)
                    uassign[u + 1] = "turn_head"
        if spk and spk[2] == "label_missing" and u + 1 < len(units) and units[u + 1][0] == k \
                and units[u + 1][1] == "para" and units[u + 1][2].lstrip(INDENT_CHARS)[:1] in MARKERS \
                and (_speaker_line(units[u + 1][2], lex) or (None,))[0]:
            # '◯! ○박종근 위원  李차관, …': a stray marker right before a speaker marker of the same
            # paragraph is text of the running turn, not a turn without a label
            spk = None
            bump("label_missing_before_marker")
        marker_head = None
        if first_line and s[:1] in MARKERS:
            # a marker line that is an agenda heading, not a speaker ('○ 위원장(金德圭)인사',
            # '○5분자유발언', '○의사진행의건', '○참고인(崔祐英‧崔成龍) 신문'): it repeats a cover agenda
            # item, or it is a whole-line 'label' that fails the label evidence (_label_only_ok)
            if _agenda_key(s) in cover_keys:
                marker_head = "marker_cover_match"
            elif spk is not None and spk[2] == "label_only" and not _label_only_ok(spk[0], lex):
                marker_head = "marker_heading"
            if marker_head:
                spk = None
        if spk and not s.startswith("("):
            # (never after an end marker: the body is closed until an accepted sitting start)
            close()
            reattach = None
            label, first, how, marker = spk
            pos_, name, split_how = split_label(label) if label is not None else (None, None, "missing")
            bump("label_" + how)
            bump("marker_" + ("U+25EF" if marker == "◯" else "U+25CB"))
            if raw[:1] in INDENT_CHARS:
                bump("indented_marker")
            if not first_line:
                bump("turn_head_not_paragraph_start")
            cur = {"turn_seq": len(turns) + 1, "speaker_label_raw": label, "speaker_pos": pos_,
                   "speaker_name": name, "label_split": split_how, "label_how": how,
                   "label_lex_count": lex.get(label) if label is not None else 0,
                   "name_has_hanja": bool(HANJA_RE.search(name or "")),
                   "lines": [], "embedded_tables": [],
                   "agenda_ordinal": st["agenda_ordinal"], "agenda_text": st["agenda_text"],
                   "time_hhmm": st["hhmm"], "time_marker": st["time"],
                   "speech_date": st["date"].isoformat() if st["date"] else None,
                   "speech_date_how": st["date_how"],
                   "time_hhmm_end": None, "speech_date_end": None,
                   "after_end_marker": st["ended"], "sitting_seq": st["sitting"],
                   "time_regress": st["regress"], "n_reattached": 0,
                   # the text after the label as printed (whitespace kept for the sep rules), used
                   # by build_turns to detect a label repeated at the start of the spoken text
                   "first_text_raw": first}
            if norm(first):
                cur["lines"].append({"text": norm(first)})
            uassign[u] = "turn_head"
            prev_blank = False
            continue
        if starts.get(u) == "after_end_open":
            # an opening marker printed without a clock ('(개의)'): a new sitting, the clock is unknown
            close()
            reattach = None
            new_sitting("after_end_open", s)
            events.append({"kind": "note", "text": s, "after_turn_seq": len(turns), "within_turn": False,
                           "sitting_boundary": True, "sitting_seq": st["sitting"]})
            uassign[u] = "event_note"
            prev_blank = False
            continue
        if s.startswith("(") and END_NOTE_RE.match(s):
            # '(계속개의되지 않았음)': a recess that never resumed ends the meeting like an end marker (the
            # viewer XML prints it as a note after the last turn, not in the turn)
            close()
            reattach = None
            events.append({"kind": "note", "text": s, "after_turn_seq": len(turns), "within_turn": False,
                           "is_end": True, "sitting_seq": st["sitting"]})
            st["ended"] = st["sit_ended"] = True
            uassign[u] = "event_note"
            bump("end_note")
            prev_blank = False
            continue
        tl = (_time_line(s) or (_clock_line(s) if starts.get(u) == "after_end_clock" else None)) \
            if s.startswith("(") else None
        if tl:
            is_end = bool(tl["action"] and nows(tl["action"]) in END_ACTIONS_EXACT)
            h, mi = tl["h"], tl["mi"]
            ev = {"kind": "time", "text": s, "after_turn_seq": len(turns) + (1 if cur else 0),
                  "hhmm": f"{h:02d}:{mi:02d}", "action": tl["action"], "within_turn": False}
            boundary = False
            if starts.get(u) in ("after_end_time", "after_end_clock"):
                # an accepted sitting start after an end marker (_sitting_starts)
                close()
                reattach = None
                new_sitting(starts[u], s)
                ev["after_turn_seq"] = len(turns)
                boundary = True
            if is_end:
                # the running turn ends at the end marker: nothing printed after it is turn text
                close()
            minutes = h * 60 + mi
            if st.pop("check_boundary_regress", False) and st.get("prev_clock") is not None \
                    and minutes < st["prev_clock"] and not tl["mo"]:
                ev["time_regress_boundary"] = True     # the new sitting starts earlier in the day
                bump("time_regress_at_boundary")
            next_day = minutes >= 24 * 60        # '(24시 산회)', '(3월19일 24시 경과)'
            if next_day:
                ev["hhmm_printed"] = ev["hhmm"]
                minutes -= 24 * 60
                ev["hhmm"] = f"{minutes // 60:02d}:{minutes % 60:02d}"
                bump("time_24h_normalized")
            if tl["rollover"]:
                ev["kind"] = "day_rollover"
                base = _md_date(tl["mo"], tl["d"], st["date"] or st["ref_date"]) if tl["mo"] else st["date"]
                if base is not None:
                    set_date(base + _dt.timedelta(days=1), "rollover")
                    ev["new_date"] = st["date"].isoformat()
                st["time"], st["hhmm"], st["clock"], st["regress"] = s, ev["hhmm"], minutes, False
            else:
                date_set = False
                if tl["mo"]:
                    dd = _md_date(tl["mo"], tl["d"], st["date"] or st["ref_date"])
                    if dd is not None:
                        if st["date"] is not None and dd < st["date"]:
                            ev["date_regress"] = True
                            bump("date_regress")
                        if next_day:
                            dd = dd + _dt.timedelta(days=1)
                        set_date(dd, "marker")
                        ev["new_date"] = dd.isoformat()
                        date_set = True
                elif next_day and st["date"] is not None:
                    set_date(st["date"] + _dt.timedelta(days=1), "rollover")
                    ev["new_date"] = st["date"].isoformat()
                    date_set = True
                regress = False
                if not date_set and st["clock"] is not None and minutes < st["clock"]:
                    if st["clock"] >= 20 * 60 and minutes <= 6 * 60 and st["date"] is not None:
                        # an unprinted midnight: '(23시38분)' … '(00시01분 감사종료)'
                        set_date(st["date"] + _dt.timedelta(days=1), "rollover")
                        ev["new_date"] = st["date"].isoformat()
                        ev["implicit_rollover"] = True
                        bump("implicit_midnight_rollover")
                    else:
                        regress = True
                        ev["time_regress"] = True
                        bump("time_regress")
                st["time"], st["hhmm"], st["clock"] = s, ev["hhmm"], minutes
                st["regress"] = regress
            if boundary:
                ev["sitting_boundary"] = True
            if is_end:
                st["ended"] = True       # a meeting-end marker of this sitting was printed
                st["sit_ended"] = True   # the body is closed until an accepted sitting start
                reattach = None
                ev["is_end"] = True
            ev["sitting_seq"] = st["sitting"]
            ev["_turn"] = cur
            events.append(ev)
            uassign[u] = "time"
            prev_blank = False
            continue
        # agenda headings are whole unindented paragraphs, never a line after a line break;
        # a heading that repeats a cover agenda item may carry 1-2 leading spaces after a blank
        # line (' 1. 소위원회 구성의 건')
        lead = len(raw) - len(raw.lstrip(INDENT_CHARS))
        unindented = lead == 0 and first_line
        # one-digit item numbers are right-aligned with a single pad space (' 2. …' above '10. …')
        num_pad = lead == 1 and bool(re.match(r"^\d[.．]", s))
        is_head = False
        if marker_head:
            is_head, how = True, marker_head
        elif first_line and not s.startswith("(") and s[0] not in MARKERS:
            key = _agenda_key(s)
            if key in cover_keys and (unindented or num_pad or
                                      (lead <= 2 and (prev_blank or st["last"] == "agenda"))):
                is_head, how = True, "cover_match" if unindented else "cover_match_indented"
            elif (unindented or num_pad) and AGENDA_NUM_RE.match(s) and \
                    (prev_blank or st["last"] == "agenda" or cur is None):
                is_head, how = True, "numbered" if unindented else "numbered_padded"
            elif lead <= 2 and cur is None and st["last"] == "agenda" and AGENDA_NUM_RE.match(s):
                # a run of numbered items right after an agenda heading, with the heading's
                # indentation ('  1. …' / '  2. …'): more agenda items, not orphan text
                is_head, how = True, "numbered_run"
            elif lead <= 2 and cur is None and st["last"] == "rule" and AGENDA_NUM_RE.match(s):
                is_head, how = True, "after_rule"      # 16대: '  1. …' under a rule line
        if is_head:
            if cur is not None:
                reattach = (cur, len(events)) if not st["sit_ended"] else None
            close()
            rec = {"ordinal": len(agenda) + 1, "text": s, "match": how,
                   "after_turn_seq": len(turns), "bill_no": None, "sitting_seq": st["sitting"]}
            agenda.append(rec)
            st["agenda_ordinal"], st["agenda_text"] = rec["ordinal"], s
            st["last"] = "agenda"
            uassign[u] = "agenda"
            prev_blank = False
            bump("agenda_" + how)
            continue
        st["last"] = None
        if cur is None and reattach is not None and lead >= 1 and not _is_paren_line(s) \
                and turns and turns[-1] is reattach[0] and reattach_continuations:
            # the speaker goes on after an agenda heading (and time marker) with no new ◯ marker
            # ('3. 지방투자촉진 특별법안…' / '(14시29분)' / '  다음은 의사일정 제3항 … 상정합니다.'):
            # an indented line continues the turn that the heading closed
            cur = turns.pop()
            for ev in events[reattach[1]:]:
                if ev["kind"] in ("time", "day_rollover") and not ev["within_turn"] \
                        and ev["after_turn_seq"] == cur["turn_seq"]:
                    ev["within_turn"] = True
                    ev["reattached"] = True
                    cur["time_hhmm_end"] = st["hhmm"]
                    cur["speech_date_end"] = st["date"].isoformat() if st["date"] else None
            reattach = None
            cur["n_reattached"] += 1
            cur["lines"].append({"text": s})
            uassign[u] = "turn_line"
            bump("continuation_reattached")
            prev_blank = False
            continue
        if cur is not None:
            if unindented and not _is_paren_line(s):
                bump("unindented_continuation")
            # a time marker between two lines of the same turn: the turn continues
            for ev in reversed(events):
                if ev.get("_turn") is cur and ev["kind"] in ("time", "day_rollover") and not ev["within_turn"]:
                    ev["within_turn"] = True
                    cur["time_hhmm_end"] = st["hhmm"]
                    cur["speech_date_end"] = st["date"].isoformat() if st["date"] else None
                else:
                    break
            cur["lines"].append({"text": s})
            uassign[u] = "turn_line"
        else:
            kind = "note" if _is_paren_line(s) else "other"
            events.append({"kind": kind, "text": s, "after_turn_seq": len(turns), "within_turn": False,
                           "sitting_seq": st["sitting"]})
            uassign[u] = "event_" + kind
            bump("orphan_" + kind)
            if kind == "other":
                reattach = None
        prev_blank = False
    close()
    for ev in events:
        ev.pop("_turn", None)
    for sg in sittings:
        sg["n_turns"] = sum(1 for t in turns if t["sitting_seq"] == sg["sitting_seq"])
    # after_final_end_marker: the turn starts after the document's last meeting-end time marker (a
    # marker printed inside turn k, after_turn_seq k, precedes turn k+1 only)
    last_end = max((e["after_turn_seq"] for e in events if e.get("is_end")), default=None)
    for t in turns:
        t["after_final_end_marker"] = last_end is not None and t["turn_seq"] > last_end

    # ---- appendix
    app_items = items[appendix_start:]
    for k in range(appendix_start, len(items)):
        assign[k] = "appendix"
    res["footer"] = _parse_footer(app_items)
    counters["appendix_speaker_like"] = sum(
        1 for it in app_items if it["kind"] == "para" and (_speaker_line(it["text"]) or (None,))[0]
        and (_speaker_line(it["text"])[2] == "sep") and len(norm(_speaker_line(it["text"])[1])) > 30)

    # ---- finalize turns to the CONTRACT shape
    out = []
    for t in turns:
        _finalize_turn(t)
        t.update({"conf_num": conf_num, "source": "hwp", "speaker_mem_id": None,
                  "speaker_area": None, "n_fragments": 1})
        out.append(t)
    res["turns"] = out
    res["meeting"] = meeting

    # ---- accounting: every item outside the body and every body unit (paragraph line) is
    # assigned exactly once; body units must add up to their paragraphs character for character
    cat_chars, cat_n = {}, {}

    def add(c, text):
        cat_n[c] = cat_n.get(c, 0) + 1
        cat_chars[c] = cat_chars.get(c, 0) + len(nows(text))

    for k, it in enumerate(items):
        if not (body_start <= k < appendix_start):
            add(assign.get(k, "unassigned"), it["text"])
    unit_chars = {}
    for u, (k, ukind, raw, _) in enumerate(units):
        txt = raw if ukind == "para" else items[k]["text"]
        add(uassign.get(u, "unassigned"), txt)
        unit_chars[k] = unit_chars.get(k, 0) + len(nows(txt))
    counters["unit_char_mismatch"] = sum(1 for k, n in unit_chars.items() if n != len(nows(items[k]["text"])))
    total_chars = sum(len(nows(p["text"])) for p in paras if p["zone"] not in ("header", "footer"))
    turn_chars = sum(len(nows(t["speaker_label_raw"])) + len(nows(t["text_raw"])) for t in out)
    marker_chars = sum(1 for t in out)  # one ◯ per turn head
    res["stats"].update({
        "n_turns": len(out),
        "n_chars": sum(t["n_chars"] for t in out),
        "n_unique_speakers": len({t["speaker_label_raw"] for t in out}),
        "n_hanja_names": sum(1 for t in out if t["name_has_hanja"]),
        "n_label_unsplit": sum(1 for t in out if t["label_split"] == "unsplit"),
        "n_agenda_anchors": len(agenda),
        "n_agenda_header": len(agenda_header),
        "n_time_markers": sum(1 for e in events if e["kind"] == "time"),
        "n_day_rollovers": sum(1 for e in events if e["kind"] == "day_rollover"),
        "n_other_events": sum(1 for e in events if e["kind"] in ("other", "note", "table")),
        "n_stage_lines": sum(len(t["stage_texts"]) for t in out),
        "n_interjections": sum(len(t["interjections"]) for t in out),
        "n_oath_signature_lines": sum(t["n_oath_signature"] for t in out),
        "n_embedded_tables": sum(len(t["embedded_tables"]) for t in out),
        "n_turns_after_end_marker": sum(1 for t in out if t["after_end_marker"]),
        "n_turns_after_final_end_marker": sum(1 for t in out if t["after_final_end_marker"]),
        "n_sittings": len(res["sittings"]),
        "n_turns_later_sittings": sum(1 for t in out if t["sitting_seq"] > 1),
        "n_time_regress": counters.get("time_regress", 0),
        "n_turns_time_regress": sum(1 for t in out if t["time_regress"]),
        "n_reattached": counters.get("continuation_reattached", 0),
        "n_footer_sections": len(res["footer"]),
        "n_paragraphs": len(paras),
        "n_items": len(items),
        "n_units": len(units),
        "items_by_category": cat_n,
        "chars_by_category": cat_chars,
        "chars_total": total_chars,
        "chars_turns": turn_chars,
        # turn text = turn_head + turn_line + table_in_turn units minus the ◯ markers
        "chars_turn_items": (cat_chars.get("turn_head", 0) + cat_chars.get("turn_line", 0)
                             + cat_chars.get("table_in_turn", 0) - marker_chars),
        "n_unassigned": cat_n.get("unassigned", 0),
        "counters": counters,
    })
    res["status"] = "ok" if out else "ok_no_turns"
    return res


# ----------------------------------------------------------------------------- CLI

def _main(argv):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("file")
    ap.add_argument("--paragraphs", action="store_true", help="dump extract_paragraphs() output")
    ap.add_argument("--brief", action="store_true", help="drop per-line detail from turns")
    ap.add_argument("--indent", type=int, default=1)
    a = ap.parse_args(argv)
    with open(a.file, "rb") as fh:
        data = fh.read()
    if a.paragraphs:
        out = extract_paragraphs(data)
    else:
        out = parse_hwp(data)
        if a.brief:
            for t in out["turns"]:
                t.pop("lines", None)
    json.dump(out, sys.stdout, ensure_ascii=False, indent=a.indent, default=str)
    sys.stdout.write("\n")


if __name__ == "__main__":
    _main(sys.argv[1:])


# ----------------------------------------------------------------------------- DataFrame

TURN_COLUMNS = ["conf_num", "turn_seq", "source", "speaker_label_raw", "speaker_pos", "speaker_name",
                "speaker_mem_id", "speaker_area", "text_raw", "text", "has_stage", "stage_kinds",
                "n_fragments", "agenda_ordinal", "agenda_text", "time_hhmm", "speech_date"]
EXTRA_COLUMNS = ["label_split", "label_how", "label_lex_count", "name_has_hanja", "time_hhmm_end",
                 "speech_date_end", "speech_date_how", "after_end_marker", "after_final_end_marker",
                 "sitting_seq", "time_regress",
                 "n_reattached", "n_table_lines",
                 "n_oath_signature", "n_lines", "n_chars", "stage_texts", "interjections",
                 "inline_stage_parens", "embedded_tables"]


def turns_dataframe(res, conf_num=None, extra=True):
    """parse_hwp() turns as a DataFrame with the CONTRACT turn columns and dtypes
    (plus EXTRA_COLUMNS when extra=True)."""
    import pandas as pd
    cols = TURN_COLUMNS + (EXTRA_COLUMNS if extra else [])
    rows = []
    for t in res["turns"]:
        r = {c: t.get(c) for c in cols}
        if conf_num is not None:
            r["conf_num"] = conf_num
        if extra:
            r["interjections"] = json.dumps(t.get("interjections") or [], ensure_ascii=False)
        rows.append(r)
    df = pd.DataFrame(rows, columns=cols)
    df = df.astype({"conf_num": "int64" if len(df) and df.conf_num.notna().all() else "Int64",
                    "turn_seq": "int32", "speaker_mem_id": "Int64",
                    "n_fragments": "int16", "agenda_ordinal": "Int32", "has_stage": "bool"})
    for c in ("source", "speaker_label_raw", "speaker_pos", "speaker_name", "speaker_area",
              "text_raw", "text", "agenda_text", "time_hhmm", "speech_date"):
        df[c] = df[c].astype("string")
    return df
