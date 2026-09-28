"""Tests for hwp_parser.py: record-level unit tests on synthetic streams and golden tests on
real 국회 minutes HWP files (skipped when the raw file is not on disk).

Run:  cd v10/code/pipeline && python -m pytest -q test_hwp_parser.py
"""
import json
import struct
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
import hwp_parser as H  # noqa: E402

V10 = HERE.parent.parent
RAW_HWP = V10 / "raw" / "hwp"
SAMPLES_HWP = V10 / "raw" / "samples_hwp"


# ----------------------------------------------------------------------------- builders

def rec(tag, level, payload):
    size = len(payload)
    if size >= 0xFFF:
        return struct.pack("<I", tag | (level << 10) | (0xFFF << 20)) + struct.pack("<I", size) + payload
    return struct.pack("<I", tag | (level << 10) | (size << 20)) + payload


def w(s):
    return s.encode("utf-16le")


def ctrl8(code, cid=None):
    """An 8-WCHAR control: code, 2 WCHARs ctrl id (or zeros), 4 WCHARs info, code."""
    body = cid[::-1].encode("latin-1") if cid else b"\0" * 4
    return struct.pack("<H", code) + body + b"\0" * 8 + struct.pack("<H", code)


def c1(code):
    return struct.pack("<H", code)


def para_header(nchars):
    return struct.pack("<II", nchars, 0) + b"\0" * 14


def ctrl_header(cid):
    return cid[::-1].encode("latin-1") + b"\0" * 8


def list_header_cell(col, row, nparas=1):
    return struct.pack("<HHI", nparas, 0, 0) + struct.pack("<4H", col, row, 1, 1) + b"\0" * 24


def paragraph(level, text_payload, extra=b""):
    return (rec(H.TAG_PARA_HEADER, level, para_header(len(text_payload) // 2))
            + rec(H.TAG_PARA_TEXT, level + 1, text_payload) + extra)


def text_para(level, s):
    return paragraph(level, w(s) + c1(13))


# ----------------------------------------------------------------------------- layer 1

def test_iter_records_normal_and_extended_size():
    big = b"x" * 5000                     # >= 0xFFF: extended size DWORD
    buf = rec(66, 0, b"abc") + rec(67, 1, big) + rec(71, 2, b"")
    out = list(H.iter_records(buf))
    assert [(t, l, len(p)) for t, l, p in out] == [(66, 0, 3), (67, 1, 5000), (71, 2, 0)]
    # exactly 0xFFE stays in the header, 0xFFF must use the extension
    assert struct.unpack_from("<I", rec(67, 1, b"y" * 0xFFE))[0] >> 20 == 0xFFE
    b2 = rec(67, 1, b"y" * 0xFFF)
    assert struct.unpack_from("<I", b2)[0] >> 20 == 0xFFF
    assert list(H.iter_records(b2))[0][2] == b"y" * 0xFFF


def test_iter_records_truncated():
    buf = rec(66, 0, b"abcdef")[:-2]
    with pytest.raises(H.HwpError):
        list(H.iter_records(buf))
    with pytest.raises(H.HwpError):
        list(H.iter_records(b"\x01\x02"))
    ext = struct.pack("<I", 67 | (0xFFF << 20))  # extended size missing
    with pytest.raises(H.HwpError):
        list(H.iter_records(ext))


def test_decode_char_controls():
    payload = w("A") + c1(10) + w("B") + c1(30) + w("C") + c1(31) + w("D") + c1(24) + w("E") + c1(13)
    toks = H.decode_para_text(payload)
    assert toks == [("text", "A\nB C D-E")]


def test_decode_inline_and_field_controls():
    st = {}
    payload = (w("x") + ctrl8(9) + w("y") + ctrl8(3, "%clk") + w("z") + ctrl8(4, "%clk")
               + w("!") + c1(13))
    toks = H.decode_para_text(payload, st)
    assert toks == [("text", "x\ty"), ("ctrl", 3, "%clk"), ("text", "z!")]
    assert st.get("ctrl_trailer_mismatch", 0) == 0


def test_decode_extended_table_and_trailer_check():
    st = {}
    payload = w("a") + ctrl8(11, "tbl ") + w("b")
    assert H.decode_para_text(payload, st) == [("text", "a"), ("ctrl", 11, "tbl "), ("text", "b")]
    bad = struct.pack("<H", 11) + b" lbt" + b"\0" * 8 + struct.pack("<H", 99)
    H.decode_para_text(bad, st)
    assert st["ctrl_trailer_mismatch"] == 1


def test_decode_surrogate_pair_and_truncated_control():
    st = {}
    s = "\U00020000가"               # CJK Ext-B char (surrogate pair) + hangul
    assert H.decode_para_text(w(s), st) == [("text", s)]
    toks = H.decode_para_text(w("q") + struct.pack("<3H", 9, 0, 0), st)   # tab cut short
    assert toks == [("text", "q")] and st["truncated_ctrl"] == 1


def _decode_reference(payload):
    """Per-WCHAR reference decoder (the spec, written plainly) for the equivalence test."""
    n = len(payload) // 2
    w = struct.unpack_from(f"<{n}H", payload, 0)
    toks, buf, i = [], [], 0
    while i < n:
        c = w[i]
        if c >= 32:
            buf.append(c)
            i += 1
        elif c in H.CHAR_CTRLS:
            buf.extend({10: [10], 24: [0x2D], 30: [0x20], 31: [0x20]}.get(c, []))
            i += 1
        elif i + 8 > n:
            break
        elif c in H.INLINE_CTRLS:
            buf.extend([9] if c == 9 else [])
            i += 8
        else:
            if buf:
                toks.append(("text", struct.pack(f"<{len(buf)}H", *buf).decode("utf-16le", "replace")))
                buf = []
            toks.append(("ctrl", c, struct.pack("<2H", w[i + 1], w[i + 2])[::-1].decode("latin-1")))
            i += 8
    if buf:
        toks.append(("text", struct.pack(f"<{len(buf)}H", *buf).decode("utf-16le", "replace")))
    return toks


def test_decode_matches_reference_on_random_payloads():
    import random
    rng = random.Random(8374)
    alphabet = "가나다 국회의원 ABC()…·一鿿"
    for _ in range(2000):
        parts = []
        for _ in range(rng.randint(0, 12)):
            r = rng.random()
            if r < 0.5:
                parts.append(w("".join(rng.choice(alphabet) for _ in range(rng.randint(1, 6)))))
            elif r < 0.75:
                parts.append(c1(rng.choice(sorted(H.CHAR_CTRLS))))
            else:
                code = rng.choice(sorted(H.INLINE_CTRLS | H.EXTENDED_CTRLS))
                parts.append(ctrl8(code, rng.choice([None, "tbl ", "%clk", "head", "gso "])))
        payload = b"".join(parts)
        if rng.random() < 0.1:                       # cut a control short
            payload = payload[:max(0, len(payload) - rng.randint(1, 10) * 2)]
        assert H.decode_para_text(payload) == _decode_reference(payload), payload


def test_build_tree_and_level_jump():
    st = {}
    recs = [(66, 0, b""), (67, 1, b""), (71, 1, b""), (77, 2, b""), (66, 0, b""), (67, 3, b"")]
    root = H.build_tree(recs, st)
    assert [c.tag for c in root.children] == [66, 66]
    assert [c.tag for c in root.children[0].children] == [67, 71]
    assert root.children[0].children[1].children[0].tag == 77
    assert st["level_jumps"] == 1 and root.children[1].children[0].tag == 67


def _table_stream():
    """'before' [table 1x2: 'cell A' | 'cell B'] 'after' ; then a paragraph 'next'."""
    t = w("before") + ctrl8(11, "tbl ") + w("after") + c1(13)
    table = (rec(H.TAG_CTRL_HEADER, 1, ctrl_header("tbl "))
             + rec(H.TAG_TABLE, 2, struct.pack("<IHH", 0, 1, 2))
             + rec(H.TAG_LIST_HEADER, 2, list_header_cell(0, 0))
             + text_para(2, "cell A")
             + rec(H.TAG_LIST_HEADER, 2, list_header_cell(1, 0))
             + text_para(2, "cell B") + text_para(2, "cell B2"))
    return paragraph(0, t, table) + text_para(0, "next")


def test_table_cells_in_reading_order():
    paras, st = H.paragraphs_from_sections([_table_stream()])
    got = [(p["text"], p["in_table"], p["table_id"], p["row"], p["col"], p["frag"]) for p in paras]
    assert got == [("before", False, None, None, None, 0),
                   ("cell A", True, 1, 0, 0, 0),
                   ("cell B", True, 1, 0, 1, 0),
                   ("cell B2", True, 1, 0, 1, 0),
                   ("after", False, None, None, None, 1),
                   ("next", False, None, None, None, 0)]
    assert st["n_tables"] == 1 and st.get("ctrl_id_mismatch", 0) == 0
    items, running = H._logical_items(paras)
    assert [it["kind"] for it in items] == ["para", "table", "para"]
    assert items[0]["text"] == "beforeafter" and items[1]["cells"][1]["text"] == "cell B\ncell B2"


def test_header_zone_and_extended_record_in_stream():
    long = "가" * 2100                       # 4,200+ bytes: PARA_TEXT uses the extended size
    head = (rec(H.TAG_CTRL_HEADER, 1, ctrl_header("head"))
            + rec(H.TAG_LIST_HEADER, 2, struct.pack("<HHI", 1, 0, 0))
            + text_para(2, "제294회－제15차"))
    sec = paragraph(0, ctrl8(16, "head") + w(long) + c1(13), head)
    paras, st = H.paragraphs_from_sections([sec])
    assert [(p["zone"], p["text"]) for p in paras] == [("header", "제294회－제15차"), ("body", long)]
    assert st.get("nchars_mismatch", 0) == 0


def test_nchars_mismatch_and_ctrl_without_header_are_counted():
    sec = (rec(H.TAG_PARA_HEADER, 0, para_header(99)) + rec(H.TAG_PARA_TEXT, 1, w("ab") + ctrl8(11, "tbl ")))
    paras, st = H.paragraphs_from_sections([sec])
    assert paras[0]["text"] == "ab"
    assert st["nchars_mismatch"] == 1 and st["ctrl_without_header"] == 1


def test_record_error_keeps_prefix():
    sec = text_para(0, "ok") + rec(66, 0, b"abcdef")[:-3]
    errs = []
    paras, st = H.paragraphs_from_sections([sec], errs)
    assert [p["text"] for p in paras] == ["ok"] and st["record_errors"] == 1 and errs


def test_read_hwp_rejects_non_hwp5():
    assert H.read_hwp(b"HWP Document File V3.00 \x1a\x01\x02\x03\x04\x05" + b"\0" * 100)["status"] == "hwp3"
    assert H.read_hwp(b"<html>Bad Request.</html>")["status"] == "not_ole"
    assert H.read_hwp(b"PK\x03\x04rest")["status"] == "hwpx_unsupported"
    assert H.parse_hwp(b"not a file")["status"] == "not_ole"


# ----------------------------------------------------------------------------- layer 2 units

def test_split_label():
    assert H.split_label("위원장 홍길동") == ("위원장", "홍길동", "pos_name")
    assert H.split_label("권영진 위원") == ("위원", "권영진", "name_pos")
    assert H.split_label("국무총리 후보자 김태호") == ("국무총리 후보자", "김태호", "pos_name")
    assert H.split_label("宋榮珍議員") == ("議員", "宋榮珍", "fused")
    assert H.split_label("議長職務代行 趙舜衡") == ("議長職務代行", "趙舜衡", "pos_name")


def test_speaker_line_forms():
    assert H._speaker_line("◯위원장 김부겸  좌석을 정돈해 주시기 바랍니다.")[:3] == \
        ("위원장 김부겸", "좌석을 정돈해 주시기 바랍니다.", "sep")
    assert H._speaker_line("○국방부장관 이종섭\t네.")[:3] == ("국방부장관 이종섭", "네.", "sep")
    assert H._speaker_line("◯김형오 의원 존경하는 국민 여러분!")[:3] == \
        ("김형오 의원", "존경하는 국민 여러분!", "single_space_name_pos")
    assert H._speaker_line("◯예산결산특별위원장 이주영")[2] == "label_only"
    assert H._speaker_line("  (◯박지원 의원 의석에서 ― 뭐요?)") is None
    assert H._speaker_line("  그러면 의결하겠습니다.") is None


def test_time_line():
    assert H._time_line("(10시05분)")["action"] is None
    t = H._time_line("(10시58분 산회)")
    assert (t["h"], t["mi"], t["action"]) == (10, 58, "산회")
    assert H._time_line("(3월19일 24시 경과)")["rollover"]
    assert H._time_line("(9월26일 01시15분 산회)")["mo"] == "9"
    assert H._time_line("(10시에 개회하기로 합의하였음)") is None
    assert H._time_line("(장내 소란)") is None


def _p(text, **kw):
    d = {"text": text, "in_table": False, "table_id": None, "outer_table_id": None, "row": None,
         "col": None, "cell": None, "container": "body", "zone": "body", "depth": 0,
         "section": 0, "para_id": None, "frag": 0}
    d.update(kw)
    return d


def _doc(body, cover=None, header=("제300회－제2차(2011년6월1일)",)):
    """Synthetic paragraph list: cover table + running header + body lines."""
    out, pid = [], 0
    for r, line in enumerate(cover or []):
        pid += 1
        out.append(_p(line, in_table=True, table_id=1, outer_table_id=1, row=r, col=0, cell=r + 1,
                      container="table", depth=1, para_id=pid))
    for h in header:
        pid += 1
        out.append(_p(h, container="header", zone="header", depth=1, para_id=pid))
    for line in body:
        pid += 1
        if isinstance(line, dict):
            line = dict(line, para_id=pid)
            out.append(line)
        else:
            out.append(_p(line, para_id=pid))
    return out


COVER = ["제300회국회", "(임시회)", "교육과학기술위원회회의록", "제2호", "국회사무처",
         "일시  2011년6월1일(수)", "장소  교육과학기술위원회회의실", "의사일정",
         "1. 간사 선임의 건", "상정된 안건", "1. 간사 선임의 건\t1"]


def test_parse_minutes_grammar():
    body = [
        "",
        "    (보고)",
        "(10시05분 개의)",
        "◯위원장 홍길동  성원이 되었으므로 회의를 개의하겠습니다.",
        "  먼저 보고가 있겠습니다.",
        "◯입법조사관 김철수  보고사항을 말씀드리겠습니다.",
        "(보고사항은 끝에 실음)",
        "",
        "1. 간사 선임의 건",
        "(10시07분)",
        "◯위원장 홍길동  의사일정 제1항을 상정합니다.",
        "    (｢없습니다｣ 하는 위원 있음)",
        "(10시08분 정회)",
        "  이의가 없으므로 가결되었음을 선포합니다.",
        "◯김영희 위원  질문하겠습니다.",
        "  선서",
        "  본인은 양심에 따라 성실하게 증언할 것을 맹서합니다.",
        "  2011년 6월 1일",
        "  증인 박문수",
        "(6월1일 24시 경과)",
        "  계속합니다.",
        "(10시30분 산회)",
        "◯출석 위원(2인)",
        "홍길동  김영희 ",
        "◯청가 위원(1인)",
        "박  진",
        "【보고사항】",
        "◯의안 회부",
    ]
    res = H.parse_minutes(_doc(body, COVER), conf_num=123)
    m = res["meeting"]
    assert (m["session_no"], m["session_type"], m["committee_raw"], m["doc_no"], m["date"],
            m["place"], m["sitting"]) == (300, "임시회", "교육과학기술위원회", 2, "2011-06-01",
                                          "교육과학기술위원회회의실", "제2차")
    assert [a["section"] for a in res["agenda_header"]] == ["의사일정", "상정된안건"]
    assert res["agenda_header"][1]["page"] == 1
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["위원장 홍길동", "입법조사관 김철수", "위원장 홍길동", "김영희 위원"]
    assert [x["turn_seq"] for x in t] == [1, 2, 3, 4]
    assert all(x["source"] == "hwp" and x["conf_num"] == 123 for x in t)
    assert t[0]["time_hhmm"] == "10:05" and t[0]["agenda_ordinal"] is None
    assert t[1]["text_raw"] == "보고사항을 말씀드리겠습니다.\n(보고사항은 끝에 실음)"
    assert t[1]["text"] == "보고사항을 말씀드리겠습니다." and t[1]["stage_kinds"] == ["appendix_note"]
    assert t[2]["agenda_ordinal"] == 1 and t[2]["agenda_text"] == "1. 간사 선임의 건"
    assert t[2]["time_hhmm"] == "10:07" and t[2]["time_hhmm_end"] == "10:08"
    assert "이의가 없으므로" in t[2]["text"] and "collective_response" in t[2]["stage_kinds"]
    assert t[3]["speaker_pos"] == "위원" and t[3]["speaker_name"] == "김영희"
    assert t[3]["n_oath_signature"] == 2 and "증인 박문수" not in t[3]["text"]
    assert t[3]["speech_date_end"] == "2011-06-02"
    kinds = [e["kind"] for e in res["events"]]
    assert kinds.count("time") == 4 and kinds.count("day_rollover") == 1 and kinds.count("note") == 1
    assert sum(1 for e in res["events"] if e.get("within_turn")) == 2
    titles = [f["title"] for f in res["footer"]]
    assert titles == ["◯출석 위원(2인)", "◯청가 위원(1인)", "【보고사항】", "◯의안 회부"]
    assert res["footer"][0]["names"] == ["홍길동", "김영희"] and res["footer"][1]["names"] == ["박진"]
    s = res["stats"]
    assert s["appendix_how"] == "after_end_marker" and s["n_unassigned"] == 0
    assert s["chars_turns"] == s["chars_turn_items"]


def test_parse_cover_multiline_title_and_subcommittee():
    lines = [(0, "第304回國會"), (0, "(臨時會)"), (1, "空港․發電所․液化天然가스引受基地"),
             (1, "周邊對策特別委員會會議錄"), (1, "(發電所·液化天然가스引受基地法案審査小委員會)"),
             (2, "第  2  號"), (3, "國 會 事 務 處"), (4, "日  時  2011年11月1日(火)")]
    m, agenda = H._parse_cover(lines)
    assert m["committee_raw"] == "空港․發電所․液化天然가스引受基地周邊對策特別委員會"
    assert m["subcommittee"] == "發電所·液化天然가스引受基地法案審査小委員會"
    assert (m["session_no"], m["session_type"], m["doc_no"], m["date"]) == (304, "臨時會", 2, "2011-11-01")
    assert m["cover_notes"] == [] and m["title_lines_joined"] == 1


def test_parse_minutes_table_inside_turn_and_orphans():
    body = [
        "◯위원장 홍길동  표를 보시겠습니다.",
        _p("표 셀", in_table=True, table_id=2, outer_table_id=2, row=0, col=0, cell=1, container="table"),
        "  이상입니다.",
        "",
        "2. 다음 안건",
        "주인 없는 줄",
    ]
    res = H.parse_minutes(_doc(body))
    t = res["turns"]
    # a table inside a turn is turn text (cell lines in reading order), not an event
    assert len(t) == 1 and t[0]["text_raw"] == "표를 보시겠습니다.\n표 셀\n이상입니다."
    assert t[0]["text"] == t[0]["text_raw"] and t[0]["n_table_lines"] == 1
    assert t[0]["embedded_tables"] == ["표 셀"]
    assert not any(e["kind"] == "table" for e in res["events"])
    assert res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]
    assert res["stats"]["counters"]["orphan_other"] == 1
    assert res["stats"]["appendix_how"] == "none"
    assert res["stats"]["n_unassigned"] == 0


def test_appendix_by_heading_without_end_marker():
    body = ["◯위원장 홍길동  회의를 마치겠습니다.", "◯출석 위원(1인)", "홍길동"]
    res = H.parse_minutes(_doc(body))
    assert len(res["turns"]) == 1 and res["stats"]["appendix_how"] == "appendix_heading"
    assert res["footer"][0]["names"] == ["홍길동"]


def test_appendix_boundary_rules():
    # '비공개감사종료' is not the end of the meeting; the real end marker is
    body = ["(10시16분 감사개시)", "◯위원장 홍길동  감사를 시작합니다.", "(10시18분 비공개감사개시)",
            "(10시32분 비공개감사종료)", "◯김영희 위원  질의합니다.", "(19시46분 감사종료)",
            "◯출석 감사위원(2인)", "홍길동  김영희"]
    res = H.parse_minutes(_doc(body))
    assert len(res["turns"]) == 2 and res["stats"]["appendix_how"] == "after_end_marker"
    assert res["footer"][0]["title"] == "◯출석 감사위원(2인)"
    # two sittings in one file: the first 산회 and attendance list are body; the appendix
    # starts at the heading after the last turn ('◯정부측 및 기타  참석자' has a double space)
    body = ["◯위원장 홍길동  개의합니다.", "(14시12분 산회)", "◯출석 위원(1인)", "홍길동",
            "(10시37분)", "◯위원장 홍길동  속개합니다.", "◯정부측 및 기타  참석자", "  국방부"]
    res = H.parse_minutes(_doc(body))
    assert [t["text_raw"] for t in res["turns"]] == ["개의합니다.", "속개합니다."]
    assert res["stats"]["appendix_how"] == "appendix_heading"
    assert res["stats"]["end_markers_before_last_turn"] == 1
    assert res["stats"]["appendix_heads_before_last_turn"] == 1
    assert res["footer"][0]["title"] == "◯정부측 및 기타 참석자"
    # a plenary guest list and a session notice are appendix sections, not speakers
    for head in ("◯내빈  참석자", "◯제281회국회(임시회) 집회요구"):
        res = H.parse_minutes(_doc(["◯의장 김형오  개의합니다.", "(10시05분)", "◯의사국장 이종후  보고드립니다.",
                                    head, "일 시", "2009년 2월 2일"]))
        assert [t["speaker_label_raw"] for t in res["turns"]] == ["의장 김형오", "의사국장 이종후"]
        assert res["stats"]["appendix_how"] == "appendix_heading"


def test_speaker_line_with_lexicon():
    lex = H._Lexicon({"공정거래위원장 백용호": 30, "위원장 김영선": 50, "국토해양부장관": 3,
                      "국토해양부장관 정종환": 2, "한국농촌공사감사실장 황승현": 1})
    # single space between label and text: split at the known label
    assert H._speaker_line("◯공정거래위원장 백용호 신문고시 처리 건이……", lex)[:3] == \
        ("공정거래위원장 백용호", "신문고시 처리 건이……", "lexicon_prefix")
    assert H._speaker_line("◯한국토지공사사장 이종상 ……", H._Lexicon({"한국토지공사사장 이종상": 4}))[:3] == \
        ("한국토지공사사장 이종상", "……", "lexicon_prefix")
    # a sep label that swallowed the first word is trimmed to the frequent known label
    assert H._speaker_line("◯위원장 김영선 예  알겠습니다.", lex)[:3] == \
        ("위원장 김영선", "예 알겠습니다.", "sep_trimmed_by_lexicon")
    # ... but a name is never moved out of a label
    assert H._speaker_line("◯국토해양부장관 정종환  답변드리겠습니다.", lex)[:2] == \
        ("국토해양부장관 정종환", "답변드리겠습니다.")
    # position, name and text each set off by a double space
    assert H._speaker_line("◯한국농촌공사감사실장  황승현  다른 부처라고 말씀하심은", lex)[:3] == \
        ("한국농촌공사감사실장 황승현", "다른 부처라고 말씀하심은", "sep_joined_pos_name")
    # a sep label that is a sentence is not a label; the name-title pattern wins
    assert H._speaker_line("◯김희철 위원 그래서 에너지는 21세기의 유망 산업입니다.  울산이 자동차", lex)[:2] == \
        ("김희철 위원", "그래서 에너지는 21세기의 유망 산업입니다. 울산이 자동차")
    # names ending in 오 / 서 are names
    assert H._speaker_line("◯의장 김형오  의석을 정돈해 주시기 바랍니다.")[:3] == \
        ("의장 김형오", "의석을 정돈해 주시기 바랍니다.", "sep")
    assert H._plausible_label("의장 김형오") and H._name_like("김민서") and not H._name_like("좋습니다")
    # a known label glued to the text
    assert H._speaker_line("◯위원장 김영선이어서 의사일정 제60항", lex)[:3] == \
        ("위원장 김영선", "이어서 의사일정 제60항", "lexicon_prefix_fused")
    # marker lines that are not speaker lines: a bullet in a quoted notice, a self-introduction
    assert H._speaker_line("   ○처    우", lex) is None
    assert H._speaker_line("  ◯ 통일교육원장입니다.", lex) is None
    assert H._speaker_line("◯진술인")[:3] == ("진술인", "", "label_only")
    # a stray period after a known label; a marker without any label keeps the turn boundary
    assert H._speaker_line("◯위원장 김영선. 자, 잠깐만요.", lex)[:3] == \
        ("위원장 김영선", ". 자, 잠깐만요.", "lexicon_prefix_punct")
    # label_missing: the label is null and the stray character stays in the text
    assert H._speaker_line("◯`", lex)[:3] == (None, "`", "label_missing")


def test_line_breaks_and_midline_markers():
    body = [
        "◯홍재형 위원  한국은행이 자금 지원 해 줍니까?\n◯금융위원장 전광우  이 부분은 아니고요.",
        "◯배영식 위원  한 1분만요. ◯위원장대리 이광재  예.",
        "◯위원장대리 이광재  회사원 ○○○ 씨 등 2명을 채택합니다. (◯박지원 의원 의석에서 ― 뭐요?)",
        "◯금융위원장 전광우  네.",
    ]
    res = H.parse_minutes(_doc(body))
    got = [(t["speaker_label_raw"], t["text_raw"]) for t in res["turns"]]
    assert got == [("홍재형 위원", "한국은행이 자금 지원 해 줍니까?"),
                   ("금융위원장 전광우", "이 부분은 아니고요."),
                   ("배영식 위원", "한 1분만요."),
                   ("위원장대리 이광재", "예."),
                   ("위원장대리 이광재", "회사원 ○○○ 씨 등 2명을 채택합니다. (◯박지원 의원 의석에서 ― 뭐요?)"),
                   ("금융위원장 전광우", "네.")]
    c = res["stats"]["counters"]
    assert c["para_line_breaks"] == 1 and c["midline_marker_split"] == 1
    assert c["turn_head_not_paragraph_start"] == 2 and c["unit_char_mismatch"] == 0
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


def test_label_broken_over_two_lines_and_latin_initials():
    body = ["◯위원장 변웅전  실무자 일어나세요.", "◯한국보건산업진흥원GlobolHealthcareBusiness",
            "Center장 장경원  외국인 환자에 포함이 됩니다.", "◯기상청기상선진화추진단장 Kenneth C. Crawford  네.",
            "◯위원장 변웅전  네."]
    res = H.parse_minutes(_doc(body))
    got = [(t["speaker_label_raw"], t["text_raw"], t["label_how"]) for t in res["turns"]]
    assert got == [("위원장 변웅전", "실무자 일어나세요.", "sep"),
                   ("한국보건산업진흥원GlobolHealthcareBusinessCenter장 장경원", "외국인 환자에 포함이 됩니다.",
                    "label_joined_next_line"),
                   ("기상청기상선진화추진단장 Kenneth C. Crawford", "네.", "sep"),
                   ("위원장 변웅전", "네.", "sep")]
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


def test_cover_agenda_heading_with_leading_space():
    body = ["◯방위사업청장 양치규  감사합니다.", "", " 1. 간사 선임의 건", "(11시35분)",
            "◯위원장 홍길동  상정합니다.", "  1. 이 줄은 발언의 일부입니다."]
    res = H.parse_minutes(_doc(body, COVER))
    assert [a["match"] for a in res["agenda"]] == ["cover_match_indented"]
    t = res["turns"]
    assert t[0]["text_raw"] == "감사합니다." and t[1]["agenda_ordinal"] == 1
    assert t[1]["text_raw"] == "상정합니다.\n1. 이 줄은 발언의 일부입니다."
    # right-aligned one-digit numbers: ' 1.' ... ' 9.' above '10.'
    body = ["", " 1. 가법 일부개정법률안", " 2. 나법 일부개정법률안", "10. 다법 일부개정법률안",
            "◯위원장 홍길동  일괄 상정합니다."]
    res = H.parse_minutes(_doc(body))
    assert [a["match"] for a in res["agenda"]] == ["numbered_padded", "numbered_padded", "numbered"]
    assert res["stats"]["counters"].get("orphan_other", 0) == 0
    # a run of indented items after an agenda heading
    body = ["◯소위원장 이혜훈  개의하겠습니다.", "", "  1. 가법 일부개정법률안(계속)", "  2. 나법 일부개정법률안(계속)",
            "  3. 다법 일부개정법률안(계속)", "(10시05분)", "◯소위원장 이혜훈  일괄 상정합니다."]
    cover = ["제284회국회", "(정기회)", "조세소위원회회의록", "제9호", "의사일정", "1. 가법 일부개정법률안(계속)"]
    res = H.parse_minutes(_doc(body, cover))
    assert [a["match"] for a in res["agenda"]] == ["cover_match_indented", "numbered_run", "numbered_run"]
    assert [t["text_raw"] for t in res["turns"]] == ["개의하겠습니다.", "일괄 상정합니다."]
    assert res["stats"]["counters"].get("orphan_other", 0) == 0


# ----------------------------------------------------------------------------- review fixes
# (adversarial review of the component: weak single-space labels, in-body appendix blocks,
# ceremony attendance, continuation after agenda headings, several sittings, time edge cases)

def test_single_space_labels_need_attestation():
    lex = H._Lexicon({"위원장 홍길동": 5, "홍길동 위원": 2})
    # true single-space labels: a position word and a surname-initial name
    assert H._speaker_line("◯위원장대리 우윤근 수고하셨습니다.", lex)[:3] == \
        ("위원장대리 우윤근", "수고하셨습니다.", "single_space_pos_name")
    assert H._speaker_line("◯입법조사관 김학배 입법조사관입니다.", lex)[:3] == \
        ("입법조사관 김학배", "입법조사관입니다.", "single_space_pos_name")
    # Hanja names in CJK compatibility ideographs ('李' U+F9E1) are folded before the surname check
    assert H._speaker_line("◯李恩宰 委員 한나라당의 이은재 위원입니다.", lex)[2] == "single_space_name_pos"
    # vote-name blocks, written answers and document text are not speaker lines
    for line in ("◯가족관계의 등록 등에 관한 법률 일부개정법률안(대안)",
                 "◯할부거래에 관한 법률 일부개정법률안",
                 "○위원님께서 요구하신 ‘구제역 대국민 홍보 개선대책’ 을 붙임과 같이 제출합니다.",
                 "○축산농장 출입 차량 및 탑승자에 대한 소독",
                 "○발생 초기에 정확한 실태와 국민, 축산농 대상으로",
                 "○소독대상 확대：축산인 →  축산관계자 및 일반국민(필요시)"):
        sl = H._speaker_line(line, lex)
        assert sl is None or sl[2] in ("label_only", "sep_implausible"), (line, sl)
    # a redacted name at the start of a continuation line is text
    assert H._speaker_line("○○○ 씨가 출석했습니다.", lex) is None
    # a redacted label set off by a double space is still a label
    assert H._speaker_line("◯○○○ 증인  예, 그렇습니다.", lex)[:2] == ("○○○ 증인", "예, 그렇습니다.")
    # a one-token known label followed by an unattested word keeps the known label
    lex2 = H._Lexicon({"국토해양부장관": 3})
    assert H._speaker_line("◯국토해양부장관 정부 입장은 이렇습니다.", lex2)[:3] == \
        ("국토해양부장관", "정부 입장은 이렇습니다.", "lexicon_prefix")


VOTE_DOC = [
    "(10시05분 개의)",
    "◯의장 김형오  개의하겠습니다.",
    "◯부의장 이윤성  산회를 선포합니다.",
    "(18시47분 산회)",
    "",
    "【전자투표 찬반 의원 성명】",
    "◯卽決審判에關한節次法 일부개정법률안",
    "  투표 의원(185인)",
    "강길부  강명순  강봉균  강용석 ",
    "",
    "◯가족관계의 등록 등에 관한 법률 일부개정법률안(대안)",
    "  투표 의원(184인)",
    "강길부  강명순  강봉균  강용석 ",
    "(이군현․안홍준 의원 표결기 조작 지체. 실제 투표 의원 189인, 찬성 의원 189인임)",
    "◯조달사업에 관한 법률 일부개정법률안(대안)",
    "강길부  강명순",
    "◯출석 의원(255인)",
    "강길부  강명순",
    "【보고사항】",
    "◯의안 제출",
    _p("캐나다총리(스티븐 하퍼) 연설\n日  時  2009年12月7日(月) 午後 2時40分\n場  所  國會本會議場",
       in_table=True, table_id=9, outer_table_id=9, row=0, col=0, cell=1, container="table"),
    "o 환영사",
    "◯의장 김형오  존경하는 스티븐 하퍼 캐나다총리 내외분!",
    "◯캐나다총리 스티븐 하퍼  존경하는 대한민국 국회의장님, 의원 여러분!",
    "(15시05분)",
]


def test_vote_blocks_after_end_are_events_and_appended_ceremony_is_a_sitting():
    res = H.parse_minutes(_doc(VOTE_DOC, header=("제284회－제13차(2009년12월7일)",)))
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["의장 김형오", "부의장 이윤성", "의장 김형오", "캐나다총리 스티븐 하퍼"]
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2]
    # after_end_marker is reset by the new sitting (researcher decision 2026-09-26); the appended
    # ceremony still starts after the document's last end marker
    assert [x["after_end_marker"] for x in t] == [False, False, False, False]
    assert [x["after_final_end_marker"] for x in t] == [False, False, True, True]
    assert res["stats"]["counters"]["after_end_reset_at_new_sitting"] == 1
    assert res["stats"]["n_turns_after_final_end_marker"] == 2
    assert t[2]["speech_date"] == "2009-12-07" and t[2]["speech_date_how"] == "sub_cover"
    assert t[2]["time_hhmm"] == "14:40"       # printed on the sub-cover, not the previous sitting's 18:47
    inner = [e["text"] for e in res["events"] if e["kind"] == "appendix_inner"]
    assert "◯가족관계의 등록 등에 관한 법률 일부개정법률안(대안)" in inner and "강길부 강명순" in inner
    assert not any("강길부" in x["text"] for x in t)
    sg = res["sittings"]
    assert [(s["sitting_seq"], s["how"], s["start_turn_seq"], s["n_turns"]) for s in sg] == \
        [(1, "first", 1, 2), (2, "sub_cover", 3, 2)]
    c = res["stats"]["counters"]
    assert c.get("label_single_space_pos_name", 0) == 0
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


def test_written_answers_after_end_go_to_the_appendix():
    body = ["◯소위원장 이범관  오늘 회의를 마치겠습니다.", "(11시59분 산회)", "",
            "【서면질의․답변서】", "(답변서)", "○행정안전부장관 맹형규", "<구두질의에 대한 답변>",
            "○위원님께서 요구하신 ‘구제역 대국민 홍보 개선대책’ 을 붙임과 같이 제출합니다.",
            "○소독대상 확대：축산인 →  축산관계자 및 일반국민(필요시)",
            "○농림수산식품부장관 유정복", "◯출석 위원(6인)", "김학용  이범관"]
    res = H.parse_minutes(_doc(body))
    assert [x["speaker_label_raw"] for x in res["turns"]] == ["소위원장 이범관"]
    assert res["stats"]["appendix_how"] == "after_end_marker"
    assert [f["title"] for f in res["footer"]][:2] == ["【서면질의․답변서】", "(답변서)"] or \
        res["footer"][0]["title"] == "【서면질의․답변서】"


def test_ceremony_attendance_goes_to_the_footer():
    body = ["(14시01분 개식)", "◯국제국장 김춘순  개원식을 시작하겠습니다.",
            "◯국제국장 김춘순  이상으로 개원식을 모두 마치겠습니다. ", "  감사합니다. ", "(14시51분 폐식)", "",
            "◯참석 의원(286인)", "강기정  강길부  강명순  박  진", "◯내빈 참석자"]
    res = H.parse_minutes(_doc(body))
    t = res["turns"]
    assert t[-1]["text_raw"] == "이상으로 개원식을 모두 마치겠습니다.\n감사합니다."
    assert res["stats"]["appendix_how"] == "after_end_marker"
    f = [x for x in res["footer"] if x["title"] == "◯참석 의원(286인)"]
    assert f and f[0]["names"] == ["강기정", "강길부", "강명순", "박진"]


def test_oath_signatories_name_list_is_not_spoken_text():
    body = ["◯의장 김형오  “선서, 나는 … 국민 앞에 엄숙히 선서합니다.”", "2008년 7월 11일", "국회의원", "김형오",
            "강기정 강길부 강명순 강봉균 강석호 강성종 강성천 강승규 박  진 황우여 황진하",
            "◯국제국장 김춘순  다음은 대통령 연설이 있겠습니다."]
    t = H.parse_minutes(_doc(body))["turns"]
    assert t[0]["n_oath_signature"] == 4 and "강기정" not in t[0]["text"] and "강기정" in t[0]["text_raw"]


def test_continuation_after_agenda_heading_is_reattached():
    body = ["◯위원장 홍길동  가결되었음을 선포합니다. ", "", "3. 지방투자촉진 특별법안(계속)", "(14시29분)",
            "  다음은 의사일정 제3항을 상정합니다. ", "  수석전문위원 보고해 주시기 바랍니다. ",
            "◯수석전문위원 권대수  유인물 5페이지가 되겠습니다."]
    res = H.parse_minutes(_doc(body))
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["위원장 홍길동", "수석전문위원 권대수"]
    assert t[0]["text_raw"] == "가결되었음을 선포합니다.\n다음은 의사일정 제3항을 상정합니다.\n수석전문위원 보고해 주시기 바랍니다."
    assert t[0]["n_reattached"] == 1 and t[0]["agenda_ordinal"] is None and t[1]["agenda_ordinal"] == 1
    assert t[0]["time_hhmm_end"] == "14:29"
    ev = [e for e in res["events"] if e["kind"] == "time"]
    assert ev[0]["within_turn"] and ev[0]["after_turn_seq"] == 1
    assert res["stats"]["counters"].get("orphan_other", 0) == 0
    assert res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"] and res["stats"]["n_unassigned"] == 0
    # the option off: the lines stay orphan events (the previous behaviour)
    res = H.parse_minutes(_doc(body), reattach_continuations=False)
    assert res["stats"]["counters"]["orphan_other"] == 2 and res["turns"][0]["text_raw"] == "가결되었음을 선포합니다."
    # a line after an end marker is never reattached; with no sitting start after the end marker
    # (no opening marker, no cover or time marker followed by an attested speaker) the rest of the
    # body is appendix, a speaker line included (appendix rule 2026-09-28; before, the speaker line
    # opened a second sitting)
    body2 = ["◯위원장 홍길동  산회를 선포합니다.", "(12시16분 산회)", "", "1. 다른 안건", "  이 줄은 발언이 아닙니다.",
             "◯위원장 홍길동  개의합니다."]
    res = H.parse_minutes(_doc(body2))
    assert [x["text_raw"] for x in res["turns"]] == ["산회를 선포합니다."]
    assert res["stats"]["counters"].get("orphan_other", 0) == 0 and res["stats"]["appendix_how"] == "after_end_marker"
    foot = [x for f in res["footer"] for x in [f["title"]] + f["lines"]]
    assert "  이 줄은 발언이 아닙니다." in foot and "◯위원장 홍길동  개의합니다." in foot
    # the same speaker line after an opening marker starts sitting 2
    res = H.parse_minutes(_doc(body2[:5] + ["(14시00분 개의)"] + body2[5:]))
    assert [x["sitting_seq"] for x in res["turns"]] == [1, 2] and res["turns"][0]["text_raw"] == "산회를 선포합니다."
    # the lines between the end marker and the opening marker are events, not orphans or turn text
    assert [e["text"] for e in res["events"] if e.get("after_end")] == ["1. 다른 안건", "이 줄은 발언이 아닙니다."]
    assert res["stats"]["counters"].get("orphan_other", 0) == 0


MULTI_SITTING = [
    "(10시13분 개의)",
    "◯위원장 홍길동  개의하겠습니다.",
    "◯위원장 홍길동  산회를 선포합니다.",
    "(12시16분 산회)",
    "◯출석 위원(2인)",
    "김충환  조영택",
    "지방행정체제 개편방안(대구․경북)",
    "(10시02분)",
    "◯위원장대리 권경석  개의하겠습니다.",
    "(12시33분)",
    "◯위원장대리 권경석  마치겠습니다.",
    "◯출석 위원(4인)",
    "권경석  노철래",
]


def test_several_sittings_in_one_file():
    res = H.parse_minutes(_doc(MULTI_SITTING, COVER))
    t = res["turns"]
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2]
    assert [x["speech_date_how"] for x in t] == ["cover", "cover", "inherited", "inherited"]
    assert [x["speech_date"] for x in t] == ["2011-06-01"] * 4
    assert [x["time_hhmm"] for x in t] == ["10:13", "10:13", "10:02", "12:33"]
    assert not any(x["time_regress"] for x in t)     # the restart is a sitting boundary, not a regression
    sg = res["sittings"]
    assert [(s["how"], s["start_turn_seq"], s["n_turns"]) for s in sg] == [("first", 1, 2), ("after_end_time", 3, 2)]
    ev = [e for e in res["events"] if e.get("hhmm") == "10:02"][0]
    assert ev["sitting_boundary"] and ev["time_regress_boundary"] and ev["sitting_seq"] == 2
    assert res["stats"]["n_sittings"] == 2 and res["stats"]["n_turns_later_sittings"] == 2
    # later sittings without a printed date: no date when later_sitting_date='null'
    res = H.parse_minutes(_doc(MULTI_SITTING, COVER), later_sitting_date="null")
    assert [x["speech_date"] for x in res["turns"]] == ["2011-06-01", "2011-06-01", None, None]
    # a morning session printed after the afternoon one
    body = ["(13시53분 개의)", "◯委員長 秋美愛  개의합니다.", "(14시12분 산회)", "", "【오전회의 내용】", "(10시37분)",
            "◯홍희덕 위원  위원장님, 오늘 어떻게 하실 요량이십니까?"]
    res = H.parse_minutes(_doc(body))
    assert [x["sitting_seq"] for x in res["turns"]] == [1, 2]
    assert res["sittings"][1]["how"] == "sitting_head"
    assert [e["kind"] for e in res["events"]].count("sitting_head") == 1


def test_time_markers_midnight_and_regressions():
    body = ["(23시38분)", "◯위원장 홍길동  계속하겠습니다.", "(00시01분 감사종료)"]
    res = H.parse_minutes(_doc(body, COVER))
    ev = res["events"][-1]
    assert ev["implicit_rollover"] and ev["new_date"] == "2011-06-02"
    body = ["(23시10분)", "◯위원장 홍길동  산회하겠습니다.", "(24시 산회)"]
    res = H.parse_minutes(_doc(body, COVER))
    ev = res["events"][-1]
    assert (ev["hhmm"], ev["hhmm_printed"], ev["new_date"]) == ("00:00", "24:00", "2011-06-02")
    body = ["(21시20분)", "◯위원장 홍길동  계속합니다.", "(6월1일 24시 경과)", "◯김영희 위원  질의합니다."]
    t = H.parse_minutes(_doc(body, COVER))["turns"]
    assert (t[1]["time_hhmm"], t[1]["speech_date"], t[1]["speech_date_how"]) == ("00:00", "2011-06-02", "rollover")
    # a printed time earlier than the previous one in the same sitting is flagged, not corrected
    body = ["(16시12분 감사중지)", "(14시30분 감사계속)", "◯위원장 홍길동  감사를 계속하겠습니다.",
            "(16시40분)", "◯김영희 위원  질의합니다."]
    res = H.parse_minutes(_doc(body, COVER))
    assert [x["time_regress"] for x in res["turns"]] == [True, False]
    assert sum(1 for e in res["events"] if e.get("time_regress")) == 1 and res["stats"]["n_time_regress"] == 1


def test_md_date_leap_day_and_nearest_year():
    import datetime as dt
    assert H._md_date("2", "29", dt.date(2013, 9, 1)) == dt.date(2012, 2, 29)
    assert H._md_date("2", "29", dt.date(2012, 9, 1)) == dt.date(2012, 2, 29)
    assert H._md_date("12", "31", dt.date(2012, 1, 5)) == dt.date(2011, 12, 31)
    assert H._md_date("1", "2", dt.date(2011, 12, 30)) == dt.date(2012, 1, 2)
    body = ["(2월29일 10시00분)", "◯위원장 홍길동  개의합니다."]
    res = H.parse_minutes(_doc(body, ["일시  2013년9월1일(일)"]))
    assert res["turns"][0]["speech_date"] == "2012-02-29"


def test_running_header_with_committee_name_and_investigation_cover():
    for rh, exp in (("제284회-보건복지가족제10차", "제10차"), ("제280회-행정안전소위1차(2009년2월3일)", "제1차"),
                    ("제294회－제15차", "제15차"), ("2009년도국감-행정안전제2반", None)):
        res = H.parse_minutes(_doc(["◯위원장 홍길동  개의합니다."], header=(rh,)))
        assert res["meeting"].get("sitting") == exp, rh
    lines = [(0, "第301回國會"), (0, "(臨時會․閉會中)"), (1, "貯蓄銀行非理疑惑眞相糾明을위한國政調査特別委員會調査錄"),
             (2, "國 會 事 務 處"), (3, " 被調査機關  釜山地方國稅廳"), (3, " 日  時  2011年7月25日(月)")]
    m, _ = H._parse_cover(lines)
    assert m["committee_raw"] == "貯蓄銀行非理疑惑眞相糾明을위한國政調査特別委員會"
    assert m["investigated_agencies"] == ["釜山地方國稅廳"] and m["cover_notes"] == [] and m["date"] == "2011-07-25"


def test_label_missing_is_null_and_keeps_the_character():
    res = H.parse_minutes(_doc(["◯위원장 홍길동  질의하시지요.", "◯`", "  서울대병원장님!", "◯위원장 홍길동  네."]))
    t = res["turns"]
    assert t[1]["speaker_label_raw"] is None and t[1]["label_how"] == "label_missing"
    assert t[1]["text_raw"].startswith("`")
    assert res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


def test_text_controls_rendered_or_counted():
    # 글자 겹침 (tcps) shows its text inline; an unknown text-less control is counted by id
    tcps = rec(H.TAG_CTRL_HEADER, 1, "tcps"[::-1].encode("latin-1") + struct.pack("<H", 2) + w("①②") + b"\0" * 4)
    other = rec(H.TAG_CTRL_HEADER, 1, ctrl_header("xyzw"))
    sec = paragraph(0, w("a") + ctrl8(23, "tcps") + w("b") + ctrl8(21, "xyzw") + w("c") + c1(13), tcps + other)
    paras, st = H.paragraphs_from_sections([sec])
    assert [p["text"] for p in paras] == ["a①②bc"]
    assert st["ctrl_text_tcps"] == 1 and st["ctrl_unrendered_xyzw"] == 1


def test_16th_term_layout_cover_runhead_and_rule_line():
    # the 16대 layout (HWP fallback files): session, title and number on one cover line, Hanja
    # running header, a rule line before an indented agenda heading
    lines = [(0, "第227回國會 女性特別委員會會議錄 第 2 號"), (1, "  (\uf9f6時會)"), (2, "國 會 事 務 處"),
             (3, "日  時  2002年2月26日(火)")]
    m, _ = H._parse_cover(lines)
    assert (m["session_no"], m["committee_raw"], m["doc_no"], m["subcommittee"], m["date"]) == \
        (227, "女性特別委員會", 2, None, "2002-02-26")
    assert m["session_type"] == "\uf9f6時會"
    m, _ = H._parse_cover([(0, "文化觀光委員會會議錄\t第 1 號  "), (1, "國會本會議會議錄"), (1, "(임 시 회 의 록)")])
    assert m["doc_no"] == 1 and m["committee_raw"] == "文化觀光委員會" and m.get("provisional") and m["subcommittee"] is None
    body = ["○委員長 崔在昇  수고하셨습니다.", "ꠏ" * 40, "  2. 2001회계연도한국방송공사결산승인안",
            "(14시35분)", "○委員長 崔在昇  다음은 결산승인안을 심의할 순서입니다."]
    res = H.parse_minutes(_doc(body, header=("(第230回―文化觀光第1次)",)))
    assert res["meeting"]["sitting"] == "제1차"
    assert [t["text_raw"] for t in res["turns"]] == ["수고하셨습니다.", "다음은 결산승인안을 심의할 순서입니다."]
    assert [a["match"] for a in res["agenda"]] == ["after_rule"] and res["turns"][1]["agenda_ordinal"] == 1
    assert [e["kind"] for e in res["events"]].count("rule") == 1
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


# ----------------------------------------------------------------------------- golden files
# Expected values were recorded from the parser output after validation against the v9 XLSX
# rows (validate_hwp_parser.py --make-golden). XLSX agreement is re-checked when the extracted
# v9 rows are on disk.

def _find(conf_num):
    for p in (RAW_HWP / f"{conf_num // 1000:03d}" / f"{conf_num}.hwp", SAMPLES_HWP / f"{conf_num}.hwp"):
        if p.exists():
            return p
    return None


GOLDEN = json.loads((HERE / "test_hwp_parser_golden.json").read_text(encoding="utf-8")) \
    if (HERE / "test_hwp_parser_golden.json").exists() else {}


@pytest.mark.parametrize("conf_num", sorted(GOLDEN, key=int))
def test_golden(conf_num):
    exp = GOLDEN[conf_num]
    p = _find(int(conf_num))
    if p is None:
        pytest.skip(f"{conf_num}.hwp not on disk")
    res = H.parse_hwp(p.read_bytes(), conf_num=int(conf_num))
    assert res["status"] == exp["status"]
    s = res["stats"]
    assert s["n_turns"] == exp["n_turns"]
    assert s["n_unassigned"] == 0 and s["counters"].get("unit_char_mismatch", 0) == 0
    assert s["chars_turns"] == s["chars_turn_items"]
    turns = res["turns"]
    assert [t["turn_seq"] for t in turns] == list(range(1, len(turns) + 1))
    assert all(t["source"] == "hwp" and t["conf_num"] == int(conf_num) for t in turns)
    assert [t["speaker_label_raw"] for t in turns[:3]] == exp["first_labels"]
    assert [t["speaker_label_raw"] for t in turns[-2:]] == exp["last_labels"]
    assert sum(len(H.nows(t["text_raw"])) for t in turns) == exp["chars_text_raw"]
    assert sum(len(H.nows(t["text"])) for t in turns) == exp["chars_text"]
    assert res["meeting"].get("date") == exp["date"]
    assert res["meeting"].get("committee_raw") == exp["committee_raw"]
    assert res["meeting"].get("subcommittee") == exp["subcommittee"]
    assert s["appendix_how"] == exp["appendix_how"]
    assert s["n_agenda_anchors"] == exp["n_agenda_anchors"]
    assert s["n_time_markers"] == exp["n_time_markers"]
    assert len(res["footer"]) == exp["n_footer_sections"]
    assert s["counters"].get("orphan_other", 0) == exp["orphan_other"]
    # fields recorded since the review fixes (older entries do not carry them)
    if "n_sittings" in exp:
        assert s["n_sittings"] == exp["n_sittings"]
        assert [[sg["sitting_seq"], sg["start_turn_seq"], sg["date"], sg["date_how"]]
                for sg in res["sittings"]] == exp["sittings"]
    if "n_turns_after_end_marker" in exp:
        assert s["n_turns_after_end_marker"] == exp["n_turns_after_end_marker"]
    if "n_turns_after_final_end_marker" in exp:
        assert s["n_turns_after_final_end_marker"] == exp["n_turns_after_final_end_marker"]
        assert sum(1 for t in turns if t["after_final_end_marker"]) == exp["n_turns_after_final_end_marker"]
    if "n_weak_labels" in exp:
        import validate_hwp_parser as V
        assert sum(1 for t in turns if t.get("label_how") not in V.STRONG_HOW) == exp["n_weak_labels"]
        assert sum(1 for r in V.anomaly_rows(res, int(conf_num)) if r["kind"] == "name_list_text") == \
            exp["n_name_list_turns"]
    if "label_counts" in exp:
        # label -> turns, pinned for files without an XLSX reference (16대 files that agree with the
        # viewer XML label for label)
        got = {}
        for t in turns:
            got[t["speaker_label_raw"]] = got.get(t["speaker_label_raw"], 0) + 1
        assert got == exp["label_counts"]
    if "xlsx_n_rows" in exp:
        import validate_hwp_parser as V
        if not V.ROWS.exists():
            pytest.skip("v9 XLSX rows not extracted")
        import duckdb
        rows = V.load_rows([exp["v9_meeting_id"]], duckdb.connect()).get(exp["v9_meeting_id"], [])
        assert len(rows) == exp["xlsx_n_rows"]
        m, _ = V.compare(res, rows)
        # regression guard: never below the recorded agreement (the residual differences of the
        # recorded meetings are XLSX-side defects, see validate_hwp_parser residual classes)
        assert m["pos_agree_dueum"] >= exp["pos_agree_dueum"] - 1e-9
        assert m["aligned_agree_dueum"] >= exp["aligned_agree_dueum"] - 1e-9


# ----------------------------------------------------------------------------- R1 (2026-09-26 audit fixes)

def test_audit_cover_agencies():
    """국정감사 covers: '被監査機關' (and '報告機關' on a cover that says 國政監査) give audited_agencies,
    split at the viewer's separators but never inside parentheses; '被調査機關' stays investigated."""
    lines = [(0, "2011年度"), (0, "國政監査"), (1, "知識經濟委員會會議錄"), (2, "國 會 事 務 處"),
             (3, " 被監査機關  中小企業廳․中小企業振興公團․韓國벤처投資株式會社"), (3, " 日  時  2011年9月20日(火)")]
    m, _ = H._parse_cover(lines)
    assert m["audited_agencies"] == ["中小企業廳", "中小企業振興公團", "韓國벤처投資株式會社"]
    assert m["audited_agencies_raw"] == ["中小企業廳․中小企業振興公團․韓國벤처投資株式會社"]
    assert m["audited_agencies_label"] == "被監査機關" and "investigated_agencies" not in m
    assert m["cover_notes"] == ["2011年度", "國政監査"] and m["date"] == "2011-09-20"
    lines = [(0, "2008年度"), (0, "國政監査"), (1, "企劃財政委員會會議錄"), (1, "第  2 班"),
             (2, " 報告機關 韓國銀行光州全南本部(全北本部 포함)"), (3, " 日  時  2008年10月16日(木)")]
    m, _ = H._parse_cover(lines)
    assert m["audited_agencies"] == ["韓國銀行光州全南本部(全北本部 포함)"]
    assert m["audited_agencies_label"] == "報告機關" and "報告機關" not in " ".join(m["cover_notes"])
    # '報告機關' on a cover that is not an audit stays a cover note (unchanged behaviour)
    m, _ = H._parse_cover([(0, "第300回國會"), (1, "本會議會議錄"), (2, "報告機關 韓國銀行")])
    assert "audited_agencies" not in m and m["cover_notes"] == ["報告機關 韓國銀行"]


def test_split_agencies_paren_aware():
    assert H.split_agencies("韓國銀行全北本部(光州全南․大田忠南․忠北 本部 포함)") == \
        ["韓國銀行全北本部(光州全南․大田忠南․忠北 本部 포함)"]
    assert H.split_agencies("A․B(C, D)|E，F·G") == ["A", "B(C, D)", "E", "F", "G"]
    assert H.split_agencies("") == [] and H.split_agencies(" ․ ") == []


def test_after_end_marker_resets_at_new_sitting_and_final_marker():
    res = H.parse_minutes(_doc(MULTI_SITTING, COVER))
    t = res["turns"]
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2]
    # the second sitting starts after '(12시16분 산회)': not after an end of its own sitting ...
    assert [x["after_end_marker"] for x in t] == [False, False, False, False]
    # ... but after the document's last end marker (the second sitting prints none)
    assert [x["after_final_end_marker"] for x in t] == [False, False, True, True]
    # the body is closed after an end marker until an accepted sitting start, and a new sitting
    # resets the flag, so HWP turns never carry after_end_marker; a final end marker after the last
    # turn flags nothing
    body = ["(10시00분 개의)", "◯위원장 홍길동  개의합니다.", "(11시00분 산회)"]
    t = H.parse_minutes(_doc(body, COVER))["turns"]
    assert not t[0]["after_end_marker"] and not t[0]["after_final_end_marker"]


def test_first_text_raw_keeps_spacing():
    body = ["◯위원장 홍길동  위원장 홍길동  개의합니다.", "◯김철수 위원  질문합니다."]
    t = H.parse_minutes(_doc(body, COVER))["turns"]
    assert t[0]["first_text_raw"] == "위원장 홍길동  개의합니다."
    assert t[0]["text"] == "위원장 홍길동 개의합니다."       # the parser keeps the text as printed
    assert t[1]["first_text_raw"] == "질문합니다."


# ----------------------------------------------------------------------------- 16대 layout (2026-09-28)
# Rules for the 16대 / 17대 files (hwp_parser_16 task): ideographic-space separator, spaced Hanja
# names, Hanja titles and surnames, label trimming and joins, marker agenda headings, mid-line
# markers and body tables that hold speech. Each test names the rule it covers.

IDEO = "　"


def _sl(line, lex=None):
    r = H._speaker_line(line, lex)
    return r[:3] if r else None


def test_ideographic_space_separator_16th_layout():
    # R1: the label ends at one U+3000; ASCII double spaces inside it only justify position and name
    assert _sl("○委員長  李允洙" + IDEO + "의석을 정돈해 주시기 바랍니다.") == \
        ("委員長 李允洙", "의석을 정돈해 주시기 바랍니다.", "sep")
    assert _sl("○金文洙  委員" + IDEO + "위원장님! 의사진행발언입니다.")[:2] == ("金文洙 委員", "위원장님! 의사진행발언입니다.")
    assert _sl("○委員長 李  協" + IDEO + "의석을")[0] == "委員長 李 協"
    assert _sl("○薛  勳委員" + IDEO + "여론조사한 결과물이")[0] == "薛 勳委員"
    assert _sl("○大法官候補者 李揆弘" + IDEO + "“선서. 공직후보자인 본인은")[:2] == ("大法官候補者 李揆弘", "“선서. 공직후보자인 본인은")
    assert _sl("○李良熙委員" + IDEO + "위원장, 질의에 앞서서…….")[0] == "李良熙委員"
    assert _sl("○證人" + IDEO + "예.")[:2] == ("證人", "예.")
    # 'POS' + U+3000 + 'NAME  text': the name joins the label
    assert _sl("○國民生活體育協議會長" + IDEO + "嚴三鐸  국민생활체육협의회 회장입니다.")[:2] == \
        ("國民生活體育協議會長 嚴三鐸", "국민생활체육협의회 회장입니다.")
    # a spaced name inside a justified piece, a rare Hanja surname, a Latin name
    assert _sl("○韓國勞動敎育院長  李 銑" + IDEO + "한국노동교육원장입니다.")[0] == "韓國勞動敎育院長 李 銑"
    assert _sl("○韓國冷藏株式會社社長 心基燮" + IDEO + "우리 정부에서")[0] == "韓國冷藏株式會社社長 心基燮"
    assert _sl("○證人 Mr. Michael Richter" + IDEO + "Yes.")[0] == "證人 Mr. Michael Richter"
    # appendix lines with an ideographic space are no labels; a double-space label keeps its U+3000 text
    assert _sl("○현황" + IDEO + "－실적：총 682개 주제") is None
    sl = H._speaker_line("○2003 예 산      199억 3100만 원")
    assert sl is None or not H._label_shape_ok(sl[0])
    assert _sl("○委員長 李祥羲  金 위원" + IDEO + "말씀하세요.")[:2] == ("委員長 李祥羲", "金 위원" + IDEO + "말씀하세요.")


def test_spaced_names_hanja_titles_and_split_label():
    # R2 / R5 / R11: spaced two-syllable names, 'NAME 委員長', positions that end in 委員
    assert H.split_label("委員長 李 協") == ("委員長", "李協", "pos_name")
    assert H.split_label("薛 勳委員") == ("委員", "薛勳", "fused")
    assert H.split_label("南宮 晳議員") == ("議員", "南宮晳", "fused")
    assert H.split_label("李 協 委員") == ("委員", "李協", "name_pos")
    assert H.split_label("劉容泰 委員長") == ("委員長", "劉容泰", "name_pos")
    assert H.split_label("委員長 金成長") == ("委員長", "金成長", "pos_name")
    assert H.split_label("首席專門委員")[:2] == ("首席專門委員", None)
    assert H.split_label("전문위원")[:2] == ("전문위원", None)
    assert H.split_label("宋榮珍議員") == ("議員", "宋榮珍", "fused")
    assert H.split_label("2003 예 산")[1] == "산"          # a spaced word is not a spaced name
    # a spaced name justified with the separator's double space (double-space layout)
    assert _sl("○서울特別市長 高  建  “선서. 본인은")[:2] == ("서울特別市長 高 建", "“선서. 본인은")
    # R4: Hanja titles and surnames
    for pos in ("大法官候補者", "國防部次官補", "證人", "陳述人", "韓國銀行總裁"):
        assert H._pos_attested(pos, H._Lexicon()), pos
    assert _sl("○委員長 田瑢源" + IDEO + "개의하겠습니다.")[0] == "委員長 田瑢源"
    assert _sl("○國防部次官補 李鍾圭 제가 말씀올리겠습니다.") == \
        ("國防部次官補 李鍾圭", "제가 말씀올리겠습니다.", "single_space_pos_name")
    # R16: spaced Hanja names in the single-space steps, never Hangul words
    assert _sl("○薛 勳委員 金周慶 증인 계십니까?")[:2] == ("薛 勳 委員", "金周慶 증인 계십니까?")
    assert _sl("○國防部調達本部物資部長 張 熺 물자부장입니다.")[0] == "國防部調達本部物資部長 張 熺"
    assert _sl("◯위원장 이 건 관련해서 말씀드리면") is None


def test_label_trim_and_name_joins():
    lex = H._Lexicon({"朴明煥 委員": 10, "반장 김성곤": 70, "정해걸 위원": 40, "金鎭載委員": 5,
                      "環境管理公團專務理事 金德治": 3, "首席專門委員": 3, "李在禎 委員": 3})
    # R12: a label that swallowed speech is cut back to a title / document label
    assert _sl("○國防部長官 趙成台 그것은 장관책임입니다마는······  ", lex)[:2] == \
        ("國防部長官 趙成台", "그것은 장관책임입니다마는······ ")
    assert _sl("○委員長 千容宅 의사일정  제1항을 상정합니다.", lex)[:2] == ("委員長 千容宅", "의사일정 제1항을 상정합니다.")
    assert _sl("○朴承國委員 한나라당  朴承國 위원입니다.", lex)[0] == "朴承國委員"
    assert _sl("◯朴明煥 委員-  따라서", lex)[:3] == ("朴明煥 委員", "- 따라서", "sep_trimmed_by_lexicon")
    assert _sl("◯반장 김성곤｣  수고하셨습니다.", lex)[0] == "반장 김성곤"
    assert _sl("◯정해걸 위원걸  그런데", lex)[0] == "정해걸 위원걸"     # a stray syllable stays (XLSX text)
    # R8: a label that has its name sheds a repeated name; one without a name never loses it
    assert _sl("○金鎭載委員 金鎭載  위원입니다.", lex)[:2] == ("金鎭載委員", "金鎭載 위원입니다.")
    assert _sl("◯국토해양부장관 정종환  답변드리겠습니다.", H._Lexicon({"국토해양부장관": 3}))[0] == "국토해양부장관 정종환"
    # names are never trimmed off
    for line, label in (("◯한국투자공사투자운용본부장 구안 옹  네.", "한국투자공사투자운용본부장 구안 옹"),
                        ("◯국무총리 후보자 김태호  네.", "국무총리 후보자 김태호"),
                        ("◯전문위원 박기준 위원  네.", "전문위원 박기준 위원"),
                        ("◯미합중국대통령 도널드 J. 트럼프  존경하는", "미합중국대통령 도널드 J. 트럼프")):
        assert _sl(line, lex)[0] == label
    # R3 / R13 / R11: 'POS  NAME' joins
    assert _sl("○서울올림픽記念國民體育振興公團理事長  崔一鴻 ", lex)[:2] == ("서울올림픽記念國民體育振興公團理事長 崔一鴻", "")
    assert _sl("○環境管理公團專務理事  金德治 예, 청소하고 있습니다.", lex)[:3] == \
        ("環境管理公團專務理事 金德治", "예, 청소하고 있습니다.", "sep_joined_pos_name")
    assert _sl("◯首席專門委員  朴峰秀  과태료로 되어 있지요?", lex)[0] == "首席專門委員 朴峰秀"
    assert _sl("○李在禎  委員 네, 말씀드리겠습니다.", lex)[0] == "李在禎 委員"
    assert _sl("○松雄委員  金德培  말씀하세요.", lex)[0] == "松雄委員"
    assert _sl("○沈揆喆  沈揆喆 위원입니다.", lex)[0] == "沈揆喆"
    # R6: a heading split at a spaced name inside parentheses is not a label
    sl = H._speaker_line("○간사(黃祐呂‧薛  勳)인사", lex)
    assert sl is None or sl[2] == "label_only"
    # R16: a document label glued to digits
    lex2 = H._Lexicon({"鐵道廳企劃本部企劃豫算課建設投資팀長 崔德律": 3})
    assert _sl("◯鐵道廳企劃本部企劃豫算課建設投資팀長 崔德律105억 원을", lex2) == \
        ("鐵道廳企劃本部企劃豫算課建設投資팀長 崔德律", "105억 원을", "lexicon_prefix_fused")


def test_marker_lines_that_are_agenda_headings():
    # R7 / R14: marker lines repeating a cover agenda item, one-word non-labels and name lists
    cover = ["第212回國會", "(臨時會)", "國務總理(李漢東)任命同意에관한人事聽聞特別委員會會議錄", "第1號", "國會事務處",
             "日  時  2000年6月14日(水)", "議事日程", "○ 위원장(金德圭)인사", "○ 간사(安商守‧薛 勳)인사"]
    body = ["○委員長 金德圭" + IDEO + "위원장 인사를 드리겠습니다.",
            "○위원장(金德圭)인사",
            "○委員長 金德圭" + IDEO + "감사합니다.",
            "○간사(安商守․薛  勳)인사",
            "○安商守委員" + IDEO + "간사 安商守입니다.",
            "○5분자유발언",
            "○金榮春委員" + IDEO + "발언하겠습니다.",
            "○참고인(崔祐英‧崔成龍) 신문",
            "○委員長 金德圭" + IDEO + "참고인 신문을 시작합니다.",
            "○진술인",
            "  진술인입니다."]
    res = H.parse_minutes(_doc(body, cover, header=("(第212回―人事聽聞第1次)",)))
    assert [t["speaker_label_raw"] for t in res["turns"]] == \
        ["委員長 金德圭", "委員長 金德圭", "安商守委員", "金榮春委員", "委員長 金德圭", "진술인"]
    assert [a["match"] for a in res["agenda"]] == \
        ["marker_cover_match", "marker_cover_match", "marker_heading", "marker_heading"]
    assert res["turns"][5]["text_raw"] == "진술인입니다."
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]
    assert not H._label_only_ok("중국여객기 추락사고 보고") and H._label_only_ok("대통령비서실장 韓光玉")


def test_midline_marker_rules_16th_layout():
    lex = H._Lexicon({"保健福祉部長官 金花中": 30, "海洋水産部長官 柳三男": 5, "許泰烈 委員": 6,
                      "위원장대리 이광재": 3, "박종근 위원": 4})
    sm = H._split_midline
    # R9: stray characters at the line start, a marker glued to a label-only head
    assert sm("-◯保健福祉部長官 金花中  예.", lex) == ["-", "◯保健福祉部長官 金花中  예."]
    assert sm("○海洋水産部長官 柳三男○許泰烈 委員" + IDEO + "장관님, 제가", lex) == \
        ["○海洋水産部長官 柳三男", "○許泰烈 委員" + IDEO + "장관님, 제가"]
    # after a sentence end: a title label set off by the separator counts without the lexicon
    assert sm("답변하십시오. ○環境部長官 金明子" + IDEO + "예, 알겠습니다.", lex) == \
        ["답변하십시오. ", "○環境部長官 金明子" + IDEO + "예, 알겠습니다."]
    # never inside an open quotation or parentheses, never without the separator or a title
    for line in ("그때 회의록에 “그러면 ○委員長 金明潤  알겠습니다” 라고",
                 "채택합니다. (◯박지원 의원 의석에서 ― 뭐요?)",
                 "답변하십시오. ○環境部長官  예, 알겠습니다.", "항목은 ○인건비 ○운영비", "○○○ 씨가 출석했습니다."):
        assert sm(line, lex) == [line], line
    # R10: a bare marker before a speaker marker of the same paragraph is text of the running turn
    body = ["◯위원장대리 이광재  질의하시지요.", "◯박종근 위원  네.", "◯! ○박종근 위원  李차관, 이 법은", "◯위원장대리 이광재  네."]
    res = H.parse_minutes(_doc(body))
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["위원장대리 이광재", "박종근 위원", "박종근 위원", "위원장대리 이광재"]
    assert t[1]["text_raw"] == "네.\n◯!" and t[2]["text_raw"] == "李차관, 이 법은"
    assert res["stats"]["counters"]["label_missing_before_marker"] == 1
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


def test_body_table_that_holds_speech_is_read_as_body():
    # R15: speech that runs on inside a table frame; a data table without a named label is kept
    tbl = {"in_table": True, "table_id": 7, "outer_table_id": 7, "container": "table", "depth": 1}
    body = ["○委員長 李揆澤" + IDEO + "토론하십시오.",
            "○金敬天委員" + IDEO + "서남대 문제는 잘 해보려고 애를 쓰는",
            _p("데 거기에다가 충격을 가하면 되겠는가 하는 생각이 들었습니다.", row=0, col=0, cell=1, **tbl),
            _p("○委員長 李揆澤" + IDEO + "아주 진지한 토론을 했습니다.", row=0, col=0, cell=1, **tbl),
            _p("    (｢없습니다｣하는 위원 있음)", row=0, col=0, cell=1, **tbl),
            "○委員長 李揆澤" + IDEO + "가결되었음을 선포합니다.",
            _p("○기본사업비  199억", in_table=True, table_id=8, outer_table_id=8, row=0, col=0, cell=1,
               container="table", depth=1),
            _p("○기본사업비  181억", in_table=True, table_id=8, outer_table_id=8, row=1, col=0, cell=2,
               container="table", depth=1)]
    res = H.parse_minutes(_doc(body))
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["委員長 李揆澤", "金敬天委員", "委員長 李揆澤", "委員長 李揆澤"]
    assert t[1]["text_raw"] == "서남대 문제는 잘 해보려고 애를 쓰는\n데 거기에다가 충격을 가하면 되겠는가 하는 생각이 들었습니다."
    assert t[2]["stage_kinds"] and t[2]["text"] == "아주 진지한 토론을 했습니다."
    assert t[3]["n_table_lines"] == 2            # the budget table stays a table of the turn
    c = res["stats"]["counters"]
    assert c["table_read_as_body"] == 1 and c.get("unit_char_mismatch", 0) == 0
    assert res["stats"]["n_unassigned"] == 0 and res["stats"]["chars_turns"] == res["stats"]["chars_turn_items"]


# ----------------------------------------------------------------------------- appendix after the end (2026-09-28)
# Material printed after the meeting-end marker (audit of 2026-09-28: 245 pseudo-turns in 10 meetings, report
# text glued to the chair's closing turn): the running turn ends at the marker, and a new sitting starts only on
# evidence (opening marker, or a cover / cover lines / sitting heading / time marker followed by a speaker line
# with an attested label). Everything else is appendix, kept verbatim, never turns or turn text.

def _texts(res):
    """Every footer and event text of a parse (to check that nothing is dropped)."""
    out = [e["text"] for e in res["events"]]
    for f in res["footer"]:
        out += [f["title"] or ""] + f["lines"] + [c["text"] for tb in f["tables"] for c in tb["cells"]]
    return out


def test_material_after_the_end_marker_is_appendix_not_turns():
    # 25765 / 25595 / 26068 pattern: 산회, then an appended review report and budget items ('○2003 예산안  …',
    # '○인 건 비  …'), then written 제안설명서 whose speakers are members of the committee
    body = ["(10시05분 개의)",
            "◯委員長 鄭均桓  개의하겠습니다.",
            "◯李訓平 委員  질의하겠습니다.",
            "◯委員長 鄭均桓  오늘 회의는 이것으로 마치겠습니다.",
            "  산회를 선포합니다.",
            "(18시36분 산회)",
            "(참 조)",
            "예산안개요】",
            "○2003 예산안  3억 7300만 원",
            "○인 건 비  1,274억5,000만원(1.9% 증),",
            "【제안설명서】",
            "◯李訓平 委員  존경하는 위원장님 그리고 위원 여러분!",
            "  정무위원회 새천년민주당 이훈평 의원입니다.",
            "◯鄭義和 委員  국가를당사자로하는계약에관한법률중개정법률안에 대한 제안 설명을 드리겠습니다.",
            "◯출석 위원(2인)",
            "李訓平  鄭義和"]
    res = H.parse_minutes(_doc(body, COVER))
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["委員長 鄭均桓", "李訓平 委員", "委員長 鄭均桓"]
    # the chair's closing turn ends at the end marker
    assert t[-1]["text_raw"] == "오늘 회의는 이것으로 마치겠습니다.\n산회를 선포합니다."
    assert [x["sitting_seq"] for x in t] == [1, 1, 1] and len(res["sittings"]) == 1
    assert not any(x["after_end_marker"] or x["after_final_end_marker"] for x in t)
    s = res["stats"]
    assert s["appendix_how"] == "after_end_marker" and s["appendix_moved"]["marker"] == "(18시36분 산회)"
    # (the attendance list was appendix already: 8 more lines move from the body to the appendix)
    assert s["appendix_moved"]["from_how"] == "appendix_heading" and s["appendix_moved"]["items"] == 8
    # kept verbatim in the footer, nothing dropped, every character accounted for
    got = _texts(res)
    for line in body[6:]:
        assert line in got, line
    assert s["n_unassigned"] == 0 and s["chars_turns"] == s["chars_turn_items"]
    ev = [e for e in res["events"] if e.get("is_end")]
    assert len(ev) == 1 and not ev[0]["within_turn"] and ev[0]["after_turn_seq"] == 3


def test_recess_that_never_resumed_ends_the_meeting():
    # 23930 pattern: '(계속개의되지 않았음)' printed inside the chair's turn, then a report
    body = ["◯委員長代理 千正培  정회를 선포합니다.",
            "(16시18분 회의중지)",
            "(계속개의되지 않았음)",
            "………………………",
            "검토보고서】",
            "◯성과상여금(신규)  15억7,400만원",
            "◯경상 및 기준성 기본사업비  △1억300만원"]
    res = H.parse_minutes(_doc(body, COVER))
    t = res["turns"]
    assert [x["speaker_label_raw"] for x in t] == ["委員長代理 千正培"]
    assert t[0]["text_raw"] == "정회를 선포합니다."
    note = [e for e in res["events"] if e["kind"] == "note"]
    assert note and note[0]["is_end"] and note[0]["text"] == "(계속개의되지 않았음)"
    assert res["stats"]["appendix_how"] == "after_end_marker"
    assert "◯성과상여금(신규)  15억7,400만원" in _texts(res)
    assert t[0]["after_final_end_marker"] is False


def test_sitting_start_after_the_end_needs_evidence():
    head = ["(10시00분 개의)", "◯위원장 이병석  개의하겠습니다.", "◯위원장 이병석  감사 종료를 선포합니다.",
            "(17시38분 감사종료)", ""]
    # an opening marker starts a sitting by itself
    res = H.parse_minutes(_doc(head + ["(14시40분 감사계속)", "◯허천 위원  질의하겠습니다."], COVER))
    assert [x["sitting_seq"] for x in res["turns"]] == [1, 1, 2]
    assert res["sittings"][1]["how"] == "after_end_time"
    # ... also when printed without a clock
    res = H.parse_minutes(_doc(head + ["(계속개의)", "◯2003 예산안  3억 원"], COVER))
    assert [x["sitting_seq"] for x in res["turns"]] == [1, 1, 2] and res["sittings"][1]["how"] == "after_end_open"
    assert res["turns"][2]["time_hhmm"] is None
    # a clock marker with an action _time_line does not take, followed by attested speakers (32689)
    body = head + ["국정감사 개시 전 독도 연결 화상통화 내용", "(13시53분 화상통화 개시)",
                   "◯위원장 이병석  김성도 선생님, 안녕하십니까?", "◯독도주민 김성도  예, 안녕하십니까?"]
    res = H.parse_minutes(_doc(body, COVER))
    t = res["turns"]
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2] and res["sittings"][1]["how"] == "after_end_clock"
    assert t[1]["text_raw"] == "감사 종료를 선포합니다."       # the heading is not glued to the closing turn
    ev = [e for e in res["events"] if e["text"] == "(13시53분 화상통화 개시)"][0]
    assert ev["kind"] == "time" and ev["hhmm"] == "13:53" and ev["sitting_boundary"]
    assert t[2]["time_hhmm"] == "13:53"
    assert [e["text"] for e in res["events"] if e.get("after_end")] == ["국정감사 개시 전 독도 연결 화상통화 내용"]
    # a time marker followed by an appendix line that is no attested label: no sitting, appendix
    body = head + ["(10시)", "○2003 예산안  3억 7300만 원", "○2002 예산  2억 원"]
    res = H.parse_minutes(_doc(body, COVER))
    assert [x["speaker_label_raw"] for x in res["turns"]] == ["위원장 이병석", "위원장 이병석"]
    assert res["stats"]["counters"]["sitting_start_rejected_after_end_time"] == 1
    assert res["stats"]["appendix_how"] == "after_end_marker"
    # an audit plan table with '일 시' and a date is no cover; its budget-item 'labels' are not attested (24634)
    tbl = _p("일      시\n대 상 기 관\n감 사 장 소\n2001.9.15(토)\n10：00\n○기획예산처", in_table=True, table_id=5,
             outer_table_id=5, row=0, col=0, cell=1, container="table", depth=1)
    body = head + [tbl, "○2001년도 주요업무 추진현황", "○기타 필요한 사항"]
    res = H.parse_minutes(_doc(body, COVER))
    assert len(res["turns"]) == 2 and len(res["sittings"]) == 1
    # a speech cover followed by the chair's welcome (30742, 33499): a later sitting dated by the cover
    cover2 = _p("캐나다총리(스티븐 하퍼) 연설\n日  時  2009年12月7日(月) 午後 2時40分\n場  所  國會本會議場",
                in_table=True, table_id=6, outer_table_id=6, row=0, col=0, cell=1, container="table", depth=1)
    body = head + [cover2, "◯위원장 이병석  존경하는 하퍼 총리님!", "◯캐나다총리 스티븐 하퍼  감사합니다."]
    res = H.parse_minutes(_doc(body, COVER))
    t = res["turns"]
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2] and res["sittings"][1]["how"] == "sub_cover"
    assert t[2]["speech_date"] == "2009-12-07" and t[2]["speech_date_how"] == "sub_cover"
    # the same cover followed by speakers that are not attested ('○2003 예산안'): appendix
    body = head + [cover2, "○2003 예산안  3억 7300만 원"]
    res = H.parse_minutes(_doc(body, COVER))
    assert len(res["turns"]) == 2 and res["stats"]["counters"]["sitting_start_rejected_sub_cover"] == 1


def test_sub_cover_and_label_attestation_helpers():
    def table(text):
        return {"kind": "table", "cells": [{"row": 0, "col": 0, "text": text}], "text": text, "table_id": 1}
    # covers: a 회의록 title with a date, a 日時 line whose value is a date (on the line or the next one)
    assert H._sub_cover(table("第251回國會\n(臨時會)\n敎育委員會會議錄\n第  1 號\n國 會 事 務 處\n日  時  2004年12月14日(火)"))
    assert H._sub_cover(table("日  時\n2001년3월2일 오후 2시\n集會根據\n헌법 제47조제1항"))["date"] == "2001-03-02"
    assert H._sub_cover(table("유엔사무총장(반기문) 연설\n\n일  시  2012년10월30일(화) 오전 11시\n장  소  국회본회의장"))
    # not covers: '일시 철수' in a personnel table (23928), an audit schedule headed '일 시 | 대 상 기 관' (24634)
    assert H._sub_cover(table("구    분\n성    명\n미주(미국) 주재관\n이한길 부이사관\n(2000. 4. 1～2003. 3.31)\n"
                              "러시아 주재관\n일시 철수\n(1998. 8. 20)")) is None
    assert H._sub_cover(table("일      시\n대 상 기 관\n감 사 장 소\n감  사  반\n2001.9.15(토)\n10：00")) is None
    lex_pre = H._Lexicon({"위원장 이병석": 5, "李訓平 委員": 1})
    ok = H._label_attested
    assert ok(("위원장 이병석", "안녕하십니까?", "sep"), lex_pre)                # a label of the document
    assert ok(("위원장대리 권경석", "바로 시작하겠습니다.", "sep"), lex_pre)      # title pattern
    assert ok(("홍희덕 위원", "위원장님", "sep"), lex_pre)                        # 'NAME 위원'
    assert not ok(("2001년도 주요업무 추진현황", "", "label_only"), lex_pre)       # a budget item heading
    assert not ok(("중앙인사위원회 위원장 金光雄", "", "label_only"), lex_pre)     # a written-answer header
    assert not ok(("2003 예산안", "3억 7300만 원", "sep"), lex_pre)
    assert not ok(("대 사 김경근", "", "label_only"), lex_pre)                      # an attendee list (27240)
    # end lines and clock lines
    assert H._end_line("(18시36분 산회)") and H._end_line("(계속개의되지 않았음)") and H._end_line("(24시 散會)")
    assert not H._end_line("(16시18분 회의중지)") and not H._end_line("(13시17분 비공개감사종료)")
    assert H._clock_line("(13시53분 화상통화 개시)")["action"] == "화상통화 개시"
    assert H._clock_line("(14시37분)") is None and H._clock_line("(18시 산회)") is None
