"""Tests for build_turns.py (run: python -m pytest -q test_build_turns.py).

Uses saved viewer samples in v10/raw/samples/, a stub hwp_parser, and synthetic v9 rows.
The end-to-end tests build into a temporary output root with a temporary crawl-state db and
raw tree, so they never touch v10/interim/pipeline/.
"""
from __future__ import annotations

import gzip
import sqlite3
import sys
import types
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import build_turns as bt  # noqa: E402

SAMPLES = bt.V10 / "raw" / "samples"
# small pages: 19대 subcommittee, 16대 special committee (Hanja era), 21대 plenary with votes
XML_OK = [37392, 27150, 47924]
XML_BAD = 35402          # 18대: HTTP 400 body saved as sample
PLENARY_VOTES = 47924


def _sample(n):
    return (SAMPLES / f"{n}_view.html.gz").read_bytes()


def _dom_counts(page_gz):
    """Independent recount: non-whitespace chars of outermost span.spk_sub inside div.speaker."""
    from lxml import html as LH
    t = LH.fromstring(gzip.decompress(page_gz))
    body = t.xpath('//div[@id="minutes"]/div[contains(@class,"minutes_body")]')[0]
    n = 0
    for sp in body.iter("span"):
        if sp.get("class") != "spk_sub":
            continue
        anc = sp.getparent()
        in_speaker, nested = False, False
        while anc is not None:
            cls = (anc.get("class") or "").split()
            if anc.tag == "span" and anc.get("class") == "spk_sub":
                nested = True
            if anc.tag == "div" and "speaker" in cls:
                in_speaker = True
            anc = anc.getparent()
        if in_speaker and not nested:
            n += len(bt.nows(sp.text_content()))
    return n


# ----------------------------------------------------------------------------- XML adapter

@pytest.fixture(scope="module")
def xml_results():
    return {n: bt.xml_extract(n, _sample(n), term=None, sha1="x") for n in XML_OK}


def test_xml_status_and_exact_text(xml_results):
    for n, r in xml_results.items():
        assert r["status"] == "ok", n
        cov = r["coverage"]
        assert cov["ok_all"], (n, cov)
        # the task's exact-coverage check: DOM span.spk_sub chars == sum of turn text_raw chars
        # (minus text of embedded non-sentence nodes, which text_raw also keeps)
        dom = _dom_counts(_sample(n))
        assert dom == cov["dom_spk_sub_chars"], n
        tr = sum(len(bt.nows(t["text_raw"])) for t in r["tables"]["turns"])
        assert tr == dom + cov["embedded_chars"], n


def test_xml_contiguous_turn_seq_and_fields(xml_results):
    for n, r in xml_results.items():
        turns = r["tables"]["turns"]
        assert [t["turn_seq"] for t in turns] == list(range(1, len(turns) + 1))
        for t in turns:
            assert t["source"] == "xml" and t["conf_num"] == n
            assert set(bt.CONTRACT_TURN_COLUMNS) <= set(t)
            assert t["speaker_mem_id"] is None or t["speaker_mem_id"] > 0
            # text is a subset of text_raw lines (a stripped label repeat shortens the first line)
            if t["text"] is not None and not t["text_label_prefix_stripped"]:
                assert set(filter(None, t["text"].split("\n"))) <= set(t["text_raw"].split("\n"))
            assert t["has_stage"] == bool(t["stage_texts"])


def test_xml_rollcall_matches_reported_counts(xml_results):
    r = xml_results[PLENARY_VOTES]
    groups = r["tables"]["rollcall_groups"]
    assert groups, "plenary sample has electronic votes"
    for g in groups:
        if g["vote_group"] in ("찬성", "반대", "기권") and g["n_reported"] is not None:
            assert g["n_reported"] == g["n_names"], g
    names = r["tables"]["rollcall"]
    assert len(names) == sum(g["n_names"] for g in groups)
    assert {x["vote_group"] for x in names} <= {"찬성", "반대", "기권", "투표"}
    att = Counter(a["category"] for a in r["tables"]["attendance"])
    assert att["present"] > 0


def test_rollcall_label_rows_and_notes():
    """XML: name rows printed as p.tit_sm labels are names; a parenthetical line is a note.
    HWP: a parenthetical line under a vote list is a note, not a name."""
    sections = [{"title": "【전자투표 찬반 의원 성명】◯A법안", "groups": [
        {"label": "투표의원(5인)", "names": [], "lines": []},
        {"label": "찬성의원(4인)", "names": [], "lines": []},
        {"label": "강기정 박 진 김철수", "names": [], "lines": []},
        {"label": "이영희", "names": [], "lines": []},
        {"label": "기권 의원(1인)", "names": [{"name": "홍길동"}],
         "lines": ["(홍길동 의원 버튼 미조작. 실제 기권 의원 1인임)"]},
        {"label": "◯출석 의원(3인)", "names": [], "lines": []}]}]
    votes, groups, names = bt.rollcall_from_xml_sections(sections)
    g = {x["vote_group"]: x for x in groups}
    assert votes == 1 and g["찬성"]["n_names"] == 4 and g["찬성"]["method"] == "xml_footer_label_rows"
    assert [x["name"] for x in names if x["vote_group"] == "찬성"] == ["강기정", "박진", "김철수", "이영희"]
    assert g["기권"]["n_names"] == 1 and g["기권"]["note"].startswith("(홍길동")
    hsec = [{"title": "◯A법안", "lines": ["  투표 의원(3인)", "  기권 의원(2인)", "홍길동  김철수",
                                          "(박영선 의원 표결기 조작 지체. 실제 기권 의원 2인임)"], "tables": []}]
    att = bt.attendance_from_xml_sections([
        {"title": "◯출석 의원(3인)", "groups": [{"label": None, "names": [], "lines": ["강길부", "박 진", "(1인 착오)"]}]},
        {"title": "◯출석 국무위원", "groups": [{"label": None, "names": [], "lines": ["외교부"]}]}])
    assert [(a["item_kind"], a.get("name")) for a in att] == [
        ("line_name", "강길부"), ("line_name", "박진"), ("note", None), ("line", None)]
    _, hg, hn, _ = bt.rollcall_attendance_from_hwp(hsec)
    k = [x for x in hg if x["vote_group"] == "기권"][0]
    assert k["n_names"] == 2 and k["note"].startswith("(박영선") and len(hn) == 2


def test_xml_bad_request_is_reported_not_raised():
    r = bt.xml_extract(XML_BAD, _sample(XML_BAD))
    assert r["status"] == "bad_request"
    assert r["tables"] == {} and r["coverage"] is None


def test_xml_leading_textnode_and_comment_tail_kept():
    """div.txt text before the first child (17대 pages without span.spk_sub) and the tail of an
    HTML comment are part of text_raw, in document order."""
    page = ('<html><head><meta charset="utf-8"></head><body><div id="header"><div class="tit"><h2>'
            '<strong>제17대국회 제250회 (정기회) 제14차 국회본회의</strong>'
            '<span class="date">2004. 12. 9.</span></h2></div></div>'
            '<div id="minutes"><div class="minutes_body">'
            '<div class="speaker item0" id="spk_1" data-mem_id="0" data-name="김원기" data-pos="의장">'
            '<div class="talk"><div class="txt">의석을 정돈해 주시기 바랍니다.<br/>둘째 문장입니다.'
            '<!-- c -->셋째 문장입니다.<span class="spk_sub" id="s1">넷째.</span></div></div></div>'
            '</div></div></body></html>').encode("utf-8")
    r = bt.xml_extract(2, page)
    t = r["tables"]["turns"][0]
    assert t["text_raw"].split("\n") == ["의석을 정돈해 주시기 바랍니다.", "둘째 문장입니다.", "셋째 문장입니다.", "넷째."]
    assert r["coverage"]["ok_txt"] and r["coverage"]["ok_body"] and r["coverage"]["ok_all"]
    assert r["counters"]["xml_added_textnode"] == 1 and r["counters"]["xml_added_comment_tail"] == 1
    assert bt.pv._speaker_block is bt._PV_SPEAKER_BLOCK   # the swap is undone


def test_footer_appendix_text_kept():
    """Nested minutes_body inside the footer (appendix) is kept as footer lines."""
    page = ('<html><head><meta charset="utf-8"></head><body><div id="header"><div class="tit"><h2>'
            '<strong>제21대 제400회 제1차 국회본회의</strong>'
            '<span class="date">2022. 9. 1.</span></h2></div></div>'
            '<div id="minutes"><div class="minutes_body">'
            '<div class="speaker item0" id="spk_1" data-mem_id="0" data-name="A" data-pos="B">'
            '<div class="talk"><div class="txt"><span class="spk_sub" id="s1">hello world.</span>'
            '</div></div></div></div>'
            '<div class="minutes_footer"><p class="tit">APPX</p>'
            '<div class="minutes_body"><div class="speaker"><div class="talk"><div class="txt">'
            '<span class="spk_sub">appendix speech text</span></div></div></div></div>'
            '</div></div></body></html>').encode("utf-8")
    r = bt.xml_extract(1, page)
    assert r["status"] == "ok"
    lines = [f["line_text"] for f in r["tables"]["footer"] if f["item_kind"] == "line"]
    assert "appendix speech text" in lines
    assert r["coverage"]["ok_footer"] and r["coverage"]["ok_all"]


# ----------------------------------------------------------------------------- XLSX adapter

def _v9_rows():
    base = dict(meeting_id="99999", term=18, hearing_type="상임위원회", committee="국방위원회",
                date="2009-01-02", session="제280회", sub_session="제1차", member_id=None)
    rows = [
        dict(base, speech_order="10", speaker="김철수 위원", agenda="1. A법안", speech_text="질문합니다. (웃음) 네."),
        dict(base, speech_order="2", speaker="위원장 홍길동", agenda="1. A법안", speech_text="개의하겠습니다."),
        dict(base, speech_order="9", speaker="국방부장관 이종섭", agenda="1. A법안", speech_text="답변 드리겠습니다."),
        dict(base, speech_order="1", speaker="위원장 홍길동", agenda=None, speech_text="(10시 00분 개의)"),
        dict(base, speech_order="11", speaker="이수진(비) 위원", agenda="2. B법안", speech_text="의견 있습니다."),
    ]
    return rows


def test_xlsx_numeric_order_split_and_text():
    rows = _v9_rows()
    r = bt.xlsx_meeting_tables(5, 18, rows, v9_meeting_id="99999")
    t = r["tables"]["turns"]
    # numeric order (string order would put '10' and '11' before '2')
    assert [x["source_speech_order"] for x in t] == ["1", "2", "9", "10", "11"]
    assert [x["turn_seq"] for x in t] == [1, 2, 3, 4, 5]
    assert [(x["speaker_pos"], x["speaker_name"]) for x in t] == [
        ("위원장", "홍길동"), ("위원장", "홍길동"), ("국방부장관", "이종섭"), ("위원", "김철수"), ("위원", "이수진(비)")]
    assert t[3]["speaker_label_raw"] == "김철수 위원"
    assert t[3]["stage_texts"] == ["(웃음)"] and t[3]["text"] == "질문합니다. 네."
    assert t[0]["has_stage"] and t[0]["text"] is None          # NULL, never '' (1.8)
    assert [x["agenda_ordinal"] for x in t] == [None, 1, 1, 1, 2]
    assert [a["after_turn_seq"] for a in r["tables"]["agenda"]] == [1, 4]
    assert r["coverage"]["ok_all"]
    assert sum(len(bt.nows(x["text_raw"])) for x in t) == sum(len(bt.nows(x["speech_text"])) for x in rows)


@pytest.mark.parametrize("label,expected", [
    ("박영선 위원", ("위원", "박영선")), ("위원장 류선호", ("위원장", "류선호")),
    ("국방부장관 김태영", ("국방부장관", "김태영")), ("증인 존 리", ("증인", "존 리")),
    ("최경환 위원(국)", ("위원", "최경환(국)")), ("박은수", (None, "박은수")),
    ("여성가족부 장관", ("여성가족부 장관", None)), ("宋榮珍議員", ("議員", "宋榮珍")),
])
def test_split_xlsx_label(label, expected):
    assert bt.split_xlsx_label(label)[:2] == expected


# ----------------------------------------------------------------------------- HWP adapter (stub)

def _stub_parse_hwp(data, conf_num=None):
    turns = [
        {"turn_seq": 1, "speaker_label_raw": "위원장 홍길동", "speaker_pos": "위원장", "speaker_name": "홍길동",
         "text_raw": "개의하겠습니다.\n(웃음)", "text": "개의하겠습니다.", "has_stage": True,
         "stage_kinds": ["laughter"], "stage_texts": ["(웃음)"], "agenda_ordinal": 1, "agenda_text": "1. A",
         "time_hhmm": "10:00", "speech_date": "2009-01-02", "n_lines": 2, "interjections": []},
        {"turn_seq": 2, "speaker_label_raw": "김철수 위원", "speaker_pos": "위원", "speaker_name": "김철수",
         "text_raw": "질문.", "text": "질문.", "has_stage": False, "stage_kinds": [], "stage_texts": [],
         "agenda_ordinal": 1, "agenda_text": "1. A", "time_hhmm": "10:00", "speech_date": "2009-01-02",
         "n_lines": 1},
    ]
    lab = sum(len(bt.nows(t["speaker_label_raw"])) for t in turns)
    tr = sum(len(bt.nows(t["text_raw"])) for t in turns)
    footer = [{"title": "◯출석 위원(2인)", "lines": ["홍길동  김철수"], "tables": [], "names": ["홍길동", "김철수"]},
              {"title": "◯찬성 의원(2인)", "lines": ["홍길동  김철수"], "tables": []}]
    fch = sum(len(bt.nows(s["title"])) + sum(len(bt.nows(x)) for x in s["lines"]) for s in footer)
    cats = {"turn_head": lab + 2 + 10, "turn_line": tr - 10, "agenda": 3, "time": 10, "appendix": fch, "cover": 7}
    return {"status": "ok", "meeting": {"session_no": 280, "doc_title": "國防委員會會議錄", "committee_raw": "國防委員會",
                                        "date": "2009-01-02", "doc_no": 1},
            "agenda_header": [{"section": "議事日程", "text": "1. A", "page": 1}],
            "agenda": [{"ordinal": 1, "text": "1. A", "match": "cover_match", "after_turn_seq": 0}],
            "turns": turns,
            "events": [{"kind": "time", "text": "(10시00분開議)", "after_turn_seq": 0, "hhmm": "10:00",
                        "action": "開議", "within_turn": False}],
            "footer": footer,
            "stats": {"chars_turn_items": lab + tr, "chars_total": sum(cats.values()), "n_unassigned": 0,
                      "chars_by_category": cats},
            "reader": {"status": "ok"}}


@pytest.fixture
def stub_hwp(monkeypatch):
    mod = types.ModuleType("hwp_parser")
    mod.parse_hwp = _stub_parse_hwp
    monkeypatch.setattr(bt, "_HWP", mod)
    return mod


def test_hwp_adapter_with_stub(stub_hwp):
    r = bt.hwp_extract(7, b"fake", term=18, sha1="s")
    assert r["status"] == "ok"
    t = r["tables"]["turns"]
    assert [x["turn_seq"] for x in t] == [1, 2] and all(x["source"] == "hwp" for x in t)
    assert set(bt.CONTRACT_TURN_COLUMNS) <= set(t[0])
    assert r["header"]["h_committee"] == "國防委員會" and r["header"]["h_session"] == 280
    rc = r["tables"]["rollcall"]
    assert [x["name"] for x in rc] == ["홍길동", "김철수"] and rc[0]["vote_group"] == "찬성"
    att = r["tables"]["attendance"]
    assert [a["name"] for a in att] == ["홍길동", "김철수"] and att[0]["category"] == "present"
    cov = r["coverage"]
    assert cov["ok_txt"] and cov["ok_footer"] and cov["ok_all"]


def test_hwp_real_file_if_present():
    """Real parser on one crawled HWP file that the crawler recorded as ok (skipped if none or the
    parser is absent). Any failure status fails the test."""
    try:
        import hwp_parser  # noqa: F401
    except ImportError:
        pytest.skip("hwp_parser not available")
    db = bt.V10 / "interim" / "crawl_state.sqlite"
    if not db.exists():
        pytest.skip("no crawl state")
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    ok = [r[0] for r in c.execute("SELECT conf_num FROM fetch WHERE kind='hwp' AND status='ok' ORDER BY conf_num LIMIT 200")]
    c.close()
    files = [bt.hwp_path(bt.Config(), n) for n in ok]
    files = [f for f in files if f.exists()]
    if not files:
        pytest.skip("no crawled hwp files")
    f = files[0]
    r = bt.hwp_extract(int(f.stem), f.read_bytes(), term=18)
    assert r["status"] in ("ok", "ok_no_turns"), (f, r["status"])
    assert r["coverage"] is not None and r["coverage"]["turn_seq_contiguous"]
    assert r["coverage"]["ok_all"], r["coverage"]


# ----------------------------------------------------------------------------- planner

def test_plan_precedence(tmp_path):
    import pandas as pd
    cfg = bt.Config(out=tmp_path / "out", raw=tmp_path / "raw")
    for p in (bt.view_path(cfg, 1), bt.hwp_path(cfg, 1), bt.hwp_path(cfg, 2), bt.hwp_path(cfg, 3),
              bt.view_path(cfg, 5)):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
    universe = pd.DataFrame({"CONFER_NUM": [1, 2, 3, 4, 5], "DAE_NUM": [19, 18, 18, 18, 18]})
    crosswalk = pd.DataFrame({"v9_source": ["xlsx", "xlsx"], "term": [18, 18], "api_CONFER_NUM": [2.0, 5.0],
                              "meeting_id": ["m2", "m5"]})
    crawl = {(1, "view"): ("ok", "a"), (1, "hwp"): ("ok", "b"), (2, "hwp"): ("ok", "c"),
             (2, "view"): ("http_400", None), (3, "hwp"): ("ok", "d"), (3, "view"): ("http_400", None),
             (5, "view"): ("ok", "e"), (77, "summary"): ("ok", "z")}
    tasks, pending, actions = bt.plan(cfg, universe, crosswalk, crawl, {}, {"xml", "xlsx", "hwp"})
    got = {t["conf_num"]: t["source"] for t in tasks}
    # xml > hwp; XLSX is no longer a production source (researcher decision 2026-09-26): the 18대
    # meetings in the v9 XLSX crosswalk (2, 5) are built from HWP / the view page
    assert got == {1: "xml", 2: "hwp", 3: "hwp", 5: "xml"}
    assert actions["xlsx_in_sources_not_production"] == 1 and "xlsx_preferred_over_view_ok" not in actions
    # 4 is in the universe and not crawled; 77 has only a summary page and is outside the universe
    assert pending["not_crawled_yet"] == 1 and pending["outside_universe_not_crawled_yet"] == 1
    # an unusable view page falls through to the next source while its hash AND version are unchanged
    ver = bt.adapter_version("xml")
    tasks, _, _ = bt.plan(cfg, universe, crosswalk, crawl, {}, {"xml", "xlsx", "hwp"},
                          unusable={(1, "xml"): ("no_body_images", "a", ver)})
    assert {t["conf_num"]: t["source"] for t in tasks}[1] == "hwp"
    # ... but is retried after an adapter/parser change, and with rebuild=True
    for kw in ({"unusable": {(1, "xml"): ("no_body_images", "a", "0.9+old+old")}},
               {"unusable": {(1, "xml"): ("no_body_images", "a", ver)}, "rebuild": True}):
        tasks, _, _ = bt.plan(cfg, universe, crosswalk, crawl, {}, {"xml", "xlsx", "hwp"}, **kw)
        assert {t["conf_num"]: t["source"] for t in tasks}[1] == "xml", kw
    # already built with the same source and hash -> skipped
    built = {1: {"source": "xml", "raw_sha1": "a"}}
    tasks, _, actions = bt.plan(cfg, universe, crosswalk, crawl, built, {"xml", "xlsx", "hwp"})
    assert 1 not in {t["conf_num"] for t in tasks} and actions["skip_built"] == 1
    built = {1: {"source": "xml", "raw_sha1": "OLD"}}
    tasks, _, actions = bt.plan(cfg, universe, crosswalk, crawl, built, {"xml", "xlsx", "hwp"})
    assert actions["rebuild_raw_changed"] == 1
    # built from a file the current version cannot parse: keep the previous build, no fall-through
    built = {1: {"source": "xml", "raw_sha1": "a", "builder_version": "1.5+old"}}
    tasks, pending, actions = bt.plan(cfg, universe, crosswalk, crawl, built, {"xml", "xlsx", "hwp"},
                                      unusable={(1, "xml"): ("no_body_images", "a", ver)})
    assert 1 not in {t["conf_num"] for t in tasks}
    assert actions["keep_previous_build_xml_new_parse_no_body_images"] == 1
    assert not any("unusable" in k for k in pending)
    # built from XLSX earlier: rebuilt from HWP; with no production source at all: dropped (counted)
    built = {2: {"source": "xlsx", "raw_sha1": "v9", "term": 18}, 4: {"source": "xlsx", "raw_sha1": "v9", "term": 18}}
    tasks, pending, actions = bt.plan(cfg, universe, crosswalk, crawl, built, {"xml", "hwp"})
    by = {t["conf_num"]: (t["source"], t["action"]) for t in tasks}
    assert by[2] == ("hwp", "rebuild_from_xlsx_to_hwp") and by[4] == ("xlsx", "drop_non_production_source")
    assert actions["drop_non_production_source_xlsx"] == 1 and pending.get("not_crawled_yet", 0) == 0
    # an HWP that exists but is not selected in this run does not drop the XLSX build
    tasks, pending, _ = bt.plan(cfg, universe, crosswalk, crawl, {2: built[2]}, {"xml"})
    assert 2 not in {t["conf_num"] for t in tasks} and pending["source_hwp_not_selected"] == 2   # 2 and 3


# ----------------------------------------------------------------------------- meetings helpers

def test_committee_keys_and_hearing_types():
    assert bt.hearing_type_for("특별위원회", "대법관(오석준)임명동의에관한인사청문특별위원회") == "인사청문특별위원회"
    assert bt.hearing_type_for("특별위원회", "정치개혁특별위원회") == "특별위원회"
    assert bt.committee_key_for("상임위원회", "국방위원회")[0] is not None
    assert bt.committee_key_for("상임위원회", "기후에너지환경노동위원회") == ("environment_labor", "v10_new_standing")
    assert bt.committee_key_for("전원위원회", "x")[0] == "committee_of_whole"
    assert bt.is_confirmation_agenda("1. 국무위원후보자(통일부장관 조명균) 인사청문회")
    assert bt.is_confirmation_agenda("1. 국세청장후보자(한승희) 인사청문회(계속)")
    assert not bt.is_confirmation_agenda("1. 국세청장후보자(한승희) 인사청문경과보고서 채택의 건")
    assert not bt.is_confirmation_agenda("1. 한국방송공사 사장후보자(양승동) 인사청문회 실시의 건")


# ----------------------------------------------------------------------------- end to end

@pytest.fixture
def e2e(tmp_path, stub_hwp):
    """Temporary raw tree (3 sample view pages + 1 fake HWP), crawl db, and output root."""
    raw = tmp_path / "raw"
    cfg = bt.Config(out=tmp_path / "out", raw=raw, crawl_db=tmp_path / "crawl.sqlite")
    rows = []
    for n in XML_OK:
        p = bt.view_path(cfg, n)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(_sample(n))
        rows.append((n, "view", "ok", "sha" + str(n)))
    hwp_n = 29999999         # outside the universe: exercises the not-in-universe path
    p = bt.hwp_path(cfg, hwp_n)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"fake hwp")
    rows.append((hwp_n, "hwp", "ok", "shahwp"))
    con = sqlite3.connect(cfg.crawl_db)
    con.execute("CREATE TABLE fetch (conf_num INTEGER, kind TEXT, status TEXT, sha1 TEXT)")
    con.executemany("INSERT INTO fetch VALUES (?,?,?,?)", rows)
    con.commit()
    con.close()
    return cfg, XML_OK + [hwp_n]


def _snapshot(cfg):
    out = {}
    for tb in bt.ALL_TABLES:
        d = bt.table_dir(cfg, tb)
        files = sorted(d.glob("*/*/*.parquet")) if d.exists() else []
        out[tb] = (len(files), sum(pq.read_metadata(f).num_rows for f in files))
    return out


def test_end_to_end_incremental_idempotent(e2e):
    cfg, only = e2e
    s1 = bt.run(cfg, sources=("xml", "hwp"), workers=1, only=only)
    assert s1["results"] == {"xml:ok": 3, "hwp:ok": 1}
    snap1 = _snapshot(cfg)
    turns = bt.read_table("turns", out=cfg.out)
    # CONTRACT dtypes
    assert str(turns.conf_num.dtype) == "Int64" and str(turns.turn_seq.dtype) == "Int32"
    assert str(turns.speaker_mem_id.dtype) == "Int64" and str(turns.agenda_ordinal.dtype) == "Int32"
    assert str(turns.n_fragments.dtype) == "Int16" and str(turns.has_stage.dtype) == "boolean"
    assert str(turns.text_raw.dtype) == "string"
    for c in bt.CONTRACT_TURN_COLUMNS:
        assert c in turns.columns
    # contiguous turn_seq per meeting
    for n, g in turns.groupby("conf_num"):
        assert sorted(g.turn_seq.tolist()) == list(range(1, len(g) + 1)), n
    # no text loss against the adapters
    for n in XML_OK:
        r = bt.xml_extract(n, _sample(n))
        want = sum(len(bt.nows(t["text_raw"])) for t in r["tables"]["turns"])
        got = sum(len(bt.nows(x)) for x in turns.loc[turns.conf_num == n, "text_raw"])
        assert want == got, n
    meetings = bt.read_table("meetings", out=cfg.out)
    assert meetings.conf_num.is_unique
    built = meetings[meetings.is_built.fillna(False)]
    assert set(built.conf_num) == set(only)
    assert (built.set_index("conf_num").n_turns.loc[XML_OK] ==
            turns.groupby("conf_num").size().loc[XML_OK]).all()
    # second run: nothing to do, outputs unchanged
    s2 = bt.run(cfg, sources=("xml", "hwp"), workers=1, only=only)
    assert s2["tasks_by_source"] == {} and s2["plan_actions"].get("skip_built") == 4
    assert _snapshot(cfg) == snap1
    # a changed raw hash rebuilds exactly that meeting and keeps totals
    c = sqlite3.connect(cfg.crawl_db)
    c.execute("UPDATE fetch SET sha1='new' WHERE conf_num=?", (XML_OK[0],))
    c.commit()
    c.close()
    s3 = bt.run(cfg, sources=("xml", "hwp"), workers=1, only=only)
    assert s3["plan_actions"] == {"skip_built": 3, "rebuild_raw_changed": 1}
    turns3 = bt.read_table("turns", out=cfg.out)
    assert len(turns3) == len(turns)
    assert turns3.groupby(["conf_num", "turn_seq"]).size().max() == 1


def test_end_to_end_pool(e2e):
    """Same build through the multiprocessing pool (spawn)."""
    cfg, only = e2e
    xml_only = [n for n in only if n in XML_OK]
    s = bt.run(cfg, sources=("xml",), workers=2, only=xml_only, build_meetings_table=False)
    assert s["results"] == {"xml:ok": 3}
    assert s["coverage"].get("xml:ok_all:True") == 3


@pytest.mark.parametrize("label,expected", [
    ("保健福祉部長官崔善政", ("保健福祉部長官", "崔善政", "v10_hanja_suffix_name")),
    ("委員長田瑢源", ("委員長", "田瑢源", "v10_hanja_suffix_name")),
    ("委員長 李嬿淑", ("委員長", "李嬿淑", "v10_space_pos_name")),
    ("薛 勳委員", ("委員", "薛勳", "v10_name_role_spaced")),
    ("監査院長", (None, None, None)),
    ("法務部長官", (None, None, None)),
    ("産業資源部企劃管理室長", (None, None, None)),
    # review 2026-09-26: position-only labels are never cut inside the title
    ("委員長代理", (None, None, None)),
    ("民主平和統一諮問會議事務處長", (None, None, None)),
    ("國防部軍備統制官室次長", (None, None, None)),
    ("委員長代理 朴源弘", ("委員長代理", "朴源弘", "v10_space_pos_name")),
    # 2-character names
    ("副總理兼財政經濟部長官陳稔", ("副總理兼財政經濟部長官", "陳稔", "v10_hanja_suffix_name")),
    ("서울特別市長高建", ("서울特別市長", "高建", "v10_hanja_suffix_name")),
    ("韓國勞動敎育院長李 銑", ("韓國勞動敎育院長", "李銑", "v10_hanja_suffix_name_spaced")),
    # Hangul name after a Hanja title: split at the script boundary, never inside the title
    ("證人김동호", ("證人", "김동호", "v10_hanja_pos_hangul_name")),
    ("副總理兼財政經濟部長官진념", ("副總理兼財政經濟部長官", "진념", "v10_hanja_pos_hangul_name")),
    ("委員長대리", (None, None, None)),
    ("수석전문위원姜長錫", ("수석전문위원", "姜長錫", "v10_hangul_pos_hanja_name")),
    # names ending in a title character stay whole; two fused names are not split
    ("委員長金斗官", ("委員長", "金斗官", "v10_hanja_suffix_name")),
    ("委員長劉容泰金樂冀", (None, None, None)),
    ("金龍學委員 金龍學", ("委員", "金龍學", "v10_space_pos_name_dup")),
    ("環境部上下水道局長南宮垠", ("環境部上下水道局長", "南宮垠", "v10_hanja_suffix_name")),
    # compatibility ideographs: 理 printed as U+F9E4 (returned as printed)
    ("國務總\uF9E4李漢東", ("國務總\uF9E4", "李漢東", "v10_hanja_suffix_name")),
    ("\uF90A大中委員長", ("委員長", "\uF90A大中", "v10_name_role_spaced")),
])
def test_split_fused_label(label, expected):
    assert bt.split_fused_label(label) == expected


def test_char_classes_code_points():
    """The Hanja classes must end at U+F900-U+FAFF (an NFC save once turned U+F900 into U+8C48,
    which made the classes cover every Hangul syllable)."""
    import re
    assert re.fullmatch(bt._HJ, "\uF900") and re.fullmatch(bt._HJ, "\uFAFF") and re.fullmatch(bt._HJ, "李")
    for ch in ("진", "\uE000", "\uD7B0", "a"):
        assert not re.fullmatch(bt._HJ, ch), ch
    assert not bt.CJK_NAME_RE.match("\uE000\uE001") and bt.CJK_NAME_RE.match("김 金")
    assert not bt.NAME_LIST_RE.match("\uE000") and bt.NAME_LIST_RE.match("강기정 박 진 金大中")
    assert bt.fold_compat("\uF9E4\uF90A\uF9E1") == "理金李"
    src = Path(bt.__file__).read_text(encoding="utf-8")
    assert "\u8c48-" not in src and chr(0x8C48) + "-" not in src


def test_surname_set_disjoint_and_covers_members():
    assert not (bt.HANJA_SURNAMES & bt.POS_SUFFIX_CHARS)
    assert not ({c[0] for c in bt.HANJA_COMPOUND_SURNAMES} & bt.POS_SUFFIX_CHARS)
    f = bt.V10 / "interim" / "members_allnamember_16_22.parquet"
    if not f.exists():
        pytest.skip("members file absent")
    import unicodedata
    names = pq.read_table(f, columns=["NAAS_CH_NM"]).column(0).to_pylist()
    first = {unicodedata.normalize("NFC", x.strip())[0] for x in names if x and x.strip()}
    first = {c for c in first if bt.HANJA_RE.match(c)}
    assert first <= bt.HANJA_SURNAMES, first - bt.HANJA_SURNAMES


@pytest.mark.parametrize("pos,name,label,expected", [
    # data-name is the surname only: the printed spaced name is used
    ("財政經濟部長官", "陳", "財政經濟部長官 陳 稔 선택", ("財政經濟部長官", "陳稔", True, "spaced_name")),
    # printed twice / running into other text: data-name kept
    ("小委員長", "張在植", "小委員長 張在植 張在植 선택", ("小委員長", "張在植", False, "rejected_duplicate")),
    ("委員長", "田瑢源", "委員長 田瑢源 그리고 權丙喆 참고인 선택", ("委員長", "田瑢源", False, "rejected_not_name")),
    ("食品醫藥品安全廳長", "李榮純", "食品醫藥品安全廳長 李榮純 알겠습니다. 선택",
     ("食品醫藥品安全廳長", "李榮純", False, "rejected_not_name")),
    # the source split the title from the position: title moved to the position
    ("金融監督", "委員長", "金融監督 委員長 李瑾榮 선택", ("金融監督 委員長", "李瑾榮", True, "pos_extended")),
    ("第一銀行長", "Wolfred", "第一銀行長 Wolfred Y. Horie 선택", ("第一銀行長", "Wolfred Y. Horie", True, "latin_name")),
])
def test_name_from_label_override(pos, name, label, expected):
    sp = {"pos": pos, "name": name, "pos_norm": pos, "name_norm": name, "label_split": "attr"}
    raw, p, n, how, fused, from_label, outcome = bt._speaker_fields(sp, label)
    assert (p, n, from_label, outcome) == expected


# ----------------------------------------------------------------------------- re-votes

def test_revote_in_one_section_gets_its_own_vote_seq():
    """HWP 33387-style: a second vote printed in the same bill section after '<2차 투표>'."""
    sec = [{"title": "【전자투표 찬반 의원 성명】", "lines": [], "tables": []},
           {"title": "◯방송법 일부개정법률안에 대한 수정안", "tables": [], "lines": [
               "  투표 의원(3인)", "  찬성 의원(2인)", "홍길동  김철수", "  기권 의원(1인)", "이영희", "<2차 투표>",
               "  투표 의원(3인)", "  찬성 의원(3인)", "홍길동  김철수  이영희"]}]
    c = Counter()
    v, g, n, _ = bt.rollcall_attendance_from_hwp(sec, c)
    assert v == 2
    assert [(x["vote_seq"], x["vote_group"], x["n_names"]) for x in g] == [
        (1, "투표", 0), (1, "찬성", 2), (1, "기권", 1), (2, "투표", 0), (2, "찬성", 3)]
    assert "<2차 투표>" not in {x["name"] for x in n}
    assert g[3]["note"] == "<2차 투표>" and c["rollcall_vote_split_in_section"] == 1
    # XML: two vote blocks in one footer section, no separator
    xs = [{"title": "【전자투표 찬반 의원 성명】◯A법안", "groups": [
        {"label": "투표의원(2인)", "names": [], "lines": []},
        {"label": "찬성의원(2인)", "names": [{"name": "홍길동"}, {"name": "김철수"}], "lines": []},
        {"label": "투표의원(2인)", "names": [], "lines": []},
        {"label": "찬성의원(1인)", "names": [{"name": "홍길동"}], "lines": []},
        {"label": "반대의원(1인)", "names": [{"name": "김철수"}], "lines": []}]}]
    v, g, n = bt.rollcall_from_xml_sections(xs)
    assert v == 2 and [(x["vote_seq"], x["vote_group"]) for x in g] == [
        (1, "투표"), (1, "찬성"), (2, "투표"), (2, "찬성"), (2, "반대")]
    # a repeated group without a 투표 group also starts a new vote
    xs[0]["groups"] = [xs[0]["groups"][1], xs[0]["groups"][3]]
    assert bt.rollcall_from_xml_sections(xs)[0] == 2


def test_attendance_titles_typed():
    cases = {"◯속개 시 재석 의원(67인)": "seated_at_resumption", "◯20시38분 속개 시 재석 의원": "seated_at_resumption",
             "◯出張監査委員(1人)": "official_travel", "◯出席小委員": "present", "◯請暇小委員": "excused",
             "◯出席公職候補者": "nominee", "◯出席大法官候補者": "nominee", "◯出席立法調査官": "committee_staff",
             "◯出席立法審議官": "committee_staff", "◯출석 자문위원": "advisor", "O出席委員": "present",
             "◯委員아닌參席議員": "present_nonmember", "◯被監査機關出席者": "agency_attendee",
             "◯出席\uF9F7法審議官": "committee_staff",
             "※코로나19 방역 관련 권고에 따라 출석하지 않은 의원": "absent_listed",
             "◯出席國監委員(10人)": "present", "◯請暇國監委員": "excused", "◯出張國監委員": "official_travel",
             "◯小委員아닌出席委員": "present_nonmember", "◯委員이아닌出席議員": "present_nonmember",
             "◯委員아닌출석의원": "present_nonmember", "◯參席參考人": "reference", "◯出席審議官": "committee_staff"}
    for t, cat in cases.items():
        assert bt.attendance_category(t) == cat, t
    c = Counter()
    bt.attendance_from_xml_sections([{"title": "◯出張專門委員", "groups": []}], c)
    assert c["attendance_like_title_uncategorized"] == 1


# ----------------------------------------------------------------------------- XLSX time markers

def test_xlsx_rollover_and_time_markers():
    base = dict(term=18, hearing_type="국정감사", committee="X", date="2009-10-13", session=None,
                sub_session=None, member_id=None, agenda="1. A")
    rows = [dict(base, speech_order="1", speaker="위원장 홍길동", speech_text="(10시 05분 개의) 질의하겠습니다.(24시 경과)"),
            dict(base, speech_order="2", speaker="김철수 위원", speech_text="자정이 지났습니다."),
            dict(base, speech_order="3", speaker="위원장 홍길동", speech_text="산회합니다. (00시30분 산회)")]
    r = bt.xlsx_meeting_tables(1, 18, rows)
    t = r["tables"]["turns"]
    assert [x["speech_date"] for x in t] == ["2009-10-13", "2009-10-14", "2009-10-14"]
    assert [x["time_hhmm"] for x in t] == ["10:05", "10:05", "00:30"]
    assert [x["time_hhmm_start"] for x in t] == [None, "10:05", "10:05"]
    assert t[0]["speech_date_end"] is None               # the marker ends the turn
    # the glued '(24시 경과)' is a stage direction, so it is not in the spoken text
    assert t[0]["stage_texts"] == ["(10시 05분 개의)", "(24시 경과)"] and t[0]["text"] == "질의하겠습니다."
    assert r["header"]["date_end"] == "2009-10-14"
    assert r["counters"]["xlsx_day_rollover"] == 1 and r["counters"]["xlsx_time_marker"] == 2
    # a roll-over in the middle of a turn sets speech_date_end
    rows[0]["speech_text"] = "질의하겠습니다. (24시 경과) 계속하겠습니다."
    t = bt.xlsx_meeting_tables(1, 18, rows)["tables"]["turns"]
    assert t[0]["speech_date"] == "2009-10-13" and t[0]["speech_date_end"] == "2009-10-14"
    # text is never lost
    assert all(bt.nows(x["text_raw"]) == bt.nows(rw["speech_text"]) for x, rw in zip(t, rows))


# ----------------------------------------------------------------------------- run safety

def test_run_lock_and_run_id(tmp_path):
    cfg = bt.Config(out=tmp_path / "out", raw=tmp_path / "raw", crawl_db=tmp_path / "none.sqlite")
    ids = {bt.new_run_id() for _ in range(50)}
    assert len(ids) == 50
    # a batch written by a running build is not moved by a second run: the second run fails fast
    con = bt.open_state(cfg)
    p = bt.table_dir(cfg, "turns") / "xml" / "t20" / "bA_00001.parquet"
    bt._write_table(p, "turns", [{"conf_num": 1, "turn_seq": 1, "source": "xml", "text_raw": "x", "text": "x"}])
    with bt.run_lock(cfg):
        with pytest.raises(bt.RunLocked):
            bt.run(cfg, sources=("xml",), workers=1, only=[1], build_meetings_table=False)
        with pytest.raises(bt.RunLocked):
            with bt.run_lock(cfg):
                pass
    assert p.exists()
    con.close()


def test_pinned_import_records_executed_code(tmp_path):
    f = tmp_path / "fakeparser_bt.py"
    f.write_text("V = 'old'\n")
    mod = bt._load_pinned("fakeparser_bt", f)
    f.write_text("V = 'new'\n")                      # edited after the import
    import hashlib
    assert mod.V == "old"
    assert bt._module_sha8(mod) == hashlib.sha1(b"V = 'old'\n").hexdigest()[:8]
    sys.modules.pop("fakeparser_bt", None)


def _stub_module(sha, parse):
    mod = types.ModuleType("hwp_parser")
    mod.parse_hwp = parse
    mod.__pinned_sha8__ = sha
    return mod


def test_failed_rebuild_keeps_previous_build(tmp_path, monkeypatch):
    """A rebuild whose new parse fails never removes the previous rows; the file is retried after
    the parser changes again or with rebuild=True."""
    raw = tmp_path / "raw"
    cfg = bt.Config(out=tmp_path / "out", raw=raw, crawl_db=tmp_path / "crawl.sqlite")
    n = 29999998
    p = bt.hwp_path(cfg, n)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"fake")
    c = sqlite3.connect(cfg.crawl_db)
    c.execute("CREATE TABLE fetch (conf_num INTEGER, kind TEXT, status TEXT, sha1 TEXT)")
    c.execute("INSERT INTO fetch VALUES (?,?,?,?)", (n, "hwp", "ok", "s1"))
    c.commit()
    c.close()
    bad = lambda data, conf_num=None: {"status": "stream_error", "meeting": {}, "turns": [], "stats": {}}  # noqa: E731

    def nturns():
        fs = list((cfg.out / "turns").glob("*/*/*.parquet"))
        return sum(pq.read_metadata(f).num_rows for f in fs)

    def go(mod, **kw):
        monkeypatch.setattr(bt, "_HWP", mod)
        return bt.run(cfg, sources=("hwp",), workers=1, only=[n], build_meetings_table=False, **kw)

    s1 = go(_stub_module("good0001", _stub_parse_hwp))
    assert s1["results"] == {"hwp:ok": 1} and nturns() == 2
    s2 = go(_stub_module("bad00001", bad))              # parser edited, now fails on this file
    assert s2["plan_actions"] == {"rebuild_builder_version": 1} and s2["results"] == {"hwp:stream_error": 1}
    assert s2["rebuild_failed_kept_previous"] == {"hwp:stream_error": 1} and nturns() == 2
    s3 = go(_stub_module("bad00001", bad))              # same failing version: kept, not retried
    assert s3["plan_actions"] == {"keep_previous_build_hwp_new_parse_stream_error": 1} and s3["results"] == {}
    assert s3["pending"] == {} and nturns() == 2
    s4 = go(_stub_module("bad00001", bad), rebuild=True)   # --rebuild retries (and keeps on failure)
    assert s4["results"] == {"hwp:stream_error": 1} and nturns() == 2
    s5 = go(_stub_module("good0002", _stub_parse_hwp))  # parser fixed: rebuilt
    assert s5["plan_actions"] == {"rebuild_builder_version": 1} and s5["results"] == {"hwp:ok": 1}
    assert nturns() == 2
    con = sqlite3.connect(cfg.state_db)
    assert con.execute("SELECT count(*) FROM pending_drops").fetchone()[0] == 0
    assert con.execute("SELECT builder_version FROM built").fetchone()[0].split("+")[1] == "good0002"
    con.close()


# ----------------------------------------------------------------------------- sittings / labels (1.7)

@pytest.mark.parametrize("text,action,expected", [
    ("(12시16분 산회)", "산회", "산회"),
    ("(10월5일 00시03분 감사종료)", "감사종료", "감사종료"),
    ("(13시50분 비공개감사종료)", "감사종료", "비공개감사종료"),   # XML action drops the prefix
    ("(10時05分 開議)", "開議", "開議"),
    ("(10시05분)", None, None),
    ("(24시 경과)", None, "경과"),
    ("not a marker", "산회", "산회"),                              # falls back to the parser action
])
def test_marker_action(text, action, expected):
    assert bt.marker_action(text, action) == expected


def _t(n):
    return [{"turn_seq": i} for i in range(1, n + 1)]


def test_assign_sittings_rule():
    # 28769 shape: 개의 (0), 산회 after turn 2, 개의 after turn 2, 산회 at the end
    t = _t(5)
    c = bt.assign_sittings(t, [(0, "개의"), (2, "산회"), (2, "개의"), (5, "산회")])
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2, 2]
    # 1.8: after_end_marker is reset when the new sitting starts (researcher decision 2026-09-26)
    assert [x["after_end_marker"] for x in t] == [False] * 5
    assert [x["after_final_end_marker"] for x in t] == [False] * 5      # the last 산회 ends turn 5
    assert all(x["sitting_how"] == "end_open_markers" for x in t)
    assert c["meetings_several_sittings"] == 1 and c["turns_in_later_sittings"] == 3
    assert c["turns_after_end_marker"] == 0 and c["turns_after_final_end_marker"] == 0
    # closed-session end / vote end / pause are not meeting ends; an opening without an end is no new sitting
    t = _t(4)
    bt.assign_sittings(t, [(0, "감사개시"), (1, "비공개감사종료"), (2, "투표종료"), (2, "회의중지"), (3, "계속개의")])
    assert [x["sitting_seq"] for x in t] == [1, 1, 1, 1] and not any(x["after_end_marker"] for x in t)
    # an end marker inside the last turn: nothing after it
    t = _t(3)
    bt.assign_sittings(t, [(3, "산회")])
    assert not any(x["after_end_marker"] for x in t)
    # written answers after 산회 without a new opening: flagged, same sitting
    t = _t(3)
    c = bt.assign_sittings(t, [(2, "산회")])
    assert [x["after_end_marker"] for x in t] == [False, False, True] and {x["sitting_seq"] for x in t} == {1}
    assert [x["after_final_end_marker"] for x in t] == [False, False, True]
    assert c["meetings_with_turns_after_final_end_marker"] == 1
    # a continuation printed after an end marker re-opens (24614: '(13시17분 감사종료)' '(14시40분 감사계속)')
    t = _t(4)
    bt.assign_sittings(t, [(0, "감사개시"), (2, "감사종료"), (2, "감사계속"), (3, "감사중지"), (3, "감사계속")])
    assert [x["sitting_seq"] for x in t] == [1, 1, 2, 2] and [x["after_end_marker"] for x in t] == [False] * 4
    # the resumed sitting prints no end marker of its own: it follows the document's last end marker
    assert [x["after_final_end_marker"] for x in t] == [False, False, True, True]
    # two restarts; two opening markers after one end count once
    t = _t(6)
    bt.assign_sittings(t, [(1, "산회"), (1, "개의"), (2, "개의"), (3, "散會"), (4, "開議")])
    assert [x["sitting_seq"] for x in t] == [1, 2, 2, 2, 3, 3]


def test_assign_sittings_keeps_parser_values_and_counts_disagreement():
    t = [{"turn_seq": 1, "sitting_seq": 1, "after_end_marker": False},
         {"turn_seq": 2, "sitting_seq": 2, "after_end_marker": True},
         {"turn_seq": 3, "sitting_seq": 2, "after_end_marker": True}]
    c = bt.assign_sittings(t, [(1, "산회")], parser_sitting=True, parser_after_end=True)
    assert [x["sitting_seq"] for x in t] == [1, 2, 2] and all(x["sitting_how"] == "parser" for x in t)
    assert [x["after_end_marker"] for x in t] == [False, True, True]
    assert c["sitting_parser_vs_markers_differ"] == 2 and c.get("after_end_parser_vs_markers_differ", 0) == 0
    # a parser after_final_end_marker value is kept too; a disagreement is counted
    t = [{"turn_seq": 1, "after_final_end_marker": False}, {"turn_seq": 2, "after_final_end_marker": False}]
    c = bt.assign_sittings(t, [(1, "산회")], parser_after_final=True)
    assert [x["after_final_end_marker"] for x in t] == [False, False] and c["after_final_parser_vs_markers_differ"] == 1


def test_label_confidence_table():
    c = Counter()
    assert bt.label_confidence("sep", None, c) == "high"
    assert bt.label_confidence("single_space_pos_name", None, c) == "low"
    assert bt.label_confidence("lexicon_prefix", None, c) == "medium"
    assert bt.label_confidence("sep", "low", c) == "low" and c["label_confidence_from_parser"] == 1
    assert bt.label_confidence("some_new_rule", None, c) == "unrated" and c["label_how_unrated_some_new_rule"] == 1


def _view(n):
    p = bt.view_path(bt.Config(), n)
    if not p.exists():
        pytest.skip(f"view page {n} not crawled")
    return p.read_bytes()


def test_xml_several_sittings_real_page():
    """28769 (17대 법사위 2004-12-06): '(16시10분 산회)' after turn 2, '(16시37분 개의)' starts sitting 2."""
    r = bt.xml_extract(28769, _view(28769), term=17)
    t = r["tables"]["turns"]
    assert [x["sitting_seq"] for x in t[:3]] == [1, 1, 2] and {x["sitting_seq"] for x in t[2:]} == {2}
    # 1.8: the new sitting resets after_end_marker; the second sitting ends with its own 산회
    assert not any(x["after_end_marker"] for x in t) and not any(x["after_final_end_marker"] for x in t)
    assert all(x["label_how"] and x["label_confidence"] == "high" for x in t)
    assert r["counters"]["meetings_several_sittings"] == 1


def test_xml_written_answer_after_end_real_page():
    """28317 (본회의): one written answer printed after '(18시36분 산회)': flagged, sitting 1."""
    r = bt.xml_extract(28317, _view(28317), term=17)
    t = r["tables"]["turns"]
    assert t[-1]["after_end_marker"] and not any(x["after_end_marker"] for x in t[:-1])
    assert {x["sitting_seq"] for x in t} == {1}
    # a closed-session end ('비공개감사종료') is not a meeting end: 24247 has turns after it
    r = bt.xml_extract(24247, _view(24247), term=16)
    t = r["tables"]["turns"]
    assert not any(x["after_end_marker"] for x in t) and {x["sitting_seq"] for x in t} == {1}


def test_xlsx_sittings_from_text_markers():
    base = dict(term=18, hearing_type="상임위원회", committee="X", date="2009-11-26", session=None,
                sub_session=None, member_id=None, agenda="1. A")
    rows = [dict(base, speech_order="1", speaker="위원장 홍길동", speech_text="(10시00분 개의) 시작합니다."),
            dict(base, speech_order="2", speaker="위원장 홍길동", speech_text="산회합니다. (12시16분 산회)"),
            dict(base, speech_order="3", speaker="위원장 홍길동", speech_text="(14시00분 개의) 다시 시작합니다."),
            dict(base, speech_order="4", speaker="김철수 위원", speech_text="질의합니다.")]
    t = bt.xlsx_meeting_tables(1, 18, rows)["tables"]["turns"]
    # the opening marker is printed inside turn 3, so the new sitting (and the reset) starts at turn 4
    assert [x["sitting_seq"] for x in t] == [1, 1, 1, 2]
    assert [x["after_end_marker"] for x in t] == [False, False, True, False]
    assert [x["after_final_end_marker"] for x in t] == [False, False, True, True]
    assert all(x["label_how"] == "xlsx_speaker_column" and x["label_confidence"] == "high" for x in t)


def test_hwp_stub_fields_default_and_parser_values(stub_hwp, monkeypatch):
    r = bt.hwp_extract(7, b"fake", term=18, sha1="s")
    t = r["tables"]["turns"]
    # the stub parser supplies no label_how / after_end_marker / sitting_seq
    assert [x["label_how"] for x in t] == [None, None] and [x["label_confidence"] for x in t] == ["unrated"] * 2
    assert [x["sitting_seq"] for x in t] == [1, 1] and [x["after_end_marker"] for x in t] == [False, False]
    assert r["counters"]["hwp_label_how_missing"] == 2
    assert r["counters"]["hwp_sitting_seq_not_in_parser_meetings"] == 1

    def parse2(data, conf_num=None):
        res = _stub_parse_hwp(data, conf_num)
        for x, how in zip(res["turns"], ("sep", "single_space_pos_name")):
            x["label_how"] = how
        res["turns"][1]["after_end_marker"] = True
        res["turns"][0]["after_end_marker"] = False
        res["turns"][1]["sitting_seq"] = 2
        res["turns"][0]["sitting_seq"] = 1
        return res
    monkeypatch.setattr(stub_hwp, "parse_hwp", parse2)
    r = bt.hwp_extract(7, b"fake", term=18, sha1="s")
    t = r["tables"]["turns"]
    assert [x["label_confidence"] for x in t] == ["high", "low"]
    assert [x["after_end_marker"] for x in t] == [False, True]
    assert [x["sitting_seq"] for x in t] == [1, 2] and {x["sitting_how"] for x in t} == {"parser"}


def test_hwp_real_after_end_marker_carried():
    """32689 (18대 국감): turns after '감사종료' (a video-call transcript) start a new sitting in
    hwp_parser, so after_end_marker is reset (1.8) and after_final_end_marker flags them; the parser's
    values reach the turns table."""
    try:
        import hwp_parser  # noqa: F401
    except ImportError:
        pytest.skip("hwp_parser not available")
    f = bt.hwp_path(bt.Config(), 32689)
    if not f.exists():
        pytest.skip("32689.hwp not crawled")
    r = bt.hwp_extract(32689, f.read_bytes(), term=18)
    t = r["tables"]["turns"]
    assert not any(x["after_end_marker"] for x in t)
    n_after = sum(1 for x in t if x["after_final_end_marker"])
    assert n_after > 0 and all(x["after_final_end_marker"] and x["sitting_seq"] > 1 for x in t[len(t) - n_after:])
    assert all(x["label_how"] for x in t) and all(x["sitting_seq"] >= 1 for x in t)
    assert set(x["label_confidence"] for x in t) <= {"high", "medium", "low", "unrated"}


def test_turn_schema_has_new_fields():
    names = set(bt.SCHEMAS["turns"].names)
    assert {"after_end_marker", "sitting_seq", "sitting_how", "label_how", "label_confidence"} <= names
    assert bt.SCHEMAS["turns"].field("sitting_seq").type == bt.I16


# ----------------------------------------------------------------------------- R1 (1.8, 2026-09-26)

@pytest.mark.parametrize("raw,label,name,expected", [
    # source repeats the label before the speech, set off by the label separator
    ("金鍾河委員  그러면 타 부처와의 통상관계라고", "金鍾河委員", "金鍾河", "label"),
    ("김기현 위원  27쪽?", "김기현 위원", "김기현", "label"),
    ("한기호 위원  (손을 듦)", "한기호 위원", "한기호", "label"),
    ("統一部長官 朴在圭　위원님께서는", "統一部長官 朴在圭", "朴在圭", "label"),     # ideographic space
    ("國務總理李漢東  그런 것을", "國務總理 李漢東", "李漢東", "label"),               # whitespace-insensitive
    ("김기현 위원", "김기현 위원", "김기현", "label"),                               # the sentence is the label
    # 16대 pages printed with one space: a repeat unless the next word ends a self-introduction
    ("朴世煥委員 한나라당 朴世煥 위원입니다. ", "朴世煥委員", "朴世煥", "label"),
    ("姜三載委員 그러면 무엇 때문에 회의하는 거예요?", "姜三載委員", "姜三載", "label"),
    ("금융위원회 금융정책과 양병권 사무관입니다. ", "금융위원회금융정책과 양병권", "양병권", None),
    ("입법조사관 김학배 입니다.", "입법조사관 김학배", "김학배", None),
    # glued to a word of the speech: speech
    ("김태년 위원입니다.", "김태년 위원", "김태년", None),
    ("법원행정처 차장 권순일입니다.", "법원행정처차장 권순일", "권순일", None),
    ("홍철호, 김포 지역 출신입니다.", "홍철호 위원", "홍철호", None),
    # the name part alone needs the strong separator
    ("鄭昌和 위원입니다. 잘 부탁드립니다.", "鄭昌和委員", "鄭昌和", None),
    ("秋秉直  99년도에는 3,420억", "建設交通部企劃管理室長 秋秉直", "秋秉直", "name"),
    ("千正培 李熙圭 위원님 수고하셨습니다.", "委員長代理 千正培", "千正培", None),
    ("", "김기현 위원", "김기현", None),
])
def test_label_prefix_match_rules(raw, label, name, expected):
    assert bt.label_prefix_match(raw, label, name)[0] == expected


def test_strip_label_prefix_keeps_text_raw_and_counts():
    c = Counter()
    t = {"speaker_label_raw": "金鍾河委員", "speaker_name": "金鍾河", "label_fused": False,
         "text_raw": "金鍾河委員 그러면 가.\n둘째.", "text": "金鍾河委員 그러면 가.\n둘째."}
    bt.strip_label_prefix(t, "金鍾河委員  그러면 가.  ", "金鍾河委員 그러면 가.", c)
    assert t["text"] == "그러면 가.\n둘째." and t["text_raw"] == "金鍾河委員 그러면 가.\n둘째."
    assert t["text_label_prefix_stripped"] and t["text_label_prefix_match"] == "label"
    assert c["text_label_prefix_stripped_label"] == 1 and c["text_label_prefix_stripped_chars"] == 5
    # the first sentence is only the label: that line goes, the rest stays
    t = {"speaker_label_raw": "김기현 위원", "speaker_name": "김기현", "text": "김기현 위원\n27쪽?"}
    bt.strip_label_prefix(t, "김기현 위원", "김기현 위원", c)
    assert t["text"] == "27쪽?"
    # text that does not start with the given first sentence (a stage sentence first) is untouched
    t = {"speaker_label_raw": "김기현 위원", "speaker_name": "김기현", "text": "다른 문장."}
    bt.strip_label_prefix(t, "김기현 위원  27쪽?", "김기현 위원 27쪽?", c)
    assert t["text"] == "다른 문장." and t["text_label_prefix_stripped"] is False
    # a fused label is never used
    t = {"speaker_label_raw": "A 위원◯B 장관", "speaker_name": "A", "label_fused": True, "text": "A 위원◯B 장관 x"}
    bt.strip_label_prefix(t, "A 위원◯B 장관  x", "A 위원◯B 장관 x", c)
    assert t["text_label_prefix_stripped"] is False


def test_xml_label_prefix_real_page():
    """23960 (16대 통일외교통상위원회 2000-11-30): the viewer repeats the printed label at the start of
    div.txt ('金鍾河委員  그러면 …'); it is removed from text, text_raw is unchanged, and no character of
    the page is lost (the coverage checks still hold)."""
    r = bt.xml_extract(23960, _view(23960), term=16)
    t = {x["turn_seq"]: x for x in r["tables"]["turns"]}
    assert r["coverage"]["ok_all"]
    x = t[57]
    # (the page prints some Hanja as compatibility ideographs: compared in the matching form)
    assert bt.norm_match(x["text_raw"]).startswith("金鍾河委員 그러면") and x["text"].startswith("그러면 타 부처와의")
    assert x["text_label_prefix_stripped"] and x["text_label_prefix_match"] == "label"
    n = sum(1 for y in t.values() if y["text_label_prefix_stripped"])
    assert n == r["counters"]["text_label_prefix_stripped_label"] + r["counters"].get("text_label_prefix_stripped_name", 0)
    for y in t.values():
        if y["text_label_prefix_stripped"]:
            first = y["text_raw"].split("\n")[0]
            assert bt.nows(first).endswith(bt.nows(y["text"].split("\n")[0])) or not y["text"]


def test_label_flags_fused_text_and_misattributed():
    c = Counter()
    turns = [{"speaker_label_raw": "金花中 委員", "label_confidence": "high"},
             {"speaker_label_raw": "田溶鶴 委員그런데 법사위에 가서 보내야 됩니다.", "label_confidence": "high"},
             {"speaker_label_raw": "田溶鶴 委員그런데 법사위에 가서 보내야 됩니다. ◯首席專門委員 尙元鍾",
              "label_confidence": "high"},
             {"speaker_label_raw": "서혜석 위원◯공정거래위원장 권오승", "label_confidence": "high"},
             {"speaker_label_raw": "證人 Mr. Michael Richter", "label_confidence": "high"},
             {"speaker_label_raw": "◯위원장 홍길동", "label_confidence": "medium"}]
    bt.label_flags(turns, c)
    assert [x["label_fused"] for x in turns] == [False, False, True, True, False, False]
    assert [x["label_has_text"] for x in turns] == [False, True, True, False, False, False]
    assert [x["label_misattributed"] for x in turns] == [False, False, True, False, False, False]
    assert [x["label_confidence"] for x in turns] == ["high", "low", "low", "low", "high", "medium"]
    assert c["label_confidence_low_from_high"] == 3 and c["label_misattributed"] == 1


def test_xml_label_misattributed_real_page():
    """25519 turns 28-29: a sentence printed inside the label (turn 28, no text of its own) and the next
    label holding it plus '◯首席專門委員 尙元鍾' (turn 29, whose text is that speaker's)."""
    r = bt.xml_extract(25519, _view(25519), term=16)
    t = {x["turn_seq"]: x for x in r["tables"]["turns"]}
    assert (t[28]["label_has_text"], t[28]["label_misattributed"], t[28]["label_confidence"]) == (True, False, "low")
    assert (t[29]["label_has_text"], t[29]["label_misattributed"], t[29]["label_fused"]) == (True, True, True)
    assert t[29]["label_confidence"] == "low" and t[28]["text"] is None
    assert not t[27]["label_has_text"] and t[27]["label_confidence"] == "high"


def test_norm_match_separators_and_compat():
    assert bt.norm_match("국방부․차관") == bt.norm_match("국방부‧차관") == bt.norm_match("국방부ㆍ차관") \
        == bt.norm_match("국방부·차관") == "국방부·차관"
    assert bt.norm_match("李漢東") == "李漢東" and bt.norm_match("國務總理") == "國務總理"
    assert bt.norm_match("  김 철수  ") == "김 철수" and bt.norm_match("") is None and bt.norm_match(None) is None
    assert bt.norm_match("ＫＢＳ사장") == "KBS사장"
    t = [{"speaker_name": "李漢東", "speaker_pos": "國務總理"}, {"speaker_name": None, "speaker_pos": None}]
    bt.speaker_norm_fields(t)
    assert (t[0]["speaker_name_norm"], t[0]["speaker_pos_norm"]) == ("李漢東", "國務總理")
    assert (t[1]["speaker_name_norm"], t[1]["speaker_pos_norm"]) == (None, None)


def test_null_policy_counts_and_leaves_lists():
    c = Counter()
    tables = {"turns": [{"text": "", "text_raw": "  ", "agenda_text": "x", "stage_texts": [""]}],
              "footer": [{"pos": "", "name": "홍길동"}]}
    bt.null_policy(tables, c)
    assert tables["turns"][0]["text"] is None and tables["turns"][0]["text_raw"] is None
    assert tables["turns"][0]["agenda_text"] == "x" and tables["turns"][0]["stage_texts"] == [""]
    assert tables["footer"][0]["pos"] is None and tables["footer"][0]["name"] == "홍길동"
    assert c["null_policy_turns.text"] == 1 and c["null_policy_turns.text_raw"] == 1 and c["null_policy_footer.pos"] == 1


def test_xml_null_policy_real_pages(xml_results):
    for n, r in xml_results.items():
        for tb, rows in r["tables"].items():
            cols = [f.name for f in bt.SCHEMAS[tb] if f.type == bt.S]
            for row in rows:
                for col in cols:
                    v = row.get(col)
                    assert v is None or v.strip(), (n, tb, col)


def test_attendance_row_seq_and_duplicates():
    c = Counter()
    rows = [{"section_seq": 1, "category": "present", "name": "홍길동"},
            {"section_seq": 1, "category": "present", "name": "김철수"},
            {"section_seq": 1, "category": "present", "name": "홍길동"},
            {"section_seq": 2, "category": "present", "name": "홍길동"}]
    bt.mark_attendance_duplicates(bt.number_rows(rows), c)
    assert [r["row_seq"] for r in rows] == [1, 2, 3, 4]
    assert [r["duplicate_of_row_seq"] for r in rows] == [None, None, 1, None]
    assert c["attendance_exact_duplicate_rows"] == 1 and len(rows) == 4      # counted, never removed


def test_footer_and_attendance_keys_unique(xml_results):
    for n, r in xml_results.items():
        for tb in ("footer", "attendance"):
            seqs = [x["row_seq"] for x in r["tables"][tb]]
            assert seqs == list(range(1, len(seqs) + 1)), (n, tb)


def test_split_agencies_same_as_hwp_parser():
    import hwp_parser as H
    for v in ("中小企業廳․中小企業振興公團", "韓國銀行全北本部(光州全南․大田忠南․忠北 本部 포함)", "A|B, C(D·E)，F", ""):
        assert bt.split_agencies(v) == H.split_agencies(v), v
    assert bt.AGENCY_SEP_CHARS == H.AGENCY_SEP_CHARS


def test_hwp_stub_audited_agencies_and_prefix(stub_hwp, monkeypatch):
    def parse3(data, conf_num=None):
        res = _stub_parse_hwp(data, conf_num)
        res["meeting"].update({"audited_agencies": ["中小企業廳", "中小企業振興公團"],
                               "audited_agencies_raw": ["中小企業廳․中小企業振興公團"],
                               "audited_agencies_label": "被監査機關"})
        t0 = res["turns"][0]
        t0["first_text_raw"] = "위원장 홍길동  개의하겠습니다."
        t0["text_raw"] = "위원장 홍길동 개의하겠습니다.\n(웃음)"
        t0["text"] = "위원장 홍길동 개의하겠습니다."
        res["turns"][1]["first_text_raw"] = "질문."
        res["turns"][1]["after_final_end_marker"] = False
        t0["after_final_end_marker"] = False
        res["stats"]["chars_turn_items"] += len("위원장홍길동")
        res["stats"]["chars_by_category"]["turn_head"] += len("위원장홍길동")
        res["stats"]["chars_total"] += len("위원장홍길동")
        return res
    monkeypatch.setattr(stub_hwp, "parse_hwp", parse3)
    r = bt.hwp_extract(7, b"fake", term=18, sha1="s")
    h = r["header"]
    assert h["audited_agencies"] == ["中小企業廳", "中小企業振興公團"]
    assert h["audited_agencies_how"] == "hwp_cover_被監査機關" and h["audited_agencies_raw"] == "中小企業廳․中小企業振興公團"
    t = r["tables"]["turns"]
    assert t[0]["text"] == "개의하겠습니다." and t[0]["text_label_prefix_stripped"]
    assert t[0]["text_raw"] == "위원장 홍길동 개의하겠습니다.\n(웃음)"
    assert not t[1]["text_label_prefix_stripped"] and r["coverage"]["ok_all"]


@pytest.mark.parametrize("raw,expected", [
    ("임시회", "임시회"), ("정기회", "정기회"), ("임시회·폐회중", "임시회(폐회중)"), ("정기회·폐회중", "정기회(폐회중)"),
    ("臨時會", "임시회"), ("定期會", "정기회"), ("臨時會․閉會中", "임시회(폐회중)"), ("臨時會ㆍ閉會中", "임시회(폐회중)"),
    ("臨時會", "임시회"), ("정기회․폐회중", "정기회(폐회중)"), ("임시회(폐회중)", "임시회(폐회중)"),
    (" 임 시 회 ", "임시회"), ("제1차", None), (None, None),
])
def test_canonical_session_type(raw, expected):
    assert bt.canonical_session_type(raw) == expected


def test_construct_title_and_v9_namespace():
    assert bt.construct_title(18, "국정감사", None, None, "지식경제위원회", None, 2011, None, "2011-09-20") == \
        "제18대국회 2011년도 국정감사 지식경제위원회"
    assert bt.construct_title(18, "국정감사", None, None, "외교통상통일위원회", None, 2008, "구주반", "2008-10-11") == \
        "제18대국회 2008년도 국정감사 외교통상통일위원회 구주반"
    assert bt.construct_title(18, "상임위원회", 278, "제4차", "정무위원회", "법안심사소위원회", None, None, "2008-09-11") == \
        "제18대 제278회 제4차 정무위원회 법안심사소위원회 (2008년 09월 11일)"
    assert bt.construct_title(None, "상임위원회", 278, None, "정무위원회", None, None, None, None) is None
    ns = bt.v9_id_namespace
    assert ns("52829", "052829", 47235) == "conf_id" and ns("052829", "052829", 47235) == "conf_id"
    assert ns("N052829", "N052829", 51884) == "conf_id"
    assert ns("24205", "024300", 24205) == "confer_num"
    assert ns("48520", "050728", 41160) == "none"
    assert ns(None, "050728", 41160) is None and ns("25366", None, 25366) == "confer_num"
    assert ns("99999", None, 25366) == "none"


def test_e2e_meetings_new_columns_and_gap_meeting(e2e, tmp_path):
    """Meetings table 1.8 columns on a small build; one universe row made an id-gap meeting (CONF_ID
    null, source 'gap_scan') is built and keyed by CONFER_NUM only."""
    import pandas as pd
    cfg, only = e2e
    u = pd.read_parquet(bt.Config().universe)
    u = u[u.CONFER_NUM.isin(XML_OK)].copy()
    assert len(u) == len(XML_OK)
    gap = XML_OK[0]
    u.loc[u.CONFER_NUM == gap, ["CONF_ID", "source"]] = [None, "gap_scan"]
    cfg.universe = tmp_path / "universe.parquet"
    u.to_parquet(cfg.universe)
    bt.run(cfg, sources=("xml", "hwp"), workers=1, only=only)
    m = bt.read_table("meetings", out=cfg.out).set_index("conf_num")
    assert m.loc[gap, "conf_id"] is pd.NA and bool(m.loc[gap, "is_built"])
    assert pd.isna(m.loc[gap, "v9_meeting_id"]) or m.loc[gap, "v9_id_namespace"] in ("confer_num", "none")
    b = m[m.is_built.fillna(False) & m.in_universe.fillna(False)]
    assert b.title.notna().all() and set(b.title_source) <= {"viewer_header", "api", "constructed", "printed_cover"}
    assert set(b.session_type.dropna()) <= {"임시회", "정기회", "임시회(폐회중)", "정기회(폐회중)", "특별회"}
    assert (b.session_type.isna() == b.session_type_raw.isna()).all()
    for c in [f for f in m.columns if str(m[f].dtype) == "string"]:
        assert not (m[c].dropna().str.strip() == "").any(), c
    turns = bt.read_table("turns", out=cfg.out)
    assert {"after_final_end_marker", "text_label_prefix_stripped", "label_has_text", "label_misattributed",
            "speaker_name_norm", "speaker_pos_norm"} <= set(turns.columns)
    f = bt.read_table("footer", out=cfg.out)
    assert not f.duplicated(["conf_num", "row_seq"]).any() and f.row_seq.notna().all()


def test_run_drops_meeting_built_from_non_production_source(e2e):
    """A meeting built from XLSX (a former production source) with no view page and no HWP file is
    removed from every table and from the manifest, and the run counts it."""
    cfg, only = e2e
    bt.run(cfg, sources=("xml", "hwp"), workers=1, only=only, build_meetings_table=False)
    n = XML_OK[1]
    con = sqlite3.connect(cfg.state_db)
    con.execute("UPDATE built SET source='xlsx' WHERE conf_num=?", (n,))
    con.commit()
    con.close()
    c = sqlite3.connect(cfg.crawl_db)
    c.execute("DELETE FROM fetch WHERE conf_num=?", (n,))
    c.commit()
    c.close()
    before = bt.read_table("turns", out=cfg.out)
    s = bt.run(cfg, sources=("xml", "hwp"), workers=1, only=only, build_meetings_table=False)
    after = bt.read_table("turns", out=cfg.out)
    assert s["dropped_non_production_meetings"] == 1 and s["dropped_non_production_examples"] == [n]
    assert n not in set(after.conf_num) and len(before) - len(after) == int((before.conf_num == n).sum())
    assert s["dropped_non_production_rows"] >= int((before.conf_num == n).sum())
    con = sqlite3.connect(cfg.state_db)
    assert con.execute("SELECT count(*) FROM built WHERE conf_num=?", (n,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM pending_drops").fetchone()[0] == 0
    con.close()


def test_parse_viewer_name_role_fix_regression():
    """parse_viewer.NAME_ROLE_RE: the Hanja class ends at U+F900-U+FAFF again (an NFC save had turned
    U+F900 into U+8C48, so the class covered every Hangul syllable and the Private Use Area). Over every
    saved viewer sample the fixed and the broken pattern give identical parse_view output."""
    import glob
    import json
    import re
    pv = bt.pv
    assert re.fullmatch(r"[豈-﫿]", "豈") and pv.NAME_ROLE_RE.match("李漢東委員")
    assert not pv.NAME_ROLE_RE.match(" 위원") and pv.NAME_ROLE_RE.match("김성곤위원")
    src = (bt.CODE / "parse_viewer.py").read_text(encoding="utf-8")
    assert chr(0x8C48) + "-" not in src
    broken = re.compile("^(?P<name>[가-힣㐀-䶿一-鿿豈-﫿]{2,4})\\s*"
                        "(?P<role>委員長|委員|議員|위원장|위원|의원)$")
    pages = sorted(glob.glob(str(bt.V10 / "raw" / "samples" / "*_view.html.gz"))) + \
        sorted(glob.glob(str(bt.V10 / "raw" / "samples_random" / "*_view.html.gz")))
    if not pages:
        pytest.skip("no saved viewer samples")
    fixed = pv.NAME_ROLE_RE
    diffs = []
    try:
        for p in pages:
            data = Path(p).read_bytes()
            pv.NAME_ROLE_RE = fixed
            a = json.dumps(pv.parse_view(data), ensure_ascii=False, default=str, sort_keys=True)
            pv.NAME_ROLE_RE = broken
            b = json.dumps(pv.parse_view(data), ensure_ascii=False, default=str, sort_keys=True)
            if a != b:
                diffs.append(p)
    finally:
        pv.NAME_ROLE_RE = fixed
    assert len(pages) >= 190 and diffs == []


def test_label_prefix_match_label_followed_only_by_whitespace():
    """Regression (2026-09-27): a first sentence that is the printed label plus a single trailing
    space raised IndexError (rest.lstrip() was empty). It must be treated like an exact repeat."""
    import build_turns as bt
    kind, end = bt.label_prefix_match("委員長 李允洙 ", "委員長 李允洙", "李允洙")
    assert kind == "label"
    assert "委員長 李允洙 "[:end].strip() == "委員長 李允洙"
    assert bt.label_prefix_match("金樂冀 ", "委員 金樂冀", "金樂冀") == (None, None)


def test_plan_source_override(tmp_path):
    """XML-vs-HWP cross-check overrides (2026-09-28): a listed meeting is built from HWP when its
    HWP file is ok; an override whose source is unavailable falls back to the normal precedence and
    is counted; load_source_override rejects non-production sources and duplicate rows."""
    import pandas as pd
    import pytest
    cfg = bt.Config(out=tmp_path / "out", raw=tmp_path / "raw", source_override=tmp_path / "ovr.parquet")
    for p in (bt.view_path(cfg, 1), bt.hwp_path(cfg, 1), bt.view_path(cfg, 2), bt.view_path(cfg, 3)):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
    universe = pd.DataFrame({"CONFER_NUM": [1, 2, 3], "DAE_NUM": [19, 19, 19]})
    crosswalk = pd.DataFrame({"v9_source": [], "term": [], "api_CONFER_NUM": [], "meeting_id": []})
    crawl = {(1, "view"): ("ok", "a"), (1, "hwp"): ("ok", "b"), (2, "view"): ("ok", "c"), (3, "view"): ("ok", "d")}
    assert bt.load_source_override(cfg) == {}
    pd.DataFrame({"conf_num": [1, 2], "source": ["hwp", "hwp"],
                  "reason": ["xml_incomplete", "xml_wrong_meeting"]}).to_parquet(cfg.source_override)
    ovr = bt.load_source_override(cfg)
    tasks, _, actions = bt.plan(cfg, universe, crosswalk, crawl, {}, {"xml", "hwp"}, override=ovr)
    got = {t["conf_num"]: t["source"] for t in tasks}
    assert got == {1: "hwp", 2: "xml", 3: "xml"}
    assert actions["override_hwp_xml_incomplete"] == 1 and actions["override_source_unavailable"] == 1
    pd.DataFrame({"conf_num": [1], "source": ["xlsx"], "reason": ["x"]}).to_parquet(cfg.source_override)
    with pytest.raises(ValueError):
        bt.load_source_override(cfg)
    pd.DataFrame({"conf_num": [1, 1], "source": ["hwp", "hwp"], "reason": ["x", "y"]}).to_parquet(cfg.source_override)
    with pytest.raises(ValueError):
        bt.load_source_override(cfg)


@pytest.mark.parametrize("label,expected", [
    ("건설교통부장관 추병직", ("건설교통부장관", "추병직", "v10_space_pos_hangul_name")),
    ("산림청산림자원국장 박종호", ("산림청산림자원국장", "박종호", "v10_space_pos_hangul_name")),
    ("진술인 마이클", ("진술인", "마이클", "v10_space_pos_hangul_name")),
    # glued names are not split (a separator-free rule mis-splits position strings)
    ("통일부장관정동영", (None, None, None)),
    ("산업통상자원부장관", (None, None, None)),
    ("한국감정원장", (None, None, None)),
    ("교육부장관 대리", (None, None, None)),
    ("위원장", (None, None, None)),
    ("敎育人的資源部企劃管理室長", (None, None, None)),
])
def test_split_hangul_label(label, expected):
    assert bt.split_hangul_label(label) == expected


def test_audit_team_from_viewer_turn_and_running_header():
    B = bt
    assert B.audit_team_from_viewer_turn("2013년도국정감사 제1반") == "제1반"
    assert B.audit_team_from_viewer_turn("2016년도국정감사 아프리카․중동반") == "아프리카․중동반"
    assert B.audit_team_from_viewer_turn("2013년도국정감사") is None and B.audit_team_from_viewer_turn(None) is None
    rh = B.audit_team_from_running_header
    assert rh(["2009년도국감-행정안전제2반"], "행정안전위원회") == "제2반"
    assert rh(["2010년도국감-외교통상통일미주1반(2010년10월14일)"], "외교통상통일위원회") == "미주1반"
    assert rh("2010년도국감-외교통상통일아프리카․중동반", "외교통상통일위원회") == "아프리카․중동반"
    assert rh(["2009년도국감-환경노동"], "환경노동위원회") is None              # no team
    assert rh(["2010년도국감-기획재정제1반"], None) == "제1반"                  # committee unknown: 제N반 only
    assert rh(None, "행정안전위원회") is None
    assert B._running_header('{"meeting": {"running_header": ["x"]}}') == ["x"] and B._running_header(None) is None
