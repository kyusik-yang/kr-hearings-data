"""Tests for run_all.py (run: python3 -m pytest -q test_run_all.py).

Unit tests use synthetic frames; the end-to-end test builds three small real meetings (one XML, one XLSX,
one HWP) into temporary roots, so v10/interim/pipeline and v10/build are never touched."""
from __future__ import annotations

import dataclasses
import json
import shutil
import sqlite3
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import run_all as R  # noqa: E402
import validate as VA  # noqa: E402

TINY = [23799, 31920, 31883]      # 16대 XML (32 turns), 18대 XLSX (37 rows), 18대 HWP (34 turns)


def _cfg_file(tmp_path, edit):
    c = yaml.safe_load(R.CONFIG_DEFAULT.read_text(encoding="utf-8"))
    edit(c)
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(c, allow_unicode=True), encoding="utf-8")
    return p


# ------------------------------------------------------------------ config

def test_config_defaults_equal_component_defaults():
    cfg = R.load_config()
    p = R.validate_params(cfg)
    d = VA.Params()
    for f in dataclasses.fields(VA.Params):
        if f.name in ("build_state", "raw_root", "docs_templates"):
            continue
        assert getattr(p, f.name) == getattr(d, f.name), f.name
    sw = cfg["switchable"]
    import government as GV
    assert (sw["government"]["buffer_days"], sw["government"]["nominee_pre_days"],
            sw["government"]["nominee_post_days"]) == (GV.BUFFER_DAYS, GV.NOMINEE_PRE_DAYS, GV.NOMINEE_POST_DAYS)
    assert sw["dyads"]["exclude_after_end_marker"] is False and sw["crosswalk"]["v10_complete"] is False
    assert Path(cfg["paths"]["work_root"]) == R.PRODUCTION_WORK_ROOT


def test_config_refuses_unimplemented_fixed_choice(tmp_path):
    def edit(c):
        c["fixed"]["party_timeline"]["partyless_president_ruling"]["value"] = "all_opposition"
    with pytest.raises(R.ConfigError, match="not implemented"):
        R.load_config(_cfg_file(tmp_path, edit))


def test_config_refuses_unknown_switchable_value(tmp_path):
    def edit(c):
        c["switchable"]["legislators"]["name_fuzzy_committee"] = "maybe"
    with pytest.raises(R.ConfigError, match="name_fuzzy_committee"):
        R.load_config(_cfg_file(tmp_path, edit))


def test_config_switchable_values_reach_validate(tmp_path):
    def edit(c):
        c["switchable"]["government"]["suspended_admin"] = "acting"
        c["switchable"]["dyads"]["exclude_after_end_marker"] = True
        c["switchable"]["validate"]["dup_allowlist"] = [[32740, 32864, "source duplicate"]]
    p = R.validate_params(R.load_config(_cfg_file(tmp_path, edit)))
    assert p.suspended_admin == "acting" and p.dyads_exclude_after_end_marker
    assert p.dup_allowlist == ((32740, 32864, "source duplicate"),)


# ------------------------------------------------------------------ post-steps

def test_post_legislators_drop_nulls_every_legislators_column():
    df = pd.DataFrame({"naas_cd": ["A", "B", None], "gender": ["M", "F", None],
                       "id_method": ["exact", "name_fuzzy_committee", None], "id_confidence": ["high", "low", None],
                       "other": [1, 2, 3]})
    c = Counter()
    keep = R.post_legislators(df.copy(), ["naas_cd", "gender", "id_method", "id_confidence"], "keep", c)
    assert keep.equals(df) and c["legislators_name_fuzzy_committee_rows"] == 1
    c = Counter()
    out = R.post_legislators(df.copy(), ["naas_cd", "gender", "id_method", "id_confidence"], "drop", c)
    assert out.loc[1, "naas_cd"] is None and out.loc[1, "gender"] is None and out.loc[1, "id_confidence"] is None
    assert out.loc[1, "id_method"] == "dropped:name_fuzzy_committee" and out.loc[1, "other"] == 2
    assert out.loc[0, "naas_cd"] == "A" and c["legislators_name_fuzzy_committee_dropped"] == 1


def test_post_government_drop_nominee_in_tenure():
    df = pd.DataFrame({"minister_panel_id": ["p1", "p2"], "dual_office": [True, False], "gov_link_name": ["a", "b"],
                       "link_method": ["nominee_in_tenure:exact", "tenure:exact"]})
    c = Counter()
    out = R.post_government(df.copy(), "drop", c)
    assert out.loc[0, "minister_panel_id"] is None and out.loc[0, "link_method"] == "unmatched:nominee_in_tenure_dropped"
    assert out.loc[1, "minister_panel_id"] == "p2" and c["government_nominee_in_tenure_dropped"] == 1
    c = Counter()
    assert R.post_government(df.copy(), "keep", c).equals(df) and c["government_nominee_in_tenure_rows"] == 1


# ------------------------------------------------------------------ helpers

def test_chunks_never_split_a_meeting():
    rows = [{"conf_num": i, "n_turns": n} for i, n in enumerate([5, 5, 20, 1, 1, 1])]
    ch = R._chunks(rows, 10)
    assert [[r["conf_num"] for r in c] for c in ch] == [[0, 1], [2], [3, 4, 5]]
    assert sum(len(c) for c in ch) == len(rows)


def test_publish_moves_previous_output_never_deletes(tmp_path):
    lay = R.Layout(tmp_path / "w", tmp_path / "b", "RUN1")
    (lay.staging).mkdir(parents=True)
    (lay.release / "x.txt").parent.mkdir(parents=True, exist_ok=True)
    (lay.release / "x.txt").write_text("old")
    (lay.staging / "x.txt").write_text("new")
    r = R.Layout.publish(lay, lay.staging / "x.txt", "x.txt")
    assert (lay.release / "x.txt").read_text() == "new"
    assert (lay.build / r["superseded_to"]).read_text() == "old" and not Path(r["superseded_to"]).is_absolute()
    (lay.staging / "s.json").write_text("{}")
    r2 = lay.publish_state(lay.staging / "s.json", "partitions/x.json")
    assert (lay.state_dir / "partitions" / "x.json").exists() and not (lay.release / "partitions").exists()


def test_stage_runner_reports_rss_and_errors(tmp_path):
    cfg = R.load_config()
    r = R.run_stage("t", "stage_sleep_alloc", {"mb": 50, "seconds": 0.2}, cfg, tmp_path)
    assert r["result"] == {"held_mb": 50} and r["peak_rss_stage_process_mb"] >= 50
    with pytest.raises(R.StageError, match="requested failure"):
        R.run_stage("t", "stage_sleep_alloc", {"fail": True}, cfg, tmp_path)
    cfg2 = json.loads(json.dumps(cfg))
    cfg2["resources"]["max_rss_gb"] = 0.15
    cfg2["resources"]["rss_poll_seconds"] = 0.2
    with pytest.raises(R.StageError, match="RSS limit"):
        R.run_stage("t", "stage_sleep_alloc", {"mb": 400, "seconds": 5}, cfg2, tmp_path)


def test_subset_run_refuses_production_work_root():
    with pytest.raises(SystemExit):
        R.main(["--conf-nums", "23799", "--work-root", str(R.PRODUCTION_WORK_ROOT), "--stages", "manifest"])


def test_sample_is_seeded_and_spans_sources():
    cfg = R.load_config()
    a = R.select_sample(cfg, 30, seed=8374)
    assert a == R.select_sample(cfg, 30, seed=8374)
    assert 28769 in a and len(a) >= 30


# ------------------------------------------------------------------ end to end on three real meetings

@pytest.fixture(scope="module")
def e2e(tmp_path_factory):
    root = tmp_path_factory.mktemp("runall")
    cfg = R.load_config(overrides={"resources": {"build_turns_workers": 1}})
    uni = root / "work" / "subset_universe.parquet"
    info = R.write_subset_universe(cfg, TINY, uni)
    assert info["rows"] == len(TINY)
    r1 = R.run(cfg, root / "work", root / "build", only=TINY, universe=uni, subset_info=info)
    r2 = R.run(cfg, root / "work", root / "build", only=TINY, universe=uni, subset_info=info)
    return root, cfg, r1, r2


def test_e2e_outputs_match_manifest(e2e):
    root, cfg, r1, _ = e2e
    con = sqlite3.connect(root / "work" / "build_turns" / "state.sqlite")
    built = dict(con.execute("SELECT conf_num, n_turns FROM built").fetchall())
    con.close()
    assert {23799, 31920} <= set(built)          # xml and xlsx always build; hwp depends on the parser in place
    t = pd.read_parquet(root / "build" / "release" / "turns", columns=["conf_num", "turn_seq", "sitting_seq", "after_end_marker",
                                                                       "label_how", "label_confidence", "role_group", "text_raw"])
    assert t.groupby("conf_num").size().to_dict() == built
    assert t.groupby("conf_num").turn_seq.apply(lambda s: list(s) == list(range(1, len(s) + 1))).all()
    assert t.label_how.notna().all() and t.sitting_seq.notna().all() and t.text_raw.notna().all()
    man = json.loads((root / "build" / "release" / "MANIFEST.json").read_text())
    assert man["row_counts"]["turns"] == sum(built.values())
    assert all(len(f["sha256"]) == 64 for f in man["files"])
    assert {"turns", "dyads", "meetings", "agenda", "events", "footer", "rollcall", "attendance",
            "crosswalk_meetings", "crosswalk_turns", "duplicate_meetings"} <= set(man["row_counts"])
    assert man["config"]["switchable"]["dyads"]["exclude_after_end_marker"] is False
    rep = json.loads((root / "build" / "release" / "validation_report.json").read_text())
    st = {c["id"]: c["status"] for c in rep["checks"]}
    for c in ("dyads_recompute", "dyads_sitting", "turns_sittings", "dyads_endpoints", "turns_contiguous",
              "keys_unique", "coverage_builder", "coverage_recompute", "schema_turns", "schema_dyads",
              "ruling_recompute", "presidency_by_date", "crosswalk_integrity", "crosswalk_turns_integrity",
              "universe_accounted", "label_how_weak_share", "dyads_attributes", "dyads_flags", "duplicates_resolved",
              "release_no_local_paths", "partyless_windows", "domains"):
        assert st[c] == "PASS", (c, st[c])
    for stage in ("build_turns", "duplicates", "enrich", "tables", "dyads", "crosswalk", "validate"):
        assert "peak_rss_stage_process_mb" in r1["stages"][stage]


def test_e2e_second_run_skips_and_keeps_files(e2e):
    root, cfg, r1, r2 = e2e
    for s in ("duplicates", "enrich", "tables", "dyads", "crosswalk"):
        assert r2["stages"][s].get("skipped"), s
    assert r2["stages"]["build_turns"]["result"]["tasks_by_source"] == {}
    m1 = {f["path"]: f["sha256"] for f in json.loads((root / "build" / "release" / "MANIFEST.json").read_text())["files"]}
    assert m1 and all(not p.startswith(("_superseded", "_state", ".staging")) for p in m1)


def test_e2e_forced_enrich_supersedes_not_deletes(e2e):
    root, cfg, r1, _ = e2e
    uni = root / "work" / "subset_universe.parquet"
    before = sorted(p.name for p in (root / "build" / "release" / "turns").glob("t*"))
    r3 = R.run(cfg, root / "work", root / "build", stages=("enrich", "manifest"), force=("enrich",), only=TINY,
               universe=uni)
    assert ["t" + k for k in sorted(r3["stages"]["enrich"]["result"])] == before
    sup = root / "build" / "_superseded" / r3["run_id"] / "turns"
    assert sorted(p.name for p in sup.glob("t*")) == before



# ------------------------------------------------------------------ task R3 (2026-09-26): release layout, duplicates

def test_e2e_release_holds_only_release_files_and_no_local_paths(e2e):
    root, cfg, r1, r2 = e2e
    rel = root / "build" / "release"
    names = {p.relative_to(rel).parts[0] for p in rel.iterdir()}
    assert names <= {"turns", "dyads", "meetings.parquet", "agenda.parquet", "agenda_header.parquet", "events.parquet",
                     "footer.parquet", "rollcall.parquet", "rollcall_groups.parquet", "attendance.parquet",
                     "crosswalk_meetings.parquet", "crosswalk_turns.parquet", "duplicate_meetings.parquet",
                     "duplicate_turns", "duplicate_dyads", "validation_report.json", "docs_numbers.json", "MANIFEST.json"}, names
    assert not list(rel.rglob("_SUCCESS*")) and (root / "build" / "_state" / "crosswalk_stats.json").exists()
    assert r1["release_clean"] and r1["release_scan"]["files_with_hits"] == 0
    import getpass
    for p in rel.rglob("*.json"):
        t = p.read_text()
        assert "/private/" not in t and "/Users/" not in t and getpass.getuser() not in t, p.name
    man = json.loads((rel / "MANIFEST.json").read_text())
    assert len(man["code_version"]["sha256"]) == 64 and man["code_version"]["n_files"] > 10
    assert "run_all.py" in " ".join(man["code_version"]["files"]) and not any("test_" in k for k in man["code_version"]["files"])
    assert len(man["universe_snapshot"]["sha256"]) == 64 and man["universe_snapshot"]["rows"] == 3
    assert man["crawl_snapshot"]["available"] and man["crawl_snapshot"]["fetched_at_max"]
    assert man["tables"]["dyads"]["columns"][0] == "conf_num:int64" and man["tables"]["dyads"]["n_columns"] == 39
    assert all(t["schemas_agree"] for t in man["tables"].values())
    m = pd.read_parquet(rel / "meetings.parquet")
    assert {"duplicate_of", "duplicate_basis", "overlap_with", "overlap_kinds"} <= set(m.columns)


def test_code_version_is_content_hash(tmp_path, monkeypatch):
    a = R.code_version()
    assert a == R.code_version() and len(a["sha256"]) == 64
    assert "pipeline/run_all.py" in a["files"] and "pipeline/config.yaml" in a["files"]


def test_printed_vs_api_and_rank():
    api = {"CONF_DATE": "2017-02-21", "sitting": "제4차", "COMM_NAME": "산업통상자원위원회 법률안소위원회"}
    m_copy = {"date_printed": "2017-02-28", "sitting": "제4차", "committee_printed": "산업통상자원위원회",
              "subcommittee_printed": "법률안소위원회"}
    assert R.printed_vs_api(m_copy, api) == {"date": False, "sitting": True, "committee": True}
    api2 = {"CONF_DATE": "2017-02-28", "sitting": "제4차", "COMM_NAME": "산업통상자원위원회"}
    m_ok = {"date_printed": "2017-02-28", "sitting": "제4차", "committee_printed": "산업통상자원위원회"}
    assert R.printed_vs_api(m_ok, api2) == {"date": True, "sitting": True, "committee": True}
    # Hanja committee name: not comparable
    assert R.printed_vs_api({"committee_printed": "文化體育觀光放送通信委員會"}, {"COMM_NAME": "문화체육관광방송통신위원회"})["committee"] is None
    assert R.printed_vs_api(m_ok, None) == {"date": None, "sitting": None, "committee": None}


def test_resolve_duplicate_pairs_keeps_api_matching_copy_and_flags_overlaps():
    meetings = {42004: {"date_printed": "2017-02-28", "sitting": "제4차", "committee_printed": "산업통상자원위원회"},
                42009: {"date_printed": "2017-02-28", "sitting": "제4차", "committee_printed": "산업통상자원위원회",
                        "subcommittee_printed": "법률안소위원회"},
                35218: {"date_printed": "2011-06-17", "sitting": "제3차", "committee_printed": "文化體育觀光放送通信委員會"},
                35291: {"date_printed": "2011-06-17", "sitting": "제3차", "committee_printed": "文化體育觀光放送通信委員會"},
                1: {}, 2: {}, 3: {}}
    uni = {42004: {"CONF_DATE": "2017-02-28", "sitting": "제4차", "COMM_NAME": "산업통상자원위원회"},
           42009: {"CONF_DATE": "2017-02-21", "sitting": "제4차", "COMM_NAME": "산업통상자원위원회 법률안소위원회"},
           35218: {"CONF_DATE": "2011-06-17", "sitting": "제1차", "COMM_NAME": "문화체육관광방송통신위원회 법안심사소위원회"},
           35291: {"CONF_DATE": "2011-06-17", "sitting": "제3차", "COMM_NAME": "문화체육관광방송통신위원회 법안심사소위원회"}}
    pairs = [{"a": 42004, "b": 42009, "kind": "identical"}, {"a": 35218, "b": 35291, "kind": "identical"},
             {"a": 1, "b": 2, "kind": "identical"}, {"a": 2, "b": 3, "kind": "identical"},     # a group of three, no evidence
             {"a": 43313, "b": 43536, "kind": "partial"}]
    meetings.update({43313: {}, 43536: {}})
    d = R.resolve_duplicate_pairs(pairs, meetings, uni)
    assert d["duplicate_of"] == {42009: 42004, 35218: 35291, 2: 1, 3: 1}
    assert "date=mismatch" in d["duplicate_basis"][42009] and "sitting=mismatch" in d["duplicate_basis"][35218]
    assert "tie" in d["duplicate_basis"][2]
    assert d["overlap"] == {43313: [[43536, "partial"]], 43536: [[43313, "partial"]]}
    row = [p for p in d["pairs"] if p["a"] == 42004][0]
    assert row["kept"] == 42004 and row["duplicate"] == 42009 and row["b_date_match"] is False


def test_meetings_with_duplicates_adds_columns_only(tmp_path):
    import pyarrow as pa
    t = pa.table({"conf_num": pa.array([1, 2, 3], pa.int64()), "title": ["a", "b", "c"]})
    src, dst = tmp_path / "m.parquet", tmp_path / "o.parquet"
    pq.write_table(t, src)
    info = R.meetings_with_duplicates(src, dst, {"duplicate_of": {"2": 1}, "duplicate_basis": {"2": "x"},
                                                 "overlap": {"1": [[3, "partial"]], "3": [[1, "partial"]]}})
    o = pq.read_table(dst)
    assert o.column_names == ["conf_num", "title", "duplicate_of", "duplicate_basis", "overlap_with", "overlap_kinds"]
    assert o.column("duplicate_of").to_pylist() == [None, 1, None] and o.column("overlap_with").to_pylist() == [[3], None, [1]]
    assert info == {"rows": 3, "duplicate_of": 1, "with_overlap": 2}
    with pytest.raises(R.StageError):
        R.meetings_with_duplicates(src, dst, {"duplicate_of": {"9": 1}})


def test_pre_release_layout_moved_not_deleted(tmp_path):
    lay = R.Layout(tmp_path / "w", tmp_path / "b", "RUNX")
    (lay.build / "turns" / "t16").mkdir(parents=True)
    (lay.build / "turns" / "t16" / "part-00000.parquet").write_text("x")
    (lay.build / "MANIFEST.json").write_text("{}")
    moved = R.move_pre_release_layout(lay)
    assert sorted(moved) == ["MANIFEST.json", "turns"]
    assert (lay.superseded / "_pre_release_layout" / "turns" / "t16" / "part-00000.parquet").read_text() == "x"
    assert not (lay.build / "turns").exists()


def test_scan_release_finds_user_name(tmp_path):
    import getpass
    lay = R.Layout(tmp_path / "w", tmp_path / "b", "RUNS")
    lay.release.mkdir(parents=True)
    (lay.release / "a.json").write_text(json.dumps({"x": "v10/build/release"}))
    assert R.scan_release(lay)["files_with_hits"] == 0
    (lay.release / "b.json").write_text(json.dumps({"x": f"/Users/{getpass.getuser()}/y"}))
    sc = R.scan_release(lay)
    assert sc["files_with_hits"] == 1 and getpass.getuser() not in json.dumps(sc)


def test_config_accepts_decision6_value_and_legacy_null(tmp_path):
    cfg = R.load_config()
    assert cfg["fixed"]["party_timeline"]["partyless_president_ruling"]["value"] == "last_president_party"
    assert R.validate_params(cfg).partyless_rule == "last_president_party"

    def edit(c):
        c["fixed"]["party_timeline"]["partyless_president_ruling"]["value"] = "null"
    assert R.validate_params(R.load_config(_cfg_file(tmp_path, edit))).partyless_rule == "null"

    def edit2(c):
        c["switchable"]["duplicates"]["near_identical_min_share"] = 1.5
    with pytest.raises(R.ConfigError, match="near_identical_min_share"):
        R.load_config(_cfg_file(tmp_path, edit2))
