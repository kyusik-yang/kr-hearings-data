"""run_all.py - build the v10 release tables end to end (incremental, safe to re-run).

    python3 run_all.py                                # full build into v10/build/release/
    python3 run_all.py --sample 200                   # dry run on a seeded subset (separate roots)
    python3 run_all.py --conf-nums 28769,33754 --work-root DIR --build-root DIR
    python3 run_all.py --stages enrich,dyads --force enrich

Stages (each runs in its own spawned process; peak RSS is logged and a process above
resources.max_rss_gb is stopped):
  build_turns  build_turns.run() on the work root (incremental: only new / changed meetings are parsed;
               an adapter or parser change rebuilds the affected sources). Its manifest (state.sqlite
               `built`) is the list of meetings the release contains.
  duplicates   researcher decision 7: meeting pairs found by the duplicate-content checks of validate.py
               (dup_meeting_text, dup_long_turn, dup_shingle) on the built turns are classified: 'identical'
               (equal whole-meeting normalized text) -> one copy is kept (the copy whose printed date,
               sitting and committee match its Open API row; tie: lower conf_num), every other copy gets
               meetings.duplicate_of and its turns / dyads go to the duplicate tables instead of the release
               turns / dyads; 'near_identical' (same turn count, >= near_identical_min_share of the turns with
               equal normalized text) and 'partial' pairs are kept and flagged (meetings.overlap_with /
               overlap_kinds). -> release/duplicate_meetings.parquet (one row per pair) and _state/duplicates.json
  enrich       per term (meetings.term): the turns of the manifest's batches, without their text columns,
               go through roles.enrich -> legislators.enrich -> [post-step legislators] ->
               party_timeline.enrich -> government.enrich -> [post-step government] in chunks of at most
               resources.enrich_chunk_turns turns (a meeting is never split); the enrichment columns are
               joined back to the full turns in duckdb and written to release/turns/tNN/part-KKKKK.parquet
               (rows ordered by conf_num, turn_seq); the turns of duplicate copies go to
               release/duplicate_turns/tNN/. Every chunk is checked: same rows and keys in the same order, no
               input column modified by an enricher.
  tables       meetings (+ duplicate_of, duplicate_basis, overlap_with, overlap_kinds), agenda, agenda_header,
               events, footer, rollcall, rollcall_groups, attendance -> release/<table>.parquet
  dyads        dyads.build_dyads_file per term (slim layout) -> release/dyads/tNN/dyads.parquet, and the
               dyads of duplicate copies -> release/duplicate_dyads/tNN/dyads.parquet
  crosswalk    crosswalk.build -> release/crosswalk_meetings.parquet, crosswalk_turns.parquet
               (crosswalk_stats.json is bookkeeping: _state/)
  validate     validate.run_validation -> release/validation_report.json, release/docs_numbers.json
  manifest     release/MANIFEST.json: per file rows, columns, bytes, sha256; code version (a hash of the
               pipeline files' contents, independent of git), config values, universe snapshot hash, crawl
               snapshot time; then every release file is scanned for local paths / the user name.

Layout: build/release/ holds only the release tables, MANIFEST.json, validation_report.json and
docs_numbers.json, with repository-relative paths and no local path or user name. Bookkeeping stays outside
it: build/_state/ (state, logs, run records, stage statistics), build/_superseded/<run_id>/ (replaced
outputs, never deleted), build/.staging/. A build made with the earlier layout (release files directly
under build/) is moved to build/_superseded/<run_id>/_pre_release_layout/ at the first run.

Incremental: a stage output is rebuilt only when its fingerprint changes (inputs from the manifest, the
code of the modules it runs, the reference files the enrichers read, the duplicate decisions and the config
parameters it uses). Outputs are written under build/.staging/ and then moved into place; a replaced output
is moved to build/_superseded/<run_id>/, never deleted. One run at a time (build/_state/run_all.lock).

Parameters: config.yaml (switchable choices are applied, fixed ones must equal the component default).
No network access. Seed 8374.
"""
from __future__ import annotations

import argparse
import contextlib
import copy
import dataclasses
import datetime as dt
import fcntl
import hashlib
import json
import logging
import multiprocessing as mp
import os
import platform
import re
import resource
import shutil
import sqlite3
import sys
import time
import traceback
import warnings
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

import yaml

HERE = Path(__file__).resolve().parent            # v10/code/pipeline
CODE = HERE.parent                                # v10/code
V10 = CODE.parent
REPO = V10.parent
for _p in (str(CODE), str(HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

SEED = 8374
RUN_ALL_VERSION = "1.0"
CONFIG_DEFAULT = HERE / "config.yaml"
PRODUCTION_WORK_ROOT = V10 / "interim" / "pipeline"
DRYRUN_ROOT = V10 / "interim" / "pipeline" / "run_all" / "dryrun"
STAGES = ("build_turns", "duplicates", "enrich", "tables", "dyads", "crosswalk", "validate", "manifest")
RELEASE_DIR = "release"
# release files of the layout before 2026-09-26 (directly under build/): moved aside at the first run
PRE_RELEASE_LAYOUT = ("turns", "dyads", "meetings.parquet", "agenda.parquet", "agenda_header.parquet", "events.parquet",
                      "footer.parquet", "rollcall.parquet", "rollcall_groups.parquet", "attendance.parquet",
                      "crosswalk_meetings.parquet", "crosswalk_turns.parquet", "crosswalk_stats.json",
                      "validation_report.json", "docs_numbers.json", "MANIFEST.json")
# files whose contents make up the code version recorded in MANIFEST.json (git-independent)
CODE_VERSION_GLOBS = ("pipeline/*.py", "parse_viewer.py", "legacy_rules.py", "pipeline/config.yaml")
LOG = logging.getLogger("run_all")

# turn columns the enrichers never read (kept out of the pandas frames, joined back in duckdb)
TEXT_HEAVY_COLS = ("text_raw", "text", "stage_texts", "inline_stage_parens", "interjections", "agenda_text",
                   "agenda_top_text")
# release tables copied from the build_turns work root (besides turns)
COPY_TABLES = ("agenda", "agenda_header", "events", "footer", "rollcall", "rollcall_groups", "attendance")

# code files whose content is part of a stage fingerprint
ENRICH_CODE = ("roles.py", "legislators.py", "party_timeline.py", "government.py", "run_all.py")
SHARED_CODE = ("../legacy_rules.py",)
# reference data read by the enrichers (their paths are module constants; size + mtime fingerprinted)
REFERENCE_GLOBS = (
    "interim/pipeline/legislators/*.parquet", "interim/pipeline/party_timeline/*.parquet",
    "interim/members_party_spells_21.parquet", "interim/members_term_16_22.parquet",
    "interim/members_allnamember_16_22.parquet", "interim/party_lineage.csv", "interim/president_calendar.csv",
    "interim/meeting_universe_api.parquet", "interim/04_v9_speaker_role_table_xlsx_era.parquet",
    "raw/third_party/hanja_table_0.15.1.yml",
    # minister panel snapshots (government.py reads only these; their MANIFEST carries every sha256)
    "interim/external/minister_data_*/MANIFEST.json",
    # XML-vs-HWP source overrides and the extended universe
    "interim/pipeline/xml_hwp_crosscheck/source_override.parquet", "interim/meeting_universe_v10.parquet",
)


class ConfigError(ValueError):
    pass


class StageError(RuntimeError):
    pass


# ============================================================================ config

# Values run_all.py can honour for each switchable parameter.
SWITCHABLE_ALLOWED = {
    ("dyads", "exclude_after_end_marker"): (True, False),
    ("legislators", "name_fuzzy_committee"): ("keep", "drop"),
    ("government", "suspended_admin"): ("president", "acting"),
    ("government", "nominee_in_tenure"): ("keep", "drop"),
    ("crosswalk", "v10_complete"): (True, False),
    ("validate", "mode"): ("release", "dev"),
}
# The coded behaviour of every `fixed` entry (config.yaml must repeat it; anything else is refused).
FIXED_DEFAULTS = {
    "party_timeline": {"partyless_president_ruling": "last_president_party", "committee_table_inferences": "accept",
                       "merger_rename_date": "earlier_of_lineage_and_notice",
                       "satellite_party_camp": "main_party_before_merger",
                       "speaker_nonpartisan": "recorded_status_independent",
                       "individual_exceptions_22": "use_lineage_exceptions"},
    "legislators": {"nonleg_title_name_uniqueness_link": "no", "future_member_link": "no",
                    "label_repair_confidence_cap": "medium"},
    "government": {"acting_minister_link": "acting_heads", "ministry_naming": "name_in_force_at_the_time",
                   "panel_coverage_gaps": "leave_unlinked"},
    "roles": {"ijangjang_by_institution": "by_institution_type", "geomsajang": "agency_head",
              "beobwonjang": "other_official", "audit_team_leader_banjang": "chair",
              "company_marked_titles": "private_sector", "independent_commission_staff": "organisation_based",
              "gisulwonjang": "org_head", "national_arts_directors": "private_sector", "witness_counsel": "witness",
              "legislator_commission_head": "printed_title", "contains_matching_from_v9": "keep"},
    "build_turns": {"special_committee_key": "single_special_committee_key",
                    "agenda_adjustment_committee": "not_subcommittee_flagged",
                    "confirmation_hearing_rule": "special_committee_or_own_agenda_text",
                    "fused_hanja_label_split": "accept_flagged_rules", "orphan_taL_paragraphs_24614": "events",
                    "sitting_rule": "end_then_open_markers", "label_confidence_table": "build_turns.LABEL_CONFIDENCE"},
    "hwp_parser": {"table_lines_in_text": "keep_in_text", "quoted_transcript_markers": "keep_inside_quoting_turn",
                   "label_missing_marker": "keep_turn_label_missing",
                   "inner_attendance_vote_lists": "appendix_inner_events"},
    "dyads": {"procedural_regex": "frozen_round3", "chair_utterances": "keep_with_flags",
              "release_layout": "slim"},
    "validate": {"speech_before_meeting_date": "day_before_and_appended_records_warn"},
    "crosswalk": {"partial_label_match": "same_partial", "v9_49517_to_41344": "add_meeting"},
    "duplicates": {"identical_text": "keep_copy_matching_api", "near_identical": "keep_flagged",
                   "partial_overlap": "keep_flagged"},
}
# fixed entries whose component implements more than one value during a hand-over (every listed value is
# honoured; the first is the researcher's decision)
FIXED_ACCEPTED = {
    # researcher decision 6 (2026-09-26); 'null' = the earlier party_timeline behaviour, still accepted until
    # party_timeline.py and config.yaml carry the decision
    ("party_timeline", "partyless_president_ruling"): ("last_president_party", "null"),
    # crosswalk: add_meeting once 41344 is in the meetings table (id-gap universe); the rule itself is data-driven
    ("crosswalk", "v9_49517_to_41344"): ("add_meeting", "not_in_universe"),
    # minister-data v2 adopted (researcher 2026-09-28; rc3 approved, v2.0.0 released the same day): acting heads link to acting_heads.csv rows
    ("government", "acting_minister_link"): ("acting_heads", "never"),
    # validate.py dates_meeting since 2026-09-28: one day before the meeting date (overnight sittings) and
    # the printed date of an appended record in a later sitting are WARN, not FAIL
    ("validate", "speech_before_meeting_date"): ("day_before_and_appended_records_warn", "not_allowed"),
}
VALIDATE_PARAM_KEYS = (
    "mode", "max_missing", "dup_containment", "dup_long_min_chars", "dup_long_min_shared", "dup_max_df",
    "dup_shingle_len", "dup_shingle_min_shared", "dup_shingle_min_n", "dup_meeting_min_chars",
    "wit_title_max_share", "wit_title_max_share_by_type", "link_thresholds", "party_thresholds",
    "max_linked_party_null", "speech_date_end_slack_days", "max_meeting_span_days", "coverage_sample_xml",
    "coverage_sample_hwp", "procedural_sample", "label_weak_rules", "label_weak_max_share")


def load_config(path=CONFIG_DEFAULT, overrides=None) -> dict:
    """Read config.yaml, apply `overrides` ({section: {key: value}} merged into `switchable` / `resources` /
    `paths`), check every value, resolve paths against v10/. Raises ConfigError."""
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    for k, v in (overrides or {}).items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            for k2, v2 in v.items():
                if isinstance(v2, dict) and isinstance(cfg[k].get(k2), dict):
                    cfg[k][k2].update(v2)
                else:
                    cfg[k][k2] = v2
        else:
            cfg[k] = v
    for sec in ("paths", "resources", "switchable", "fixed", "sources"):
        if sec not in cfg:
            raise ConfigError(f"config lacks section {sec!r}")
    sw = cfg["switchable"]
    for (sec, key), allowed in SWITCHABLE_ALLOWED.items():
        val = sw.get(sec, {}).get(key)
        if val not in allowed:
            raise ConfigError(f"switchable.{sec}.{key} = {val!r}; run_all.py implements {list(allowed)}")
    g = sw["government"]
    for k in ("buffer_days", "nominee_pre_days", "nominee_post_days"):
        if not isinstance(g.get(k), int) or g[k] < 0:
            raise ConfigError(f"switchable.government.{k} must be a non-negative integer, got {g.get(k)!r}")
    for sec, items in FIXED_DEFAULTS.items():
        got = cfg["fixed"].get(sec) or {}
        for key, default in items.items():
            ent = got.get(key)
            if not isinstance(ent, dict) or "value" not in ent:
                raise ConfigError(f"fixed.{sec}.{key} is missing (component default {default!r})")
            ok = FIXED_ACCEPTED.get((sec, key), (default,))
            if str(ent["value"]) not in [str(x) for x in ok]:
                raise ConfigError(f"fixed.{sec}.{key} = {ent['value']!r} is not implemented: the component codes "
                                  f"{default!r}; honouring {ent['value']!r} needs a change in the {sec} component")
    extra = {f"{s}.{k}" for s, items in cfg["fixed"].items() for k in (items or {})} - \
        {f"{s}.{k}" for s, items in FIXED_DEFAULTS.items() for k in items}
    if extra:
        raise ConfigError(f"unknown fixed entries: {sorted(extra)}")
    val = sw["validate"]
    for k in VALIDATE_PARAM_KEYS:
        if k not in val:
            raise ConfigError(f"switchable.validate.{k} is missing")
    for a in val.get("dup_allowlist") or []:
        if not (isinstance(a, (list, tuple)) and len(a) == 3):
            raise ConfigError(f"dup_allowlist entries are [conf_num_a, conf_num_b, reason], got {a!r}")
    dp = sw.get("duplicates") or {}
    x = dp.get("near_identical_min_share")
    if not isinstance(x, (int, float)) or not 0 < float(x) <= 1:
        raise ConfigError(f"switchable.duplicates.near_identical_min_share must be in (0, 1], got {x!r}")
    bad_src = set(cfg["sources"]) - {"xml", "xlsx", "hwp"}
    if bad_src:
        raise ConfigError(f"unknown sources {bad_src}")
    for k, v in list(cfg["paths"].items()):
        p = Path(v)
        cfg["paths"][k] = str(p if p.is_absolute() else V10 / p)
    return cfg


def validate_params(cfg: dict, universe_override=None):
    """validate.Params from the config (the switchable values the checks must agree with included)."""
    import validate as VA
    v = cfg["switchable"]["validate"]
    kw = {k: v[k] for k in VALIDATE_PARAM_KEYS}
    kw["link_thresholds"] = {int(a): float(b) for a, b in v["link_thresholds"].items()}
    kw["party_thresholds"] = {int(a): float(b) for a, b in v["party_thresholds"].items()}
    kw["label_weak_rules"] = tuple(v.get("label_weak_rules") or ())
    kw["dup_allowlist"] = tuple((int(a), int(b), str(r)) for a, b, r in (v.get("dup_allowlist") or []))
    kw["suspended_admin"] = cfg["switchable"]["government"]["suspended_admin"]
    pr = str(cfg["fixed"]["party_timeline"]["partyless_president_ruling"]["value"])
    kw["partyless_rule"] = "null" if pr == "null" else "last_president_party"
    kw["dyads_exclude_after_end_marker"] = bool(cfg["switchable"]["dyads"]["exclude_after_end_marker"])
    kw["build_state"] = str(Path(cfg["paths"]["work_root"]) / "build_turns" / "state.sqlite")
    kw["raw_root"] = str(V10 / "raw")
    return VA.Params(**kw)


# ============================================================================ small helpers

def _sha256_file(path, cache=None) -> str:
    p = Path(path)
    st = p.stat()
    key = str(p)
    if cache is not None and key in cache and cache[key][0] == st.st_size and cache[key][1] == st.st_mtime_ns:
        return cache[key][2]
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    d = h.hexdigest()
    if cache is not None:
        cache[key] = (st.st_size, st.st_mtime_ns, d)
    return d


def _fp(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str, ensure_ascii=False).encode()).hexdigest()[:16]


def code_sha(names) -> dict:
    return {n: _sha256_file(HERE / n)[:16] for n in names}


def reference_fingerprint() -> dict:
    out = {}
    for g in REFERENCE_GLOBS:
        for p in sorted(V10.glob(g)):
            st = p.stat()
            out[str(p.relative_to(V10))] = [st.st_size, st.st_mtime_ns]
    try:
        import government as GV
        p = Path(GV.PANEL_PATH)
        if p.exists():
            out["minister_panel"] = [p.stat().st_size, p.stat().st_mtime_ns]
    except Exception as e:  # recorded, the fingerprint then changes when the import works again
        out["minister_panel_error"] = repr(e)
    return out


def _duck(cfg, memory=None):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory or cfg['resources']['duckdb_memory']}'")
    con.execute(f"SET threads={int(cfg['resources']['duckdb_threads'])}")
    con.execute("SET enable_progress_bar=false")
    con.execute("SET preserve_insertion_order=true")
    return con


def _s(x) -> str:
    return "'" + str(x).replace("'", "''") + "'"


def _rss_units() -> int:
    return 1 if platform.system() == "Darwin" else 1024      # ru_maxrss: bytes on macOS, KiB on Linux


def _setup_logging(build_root: Path):
    (build_root / "_state").mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.INFO)
    fh_path = str(build_root / "_state" / "run_all.log")
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == fh_path for h in LOG.handlers):
        fh = logging.FileHandler(fh_path)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        LOG.addHandler(fh)
    if not any(type(h) is logging.StreamHandler for h in LOG.handlers):
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter("%(asctime)s run_all %(message)s"))
        LOG.addHandler(sh)


@contextlib.contextmanager
def run_lock(build_root: Path):
    p = build_root / "_state" / "run_all.lock"
    p.parent.mkdir(parents=True, exist_ok=True)
    f = open(p, "a+")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        raise StageError(f"another run_all run holds {p}")
    try:
        f.seek(0)
        f.truncate()
        f.write(f"pid={os.getpid()} since={dt.datetime.now().isoformat(timespec='seconds')}\n")
        f.flush()
        yield
    finally:
        f.seek(0)
        f.truncate()
        f.flush()
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()


class Layout:
    """Paths of one build (work root = build_turns out; build root = release/ + bookkeeping)."""

    def __init__(self, work_root, build_root, run_id):
        self.work = Path(work_root)
        self.build = Path(build_root)
        self.release = self.build / RELEASE_DIR
        self.run_id = run_id
        self.state_dir = self.build / "_state"
        self.staging = self.build / ".staging"
        self.superseded = self.build / "_superseded" / run_id

    def publish(self, src: Path, rel: str) -> dict:
        """Move `src` (file or directory under .staging) to release/<rel>; a previous output there is moved to
        _superseded/<run_id>/<rel> first (never deleted)."""
        return self._move_in(src, self.release, rel)

    def publish_state(self, src: Path, rel: str) -> dict:
        """Same for a bookkeeping file: _state/<rel>."""
        return self._move_in(src, self.state_dir, rel, sup_prefix="_state")

    def retire(self, rel: str) -> Optional[str]:
        """Move release/<rel> (an output that no longer belongs to the release) to _superseded; never deleted."""
        dest = self.release / rel
        if not dest.exists():
            return None
        old = self.superseded / rel
        old.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(dest), str(old))
        return self.rel(old)

    def _move_in(self, src: Path, root: Path, rel: str, sup_prefix: str = "") -> dict:
        dest = root / rel
        moved = None
        if dest.exists():
            old = self.superseded / sup_prefix / rel if sup_prefix else self.superseded / rel
            old.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(dest), str(old))
            moved = self.rel(old)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dest)
        return {"published": rel, "superseded_to": moved}

    def rel(self, p) -> str:
        """A path relative to the build root (bookkeeping records never hold local absolute paths)."""
        try:
            return str(Path(p).relative_to(self.build))
        except ValueError:
            return str(p)

    def load_state(self) -> dict:
        p = self.state_dir / "state.json"
        return json.loads(p.read_text()) if p.exists() else {}

    def save_state(self, st: dict):
        p = self.state_dir / "state.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.parent / ".state.json.tmp"
        tmp.write_text(json.dumps(st, indent=1, sort_keys=True, default=str))
        os.replace(tmp, p)


# ============================================================================ stage runner

def _stage_child(fn_name, kwargs, q, build_root):
    """Runs in a spawned process: logging to the run log, the stage function, then its result and RSS."""
    try:
        _setup_logging(Path(build_root))
        warnings.simplefilter("default")
        res = globals()[fn_name](**kwargs)
        u = _rss_units()
        q.put(("ok", res, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * u,
               resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * u))
    except BaseException as e:  # reported to the parent, which fails the run
        q.put(("error", f"{type(e).__name__}: {e}\n{traceback.format_exc()[-6000:]}", 0, 0))


def run_stage(name, fn_name, kwargs, cfg, build_root) -> dict:
    """Run one stage function in a spawned process. Returns {'result', 'seconds', 'peak_rss_*'}. The whole
    process tree is polled every rss_poll_seconds; a process above max_rss_gb is terminated (StageError)."""
    import psutil
    lim = float(cfg["resources"]["max_rss_gb"]) * 1e9
    poll = float(cfg["resources"]["rss_poll_seconds"])
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=_stage_child, args=(fn_name, kwargs, q, str(build_root)), name=f"run_all:{name}")
    t0 = time.time()
    p.start()
    max_proc, max_tree, killed, msg = 0, 0, None, None
    ps = psutil.Process(p.pid)
    while True:
        try:
            msg = q.get(timeout=poll)
            break
        except Exception:
            pass
        try:
            procs = [ps] + ps.children(recursive=True)
            rss = []
            for x in procs:
                try:
                    rss.append((x.pid, x.memory_info().rss))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            if rss:
                max_proc = max(max_proc, max(r for _, r in rss))
                max_tree = max(max_tree, sum(r for _, r in rss))
                big = [pid for pid, r in rss if r > lim]
                if big:
                    killed = f"process {big[0]} above {cfg['resources']['max_rss_gb']} GB"
                    for x in reversed(procs):
                        with contextlib.suppress(Exception):
                            x.kill()
                    break
        except psutil.NoSuchProcess:
            pass
        if not p.is_alive() and q.empty():
            break
    p.join(timeout=60)
    secs = round(time.time() - t0, 1)
    if killed:
        raise StageError(f"stage {name}: {killed} (RSS limit resources.max_rss_gb)")
    if msg is None:
        raise StageError(f"stage {name}: process ended without a result (exit code {p.exitcode})")
    status, res, rss_self, rss_children = msg
    if status != "ok":
        raise StageError(f"stage {name} failed:\n{res}")
    out = {"result": res, "seconds": secs, "peak_rss_stage_process_mb": round(rss_self / 1e6, 1),
           "peak_rss_children_mb": round(rss_children / 1e6, 1),
           "peak_rss_polled_single_process_mb": round(max_proc / 1e6, 1),
           "peak_rss_polled_process_tree_mb": round(max_tree / 1e6, 1)}
    LOG.info("stage %s done in %.1fs, peak RSS %.0f MB (stage process), %.0f MB (largest child), "
             "%.0f MB (polled tree)", name, secs, out["peak_rss_stage_process_mb"], out["peak_rss_children_mb"],
             out["peak_rss_polled_process_tree_mb"])
    return out


def stage_sleep_alloc(mb=0, seconds=0.0, fail=False) -> dict:
    """Test stage: holds `mb` MB for `seconds`, or raises (used by test_run_all for the stage runner)."""
    buf = bytearray(int(mb * 1e6)) if mb else b""
    for i in range(0, len(buf), 4096):
        buf[i] = 1
    time.sleep(seconds)
    if fail:
        raise ValueError("requested failure")
    return {"held_mb": mb}


# ============================================================================ subset selection

def select_sample(cfg, n=200, seed=SEED, must=(28769, 28317, 24614, 33754, 33499, 34989, 32689, 33730)) -> list:
    """A seeded subset across sources for a dry run: about 40% XML (spread over terms), 30% XLSX (18대 v9)
    and 30% HWP, plus the `must` meetings that have a usable raw source (multi-sitting and post-end
    examples from the component reviews)."""
    import numpy as np
    import pandas as pd
    import build_turns as bt
    bcfg = bt.Config(universe=Path(cfg["paths"]["universe"]), crawl_db=Path(cfg["paths"]["crawl_db"]),
                     crosswalk=Path(cfg["paths"]["v9_crosswalk"]))
    uni = bt.load_universe(bcfg)
    cw = bt.load_crosswalk(bcfg)
    crawl = bt.load_crawl(bcfg)
    x18 = cw[(cw.v9_source == "xlsx") & (cw.term == 18) & cw.api_CONFER_NUM.notna()]
    xlsx = set(int(x) for x in x18.api_CONFER_NUM)
    term = dict(zip(uni.CONFER_NUM.astype(int), uni.DAE_NUM.astype(int)))
    view_ok = {c for (c, k), (s, _) in crawl.items() if k == "view" and s == "ok" and c in term}
    hwp_ok = {c for (c, k), (s, _) in crawl.items() if k == "hwp" and s == "ok" and c in term}
    xml_c = sorted(view_ok - xlsx)
    hwp_c = sorted(hwp_ok - xlsx - view_ok)
    xlsx_c = sorted(xlsx & set(term))
    rng = np.random.RandomState(seed)
    k_xml, k_xlsx = int(round(n * 0.4)), int(round(n * 0.3))
    k_hwp = n - k_xml - k_xlsx
    by_term = defaultdict(list)
    for c in xml_c:
        by_term[term[c]].append(c)
    pick = set()
    terms = sorted(by_term)
    for i, t in enumerate(terms):
        share = k_xml // len(terms) + (1 if i < k_xml % len(terms) else 0)
        cand = by_term[t]
        pick |= set(rng.choice(cand, size=min(share, len(cand)), replace=False).tolist())
    pick |= set(rng.choice(xlsx_c, size=min(k_xlsx, len(xlsx_c)), replace=False).tolist())
    pick |= set(rng.choice(hwp_c, size=min(k_hwp, len(hwp_c)), replace=False).tolist())
    usable = view_ok | hwp_ok | xlsx
    pick |= {m for m in must if m in usable and m in term}
    return sorted(int(x) for x in pick)


def write_subset_universe(cfg, conf_nums, path: Path) -> dict:
    """Subset run inputs in the work root: the universe rows of the subset, and the v9 meetings whose label
    (api_CONFER_NUM) is in the subset (so the crosswalk and its validation cover the same meetings; a v9
    meeting labelled outside the subset that carries a subset meeting's transcript is then not seen)."""
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    import pyarrow as pa
    t = pq.read_table(cfg["paths"]["universe"])
    keep = t.filter(pc.is_in(t["CONFER_NUM"], value_set=pa.array(sorted(conf_nums), t.schema.field("CONFER_NUM").type)))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(keep, path)
    v9 = pq.read_table(cfg["paths"]["v9_crosswalk"])
    col = pc.cast(v9["api_CONFER_NUM"], pa.int64())
    v9k = v9.filter(pc.fill_null(pc.is_in(col, value_set=pa.array(sorted(conf_nums), pa.int64())), False))
    v9p = path.parent / "subset_v9_crosswalk.parquet"
    pq.write_table(v9k, v9p)
    return {"subset_universe": str(path), "rows": keep.num_rows, "requested": len(conf_nums),
            "not_in_universe": sorted(set(conf_nums) - set(keep.column("CONFER_NUM").to_pylist())),
            "subset_v9_crosswalk": str(v9p), "v9_meetings_in_subset": v9k.num_rows}


# ============================================================================ stage: build_turns

def stage_build_turns(cfg, work_root, universe, sources, only=None, rebuild=False) -> dict:
    import build_turns as bt
    bcfg = bt.Config(out=Path(work_root), universe=Path(universe), crawl_db=Path(cfg["paths"]["crawl_db"]),
                     crosswalk=Path(cfg["paths"]["v9_crosswalk"]), duckdb_memory=cfg["resources"]["duckdb_memory"],
                     duckdb_threads=int(cfg["resources"]["duckdb_threads"]))
    bt._setup_logging(bcfg)
    summ = bt.run(bcfg, sources=tuple(sources), workers=int(cfg["resources"]["build_turns_workers"]),
                  batch_size=int(cfg["resources"]["build_turns_batch_size"]), only=only, rebuild=rebuild)
    con = sqlite3.connect(bcfg.state_db)
    pend = con.execute("SELECT count(*) FROM pending_drops").fetchone()[0]
    con.close()
    if pend:
        raise StageError(f"build_turns left {pend} pending drops; re-run so they are applied at start")
    keep = ("run_id", "adapter_versions", "plan_actions", "pending", "tasks_by_source", "results", "rows_written",
            "rebuild_failed_kept_previous", "timing", "total_seconds", "meetings_rows", "errors",
            "crosswalk_rows_outside_universe_unbuilt")
    out = {k: summ.get(k) for k in keep}
    c = summ.get("counters") or {}
    out["counters_selected"] = {k: v for k, v in c.items() if any(s in k for s in (
        "sitting", "after_end", "label_how", "label_confidence", "turns_after", "meetings_several",
        "turns_in_later", "hwp_label", "unrated"))}
    out["errors"] = (out.get("errors") or [])[:50]
    return out


# ============================================================================ manifest snapshot (parent)

def snapshot(cfg, lay: Layout) -> dict:
    """The build_turns manifest joined with the meetings table: per term, the meetings of the release with
    their batch and turn count. Fails on pending drops or a built meeting without a meetings row."""
    import pandas as pd
    db = lay.work / "build_turns" / "state.sqlite"
    if not db.exists():
        raise StageError(f"no build_turns manifest at {db}")
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=60)
    built = pd.read_sql_query("SELECT conf_num, source, term AS batch_term, status, batch_key, raw_sha1, n_turns, "
                              "builder_version FROM built", c)
    pend = c.execute("SELECT count(*) FROM pending_drops").fetchone()[0]
    c.close()
    if pend:
        raise StageError(f"build_turns manifest has {pend} pending drops")
    mp_ = lay.work / "meetings" / "meetings.parquet"
    if not mp_.exists():
        raise StageError(f"no meetings table at {mp_}")
    mall = pd.read_parquet(mp_)
    m = mall[["conf_num", "term", "is_built", "source", "n_turns"]]
    j = built.merge(m, on="conf_num", how="left", suffixes=("", "_m"), indicator=True)
    no_row = j[j._merge == "left_only"]
    if len(no_row):
        raise StageError(f"{len(no_row)} built meetings have no meetings row (e.g. {no_row.conf_num.head(5).tolist()})")
    j["term_key"] = j["term"].map(lambda x: "NA" if pd.isna(x) else f"{int(x):02d}")
    mism = j[(j.n_turns.fillna(-1).astype(int) != j.n_turns_m.fillna(-1).astype(int))]
    terms = {}
    for tk, g in j.sort_values("conf_num").groupby("term_key"):
        rows = g[["conf_num", "source", "batch_key", "n_turns", "builder_version", "raw_sha1"]].to_dict("records")
        # hash of this term's meetings rows only, so a meeting added to another term does not re-run this one
        mt = mall[mall.conf_num.isin(g.conf_num)].sort_values("conf_num")
        mh = hashlib.sha256(mt.to_json(orient="records", force_ascii=False, default_handler=str).encode()).hexdigest()[:16]
        terms[tk] = {"meetings": len(g), "turns": int(g.n_turns.sum()), "rows": rows, "meetings_rows_sha": mh}
    return {"n_built": int(len(built)), "by_source": built.source.value_counts().to_dict(),
            "n_turns": int(built.n_turns.sum()), "meetings_n_turns_mismatch": int(len(mism)),
            "terms": terms, "meetings_sha256": _sha256_file(mp_)}


def _manifest_table(con, rows, name="_man"):
    import pandas as pd
    df = pd.DataFrame(rows)[["conf_num", "batch_key"]]
    con.register("_man_df", df)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT CAST(conf_num AS BIGINT) conf_num, batch_key FROM _man_df")
    con.unregister("_man_df")


def _batch_files(work: Path, table: str, rows) -> list:
    keys = sorted({r["batch_key"] for r in rows})
    files = [work / table / f"{k}.parquet" for k in keys]
    return [str(f) for f in files if f.exists()], [str(f) for f in files if not f.exists()]


def _src_sql(files) -> str:
    return ("read_parquet([" + ", ".join(_s(f) for f in files) + "], filename=true, union_by_name=true, "
            "file_row_number=true)")


BATCH_KEY_SQL = "regexp_extract(t.filename, '([^/]+/[^/]+/[^/]+)\\.parquet$', 1)"


# ============================================================================ stage: duplicates
# Researcher decision 7 (2026-09-26): identical duplicate meetings keep one copy (the one whose printed date /
# committee match the source's API row), the other is marked duplicate_of and excluded from turns and dyads;
# partial overlaps are kept and flagged.

_HANGUL = re.compile(r"[가-힣]")


def _nospace(x):
    return re.sub(r"\s+", "", x) if isinstance(x, str) and x.strip() else None


def printed_vs_api(m: dict, u: Optional[dict]) -> dict:
    """Printed date / sitting / committee of a meeting (meetings.date_printed, sitting, committee_printed +
    subcommittee_printed) against its Open API row (CONF_DATE, sitting, COMM_NAME): True / False / None
    (not comparable: a value missing, no API row, or a printed committee name without Hangul, e.g. Hanja)."""
    out = {"date": None, "sitting": None, "committee": None}
    if not u:
        return out
    pd_, ad = m.get("date_printed"), u.get("CONF_DATE")
    if isinstance(pd_, str) and isinstance(ad, str) and pd_ and ad:
        out["date"] = pd_ == ad
    ps, as_ = _nospace(m.get("sitting")), _nospace(u.get("sitting"))
    if ps and as_:
        out["sitting"] = ps == as_
    pc, psub, ac = _nospace(m.get("committee_printed")), _nospace(m.get("subcommittee_printed")), _nospace(u.get("COMM_NAME"))
    if pc and ac and _HANGUL.search(pc):
        full = pc + (psub or "") if psub and _HANGUL.search(psub) else pc
        out["committee"] = bool(ac == full or (ac.startswith(pc) and (not psub or not _HANGUL.search(psub) or ac.endswith(psub))))
    return out


def _rank_key(conf_num: int, match: dict):
    """Keep order: fewest printed-vs-API mismatches, then most matches, then the lower conf_num."""
    vals = list(match.values())
    return (sum(1 for x in vals if x is False), -sum(1 for x in vals if x is True), int(conf_num))


def resolve_duplicate_pairs(pairs: list, meetings: dict, universe: dict) -> dict:
    """pairs: [{'a', 'b', 'kind', ...}] with kind identical / near_identical / partial. meetings / universe:
    {conf_num: row dict}. Identical pairs are grouped (connected components); in each group one copy is kept
    (_rank_key) and the others get duplicate_of = the kept copy. Other pairs are kept and flagged both ways.
    Returns {'duplicate_of': {c: kept}, 'duplicate_basis': {c: text}, 'overlap': {c: [[other, kind], ...]},
    'pairs': pairs with kept / duplicate / match columns}."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for pr in pairs:
        if pr["kind"] == "identical":
            ra, rb = find(int(pr["a"])), find(int(pr["b"]))
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)
    groups = defaultdict(list)
    for x in list(parent):
        groups[find(x)].append(x)
    match = {c: printed_vs_api(meetings.get(c, {}), universe.get(c)) for g in groups.values() for c in g}
    for pr in pairs:
        for c in (int(pr["a"]), int(pr["b"])):
            match.setdefault(c, printed_vs_api(meetings.get(c, {}), universe.get(c)))
    dup_of, basis = {}, {}
    for g in groups.values():
        ranked = sorted(g, key=lambda c: _rank_key(c, match[c]))
        kept = ranked[0]
        tie = len(ranked) > 1 and _rank_key(kept, match[kept])[:2] == _rank_key(ranked[1], match[ranked[1]])[:2]
        for c in ranked[1:]:
            dup_of[c] = kept

            def fmt(x):
                return ",".join(f"{k}={'match' if v else 'mismatch' if v is False else 'n/a'}" for k, v in match[x].items())
            basis[c] = (f"identical_text; kept {kept} ({fmt(kept)}); this copy ({fmt(c)})"
                        + ("; tie on printed-vs-API evidence, lower conf_num kept" if tie else ""))
    overlap = defaultdict(list)
    out_pairs = []
    for pr in pairs:
        a, b = int(pr["a"]), int(pr["b"])
        row = dict(pr)
        for side, c in (("a", a), ("b", b)):
            for k, v in match[c].items():
                row[f"{side}_{k}_match"] = v
        if pr["kind"] == "identical":
            kept_a, kept_b = dup_of.get(a, a), dup_of.get(b, b)
            row["kept"] = kept_a if kept_a == kept_b else None
            row["duplicate"] = a if a in dup_of else (b if b in dup_of else None)
            row["decision"] = "duplicate_of"
        else:
            row["kept"], row["duplicate"], row["decision"] = None, None, "kept_flagged"
            if a not in dup_of and b not in dup_of:
                overlap[a].append([b, pr["kind"]])
                overlap[b].append([a, pr["kind"]])
            else:
                row["decision"] = "kept_flagged_other_is_duplicate"
        out_pairs.append(row)
    return {"duplicate_of": dup_of, "duplicate_basis": basis,
            "overlap": {c: sorted(v) for c, v in overlap.items()}, "pairs": out_pairs}


def stage_duplicates(cfg, work_root, build_root, run_id, rows, universe, fingerprint) -> dict:
    """Find duplicate-content meeting pairs among the built turns (validate.py dup checks, no allowlist),
    classify and resolve them (resolve_duplicate_pairs). Writes release/duplicate_meetings.parquet and
    _state/duplicates.json."""
    import pandas as pd
    import validate as VA
    lay = Layout(work_root, build_root, run_id)
    lay.staging.mkdir(parents=True, exist_ok=True)
    files, missing = _batch_files(lay.work, "turns", rows)
    if missing:
        raise StageError(f"duplicates: {len(missing)} manifest batch files missing (e.g. {missing[:3]})")
    vp = cfg["switchable"]["validate"]
    params = VA.Params(mode="dev", **{k: vp[k] for k in ("dup_containment", "dup_long_min_chars", "dup_long_min_shared",
                                                         "dup_max_df", "dup_shingle_len", "dup_shingle_min_shared",
                                                         "dup_shingle_min_n", "dup_meeting_min_chars")})
    con = _duck(cfg)
    _manifest_table(con, rows)
    con.execute(f"""CREATE OR REPLACE TEMP VIEW _built_turns AS SELECT t.conf_num, t.turn_seq, t.text
                    FROM {_src_sql(files)} t SEMI JOIN _man m ON m.conf_num = t.conf_num AND m.batch_key = {BATCH_KEY_SQL}""")
    V = VA.Validator({}, params, con=con, memory_limit=cfg["resources"]["duckdb_memory"],
                     threads=int(cfg["resources"]["duckdb_threads"]))
    con.execute("CREATE OR REPLACE VIEW turns_release AS SELECT * FROM _built_turns")
    V.inputs["turns_release"] = {"kind": "view"}
    V._cols.pop("turns_release", None)
    t0 = time.time()
    res = {}
    for cid, fn in (("dup_meeting_text", VA.c_dup_meeting), ("dup_long_turn", VA.c_dup_long), ("dup_shingle", VA.c_dup_shingle)):
        r = fn(V)
        res[cid] = {"pairs": int(r.details.get("pairs_flagged", 0))}
    ident = {(int(a), int(b)): n for a, b, n in con.execute("SELECT a, b, n_chars FROM _dup_mt").fetchall()}
    longp = {(int(a), int(b)): float(c) for a, b, c in con.execute("SELECT a, b, containment FROM _dup_long").fetchall()}
    shp = {(int(a), int(b)): float(c) for a, b, c in con.execute("SELECT a, b, containment FROM _dup_sh").fetchall()}
    keys = sorted(set(ident) | set(longp) | set(shp))
    nt = {}
    if keys:
        ids = sorted({x for k in keys for x in k})
        con.execute("CREATE OR REPLACE TEMP TABLE _dids AS SELECT unnest($1::BIGINT[]) AS conf_num", [ids])
        nt = dict(con.execute("SELECT conf_num, count(*) FROM _built_turns WHERE conf_num IN (SELECT conf_num FROM _dids) GROUP BY 1").fetchall())
    share_min = float(cfg["switchable"]["duplicates"]["near_identical_min_share"])
    pairs = []
    for a, b in keys:
        pr = {"a": a, "b": b, "found_by": [n for n, d in (("dup_meeting_text", ident), ("dup_long_turn", longp),
                                                            ("dup_shingle", shp)) if (a, b) in d],
              "long_turn_containment": longp.get((a, b)), "shingle_containment": shp.get((a, b)),
              "n_turns_a": int(nt.get(a, 0)), "n_turns_b": int(nt.get(b, 0)), "equal_turn_share": None}
        if (a, b) in ident:
            pr["kind"] = "identical"
        else:
            if pr["n_turns_a"] == pr["n_turns_b"] and pr["n_turns_a"] > 0:
                eq = con.execute(f"""SELECT count(*) FILTER (WHERE {VA.norm_sql('x.text')} = {VA.norm_sql('y.text')}) FROM
                    (SELECT turn_seq, text FROM _built_turns WHERE conf_num = {a}) x JOIN
                    (SELECT turn_seq, text FROM _built_turns WHERE conf_num = {b}) y USING (turn_seq)""").fetchone()[0]
                pr["equal_turn_share"] = round(eq / pr["n_turns_a"], 6)
            pr["kind"] = "near_identical" if (pr["equal_turn_share"] or 0) >= share_min else "partial"
        pairs.append(pr)
    mcols = ["conf_num", "date_printed", "sitting", "committee_printed", "subcommittee_printed"]
    mt = pd.read_parquet(lay.work / "meetings" / "meetings.parquet")
    mt = mt[[c for c in mcols if c in mt.columns]]
    ids = sorted({x for k in keys for x in k})
    mrows = {int(r["conf_num"]): r for r in mt[mt.conf_num.isin(ids)].to_dict("records")}
    uc = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_s(universe)})").fetchall()]
    usel = ", ".join(c for c in ("CONFER_NUM", "CONF_DATE", "sitting", "COMM_NAME") if c in uc)
    urows = {}
    if ids:
        u = con.execute(f"SELECT {usel} FROM read_parquet({_s(universe)}) WHERE CONFER_NUM IN (SELECT conf_num FROM _dids)").fetchdf()
        if "source" in uc:        # id-gap meetings have no Open API row to compare with
            gap = {int(x) for x in con.execute(f"SELECT CONFER_NUM FROM read_parquet({_s(universe)}) WHERE source = 'gap_scan'").fetchdf().CONFER_NUM}
            u = u[~u.CONFER_NUM.isin(gap)]
        urows = {int(r["CONFER_NUM"]): r for r in u.to_dict("records")}
    con.close()
    dec = resolve_duplicate_pairs(pairs, mrows, urows)
    dec["fingerprint"] = fingerprint
    dec["checks"] = res
    dec["near_identical_min_share"] = share_min
    tbl = pd.DataFrame(dec["pairs"]) if dec["pairs"] else pd.DataFrame(
        columns=["a", "b", "kind", "found_by", "long_turn_containment", "shingle_containment", "n_turns_a", "n_turns_b",
                 "equal_turn_share", "kept", "duplicate", "decision"])
    for c in ("a", "b", "kept", "duplicate", "n_turns_a", "n_turns_b"):
        if c in tbl.columns:
            tbl[c] = pd.array(tbl[c], dtype="Int64")
    if "found_by" in tbl.columns:
        tbl["found_by"] = tbl["found_by"].map(lambda x: list(x) if isinstance(x, (list, tuple)) else [])
    for c in [c for c in tbl.columns if c.endswith("_match")]:
        tbl[c] = pd.array(tbl[c], dtype="boolean")
    tbl["duplicate_basis"] = [dec["duplicate_basis"].get(int(d)) if pd.notna(d) else None for d in tbl.get("duplicate", [])] \
        if len(tbl) else pd.Series([], dtype=object)
    tmp = lay.staging / "duplicate_meetings.parquet"
    tbl.to_parquet(tmp, index=False)
    pub = lay.publish(tmp, "duplicate_meetings.parquet")
    js = lay.staging / "duplicates.json"
    js.write_text(json.dumps({k: ({str(a): b for a, b in v.items()} if isinstance(v, dict) and k in ("duplicate_of", "duplicate_basis", "overlap") else v)
                              for k, v in dec.items()}, ensure_ascii=False, indent=1, default=str))
    lay.publish_state(js, "duplicates.json")
    kinds = Counter(p_["kind"] for p_ in pairs)
    return {"pairs": len(pairs), "by_kind": dict(kinds), "duplicates": len(dec["duplicate_of"]),
            "meetings_with_overlap": len(dec["overlap"]), "checks": res, "seconds": round(time.time() - t0, 1), **pub}


def load_duplicates(lay: Layout) -> dict:
    """_state/duplicates.json as {'duplicate_of': {int: int}, 'overlap': {int: [[int, kind]]}, ...} (empty when absent)."""
    p = lay.state_dir / "duplicates.json"
    if not p.exists():
        return {"duplicate_of": {}, "duplicate_basis": {}, "overlap": {}, "pairs": [], "fingerprint": None}
    d = json.loads(p.read_text())
    d["duplicate_of"] = {int(a): int(b) for a, b in d.get("duplicate_of", {}).items()}
    d["duplicate_basis"] = {int(a): b for a, b in d.get("duplicate_basis", {}).items()}
    d["overlap"] = {int(a): [[int(x), k] for x, k in v] for a, v in d.get("overlap", {}).items()}
    return d


# ============================================================================ stage: enrich
# Partition directories are named t16 ... t22 (tNA for a meeting without a term), not hive-style 'term=16':
# the files carry a `term` column, and a hive key of the same name makes pyarrow / pandas readers fail
# ('Field term has incompatible types').

def _fetch_arrow(rel):
    t = rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()
    return t.read_all() if hasattr(t, "read_all") else t


def _nullable(tbl):
    import pandas as pd
    import pyarrow as pa
    m = {pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(), pa.int64(): pd.Int64Dtype(),
         pa.string(): pd.StringDtype(), pa.large_string(): pd.StringDtype(), pa.bool_(): pd.BooleanDtype()}
    return tbl.to_pandas(types_mapper=m.get)


def _frames_equal(a, b) -> bool:
    import pandas as pd
    if len(a) != len(b):
        return False
    an, bn = a.isna().to_numpy(), b.isna().to_numpy()
    if (an != bn).any():
        return False
    try:
        return bool((a[~an].astype(object).to_numpy() == b[~bn].astype(object).to_numpy()).all())
    except Exception:
        return all(x == y for x, y in zip(a[~an].tolist(), b[~bn].tolist()))


def post_legislators(df, added_cols, mode, counts):
    """switchable.legislators.name_fuzzy_committee = drop: null every legislators column of the turns linked
    by the one-jamo typo rule; id_method records it."""
    if mode == "keep" or "id_method" not in df.columns:
        counts["legislators_name_fuzzy_committee_rows"] += int((df.get("id_method") == "name_fuzzy_committee").fillna(False).sum()) \
            if "id_method" in df.columns else 0
        return df
    m = (df["id_method"] == "name_fuzzy_committee").fillna(False).to_numpy(dtype=bool)
    counts["legislators_name_fuzzy_committee_rows"] += int(m.sum())
    counts["legislators_name_fuzzy_committee_dropped"] += int(m.sum())
    if m.any():
        for c in added_cols:
            if c != "id_method":
                if df[c].dtype == bool:
                    df[c] = df[c].astype("boolean")
                df.loc[m, c] = None
        df.loc[m, "id_method"] = "dropped:name_fuzzy_committee"
    return df


def post_government(df, mode, counts):
    """switchable.government.nominee_in_tenure = drop: panel links of tier 'nominee_in_tenure' removed."""
    lm = df["link_method"].astype("string") if "link_method" in df.columns else None
    if lm is None:
        return df
    m = lm.str.startswith("nominee_in_tenure").fillna(False).to_numpy(dtype=bool)
    counts["government_nominee_in_tenure_rows"] += int(m.sum())
    if mode == "drop" and m.any():
        for c in ("minister_panel_id", "dual_office", "gov_link_name"):
            if c in df.columns:
                if df[c].dtype == bool:
                    df[c] = df[c].astype("boolean")
                df.loc[m, c] = None
        df.loc[m, "link_method"] = "unmatched:nominee_in_tenure_dropped"
        counts["government_nominee_in_tenure_dropped"] += int(m.sum())
    return df


def enrich_frame(narrow, meetings, cfg, refs, counts, diags):
    """The enrichment chain on one chunk (a pandas frame without text columns). Returns the enriched frame
    (same rows, same order) and the list of columns the chain added."""
    import roles as RO
    import legislators as LG
    import party_timeline as PT
    import government as GV
    sw = cfg["switchable"]
    before = list(narrow.columns)
    with warnings.catch_warnings(record=True) as wl:
        warnings.simplefilter("always")
        r = RO.enrich(narrow, meetings)
        diags["roles"].append(r.attrs.get("roles_enrich"))
        c1 = list(r.columns)
        r = LG.enrich(r, meetings, ref=refs["legislators"])
        leg_added = [c for c in r.columns if c not in c1]
        r = post_legislators(r, leg_added, sw["legislators"]["name_fuzzy_committee"], counts)
        r = PT.enrich(r, meetings, resolver=refs["resolver"])
        r = GV.enrich(r, meetings, panel_index=refs["panel_index"], suspended_admin=sw["government"]["suspended_admin"])
        diags["government"].append(r.attrs.get("government"))
        r = post_government(r, sw["government"]["nominee_in_tenure"], counts)
    for w in wl:
        diags["warnings"][f"{w.category.__name__}: {str(w.message)[:200]}"] += 1
    if len(r) != len(narrow):
        raise StageError(f"enrichment changed the row count {len(narrow)} -> {len(r)}")
    for k in ("conf_num", "turn_seq"):
        if not (r[k].to_numpy() == narrow[k].to_numpy()).all():
            raise StageError(f"enrichment reordered rows (column {k})")
    modified = [c for c in before if not _frames_equal(r[c].reset_index(drop=True), narrow[c].reset_index(drop=True))]
    if modified:
        raise StageError(f"an enricher modified input columns {modified}")
    added = [c for c in r.columns if c not in before]
    return r, added


def _chunks(rows, max_turns):
    out, cur, n = [], [], 0
    for r in rows:
        if cur and n + r["n_turns"] > max_turns:
            out.append(cur)
            cur, n = [], 0
        cur.append(r)
        n += r["n_turns"]
    if cur:
        out.append(cur)
    return out


def _arrow_for_parquet(df):
    """pandas -> arrow for the enrichment columns; an all-null column becomes string (stable schema)."""
    import pyarrow as pa
    t = pa.Table.from_pandas(df, preserve_index=False)
    cols = []
    for f, col in zip(t.schema, t.columns):
        cols.append(col.cast(pa.string()) if pa.types.is_null(f.type) else col)
    return pa.Table.from_arrays(cols, names=t.schema.names)


def stage_enrich(cfg, work_root, build_root, run_id, todo, duplicate_conf_nums=()) -> dict:
    """todo: {term_key: {'rows': manifest rows, 'fingerprint': fp}}. Writes release/turns/tNN/ per term; the turns
    of meetings in duplicate_conf_nums (duplicates stage) go to release/duplicate_turns/tNN/ instead."""
    import pandas as pd
    import pyarrow.parquet as pq
    import legislators as LG
    import party_timeline as PT
    import government as GV
    lay = Layout(work_root, build_root, run_id)
    g = cfg["switchable"]["government"]
    refs = {"legislators": LG.load_reference(), "resolver": PT.Resolver(),
            "panel_index": GV._default_index(buffer_days=g["buffer_days"], nominee_pre_days=g["nominee_pre_days"],
                                             nominee_post_days=g["nominee_post_days"])}
    meetings = pd.read_parquet(lay.work / "meetings" / "meetings.parquet")
    dups = {int(x) for x in duplicate_conf_nums}
    out = {}
    for tk, spec in sorted(todo.items()):
        t0 = time.time()
        rows = spec["rows"]
        files, missing = _batch_files(lay.work, "turns", rows)
        if missing:
            raise StageError(f"term {tk}: {len(missing)} manifest batch files missing (e.g. {missing[:3]})")
        con = _duck(cfg)
        _manifest_table(con, rows)
        src = f"(SELECT t.* FROM {_src_sql(files)} t SEMI JOIN _man m ON m.conf_num = t.conf_num AND m.batch_key = {BATCH_KEY_SQL})"
        con.execute(f"CREATE OR REPLACE TEMP VIEW _t AS {src}")
        desc = [(r[0], r[1]) for r in con.execute("DESCRIBE SELECT * FROM _t").fetchall()]
        base_cols = [c for c, _ in desc if c not in ("filename", "file_row_number")]
        # text and list columns are never read by the enrichers: they stay in duckdb
        narrow_cols = [c for c, ty in desc if c in base_cols and c not in TEXT_HEAVY_COLS and not ty.endswith("[]")]
        counts, diags = Counter(), {"roles": [], "government": [], "warnings": Counter()}
        res_t = {}
        added_all = None
        groups = (("turns", [r for r in rows if int(r["conf_num"]) not in dups]),
                  ("duplicate_turns", [r for r in rows if int(r["conf_num"]) in dups]))
        for group, grows in groups:
            if group == "duplicate_turns" and not grows:
                ret = lay.retire(f"duplicate_turns/t{tk}")          # a copy no longer marked: its old partition
                if ret:
                    res_t["duplicate_turns_retired_to"] = ret
                continue
            stage_dir = lay.staging / group / f"t{tk}"
            if stage_dir.exists():
                shutil.move(str(stage_dir), str(lay.superseded / "_staging_leftover" / group / f"t{tk}"))
            stage_dir.mkdir(parents=True)
            n_in = n_out = 0
            chunks = _chunks(grows, int(cfg["resources"]["enrich_chunk_turns"]))
            for k, ch in enumerate(chunks):
                ids = [int(r["conf_num"]) for r in ch]
                exp = sum(int(r["n_turns"]) for r in ch)
                if exp == 0:          # only meetings built without turns (ok_no_turns): nothing to enrich or write
                    counts["chunks_without_turns"] += 1
                    counts["meetings_without_turns"] += len(ch)
                    continue
                con.execute("CREATE OR REPLACE TEMP TABLE _ids AS SELECT unnest($1)::BIGINT AS conf_num", [ids])
                q = (f"SELECT {', '.join(chr(34) + c + chr(34) for c in narrow_cols)} FROM _t WHERE conf_num IN "
                     f"(SELECT conf_num FROM _ids) ORDER BY conf_num, turn_seq")
                narrow = _nullable(_fetch_arrow(con.execute(q)))
                if len(narrow) != exp:
                    raise StageError(f"term {tk} chunk {k}: {len(narrow)} turns read, manifest says {exp}")
                n_in += len(narrow)
                mt = meetings[meetings.conf_num.isin(ids)]
                enr, added = enrich_frame(narrow, mt, cfg, refs, counts, diags)
                if added_all is None:
                    added_all = added
                elif added != added_all:
                    raise StageError(f"term {tk} chunk {k}: enrichment columns differ between chunks")
                con.register("_enr", _arrow_for_parquet(enr[["conf_num", "turn_seq"] + added].reset_index(drop=True)))
                del enr, narrow
                part = stage_dir / f"part-{k:05d}.parquet"
                sel = ", ".join(f't."{c}"' for c in base_cols) + ", " + ", ".join(f'e."{c}"' for c in added)
                con.execute(f"""COPY (SELECT {sel} FROM _t t JOIN _enr e
                                      ON e.conf_num = t.conf_num AND e.turn_seq = t.turn_seq
                                      WHERE t.conf_num IN (SELECT conf_num FROM _ids)
                                      ORDER BY t.conf_num, t.turn_seq)
                                TO {_s(part)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)""")
                con.unregister("_enr")
                got = pq.ParquetFile(part).metadata.num_rows
                if got != exp:
                    raise StageError(f"term {tk} chunk {k}: wrote {got} rows, expected {exp}")
                n_out += got
                LOG.info("enrich term %s %s chunk %d/%d: %d turns (%d meetings)", tk, group, k + 1, len(chunks), got, len(ch))
            succ = lay.staging / f"_{group}_t{tk}.json"
            succ.write_text(json.dumps({"fingerprint": spec["fingerprint"], "turns": n_out, "meetings": len(grows),
                                        "run_id": run_id}, indent=1))
            lay.publish_state(succ, f"partitions/{group}/t{tk}.json")
            pub = lay.publish(stage_dir, f"{group}/t{tk}")
            res_t[group] = {"turns_in": n_in, "turns_out": n_out, "meetings": len(grows), "chunks": len(chunks), **pub}
        con.close()
        rd = [d for d in diags["roles"] if d]
        gd = [d for d in diags["government"] if d]
        tt = res_t.get("turns", {})
        out[tk] = {"turns_in": tt.get("turns_in", 0), "turns_out": tt.get("turns_out", 0), "meetings": len(rows),
                   "chunks": tt.get("chunks", 0), "duplicate_copies": res_t.get("duplicate_turns"),
                   "columns_added": added_all, "post_steps": dict(counts),
                   "warnings": dict(diags["warnings"]),
                   "roles_turns_without_meeting": sum(int(d.get("turns_without_meeting", 0)) for d in rd),
                   "roles_load_status": rd[0].get("load_status") if rd and isinstance(rd[0], dict) else None,
                   "government_admin_null": _sum_dicts([d.get("admin_null") for d in gd]),
                   "government_link_method": _sum_dicts([d.get("link_method") for d in gd]),
                   "seconds": round(time.time() - t0, 1),
                   **{k: v for k, v in tt.items() if k in ("published", "superseded_to")},
                   **({"duplicate_turns_retired_to": res_t["duplicate_turns_retired_to"]} if "duplicate_turns_retired_to" in res_t else {})}
        LOG.info("enrich term %s: %d turns (+%d duplicate-copy turns), %d meetings, %.1fs", tk, out[tk]["turns_out"],
                 (res_t.get("duplicate_turns") or {}).get("turns_out", 0), len(rows), time.time() - t0)
    return out


def _sum_dicts(ds):
    c = Counter()
    for d in ds:
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, (int, float)):
                    c[str(k)] += v
    return dict(c)


# ============================================================================ stage: tables

def meetings_with_duplicates(src, dest, decisions: dict) -> dict:
    """Copy the meetings table adding the duplicate decisions (researcher decision 7): duplicate_of (BIGINT, the
    kept copy), duplicate_basis (VARCHAR), overlap_with (BIGINT[]) and overlap_kinds (VARCHAR[], parallel to
    overlap_with: near_identical / partial). No row or existing column is changed."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    t = pq.read_table(src)
    for c in ("duplicate_of", "duplicate_basis", "overlap_with", "overlap_kinds"):
        if c in t.column_names:
            raise StageError(f"meetings already has column {c}")
    cn = [int(x) for x in t.column("conf_num").to_pylist()]
    dup = {int(a): int(b) for a, b in (decisions.get("duplicate_of") or {}).items()}
    basis = {int(a): v for a, v in (decisions.get("duplicate_basis") or {}).items()}
    ov = {int(a): v for a, v in (decisions.get("overlap") or {}).items()}
    unknown = sorted((set(dup) | set(dup.values()) | set(ov)) - set(cn))
    if unknown:
        raise StageError(f"duplicate decisions name meetings absent from the meetings table: {unknown[:10]}")
    t = t.append_column("duplicate_of", pa.array([dup.get(c) for c in cn], pa.int64()))
    t = t.append_column("duplicate_basis", pa.array([basis.get(c) for c in cn], pa.string()))
    t = t.append_column("overlap_with", pa.array([[int(x) for x, _ in ov[c]] if c in ov else None for c in cn], pa.list_(pa.int64())))
    t = t.append_column("overlap_kinds", pa.array([[k for _, k in ov[c]] if c in ov else None for c in cn], pa.list_(pa.string())))
    pq.write_table(t, dest, compression="zstd")
    return {"rows": t.num_rows, "duplicate_of": len(dup), "with_overlap": len(ov)}


def stage_tables(cfg, work_root, build_root, run_id, rows, decisions=None) -> dict:
    """meetings (+ duplicate decisions) + the build_turns side tables of the manifest batches -> release/<table>.parquet."""
    import pyarrow.parquet as pq
    lay = Layout(work_root, build_root, run_id)
    lay.staging.mkdir(parents=True, exist_ok=True)
    out = {}
    src_m = lay.work / "meetings" / "meetings.parquet"
    tmp = lay.staging / "meetings.parquet"
    info = meetings_with_duplicates(src_m, tmp, decisions or {})
    out["meetings"] = {**info, **lay.publish(tmp, "meetings.parquet")}
    con = _duck(cfg)
    _manifest_table(con, rows)
    for tb in COPY_TABLES:
        files, missing = _batch_files(lay.work, tb, rows)
        dest = lay.staging / f"{tb}.parquet"
        if files:
            cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {_src_sql(files)}").fetchall()
                    if r[0] not in ("filename", "file_row_number")]
            sel = ", ".join(f't."{c}"' for c in cols)
            con.execute(f"""COPY (SELECT {sel} FROM {_src_sql(files)} t SEMI JOIN _man m
                                  ON m.conf_num = t.conf_num AND m.batch_key = {BATCH_KEY_SQL}
                                  ORDER BY t.conf_num, t.file_row_number)
                            TO {_s(dest)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)""")
            n = pq.ParquetFile(dest).metadata.num_rows
            n_all = con.execute(f"SELECT count(*) FROM {_src_sql(files)}").fetchone()[0]
        else:
            import build_turns as bt
            pq.write_table(bt.SCHEMAS[tb].empty_table(), dest)
            n = n_all = 0
        # batch files hold only manifest meetings (pending drops applied), so nothing may be left out
        if n != n_all:
            raise StageError(f"table {tb}: {n_all - n} rows in manifest batch files belong to no manifest meeting")
        out[tb] = {"rows": n, "batch_files": len(files), "batches_without_file": len(missing),
                   **lay.publish(dest, f"{tb}.parquet")}
    con.close()
    return out


# ============================================================================ stage: dyads

def stage_dyads(cfg, work_root, build_root, run_id, todo) -> dict:
    """Slim dyads per term from release/turns/tNN -> release/dyads/tNN/dyads.parquet, and from the duplicate
    copies (release/duplicate_turns/tNN, when present) -> release/duplicate_dyads/tNN/dyads.parquet. The build
    statistics go to _state/partitions/."""
    import dyads as DY
    lay = Layout(work_root, build_root, run_id)
    out = {}
    for tk, spec in sorted(todo.items()):
        res = {}
        for tin, tout in (("turns", "dyads"), ("duplicate_turns", "duplicate_dyads")):
            parts = sorted(str(p) for p in (lay.release / tin / f"t{tk}").glob("part-*.parquet"))
            if tin == "duplicate_turns" and not parts:
                ret = lay.retire(f"{tout}/t{tk}")
                if ret:
                    res["duplicate_dyads_retired_to"] = ret
                continue
            d = lay.staging / tout / f"t{tk}"
            if d.exists():
                shutil.move(str(d), str(lay.superseded / "_staging_leftover" / tout / f"t{tk}"))
            d.mkdir(parents=True)
            if parts:
                st = DY.build_dyads_file(parts, d / "dyads.parquet", meetings_path=lay.release / "meetings.parquet",
                                         memory_limit=cfg["resources"]["duckdb_memory"],
                                         threads=int(cfg["resources"]["duckdb_threads"]),
                                         chunk_turns=int(cfg["resources"]["dyads_chunk_turns"]),
                                         exclude_after_end_marker=bool(cfg["switchable"]["dyads"]["exclude_after_end_marker"]))
            else:
                st = {"n_dyads": 0, "note": "no turns partition"}
            succ = lay.staging / f"_{tout}_t{tk}.json"
            succ.write_text(json.dumps({"fingerprint": spec["fingerprint"], "run_id": run_id, "stats": st}, indent=1, default=str))
            lay.publish_state(succ, f"partitions/{tout}/t{tk}.json")
            res[tout] = {"stats": {k: v for k, v in st.items() if k != "slim_sources"}, **lay.publish(d, f"{tout}/t{tk}")}
        out[tk] = res
        LOG.info("dyads term %s: %s dyads (+%s from duplicate copies)", tk, res.get("dyads", {}).get("stats", {}).get("n_dyads"),
                 res.get("duplicate_dyads", {}).get("stats", {}).get("n_dyads", 0))
    return out


# ============================================================================ stage: crosswalk

def stage_crosswalk(cfg, work_root, build_root, run_id, universe, v9_crosswalk) -> dict:
    import crosswalk as CW
    lay = Layout(work_root, build_root, run_id)
    d = lay.staging / "crosswalk"
    if d.exists():
        shutil.move(str(d), str(lay.superseded / "_staging_leftover" / "crosswalk"))
    d.mkdir(parents=True)
    turns = sorted(str(p) for p in (lay.release / "turns").glob("t*/part-*.parquet"))
    st = CW.build(turns or None, str(lay.release / "meetings.parquet"), out_dir=d,
                  v10_complete=bool(cfg["switchable"]["crosswalk"]["v10_complete"]),
                  v9fp_path=Path(cfg["paths"]["v9_fingerprints"]), universe=universe, crawl=cfg["paths"]["crawl_db"],
                  v9_crosswalk=v9_crosswalk)
    out = {"stats": {k: v for k, v in st.items() if k not in ("relation_by_v9_source", "inputs")}}
    for f in ("crosswalk_meetings.parquet", "crosswalk_turns.parquet"):
        out[f] = lay.publish(d / f, f)
    # the statistics are bookkeeping (their numbers reach docs_numbers.json through validate.py)
    sp = d / "crosswalk_stats.json"
    sp.write_text(json.dumps(__import__("validate").relativize(json.loads(sp.read_text())), ensure_ascii=False, indent=1))
    out["crosswalk_stats.json"] = lay.publish_state(sp, "crosswalk_stats.json")
    rest = list(d.iterdir())
    if rest:        # anything else crosswalk.build left in its out_dir is kept, not deleted
        dest = lay.superseded / "_crosswalk_staging_rest"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(d), str(dest))
        out["staging_rest_moved"] = [p.name for p in rest]
    else:
        d.rmdir()
    return out


# ============================================================================ stage: validate

def validation_tables(cfg, lay: Layout, universe, v9_crosswalk=None) -> dict:
    b, w = lay.release, lay.work
    return {
        "turns": str(b / "turns" / "t*" / "part-*.parquet"),
        "meetings": str(b / "meetings.parquet"),
        "dyads": str(b / "dyads" / "t*" / "dyads.parquet"),
        "duplicate_turns": str(b / "duplicate_turns" / "t*" / "part-*.parquet"),
        "duplicate_dyads": str(b / "duplicate_dyads" / "t*" / "dyads.parquet"),
        "duplicate_meetings": str(b / "duplicate_meetings.parquet"),
        "agenda": str(b / "agenda.parquet"),
        "footer": str(b / "footer.parquet"),
        "coverage": str(w / "build_turns" / "coverage" / "*" / "*" / "*.parquet"),
        "crosswalk": str(b / "crosswalk_meetings.parquet"),
        "crosswalk_turns": str(b / "crosswalk_turns.parquet"),
        "universe": str(universe),
        "crawl": cfg["paths"]["crawl_db"],
        "v9_meetings": str(v9_crosswalk or cfg["paths"]["v9_crosswalk"]),
        "v9_speeches": str(REPO / "data" / "all_speeches_16_22_v9.parquet"),
        "calendar": cfg["paths"]["calendar"],
        "lineage": cfg["paths"]["lineage"],
    }


def stage_validate(cfg, work_root, build_root, run_id, universe, v9_crosswalk) -> dict:
    import validate as VA
    lay = Layout(work_root, build_root, run_id)
    lay.staging.mkdir(parents=True, exist_ok=True)
    rp, dp = lay.staging / "validation_report.json", lay.staging / "docs_numbers.json"
    params = validate_params(cfg)
    params.release_root = str(lay.release)
    rep = VA.run_validation(validation_tables(cfg, lay, universe, v9_crosswalk), params, out=rp, docs_out=dp)
    out = {"summary": rep["summary"], "ok": rep["ok"],
           "checks": {c["id"]: {"status": c["status"], "n_bad": c["n_bad"], "seconds": c["seconds"]} for c in rep["checks"]}}
    out["validation_report.json"] = lay.publish(rp, "validation_report.json")
    out["docs_numbers.json"] = lay.publish(dp, "docs_numbers.json")
    return out


# ============================================================================ manifest

def code_version(sha_cache=None) -> dict:
    """Git-independent code version: sha256 over the sorted (path, content sha256) list of the pipeline code
    (CODE_VERSION_GLOBS under v10/code; tests excluded), plus the per-file hashes (16 hex)."""
    files = sorted({p for g in CODE_VERSION_GLOBS for p in CODE.glob(g)
                    if p.is_file() and not p.name.startswith("test_")})
    per = {str(p.relative_to(CODE)): _sha256_file(p, sha_cache) for p in files}
    h = hashlib.sha256("\n".join(f"{k}:{v}" for k, v in sorted(per.items())).encode()).hexdigest()
    return {"sha256": h, "files": {k: v[:16] for k, v in per.items()}, "n_files": len(per)}


def crawl_snapshot(crawl_db) -> dict:
    """Latest fetch time and status counts of the crawl store (read only)."""
    p = Path(crawl_db)
    if not p.exists():
        return {"available": False}
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=60)
    try:
        mx = c.execute("SELECT max(fetched_at) FROM fetch").fetchone()[0]
        by = c.execute("SELECT kind, status, count(*) FROM fetch GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
    finally:
        c.close()
    return {"available": True, "fetched_at_max": mx, "rows": sum(n for _, _, n in by),
            "by_kind_status": {f"{k}:{st}": n for k, st, n in by}}


def universe_snapshot(universe, v9_crosswalk, sha_cache=None) -> dict:
    import pyarrow.parquet as pq
    out = {"path": Path(universe).name, "sha256": _sha256_file(universe, sha_cache),
           "rows": pq.ParquetFile(universe).metadata.num_rows,
           "v9_crosswalk": {"path": Path(v9_crosswalk).name, "sha256": _sha256_file(v9_crosswalk, sha_cache)}}
    try:
        import duckdb
        con = duckdb.connect()
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_s(universe)})").fetchall()]
        if "source" in cols:
            out["rows_by_source"] = dict(con.execute(f"SELECT source, count(*) FROM read_parquet({_s(universe)}) GROUP BY 1 ORDER BY 1").fetchall())
        con.close()
    except Exception as e:  # recorded, never fatal
        out["rows_by_source_error"] = repr(e)[:200]
    return out


def build_manifest(cfg, lay: Layout, run_info: dict, sha_cache: dict, universe=None, v9_crosswalk=None) -> dict:
    """MANIFEST.json of release/: every file with bytes, sha256 and (parquet) rows and columns; per table the row
    total and column list (schemas of a partitioned table must agree); code version, config values, universe
    snapshot hash, crawl snapshot time. Repository-relative paths only."""
    import pyarrow.parquet as pq
    import build_turns as bt
    import validate as VA
    files, tables = [], {}
    for p in sorted(lay.release.rglob("*")):
        rel = p.relative_to(lay.release)
        if p.is_dir() or p.name == "MANIFEST.json":
            continue
        ent = {"path": str(rel), "bytes": p.stat().st_size, "sha256": _sha256_file(p, sha_cache)}
        if p.suffix == ".parquet":
            pf = pq.ParquetFile(p)
            ent["rows"] = pf.metadata.num_rows
            cols = [f"{f.name}:{f.type}" for f in pf.schema_arrow]
            key = rel.parts[0] if len(rel.parts) > 1 else p.stem
            t = tables.setdefault(key, {"rows": 0, "files": 0, "columns": cols, "schemas_agree": True})
            t["rows"] += ent["rows"]
            t["files"] += 1
            if cols != t["columns"]:
                t["schemas_agree"] = False
        files.append(ent)
    for t in tables.values():
        t["n_columns"] = len(t["columns"])
    cfg_rel = VA.relativize({"switchable": cfg["switchable"], "fixed": cfg["fixed"], "resources": cfg["resources"],
                             "sources": cfg["sources"], "paths": cfg["paths"]}, REPO)
    run = VA.relativize({k: v for k, v in run_info.items() if k in ("run_id", "stages_requested", "force", "subset",
                                                                    "validation_ok", "validation_summary", "total_seconds",
                                                                    "universe", "v9_crosswalk")}, REPO)
    if isinstance(run.get("subset"), dict):
        run["subset"] = {k: v for k, v in run["subset"].items() if k in ("n", "seed", "rows", "requested", "not_in_universe")}
    return {
        "run_all_version": RUN_ALL_VERSION, "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "run": run,
        "row_counts": {k: t["rows"] for k, t in sorted(tables.items())},
        "tables": dict(sorted(tables.items())),
        "files": files,
        "code_version": code_version(sha_cache),
        "adapter_versions": {s: bt.adapter_version(s) for s in ("xml", "xlsx", "hwp")},
        "config": cfg_rel,
        "universe_snapshot": universe_snapshot(universe or cfg["paths"]["universe"], v9_crosswalk or cfg["paths"]["v9_crosswalk"], sha_cache),
        "crawl_snapshot": crawl_snapshot(cfg["paths"]["crawl_db"]),
        "python": sys.version.split()[0],
    }


def scan_release(lay: Layout) -> dict:
    """Every file of release/ (MANIFEST, validation report and docs numbers included) scanned for absolute local
    paths and the user name (validate.scan_file_for_local_paths). {'files': n, 'hits': [...]}."""
    import duckdb
    import validate as VA
    v = VA.Validator({}, VA.Params(mode="dev"))
    pattern = VA.local_path_regex(VA._user_names(v))
    v.con.close()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    hits, n = [], 0
    for p in sorted(lay.release.rglob("*")):
        if not p.is_file():
            continue
        n += 1
        h = VA.scan_file_for_local_paths(con, p, pattern)["hits"]
        if h:
            hits.append({"file": re.sub(pattern, "<local>", str(p.relative_to(lay.release))), "hits": h[:3]})
    con.close()
    return {"files": n, "files_with_hits": len(hits), "hits": hits[:20]}


def move_pre_release_layout(lay: Layout) -> list:
    """Release files of the layout before 2026-09-26 (directly under build/) -> _superseded/<run_id>/_pre_release_layout/."""
    moved = []
    for name in PRE_RELEASE_LAYOUT:
        src = lay.build / name
        if src.exists():
            dest = lay.superseded / "_pre_release_layout" / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dest))
            moved.append(name)
    return moved


# ============================================================================ orchestration

def fingerprints(cfg, snap, lay, universe_sha, decisions=None) -> dict:
    """Stage fingerprints from the manifest snapshot, code, reference data and parameters."""
    sw = cfg["switchable"]
    ref = reference_fingerprint()
    enrich_code = code_sha(ENRICH_CODE + SHARED_CODE)
    enrich_params = {"legislators": sw["legislators"], "government": sw["government"],
                     "fixed": {k: cfg["fixed"][k] for k in ("party_timeline", "legislators", "government", "roles")},
                     "text_heavy": TEXT_HEAVY_COLS}
    all_man5 = sorted((r["conf_num"], r["batch_key"], r["n_turns"], r["builder_version"], r["raw_sha1"])
                      for t in snap["terms"].values() for r in t["rows"])
    dup_params = {"validate": {k: sw["validate"][k] for k in ("dup_containment", "dup_long_min_chars", "dup_long_min_shared",
                                                              "dup_max_df", "dup_shingle_len", "dup_shingle_min_shared",
                                                              "dup_shingle_min_n", "dup_meeting_min_chars")},
                  "duplicates": sw.get("duplicates"), "fixed": cfg["fixed"].get("duplicates")}
    fp = {"enrich": {}, "dyads": {},
          "duplicates": _fp({"manifest": all_man5, "meetings": snap["meetings_sha256"], "universe": universe_sha,
                             "code": code_sha(("run_all.py", "validate.py")), "params": dup_params})}
    dec = decisions or {}
    dup_of = {int(a): int(b) for a, b in (dec.get("duplicate_of") or {}).items()}
    dec_sha = _fp({"duplicate_of": sorted(dup_of.items()),
                   "overlap": sorted((int(a), v) for a, v in (dec.get("overlap") or {}).items()),
                   "basis": sorted((int(a), v) for a, v in (dec.get("duplicate_basis") or {}).items())})
    fp["decisions"] = dec_sha
    for tk, t in snap["terms"].items():
        man = [(r["conf_num"], r["batch_key"], r["n_turns"], r["builder_version"], r["raw_sha1"]) for r in t["rows"]]
        copies = sorted(int(r["conf_num"]) for r in t["rows"] if int(r["conf_num"]) in dup_of)
        fp["enrich"][tk] = _fp({"manifest": man, "meetings": t["meetings_rows_sha"], "code": enrich_code,
                                "ref": ref, "params": enrich_params, "duplicate_copies": copies})
        fp["dyads"][tk] = _fp({"turns": fp["enrich"][tk], "meetings": t["meetings_rows_sha"], "decisions": dec_sha,
                               "code": code_sha(("dyads.py",)), "params": sw["dyads"]})
    all_man = sorted((r["conf_num"], r["batch_key"], r["n_turns"]) for t in snap["terms"].values() for r in t["rows"])
    fp["tables"] = _fp({"manifest": all_man, "meetings": snap["meetings_sha256"], "code": code_sha(("run_all.py",)),
                        "decisions": dec_sha})
    fp["crosswalk"] = _fp({"turns": fp["enrich"], "meetings": snap["meetings_sha256"], "universe": universe_sha,
                           "decisions": dec_sha, "code": code_sha(("crosswalk.py", "validate.py")), "params": sw["crosswalk"]})
    return fp


def run(cfg, work_root, build_root, stages=STAGES, force=(), only=None, universe=None, subset_info=None,
        rebuild_turns=False) -> dict:
    """One build. `universe` / subset_info['subset_v9_crosswalk'] replace the configured universe and v9
    crosswalk in a subset run (write_subset_universe)."""
    import validate as VA
    run_id = dt.datetime.now().strftime("%Y%m%dT%H%M%S_%f")
    lay = Layout(work_root, build_root, run_id)
    lay.build.mkdir(parents=True, exist_ok=True)
    _setup_logging(lay.build)
    universe = universe or cfg["paths"]["universe"]
    v9_crosswalk = (subset_info or {}).get("subset_v9_crosswalk") or cfg["paths"]["v9_crosswalk"]
    sha_cache_p = lay.state_dir / "sha256_cache.json"
    with run_lock(lay.build):
        sha_cache = json.loads(sha_cache_p.read_text()) if sha_cache_p.exists() else {}
        sha_cache = {k: tuple(v) for k, v in sha_cache.items()}
        if lay.staging.exists() and any(lay.staging.iterdir()):
            dest = lay.superseded / "_staging_leftover_at_start"
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(lay.staging), str(dest))
            LOG.info("moved leftover staging to %s", dest)
        state = lay.load_state()
        info = {"run_id": run_id, "work_root": lay.rel(lay.work), "build_root": str(lay.build.name),
                "universe": str(universe), "v9_crosswalk": str(v9_crosswalk),
                "subset": subset_info, "stages_requested": list(stages), "force": list(force), "stages": {}}
        moved = move_pre_release_layout(lay)
        if moved:
            info["pre_release_layout_moved"] = moved
            for st_ in ("enrich", "dyads", "tables", "crosswalk"):
                state.pop(st_, None)
            lay.save_state(state)
            LOG.info("earlier layout: %d release entries moved to %s", len(moved), lay.superseded / "_pre_release_layout")
        t_start = time.time()
        LOG.info("run %s: work=%s build=%s stages=%s force=%s subset=%s", run_id, lay.work, lay.build,
                 list(stages), list(force), None if not subset_info else subset_info.get("n"))

        def rec(name, r):
            info["stages"][name] = r
            _progress(lay, f"stage {name}: " + json.dumps({k: v for k, v in r.items() if k != "result"}, default=str)[:400])

        if "build_turns" in stages:
            r = run_stage("build_turns", "stage_build_turns",
                          {"cfg": cfg, "work_root": str(lay.work), "universe": str(universe),
                           "sources": list(cfg["sources"]), "only": only, "rebuild": rebuild_turns}, cfg, lay.build)
            rec("build_turns", r)
        snap = snapshot(cfg, lay)
        if only is not None:
            extra = [r["conf_num"] for t in snap["terms"].values() for r in t["rows"] if r["conf_num"] not in set(only)]
            if extra:
                raise StageError(f"subset run: the work root holds {len(extra)} meetings outside the subset")
        universe_sha = _sha256_file(universe, sha_cache) + _sha256_file(v9_crosswalk, sha_cache)[:16]
        fp = fingerprints(cfg, snap, lay, universe_sha)
        all_rows = [x for t in snap["terms"].values() for x in t["rows"]]

        def need(stage, key=None):
            if stage in force:
                return True
            have = state.get(stage, {})
            if key is None:
                return have.get("_all") != fp[stage]
            return have.get(key) != fp[stage][key]

        # duplicates: a prerequisite of enrich / tables / dyads / crosswalk (their outputs depend on the decisions)
        dec = load_duplicates(lay)
        downstream = {"enrich", "tables", "dyads", "crosswalk"} & set(stages)
        if "duplicates" in stages or (downstream and dec.get("fingerprint") != fp["duplicates"]):
            if need("duplicates") or dec.get("fingerprint") != fp["duplicates"] or not (lay.release / "duplicate_meetings.parquet").exists():
                r = run_stage("duplicates", "stage_duplicates", {"cfg": cfg, "work_root": str(lay.work),
                                                                 "build_root": str(lay.build), "run_id": run_id,
                                                                 "rows": all_rows, "universe": str(universe),
                                                                 "fingerprint": fp["duplicates"]}, cfg, lay.build)
                state["duplicates"] = {"_all": fp["duplicates"]}
                lay.save_state(state)
            else:
                r = {"skipped": True}
            rec("duplicates", r)
            dec = load_duplicates(lay)
        fp = fingerprints(cfg, snap, lay, universe_sha, dec)
        dup_copies = sorted(dec["duplicate_of"])
        info["duplicates"] = {"duplicate_of": {str(a): b for a, b in dec["duplicate_of"].items()},
                              "meetings_with_overlap": len(dec["overlap"])}
        info["snapshot"] = {k: v for k, v in snap.items() if k != "terms"}
        info["snapshot"]["terms"] = {k: {"meetings": t["meetings"], "turns": t["turns"]} for k, t in snap["terms"].items()}
        info["fingerprints"] = fp

        # terms whose partitions disappeared from the manifest: superseded, never deleted
        present = set(snap["terms"])
        for sub in ("turns", "dyads", "duplicate_turns", "duplicate_dyads"):
            existing = sorted((lay.release / sub).glob("t*")) if (lay.release / sub).exists() else []
            for d in existing:
                if d.name[1:] not in present:
                    old = lay.retire(f"{sub}/{d.name}")
                    state.get("enrich" if sub in ("turns", "duplicate_turns") else "dyads", {}).pop(d.name[1:], None)
                    LOG.info("term %s no longer in the manifest: %s moved to %s", d.name, sub, old)

        if "enrich" in stages:
            todo = {tk: {"rows": snap["terms"][tk]["rows"], "fingerprint": fp["enrich"][tk]}
                    for tk in sorted(snap["terms"])
                    if need("enrich", tk) or not (lay.release / "turns" / f"t{tk}").exists()}
            if todo:
                r = run_stage("enrich", "stage_enrich", {"cfg": cfg, "work_root": str(lay.work),
                                                         "build_root": str(lay.build), "run_id": run_id, "todo": todo,
                                                         "duplicate_conf_nums": dup_copies},
                              cfg, lay.build)
                for tk in todo:
                    state.setdefault("enrich", {})[tk] = fp["enrich"][tk]
                    state.setdefault("dyads", {}).pop(tk, None)       # dyads follow the new turns
                lay.save_state(state)
                r["terms_built"], r["terms_skipped"] = sorted(todo), sorted(set(snap["terms"]) - set(todo))
            else:
                r = {"skipped": True, "terms_skipped": sorted(snap["terms"])}
            rec("enrich", r)
        if "tables" in stages:
            if need("tables") or not (lay.release / "meetings.parquet").exists():
                r = run_stage("tables", "stage_tables", {"cfg": cfg, "work_root": str(lay.work),
                                                         "build_root": str(lay.build), "run_id": run_id, "rows": all_rows,
                                                         "decisions": {k: dec[k] for k in ("duplicate_of", "duplicate_basis", "overlap")}},
                              cfg, lay.build)
                state["tables"] = {"_all": fp["tables"]}
                for tk in list(state.get("dyads", {})):                # dyads carry meetings columns
                    if state["dyads"][tk] != fp["dyads"].get(tk):
                        state["dyads"].pop(tk)
                lay.save_state(state)
            else:
                r = {"skipped": True}
            rec("tables", r)
        if "dyads" in stages:
            todo = {tk: {"fingerprint": fp["dyads"][tk]} for tk in sorted(snap["terms"])
                    if need("dyads", tk) or not (lay.release / "dyads" / f"t{tk}").exists()}
            if todo:
                r = run_stage("dyads", "stage_dyads", {"cfg": cfg, "work_root": str(lay.work),
                                                       "build_root": str(lay.build), "run_id": run_id, "todo": todo},
                              cfg, lay.build)
                for tk in todo:
                    state.setdefault("dyads", {})[tk] = fp["dyads"][tk]
                lay.save_state(state)
            else:
                r = {"skipped": True}
            rec("dyads", r)
        if "crosswalk" in stages:
            if need("crosswalk") or not (lay.release / "crosswalk_meetings.parquet").exists():
                r = run_stage("crosswalk", "stage_crosswalk", {"cfg": cfg, "work_root": str(lay.work),
                                                               "build_root": str(lay.build), "run_id": run_id,
                                                               "universe": str(universe),
                                                               "v9_crosswalk": str(v9_crosswalk)}, cfg, lay.build)
                state["crosswalk"] = {"_all": fp["crosswalk"]}
                lay.save_state(state)
            else:
                r = {"skipped": True}
            rec("crosswalk", r)
        if "validate" in stages:
            r = run_stage("validate", "stage_validate", {"cfg": cfg, "work_root": str(lay.work),
                                                         "build_root": str(lay.build), "run_id": run_id,
                                                         "universe": str(universe), "v9_crosswalk": str(v9_crosswalk)},
                          cfg, lay.build)
            rec("validate", r)
            info["validation_ok"] = r["result"]["ok"]
            info["validation_summary"] = r["result"]["summary"]
        info["total_seconds"] = round(time.time() - t_start, 1)
        if "manifest" in stages:
            man = build_manifest(cfg, lay, info, sha_cache, universe=universe, v9_crosswalk=v9_crosswalk)
            tmp = lay.state_dir / ".MANIFEST.json.tmp"
            tmp.write_text(json.dumps(man, ensure_ascii=False, indent=1, default=str))
            lay.release.mkdir(parents=True, exist_ok=True)
            os.replace(tmp, lay.release / "MANIFEST.json")
            info["manifest_files"] = len(man["files"])
            info["row_counts"] = man["row_counts"]
            info["code_version"] = man["code_version"]["sha256"]
            sc = scan_release(lay)
            info["release_scan"] = sc
            info["release_clean"] = sc["files_with_hits"] == 0
            if not info["release_clean"]:
                LOG.error("release files with local paths / user name: %s", sc["hits"][:5])
        sha_cache_p.write_text(json.dumps(sha_cache))
        (lay.state_dir / f"run_{run_id}.json").write_text(json.dumps(info, ensure_ascii=False, indent=1, default=str))
        _progress(lay, f"run {run_id} finished in {info['total_seconds']}s validation_ok={info.get('validation_ok')} "
                       f"release_clean={info.get('release_clean')}")
        return info


def _progress(lay: Layout, line: str):
    p = lay.state_dir / "PROGRESS.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(f"[{dt.datetime.now().isoformat(timespec='seconds')}] {line}\n")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="v10 release build (incremental, safe to re-run)")
    ap.add_argument("--config", default=str(CONFIG_DEFAULT))
    ap.add_argument("--work-root", default=None, help="build_turns output root (default config paths.work_root)")
    ap.add_argument("--build-root", default=None, help="release root (default config paths.build_root)")
    ap.add_argument("--stages", default=",".join(STAGES))
    ap.add_argument("--force", default="", help="comma-separated stages to rebuild even when unchanged")
    ap.add_argument("--rebuild-turns", action="store_true", help="build_turns --rebuild for the selected meetings")
    sub = ap.add_mutually_exclusive_group()
    sub.add_argument("--conf-nums", default=None, help="comma-separated CONFER_NUMs (subset run)")
    sub.add_argument("--sample", type=int, default=None, help="seeded subset of N meetings across sources")
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--workers", type=int, default=None, help="override resources.build_turns_workers")
    a = ap.parse_args(argv)
    ov = {"resources": {"build_turns_workers": a.workers}} if a.workers else None
    cfg = load_config(a.config, ov)
    stages = tuple(s for s in a.stages.split(",") if s)
    bad = set(stages) - set(STAGES)
    if bad:
        raise SystemExit(f"unknown stages {bad}")
    force = tuple(s for s in a.force.split(",") if s)
    only, subset_info, universe = None, None, None
    work_root = Path(a.work_root or cfg["paths"]["work_root"])
    build_root = Path(a.build_root or cfg["paths"]["build_root"])
    if a.conf_nums or a.sample:
        only = [int(x) for x in a.conf_nums.split(",")] if a.conf_nums else select_sample(cfg, a.sample, a.seed)
        if not a.work_root:
            work_root = DRYRUN_ROOT / "work"
        if not a.build_root:
            build_root = DRYRUN_ROOT / "build"
        if work_root.resolve() == PRODUCTION_WORK_ROOT.resolve():
            raise SystemExit("a subset run must not use the production work root (its meetings table would "
                             "hold only the subset); pass --work-root")
        universe = work_root / "subset_universe.parquet"
        subset_info = write_subset_universe(cfg, only, universe)
        subset_info.update({"n": len(only), "seed": a.seed if a.sample else None, "conf_nums": only})
        if subset_info["not_in_universe"]:
            LOG.warning("subset: %d requested meetings are not in the universe: %s",
                        len(subset_info["not_in_universe"]), subset_info["not_in_universe"][:20])
    info = run(cfg, work_root, build_root, stages=stages, force=force, only=only, universe=universe,
               subset_info=subset_info, rebuild_turns=a.rebuild_turns)
    print(json.dumps({"run_id": info["run_id"], "validation_ok": info.get("validation_ok"),
                      "validation_summary": info.get("validation_summary"), "release_clean": info.get("release_clean"),
                      "row_counts": info.get("row_counts"), "total_seconds": info.get("total_seconds")},
                     ensure_ascii=False, indent=1, default=str))
    if info.get("validation_ok") is False or info.get("release_clean") is False:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
