"""Evaluation of legislators.enrich (v10 pipeline).

1. XML: every view page on disk (crawler output plus discovery samples) is parsed with
   parse_viewer.parse_view into contract-shaped turns, classified with roles.enrich (role_group) and
   enriched. Name-based resolution is validated against the record member-term id: for turns with a
   data-mem_id, the resolver is re-run without the id and compared with the id-based NAAS_CD.
2. HWP: every HWP file on disk is parsed with hwp_parser.parse_hwp (speaker fields only), classified
   and enriched. For meetings that also have v9 XLSX rows, the persons found per (meeting, name) are
   compared between the two sources.
3. XLSX: v9 rows of the XLSX-sourced hearing types (상임위원회, 국정감사) are aggregated with duckdb to
   one row per (meeting, speaker label, XLSX member id) with a row count, split into position and
   name, classified, enriched, and compared with v9 naas_cd. Disagreements are classified by whether
   v9's person was seated on the speech date (the resolver's own seat and committee data: a
   consistency check) and by the XLSX member id partition (independent of the resolver).
The XML and HWP turn caches are incremental and stamped with a sha1 of the parser sources; a cache
built by another parser version is discarded and everything is re-parsed. The reference tables are
read, not rebuilt; reference_rebuild_identical in eval_summary.json says whether a rebuild from the
API downloads reproduces them.
Outputs go to interim/pipeline/legislators/eval/. Usage:  python legislators_eval.py [--no-hwp]
"""
from __future__ import annotations

import concurrent.futures as cf
import glob
import gzip
import hashlib
import json
import re
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
import legislators as L  # noqa: E402

V10 = L.V10
EVAL = L.OUTD / "eval"
V9 = V10.parent / "data" / "all_speeches_16_22_v9.parquet"
UNIVERSE = V10 / "interim" / "meeting_universe_api.parquet"
CROSSWALK = V10 / "interim" / "v9_to_api_crosswalk.parquet"
XLSX_TYPES = ("상임위원회", "국정감사")


def parser_fingerprint(*files):
    """sha1 over the source text of the parser modules; a cache built by other code is discarded."""
    h = hashlib.sha1()
    for f in files:
        h.update(Path(f).name.encode())
        h.update(Path(f).read_bytes())
    return h.hexdigest()


XML_PARSER_FILES = (HERE.parent / "parse_viewer.py",)
HWP_PARSER_FILES = (HERE / "hwp_parser.py", HERE.parent / "parse_viewer.py")
FP_KEY = "__parser_sha1__"


def _load_cache(cpath, spath, files, cache):
    """(old turns, status) from an incremental cache, or empty ones when the cache was built by
    another version of the parser (status[FP_KEY] differs) or cache=False."""
    fp = parser_fingerprint(*files)
    status = json.loads(spath.read_text()) if (cache and spath.exists()) else {}
    if status.get(FP_KEY) != fp:
        if status:
            print(f"  cache {cpath.name}: parser changed ({str(status.get(FP_KEY))[:10]} -> {fp[:10]}); "
                  f"re-parsing all {len(status) - (FP_KEY in status)} files", flush=True)
        status = {}
    old = pd.read_parquet(cpath) if (status and cpath.exists()) else pd.DataFrame()
    status[FP_KEY] = fp
    return old, status


def _con():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET threads=4")
    return con


def with_roles(turns, meetings):
    """role_group from roles.enrich (production path). Falls back to the title rule (role_group
    absent) when roles.py cannot be imported or fails; the fallback is reported."""
    try:
        import roles
        m = meetings.copy()
        for c in ("hearing_type", "is_subcommittee", "subcommittee"):
            if c not in m.columns:
                m[c] = None
        r = roles.enrich(turns, m)
        t = turns.copy()
        t["role"] = r["role"].values
        t["role_group"] = r["role_group"].astype(object).where(r["role_group"].notna(), None).values
        return t, "roles.enrich"
    except Exception as e:  # noqa: BLE001
        print(f"roles.enrich failed ({type(e).__name__}: {e}); using the title rule", flush=True)
        return turns, f"title_rule ({type(e).__name__})"


# ----------------------------------------------------------------------------- XML

def xml_pages():
    pages = {}
    pats = [V10 / "raw/viewer/view/*/*.html.gz", V10 / "raw/samples/*_view.html.gz",
            V10 / "raw/samples_random/*_view.html.gz"]
    for pat in pats:
        for f in sorted(glob.glob(str(pat))):
            m = re.match(r"(\d+)", Path(f).name)
            if m:
                pages.setdefault(int(m.group(1)), f)
    return pages


def _xml_one(item):
    """Parse one view page into speaker-field rows: (conf_num, status, rows)."""
    from parse_viewer import parse_view
    c, path = item
    try:
        page = gzip.open(path).read().decode("utf-8", "replace")
        r = parse_view(page)
    except Exception as e:  # count, never drop silently
        return c, f"error:{type(e).__name__}", []
    rows = []
    for k, s in enumerate(r["speeches"], start=1):
        pos, name = s.get("pos_norm"), s.get("name_norm")
        rows.append({"conf_num": c, "turn_seq": k, "source": "xml",
                     "speaker_label_raw": " ".join(x for x in (pos, name) if x),
                     "speaker_pos": pos, "speaker_name": name,
                     "speaker_mem_id": int(s["mem_id"]) if (s.get("mem_id") or "0").isdigit() and int(s["mem_id"]) != 0 else None,
                     "speaker_area": s.get("area"), "speech_date": s.get("speech_date")})
    return c, r["status"], rows


def _finish_cache(status, files, label):
    """After parsing: when the parser source changed during the run, the cache is not stamped (the
    next run re-parses everything)."""
    fp = parser_fingerprint(*files)
    if status.get(FP_KEY) != fp:
        print(f"  WARNING {label}: parser source changed during the run; cache left unstamped", flush=True)
        status[FP_KEY] = "changed-during-run"
    return status


def xml_turns(cache=True, workers=4):
    """Contract-shaped turns (speaker fields only) for every XML view page on disk (incremental cache,
    discarded when parse_viewer.py changes)."""
    EVAL.mkdir(parents=True, exist_ok=True)
    cpath = EVAL / "xml_turns_cache.parquet"
    spath = EVAL / "xml_pages_status.json"
    old, status = _load_cache(cpath, spath, XML_PARSER_FILES, cache)
    pages = xml_pages()
    todo = [(c, pages[c]) for c in pages if str(c) not in status]
    rows = []
    t0 = time.time()
    with cf.ProcessPoolExecutor(max_workers=workers) as ex:
        for i, (c, st, out) in enumerate(ex.map(_xml_one, todo, chunksize=8)):
            status[str(c)] = st
            rows += out
            if (i + 1) % 500 == 0:
                print(f"  parsed {i + 1}/{len(todo)} pages in {time.time() - t0:.0f}s", flush=True)
    new = pd.DataFrame(rows)
    allt = pd.concat([old, new], ignore_index=True) if len(old) else new
    if len(allt):
        allt["speaker_mem_id"] = pd.array(allt["speaker_mem_id"], dtype="Int64")
        allt.to_parquet(cpath, index=False)
    status = _finish_cache(status, XML_PARSER_FILES, "xml")
    spath.write_text(json.dumps(status))
    return allt, status


def universe_meetings(conf_nums=None):
    u = pd.read_parquet(UNIVERSE, columns=["CONFER_NUM", "CONF_ID", "DAE_NUM", "CLASS_NAME_unified", "COMM_NAME",
                                           "CONF_DATE", "is_subcommittee_name"])
    m = pd.DataFrame({"conf_num": u.CONFER_NUM.astype("int64"), "conf_id": u.CONF_ID, "term": u.DAE_NUM,
                      "class_name": u.CLASS_NAME_unified, "hearing_type": None, "committee_raw": u.COMM_NAME,
                      "subcommittee": None, "date": u.CONF_DATE, "is_subcommittee": u.is_subcommittee_name})
    if conf_nums is not None:
        m = m[m.conf_num.isin(set(conf_nums))]
    return m


# ----------------------------------------------------------------------------- HWP

def _hwp_one(path):
    import hwp_parser
    c = int(Path(path).stem)
    try:
        res = hwp_parser.parse_hwp(Path(path).read_bytes(), conf_num=c)
    except Exception as e:  # noqa: BLE001
        return c, f"error:{type(e).__name__}", []
    out = []
    for k, t in enumerate(res.get("turns") or [], start=1):
        out.append({"conf_num": c, "turn_seq": t.get("turn_seq") or k, "source": "hwp",
                    "speaker_label_raw": t.get("speaker_label_raw"), "speaker_pos": t.get("speaker_pos"),
                    "speaker_name": t.get("speaker_name"), "speaker_mem_id": None,
                    "speaker_area": t.get("speaker_area"), "speech_date": t.get("speech_date")})
    return c, str(res.get("status")), out


def hwp_turns(cache=True, workers=4):
    EVAL.mkdir(parents=True, exist_ok=True)
    cpath = EVAL / "hwp_turns_cache.parquet"
    spath = EVAL / "hwp_files_status.json"
    old, status = _load_cache(cpath, spath, HWP_PARSER_FILES, cache)
    files = sorted(glob.glob(str(V10 / "raw/hwp/*/*.hwp")))
    todo = [f for f in files if Path(f).stem not in status]
    rows = []
    t0 = time.time()
    with cf.ProcessPoolExecutor(max_workers=workers) as ex:
        for i, (c, st, out) in enumerate(ex.map(_hwp_one, todo, chunksize=4)):
            status[str(c)] = st
            rows += out
            if (i + 1) % 100 == 0:
                print(f"  hwp parsed {i + 1}/{len(todo)} in {time.time() - t0:.0f}s", flush=True)
    new = pd.DataFrame(rows)
    allt = pd.concat([old, new], ignore_index=True) if len(old) else new
    if len(allt):
        for c in ("speaker_label_raw", "speaker_pos", "speaker_name", "speaker_area", "speech_date"):
            allt[c] = allt[c].astype(object)
        allt["speaker_mem_id"] = pd.array([None] * len(allt), dtype="Int64")
        allt.to_parquet(cpath, index=False)
    status = _finish_cache(status, HWP_PARSER_FILES, "hwp")
    spath.write_text(json.dumps(status))
    return allt, status


# ----------------------------------------------------------------------------- v9 XLSX

def v9_xlsx_keys():
    con = _con()
    q = f"""
    SELECT meeting_id, term, date, committee, hearing_type, speaker, member_id, role, naas_cd, count(*) AS n_rows
    FROM '{V9}'
    WHERE hearing_type IN {XLSX_TYPES}
    GROUP BY ALL
    """
    k = con.execute(q).fetchdf()
    con.close()
    return k


def v9_as_turns(k):
    pos, name = zip(*[L.split_label(s) for s in k.speaker])
    t = pd.DataFrame({"conf_num": k.meeting_id.values, "turn_seq": range(1, len(k) + 1), "source": "xlsx",
                      "speaker_label_raw": k.speaker.values, "speaker_pos": pos, "speaker_name": name,
                      "speaker_mem_id": None, "speaker_area": None, "speech_date": k.date.values,
                      "source_member_id": k.member_id.astype(object).values})
    m = (k[["meeting_id", "term", "hearing_type", "committee", "date"]].drop_duplicates("meeting_id")
         .rename(columns={"meeting_id": "conf_num", "committee": "committee_raw"}))
    m["class_name"] = m.hearing_type
    m["subcommittee"] = None
    m["is_subcommittee"] = False
    return t, m


# ----------------------------------------------------------------------------- summaries

def coverage_table(df, weight=None, by=("term",)):
    w = df[weight] if weight else pd.Series(1, index=df.index)
    d = df.assign(_w=w, _linked=df.naas_cd.notna())
    g = d.groupby(list(by)).apply(lambda x: pd.Series({
        "turns": int(x._w.sum()), "linked": int(x._w[x._linked].sum()),
        "pct_linked": round(100 * x._w[x._linked].sum() / max(x._w.sum(), 1), 3),
        "high": int(x._w[x.id_confidence == "high"].sum()), "medium": int(x._w[x.id_confidence == "medium"].sum()),
        "low": int(x._w[x.id_confidence == "low"].sum()),
        "unresolved": int(x._w[~x._linked].sum())}), include_groups=False)
    return g.reset_index()


def method_table(df, weight=None, by=("term",)):
    w = df[weight] if weight else pd.Series(1, index=df.index)
    d = df.assign(_w=w)
    return d.groupby(list(by) + ["id_method"], dropna=False)._w.sum().rename("turns").reset_index()


def _str(x):
    return None if x is None or (isinstance(x, float) and pd.isna(x)) or x is pd.NA else str(x)


def reference_rebuild_check():
    """{table: True/False}: build_tables(write=False) from the API downloads equals the saved parquet
    (both read back through parquet, so list columns compare the same way)."""
    import io
    out = {}
    built = L.build_tables(write=False, verbose=False)
    for k, df in built.items():
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        try:
            pd.testing.assert_frame_equal(pd.read_parquet(buf), pd.read_parquet(L.OUTD / f"{k}.parquet"))
            out[k] = True
        except AssertionError:
            out[k] = False
    return out


UNRES_COLS = ["source", "term", "speaker_name", "id_method", "n_rows", "speaker_label_raw", "id_note"]
NONLEG_COLS = ["source", "term", "speaker_pos", "speaker_name", "naas_cd", "id_method", "id_confidence", "n_rows",
               "id_note"]


def main(do_hwp=True):
    EVAL.mkdir(parents=True, exist_ok=True)
    ref = L.load_reference()  # saved tables; not rebuilt (reference_rebuild_identical checks them)
    R = ref["resolver"]
    report = {"reference_rebuild_identical": reference_rebuild_check()}
    covs, meths, unres, nonlegs = [], [], [], []

    # ---------------- XML
    print("XML: parsing pages ...", flush=True)
    xt, status = xml_turns()
    report["xml_parser_sha1"] = status.get(FP_KEY)
    report["xml_page_status"] = pd.Series({k: v for k, v in status.items() if k != FP_KEY}).value_counts().to_dict()
    xm = universe_meetings(xt.conf_num.unique())
    report["xml_pages_without_universe_row"] = sorted(int(x) for x in set(xt.conf_num) - set(xm.conf_num))
    xt, report["xml_role_source"] = with_roles(xt, xm)
    xe = L.enrich(xt, xm, ref)
    xe = xe.merge(xm[["conf_num", "term", "class_name", "committee_raw"]], on="conf_num", how="left")
    xe.to_parquet(EVAL / "xml_enriched.parquet", index=False)
    leg = xe[xe.leg_side.fillna(False)].assign(n_rows=1, source="xml")
    covs.append(coverage_table(leg, by=("term",)).assign(source="xml"))
    meths.append(method_table(leg, by=("term",)).assign(source="xml"))
    report["xml_pages_parsed"] = int(xt.conf_num.nunique())
    report["xml_turns"] = int(len(xt))
    report["xml_leg_side_turns"] = int(len(leg))
    report["xml_role_group_counts"] = xe.leg_side_basis.value_counts(dropna=False).to_dict()
    unres.append(leg[leg.naas_cd.isna()][UNRES_COLS])
    nonlegs.append(xe[~xe.leg_side.fillna(False)].assign(n_rows=1, source="xml")[NONLEG_COLS])
    # name-only validation against mem_id
    withid = xe[xe.speaker_mem_id.notna() & xe.id_method.isin(["mem_id", "mem_id_seat_override"])].copy()
    k = withid.drop_duplicates(["conf_num", "speaker_pos", "speaker_name", "speaker_mem_id", "speaker_area"])
    val = []
    for r in k.itertuples():
        for use_area in (False, True):
            res = R.resolve(int(r.term), r.speech_date, r.speaker_pos, r.speaker_name, None,
                            r.speaker_area if use_area else None, r.speaker_label_raw,
                            L.parent_committee(r.committee_raw), True, "legislator")
            val.append({"use_area": use_area, "conf_num": r.conf_num, "term": r.term, "name": r.speaker_name,
                        "memid_method": r.id_method, "mem_naas": r.naas_cd, "name_naas": res["naas_cd"],
                        "method": res["method"]})
    val = pd.DataFrame(val)
    val["outcome"] = ["agree" if a == b else ("null" if b is None else "disagree")
                      for a, b in zip(val.mem_naas, val.name_naas)]
    val.to_parquet(EVAL / "xml_name_vs_memid.parquet", index=False)
    report["xml_name_vs_memid"] = (val.groupby(["use_area", "term", "outcome"]).size().unstack(fill_value=0)
                                   .reset_index().to_dict("records"))
    report["xml_name_vs_memid_disagree"] = (val[val.outcome == "disagree"]
                                            .drop_duplicates(["use_area", "term", "name", "mem_naas", "name_naas"])
                                            .to_dict("records"))
    report["xml_memid_seat_override"] = (xe[xe.id_method == "mem_id_seat_override"]
                                         .groupby(["term", "speaker_name", "speaker_mem_id", "naas_cd"]).size()
                                         .rename("turns").reset_index().to_dict("records"))

    # ---------------- HWP
    he = pd.DataFrame()
    if do_hwp:
        print("HWP: parsing files ...", flush=True)
        ht, hst = hwp_turns()
        report["hwp_parser_sha1"] = hst.get(FP_KEY)
        report["hwp_file_status"] = pd.Series({k: v for k, v in hst.items() if k != FP_KEY}).value_counts().to_dict()
        if len(ht):
            hm = universe_meetings(ht.conf_num.unique())
            report["hwp_files_without_universe_row"] = sorted(int(x) for x in set(ht.conf_num) - set(hm.conf_num))
            ht, report["hwp_role_source"] = with_roles(ht, hm)
            he = L.enrich(ht, hm, ref)
            he = he.merge(hm[["conf_num", "term", "class_name", "committee_raw"]], on="conf_num", how="left")
            he.to_parquet(EVAL / "hwp_enriched.parquet", index=False)
            hleg = he[he.leg_side.fillna(False)].assign(n_rows=1, source="hwp")
            covs.append(coverage_table(hleg, by=("term",)).assign(source="hwp"))
            meths.append(method_table(hleg, by=("term",)).assign(source="hwp"))
            report["hwp_meetings"] = int(ht.conf_num.nunique())
            report["hwp_turns"] = int(len(ht))
            report["hwp_leg_side_turns"] = int(len(hleg))
            unres.append(hleg[hleg.naas_cd.isna()][UNRES_COLS])
            nonlegs.append(he[~he.leg_side.fillna(False)].assign(n_rows=1, source="hwp")[NONLEG_COLS])

    # ---------------- XLSX (v9)
    print("XLSX: aggregating v9 rows with duckdb ...", flush=True)
    k9 = v9_xlsx_keys()
    t9, m9 = v9_as_turns(k9)
    print("XLSX keys:", len(t9), "rows:", int(k9.n_rows.sum()), flush=True)
    t0 = time.time()
    t9, report["xlsx_role_source"] = with_roles(t9, m9)
    print(f"XLSX roles {time.time() - t0:.0f}s", flush=True)
    t0 = time.time()
    e9 = L.enrich(t9, m9, ref)
    print(f"XLSX enrich {time.time() - t0:.0f}s", flush=True)
    e9["n_rows"] = k9.n_rows.values
    e9["v9_role"] = k9.role.values
    e9["v9_naas_cd"] = k9.naas_cd.values
    e9["term"] = k9.term.values
    e9["date"] = k9.date.values
    e9["committee_raw"] = k9.committee.values
    e9["hearing_type"] = k9.hearing_type.values
    e9.drop(columns=[c for c in ("role",) if c in e9.columns]).to_parquet(EVAL / "xlsx_enriched_keys.parquet",
                                                                         index=False)
    report["xlsx_keys"] = int(len(e9))
    report["xlsx_rows"] = int(e9.n_rows.sum())
    v9leg = e9[e9.v9_role.isin(["legislator", "chair"])]
    report["xlsx_v9_leg_rows"] = int(v9leg.n_rows.sum())
    leg9 = e9[e9.leg_side.fillna(False)].assign(source="xlsx")
    report["xlsx_leg_side_rows"] = int(leg9.n_rows.sum())
    report["xlsx_leg_side_vs_v9_role"] = (e9.assign(v10_leg=e9.leg_side.fillna(False),
                                                    v9_leg=e9.v9_role.isin(["legislator", "chair"]))
                                          .groupby(["v10_leg", "v9_leg"]).n_rows.sum().reset_index()
                                          .to_dict("records"))
    covs.append(coverage_table(leg9, weight="n_rows", by=("term",)).assign(source="xlsx"))
    covs.append(coverage_table(v9leg.assign(source="xlsx_v9role"), weight="n_rows", by=("term",))
                .assign(source="xlsx_v9_legislator_chair_rows"))
    meths.append(method_table(leg9, weight="n_rows", by=("term",)).assign(source="xlsx"))
    unres.append(leg9[leg9.naas_cd.isna()][UNRES_COLS])
    nonlegs.append(e9[~e9.leg_side.fillna(False)].assign(source="xlsx")[NONLEG_COLS])
    covall = pd.concat(covs, ignore_index=True)
    covall = covall[["source"] + [c for c in covall.columns if c != "source"]]
    covall.to_csv(EVAL / "coverage_term_source.csv", index=False)
    pd.concat(meths, ignore_index=True).to_csv(EVAL / "methods_term_source.csv", index=False)

    # agreement with v9 naas_cd, rows where v9 has one (v9 role legislator/chair)
    both = v9leg[v9leg.v9_naas_cd.notna() & (v9leg.v9_naas_cd != "")].copy()
    both["cmp"] = [("v10_null" if _str(a) is None else ("agree" if a == b else "disagree"))
                   for a, b in zip(both.naas_cd, both.v9_naas_cd)]
    agr = both.groupby(["term", "cmp"]).n_rows.sum().unstack(fill_value=0).reset_index()
    agr.to_csv(EVAL / "agreement_v9_by_term.csv", index=False)
    dis = both[both.cmp == "disagree"].copy()
    dis["v9_person_seated"] = [R.seated(b, int(t), d)[0] for b, t, d in zip(dis.v9_naas_cd, dis.term, dis.date)]
    dis["v9_person_on_committee"] = [R.on_committee(b, L.committee_norm(L.parent_committee(c)), d)
                                     for b, c, d in zip(dis.v9_naas_cd, dis.committee_raw, dis.date)]
    dis["v10_person_on_committee"] = [R.on_committee(a, L.committee_norm(L.parent_committee(c)), d)
                                      for a, c, d in zip(dis.naas_cd, dis.committee_raw, dis.date)]
    dis["disagree_class"] = ["v9_person_not_seated" if s is False else
                             ("v9_person_not_on_committee" if oc is False and oc10 else "other")
                             for s, oc, oc10 in zip(dis.v9_person_seated, dis.v9_person_on_committee,
                                                    dis.v10_person_on_committee)]
    # disagree_class uses the same 의원이력 seat spans and 위원회경력 spells as the resolver: a
    # consistency check, not independent evidence. The source's XLSX member id is independent of the
    # resolver. v9 naas_cd is a function of (term, XLSX id) (one v9 person per cluster, checked below),
    # so the id partition cannot be evidence for v9. For each (term, name, XLSX id) cluster of
    # legislator-side keys, the number of persons v10 assigns (all rows of the cluster): one person =
    # v10's own cues reproduce the source id partition and v9 mapped that id to another person; two
    # persons = the id is shared in the source (e.g. 19대 김영주 7407) or v10 splits it in error, not
    # decidable from the id.
    e9["clean"] = [L.clean_name(x)[0] for x in e9.speaker_name]
    cl = e9[e9.leg_side.fillna(False) & e9.source_member_id.notna()]
    ck = ["term", "clean", "source_member_id"]
    cstat = cl.groupby(ck).agg(
        xid_v10_persons=("naas_cd", lambda x: len({v for v in x if _str(v)})),
        xid_v9_persons=("v9_naas_cd", lambda x: len({v for v in x if isinstance(v, str) and v}))).reset_index()
    dis["clean"] = [L.clean_name(x)[0] for x in dis.speaker_name]
    dis = dis.merge(cstat, on=ck, how="left")
    report["xlsx_id_clusters_by_v9_persons"] = cstat.xid_v9_persons.value_counts().to_dict()
    report["xlsx_id_clusters_by_v10_persons"] = cstat.xid_v10_persons.value_counts().to_dict()
    dis["xid_evidence"] = ["no_xlsx_id" if pd.isna(a) else
                           ("v10_one_person_per_id" if a == 1 else "v10_splits_id")
                           for a in dis.xid_v10_persons]
    report["v9_disagree_class_basis"] = ("disagree_class: same 의원이력 seat spans / 위원회경력 spells as the "
                                         "resolver (consistency check, not independent evidence); xid_evidence: "
                                         "persons v10 assigns within the row's (term, name, XLSX member id) cluster")
    report["v9_disagree_by_class_xid_evidence"] = (dis.groupby(["disagree_class", "xid_evidence"]).n_rows.sum()
                                                   .reset_index().to_dict("records"))
    report["v9_disagree_by_term_name_xid_evidence"] = (
        dis.groupby(["term", "clean", "xid_evidence"]).n_rows.sum().reset_index()
        .sort_values("n_rows", ascending=False).to_dict("records"))
    report["v9_disagree_by_class"] = dis.groupby(["disagree_class"]).n_rows.sum().to_dict()
    report["v9_disagree_by_class_method"] = (dis.groupby(["disagree_class", "id_method"]).n_rows.sum()
                                             .reset_index().to_dict("records"))
    (dis.groupby(["term", "speaker_name", "v9_naas_cd", "naas_cd", "id_method", "id_confidence", "disagree_class",
                  "xid_evidence"])
     .agg(rows=("n_rows", "sum"), keys=("n_rows", "size"), d0=("date", "min"), d1=("date", "max")).reset_index()
     .sort_values("rows", ascending=False).to_csv(EVAL / "disagreements_v9.csv", index=False))
    (both[both.cmp == "v10_null"].groupby(["term", "speaker_name", "v9_naas_cd", "id_method"])
     .n_rows.sum().reset_index().sort_values("n_rows", ascending=False)
     .to_csv(EVAL / "v10_null_where_v9_has.csv", index=False))
    both.groupby(["id_method", "cmp"]).n_rows.sum().unstack(fill_value=0).reset_index().to_csv(
        EVAL / "agreement_v9_by_method.csv", index=False)
    # v9 leaves naas_cd empty on some legislator/chair rows: what v10 does there
    v9null = v9leg[v9leg.v9_naas_cd.isna() | (v9leg.v9_naas_cd == "")]
    report["v9_null_leg_rows"] = int(v9null.n_rows.sum())
    report["v9_null_leg_rows_linked_by_v10"] = int(v9null[v9null.naas_cd.notna()].n_rows.sum())

    # XLSX member id clusters of homonym keys: purity of the v10 persons
    pt = ref["person_terms"]
    hom = pt.groupby(["term", "name"]).naas_cd.nunique()
    hom = set(hom[hom > 1].index)
    hx = e9[[(t, n) in hom for t, n in zip(e9.term, e9.clean)] & e9.leg_side.fillna(False)]
    xid = (hx.groupby(["term", "clean", "source_member_id"])
           .apply(lambda g: pd.Series({
               "rows": int(g.n_rows.sum()), "keys": len(g),
               "v10_persons": ",".join(sorted({x for x in g.naas_cd if _str(x)})),
               "rows_by_person": json.dumps({str(p): int(g[g.naas_cd == p].n_rows.sum())
                                             for p in sorted({x for x in g.naas_cd if _str(x)})}),
               "unresolved_rows": int(g[g.naas_cd.isna()].n_rows.sum()),
               "d0": g.date.min(), "d1": g.date.max()}), include_groups=False).reset_index())
    xid.to_csv(EVAL / "homonym_xlsx_id_clusters.csv", index=False)

    # HWP vs XLSX, meetings present in both sources (XLSX meeting ids mapped to CONFER_NUM). Both
    # sources pass the same name, term, date and committee to the same resolver, so agreement on
    # shared (meeting, name) pairs mainly checks the label parse; the one-sided pairs and the
    # person-level comparison show what one source has and the other lacks.
    if len(he):
        cw = pd.read_parquet(CROSSWALK, columns=["meeting_id", "api_CONFER_NUM"]).dropna()
        cw["conf_num"] = cw.api_CONFER_NUM.astype("int64")
        x9 = (e9[e9.leg_side.fillna(False)].drop(columns=["clean"]).rename(columns={"conf_num": "meeting_id"})
              .merge(cw[["meeting_id", "conf_num"]], on="meeting_id", how="inner"))
        x9["clean"] = [L.clean_name(x)[0] for x in x9.speaker_name]
        hl = he[he.leg_side.fillna(False)].copy()
        hl["clean"] = [L.clean_name(x)[0] for x in hl.speaker_name]
        common = sorted(set(x9.conf_num) & set(he.conf_num))
        x9c, hlc = x9[x9.conf_num.isin(common)], hl[hl.conf_num.isin(common)]

        def persons(sr):
            return ",".join(sorted({x for x in sr if _str(x)}))
        a = x9c.groupby(["conf_num", "clean"]).agg(xlsx_rows=("n_rows", "sum"), xlsx=("naas_cd", persons))
        b = hlc.groupby(["conf_num", "clean"]).agg(hwp_turns=("naas_cd", "size"), hwp=("naas_cd", persons))
        j = a.join(b, how="outer").reset_index()
        j["outcome"] = ["xlsx_only" if pd.isna(q) else ("hwp_only" if pd.isna(p) else
                        ("agree" if x == y else ("hwp_null" if not y else ("xlsx_null" if not x else "disagree"))))
                        for p, q, x, y in zip(j.xlsx_rows, j.hwp_turns, j.xlsx, j.hwp)]
        report["hwp_vs_xlsx_meetings_in_both"] = len(common)
        report["hwp_vs_xlsx_pairs"] = j.outcome.value_counts().to_dict()
        report["hwp_vs_xlsx_xlsx_only_rows"] = int(j[j.outcome == "xlsx_only"].xlsx_rows.sum())
        report["hwp_vs_xlsx_hwp_only_turns"] = int(j[j.outcome == "hwp_only"].hwp_turns.sum())
        shared = set(j[j.outcome.isin(["agree", "disagree", "hwp_null", "xlsx_null"])].conf_num)
        report["hwp_vs_xlsx_meetings_without_shared_pair"] = sorted(int(c) for c in set(common) - shared)
        # person level: set of linked persons per meeting in each source
        px = x9c[x9c.naas_cd.notna()].groupby("conf_num").naas_cd.agg(lambda z: set(z))
        ph = hlc[hlc.naas_cd.notna()].groupby("conf_num").naas_cd.agg(lambda z: set(z))
        pm = pd.DataFrame({"x": px, "h": ph}).reindex(common)
        pm["x"] = [z if isinstance(z, set) else set() for z in pm.x]
        pm["h"] = [z if isinstance(z, set) else set() for z in pm.h]
        pm["only_xlsx"] = [len(x - h) for x, h in zip(pm.x, pm.h)]
        pm["only_hwp"] = [len(h - x) for x, h in zip(pm.x, pm.h)]
        report["hwp_vs_xlsx_meetings_same_person_set"] = int(((pm.only_xlsx == 0) & (pm.only_hwp == 0)).sum())
        report["hwp_vs_xlsx_persons_only_xlsx"] = int(pm.only_xlsx.sum())
        report["hwp_vs_xlsx_persons_only_hwp"] = int(pm.only_hwp.sum())
        # coverage: legislator-side HWP turns vs XLSX legislator-side rows per meeting (turns merge
        # consecutive rows, so the ratio is below 1 by construction; a very low ratio flags a meeting)
        cov = pd.DataFrame({"xlsx_leg_rows": x9c.groupby("conf_num").n_rows.sum(),
                            "hwp_leg_turns": hlc.groupby("conf_num").size(),
                            "hwp_turns_all": he[he.conf_num.isin(common)].groupby("conf_num").size()}
                           ).reindex(common).fillna(0).astype(int)
        cov["ratio"] = (cov.hwp_leg_turns / cov.xlsx_leg_rows.clip(lower=1)).round(3)
        cov = cov.join(pm[["only_xlsx", "only_hwp"]]).reset_index().rename(columns={"index": "conf_num"})
        report["hwp_vs_xlsx_ratio_quantiles"] = cov.ratio.quantile([0.01, 0.05, 0.5, 0.95]).round(3).to_dict()
        flag = cov[(cov.ratio < 0.1) | (cov.only_xlsx > 0) | (cov.only_hwp > 0)]
        report["hwp_vs_xlsx_flagged_meetings"] = flag.sort_values("ratio").to_dict("records")
        cov.to_csv(EVAL / "hwp_vs_xlsx_meetings.csv", index=False)
        j[j.outcome != "agree"].to_csv(EVAL / "hwp_vs_xlsx_disagreements.csv", index=False)

    # unresolved (term, name) lists for all sources
    un = pd.concat(unres, ignore_index=True)
    unl = (un.groupby(["source", "term", "speaker_name", "id_method"], dropna=False)
           .agg(turns=("n_rows", "sum"), example_label=("speaker_label_raw", "first"), note=("id_note", "first"))
           .reset_index().sort_values(["source", "term", "turns"], ascending=[True, True, False]))
    unl.to_csv(EVAL / "unresolved_term_name.csv", index=False)
    report["unresolved_by_source_method"] = (un.groupby(["source", "id_method"]).n_rows.sum().reset_index()
                                             .to_dict("records"))

    # homonym (same name in term) cases, all sources
    hcols = ["term", "source", "speaker_name", "speaker_label_raw", "id_method", "id_confidence", "naas_cd",
             "n_rows", "speech_date"]
    alln = pd.concat([leg[hcols], leg9[hcols + ["v9_naas_cd"]]]
                     + ([he[he.leg_side.fillna(False)].assign(n_rows=1, source="hwp")[hcols]] if len(he) else []),
                     ignore_index=True)
    alln["clean"] = [L.clean_name(x)[0] for x in alln.speaker_name]
    hh = alln[[(t, n) in hom for t, n in zip(alln.term, alln.clean)]]
    if "v9_naas_cd" not in hh.columns:
        hh = hh.assign(v9_naas_cd=None)
    hres = (hh.groupby(["term", "clean", "source", "speaker_label_raw", "id_method", "id_confidence", "naas_cd"],
                       dropna=False)
            .agg(turns=("n_rows", "sum"),
                 v9_naas=("v9_naas_cd", lambda s: ",".join(sorted({x for x in s if isinstance(x, str) and x}))),
                 d0=("speech_date", "min"), d1=("speech_date", "max")).reset_index())
    hres.to_csv(EVAL / "homonym_resolution.csv", index=False)
    report["homonym_rows_by_method"] = (hh.groupby(["term", "clean", "id_method"], dropna=False).n_rows.sum()
                                        .reset_index().to_dict("records"))

    # non-legislator side: links and the not-linked candidates
    nl = pd.concat(nonlegs, ignore_index=True)
    nl = nl[nl.id_method.fillna("").str.startswith("nonleg_") & ~nl.id_method.isin(["nonleg_not_member"])]
    (nl.groupby(["source", "term", "speaker_pos", "speaker_name", "naas_cd", "id_method", "id_confidence"], dropna=False)
     .agg(turns=("n_rows", "sum"), note=("id_note", "first")).reset_index()
     .sort_values(["id_method", "turns"], ascending=[True, False]).to_csv(EVAL / "nonleg_links.csv", index=False))
    report["nonleg_by_method"] = nl.groupby(["source", "id_method"]).n_rows.sum().reset_index().to_dict("records")

    report["panel_rows_end_before_start"] = R.panel_bad_dates
    kcols = ["source", "leg_side", "id_method", "id_confidence", "naas_cd", "n_rows", "id_label_repair", "id_memid_status",
             "leg_date_basis"]
    allk = pd.concat([xe.assign(n_rows=1, source="xml")[kcols], e9.assign(source="xlsx")[kcols]]
                     + ([he.assign(n_rows=1, source="hwp")[kcols]] if len(he) else []), ignore_index=True)
    report["memid_status_rows"] = allk.groupby(["source", "id_memid_status"]).n_rows.sum().reset_index().to_dict("records")
    report["date_basis_rows"] = (allk.groupby(["source", "leg_date_basis"], dropna=False).n_rows.sum().reset_index()
                                 .to_dict("records"))
    report["label_repair_rows"] = (allk[allk.id_label_repair.fillna(False)]
                                   .groupby(["source", "id_method"]).n_rows.sum().reset_index().to_dict("records"))
    report["linked_rows_by_source_method_confidence"] = (
        allk[allk.naas_cd.notna()].groupby(["source", "leg_side", "id_method", "id_confidence"]).n_rows.sum()
        .reset_index().to_dict("records"))
    (EVAL / "eval_summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str))
    print(covall.to_string())
    print(agr.to_string())
    return report


if __name__ == "__main__":
    main(do_hwp="--no-hwp" not in sys.argv)
