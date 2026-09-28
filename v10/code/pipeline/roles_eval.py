"""Evaluation driver for roles.py (component 'roles').

Subcommands
-----------
xml-inventory   Parse every locally available viewer view page once (crawled pages with
                status 'ok' in crawl_state.sqlite, raw/samples, raw/samples_random and the
                verification copies) with the production adapter build_turns.xml_extract and
                write one row per speaker turn with the speaker fields only (no text beyond an
                80-character opening snippet) to interim/pipeline/roles/xml_turn_speakers.parquet.
v9-groups       Aggregate v9 XLSX-era speaker rows (hearing_type 상임위원회/국정감사) by
                (term, hearing_type, committee, speaker, has_mid, role, person_title,
                affiliation_raw, person_name) with duckdb, no text loaded.
evaluate        Run roles.classify on both inventories, write the crosswalk / agreement
                tables and print the numbers used in the component report.
audit-sample    Draw the stratified random sample of 300 distinct position strings
                (seed 8374) and write it with the v10 role for hand audit.

No network access. Reads only local files. Output directory:
v10/interim/pipeline/roles/.
"""
from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import re
import sqlite3
import sys
from concurrent.futures import ProcessPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
V10 = os.path.abspath(os.path.join(HERE, "..", ".."))
REPO = os.path.abspath(os.path.join(V10, ".."))
OUT = os.path.join(V10, "interim", "pipeline", "roles")
sys.path.insert(0, os.path.join(V10, "code"))
sys.path.insert(0, HERE)


def _duck():
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    return con


# --------------------------------------------------------------------------- XML inventory

def _view_paths():
    """{conf_num: path} for every local view page. Crawled pages only when fetch.status='ok'."""
    paths = {}
    for pat, rx in [
        (os.path.join(V10, "raw", "samples", "*_view.html.gz"), r"(\d+)_view\.html\.gz$"),
        (os.path.join(V10, "raw", "samples_random", "*_view.html.gz"), r"(\d+)_view\.html\.gz$"),
        (os.path.join(V10, "interim", "verify", "*", "*view*.html.gz"), r"(\d+)\D*\.html\.gz$"),
        (os.path.join(V10, "interim", "verify", "c11_viewer", "*.html.gz"), r"(\d+)\.html\.gz$"),
    ]:
        for p in glob.glob(pat):
            m = re.search(rx, os.path.basename(p))
            if m:
                paths.setdefault(int(m.group(1)), p)
    db = os.path.join(V10, "interim", "crawl_state.sqlite")
    if os.path.exists(db):
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        ok = {r[0] for r in con.execute("select conf_num from fetch where kind='view' and status='ok'")}
        con.close()
        for p in glob.glob(os.path.join(V10, "raw", "viewer", "view", "*", "*.html.gz")):
            cn = int(os.path.basename(p).split(".")[0])
            if cn in ok:
                paths[cn] = p  # crawled copy preferred
    return paths


def _parse_one(args):
    """One view page through the production adapter (build_turns.xml_extract), so the speaker
    fields (speaker_label_raw from the page's checkbox label, speaker_pos, speaker_name,
    speaker_mem_id) are exactly what roles.enrich receives in the pipeline."""
    cn, path = args
    import build_turns as BT
    try:
        with open(path, "rb") as fh:
            page = fh.read()
        r = BT.xml_extract(cn, page)
    except Exception as e:  # counted, never silently dropped
        return cn, path, f"error:{type(e).__name__}", []
    h = r.get("header") or {}
    rows = []
    for t in (r.get("tables") or {}).get("turns", []):
        rows.append({
            "conf_num": cn,
            "turn_seq": t.get("turn_seq"),
            "speaker_label_raw": t.get("speaker_label_raw"),
            "speaker_pos": t.get("speaker_pos"),
            "speaker_name": t.get("speaker_name"),
            "speaker_mem_id": t.get("speaker_mem_id"),
            "label_split": t.get("label_split"),
            "label_fused": t.get("label_fused"),
            "speech_date": t.get("speech_date"),
            "snippet": (t.get("text") or "")[:80],
            "page_term": h.get("h_term"),
            "page_committee": h.get("h_committee"),
            "page_subcommittee": h.get("h_subcommittee"),
            "page_is_audit": h.get("h_is_audit"),
        })
    return cn, path, r.get("status"), rows


def xml_inventory(workers=4):
    import pandas as pd
    os.makedirs(OUT, exist_ok=True)
    paths = _view_paths()
    frames, status = [], []  # one small frame per page keeps peak memory low
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for cn, path, st, rr in ex.map(_parse_one, sorted(paths.items()), chunksize=4):
            status.append({"conf_num": cn, "path": os.path.relpath(path, V10), "status": st, "n_turns": len(rr)})
            if rr:
                frames.append(pd.DataFrame(rr))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    st = pd.DataFrame(status)
    df.to_parquet(os.path.join(OUT, "xml_turn_speakers.parquet"), index=False)
    st.to_csv(os.path.join(OUT, "xml_pages_parsed.csv"), index=False)
    print(json.dumps({"pages": len(st), "status": st["status"].value_counts().to_dict(),
                      "turns": len(df)}, ensure_ascii=False))


# --------------------------------------------------------------------------- HWP inventory

def _hwp_paths():
    """{conf_num: path} for every crawled HWP file with fetch status 'ok'."""
    paths = {}
    db = os.path.join(V10, "interim", "crawl_state.sqlite")
    ok = set()
    if os.path.exists(db):
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        ok = {r[0] for r in con.execute("select conf_num from fetch where kind='hwp' and status='ok'")}
        con.close()
    for p in glob.glob(os.path.join(V10, "raw", "hwp", "*", "*.hwp")):
        cn = int(os.path.basename(p).split(".")[0])
        if cn in ok:
            paths[cn] = p
    return paths


def _parse_hwp_one(args):
    cn, path = args
    import hwp_parser
    try:
        with open(path, "rb") as fh:
            r = hwp_parser.parse_hwp(fh.read(), conf_num=cn)
    except Exception as e:  # counted, never silently dropped
        return cn, path, f"error:{type(e).__name__}", []
    rows = []
    for t in r.get("turns", []):
        rows.append({"conf_num": cn, "turn_seq": t.get("turn_seq"),
                     "speaker_label_raw": t.get("speaker_label_raw"), "speaker_pos": t.get("speaker_pos"),
                     "speaker_name": t.get("speaker_name"), "label_how": t.get("label_how"),
                     "snippet": (t.get("text") or "")[:80],
                     "doc_committee": (r.get("meeting") or {}).get("committee_raw")})
    return cn, path, r.get("status"), rows


def hwp_inventory(workers=2):
    import pandas as pd
    os.makedirs(OUT, exist_ok=True)
    paths = _hwp_paths()
    frames, status = [], []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for cn, path, st, rr in ex.map(_parse_hwp_one, sorted(paths.items()), chunksize=4):
            status.append({"conf_num": cn, "path": os.path.relpath(path, V10), "status": str(st), "n_turns": len(rr)})
            if rr:
                frames.append(pd.DataFrame(rr))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    st = pd.DataFrame(status)
    df.to_parquet(os.path.join(OUT, "hwp_turn_speakers.parquet"), index=False)
    st.to_csv(os.path.join(OUT, "hwp_files_parsed.csv"), index=False)
    print(json.dumps({"files": len(st), "status": st["status"].value_counts().to_dict(),
                      "turns": len(df)}, ensure_ascii=False))


# --------------------------------------------------------------------------- v9 groups

def v9_groups():
    os.makedirs(OUT, exist_ok=True)
    con = _duck()
    src = os.path.join(REPO, "data", "all_speeches_16_22_v9.parquet")
    dst = os.path.join(OUT, "v9_xlsx_speaker_groups.parquet")
    con.execute(f"""COPY (SELECT term, hearing_type, committee, speaker,
        (member_id IS NOT NULL AND trim(member_id) NOT IN ('','nan','None','NaN','0')) AS has_mid,
        role, person_title, affiliation_raw, person_name, count(*) AS n
        FROM '{src}' WHERE hearing_type IN ('상임위원회','국정감사') GROUP BY ALL)
        TO '{dst}' (FORMAT parquet)""")
    print(con.execute(f"select count(*), sum(n), count(distinct speaker) from '{dst}'").fetchall())


# --------------------------------------------------------------------------- evaluation

HEARING_TO_CLASS = {"상임위원회": "상임위원회", "국정감사": "국정감사"}


def _xlsx_eval_frame(with_mid: bool):
    """One row per v9 XLSX group, classified by v10 (context = term, hearing type, committee;
    is_subcommittee False because v9 XLSX has no subcommittee meetings)."""
    import pandas as pd
    import roles
    g = pd.read_parquet(os.path.join(OUT, "v9_xlsx_speaker_groups.parquet"))
    keys = g[["speaker", "has_mid", "term", "hearing_type", "committee"]].drop_duplicates()
    res = []
    for sp, hm, term, ht, com in keys.itertuples(index=False):
        pos, name = roles.split_label(sp)
        mid = 1 if (with_mid and hm) else None
        r = roles.classify(pos, name, label_raw=sp, mem_id=mid, term=int(term), class_name=HEARING_TO_CLASS.get(ht, ht),
                           hearing_type=ht, is_subcommittee=False, committee_raw=com, subcommittee=None)
        # as enrich() does for XLSX turns: the v9 label and v9's member_id
        v9c, src = roles.v9_compat(r.pos_hangul, r.name, bool(hm), None, r.pos_hangul_printed, label_exact=sp)
        # out-of-lookup estimate: the reconstructed chain only (no v9 speaker table)
        v9ch, _ = roles.v9_compat(r.pos_hangul, r.name, bool(hm), {}, r.pos_hangul_printed, label_exact=sp)
        res.append((sp, hm, term, ht, com, pos, r.role, r.role_rule, r.person_title, r.pos_hangul, r.pos_fix, v9c, src,
                    v9ch, r.affiliation_raw, r.title_raw))
    k = pd.DataFrame(res, columns=["speaker", "has_mid", "term", "hearing_type", "committee", "pos_printed", "role_v10",
                                   "role_rule", "person_title_v10", "pos_hangul", "pos_fix", "role_v9_compat",
                                   "role_v9_compat_src", "role_v9_chain_only", "affiliation_v10", "title_raw_v10"])
    return g.merge(k, on=["speaker", "has_mid", "term", "hearing_type", "committee"], how="left", validate="many_to_one")


def _universe_meetings():
    import pandas as pd
    u = pd.read_parquet(os.path.join(V10, "interim", "meeting_universe_api.parquet"),
                        columns=["CONFER_NUM", "DAE_NUM", "CLASS_NAME_unified", "COMM_NAME", "is_subcommittee_name", "CONF_DATE"])
    comm = u["COMM_NAME"].fillna("")
    meetings = pd.DataFrame({
        "conf_num": u["CONFER_NUM"].astype("int64"),
        "term": u["DAE_NUM"],
        "class_name": u["CLASS_NAME_unified"],
        "hearing_type": u["CLASS_NAME_unified"],
        "is_subcommittee": u["is_subcommittee_name"].fillna(False) | comm.str.contains("조정위원회"),
        "committee_raw": comm.str.split().str[0].where(comm != "", None),
        "subcommittee": comm.str.split().str[1],
        "date": u["CONF_DATE"],
    })
    return meetings


def _hwp_eval_frame():
    import pandas as pd
    import roles
    x = pd.read_parquet(os.path.join(OUT, "hwp_turn_speakers.parquet"))
    meetings = _universe_meetings()
    miss = sorted(set(x["conf_num"]) - set(meetings["conf_num"]))
    turns = pd.DataFrame({
        "conf_num": x["conf_num"].astype("int64"),
        "turn_seq": x["turn_seq"],
        "speaker_pos": x["speaker_pos"],
        "speaker_name": x["speaker_name"],
        "speaker_mem_id": pd.array([None] * len(x), dtype="Int64"),
        "speaker_label_raw": x["speaker_label_raw"],
    })
    e = roles.enrich(turns, meetings)
    e = e.merge(meetings[["conf_num", "term", "class_name", "is_subcommittee", "committee_raw"]], on="conf_num", how="left")
    e["label_how"] = x["label_how"].to_numpy()
    return e, len(miss)


def _xml_eval_frame():
    import pandas as pd
    import roles
    x = pd.read_parquet(os.path.join(OUT, "xml_turn_speakers.parquet"))
    meetings = _universe_meetings()
    # pages missing from the universe keep the page's own term (counted in the report)
    miss = sorted(set(x["conf_num"]) - set(meetings["conf_num"]))
    if miss:
        pm = x[x["conf_num"].isin(miss)].groupby("conf_num").agg(term=("page_term", "first"),
                                                               committee_raw=("page_committee", "first")).reset_index()
        meetings = pd.concat([meetings, pm], ignore_index=True)
    mid = pd.to_numeric(x["speaker_mem_id"], errors="coerce")
    turns = pd.DataFrame({
        "conf_num": x["conf_num"].astype("int64"),
        "turn_seq": x["turn_seq"],
        "source": "xml",
        "speaker_pos": x["speaker_pos"],
        "speaker_name": x["speaker_name"],
        "speaker_mem_id": mid.where(mid > 0).astype("Int64"),
        "speaker_label_raw": x["speaker_label_raw"],
    })
    e = roles.enrich(turns, meetings)
    e = e.merge(meetings[["conf_num", "term", "class_name", "is_subcommittee", "committee_raw"]], on="conf_num", how="left")
    e["label_fused"] = x["label_fused"].to_numpy()
    e["snippet"] = x["snippet"].to_numpy()
    return e, len(miss)


def _xlsx_pipeline_eval():
    """The production path for 18대 XLSX turns: build_turns output (interim/pipeline/turns/xlsx) with
    meetings.parquet through roles.enrich, joined to v9 on (v9_meeting_id, source_speech_order).
    No text is loaded. Returns (summary dict, frame of disagreement counts)."""
    import pandas as pd
    import roles
    P = os.path.join(V10, "interim", "pipeline")
    tglob = os.path.join(P, "turns", "xlsx", "*", "*.parquet")
    if not glob.glob(tglob):
        return None, None
    con = _duck()
    t = con.execute(f"""SELECT conf_num, turn_seq, source, speaker_label_raw, speaker_pos, speaker_name, speaker_mem_id,
        source_member_id, source_speech_order FROM read_parquet('{tglob}')""").df()
    m = pd.read_parquet(os.path.join(P, "meetings", "meetings.parquet"),
                        columns=["conf_num", "v9_meeting_id", "term", "class_name", "hearing_type", "is_subcommittee",
                                 "committee_raw", "subcommittee"])
    e = roles.enrich(t, m)
    stats = {k: v for k, v in e.attrs.get("roles_enrich", {}).items() if k != "load_status"}
    # out-of-lookup estimate on the same rows: the reconstructed chain only
    e["role_v9_chain_only"] = roles.enrich(t[["conf_num", "turn_seq", "source", "speaker_label_raw", "speaker_pos",
                                             "speaker_name", "speaker_mem_id", "source_member_id"]], m,
                                           v9_lookup={})["role_v9_compat"].to_numpy()
    e = e.merge(m[["conf_num", "v9_meeting_id"]], on="conf_num", how="left", validate="many_to_one")
    v9 = con.execute(f"""SELECT meeting_id, CAST(speech_order AS VARCHAR) AS source_speech_order, role AS v9_role
        FROM '{os.path.join(REPO, "data", "all_speeches_16_22_v9.parquet")}'
        WHERE hearing_type IN ('상임위원회','국정감사') AND term = 18""").df()
    con.close()
    j = e.merge(v9, left_on=["v9_meeting_id", "source_speech_order"], right_on=["meeting_id", "source_speech_order"],
                how="left", validate="one_to_one", indicator=True)
    both = j[j["_merge"] == "both"]
    summ = {"turns": int(len(e)), "joined_to_v9": int(len(both)), "not_joined": int((j["_merge"] != "both").sum()),
            "role_eq_v9": int((both["role"] == both["v9_role"]).sum()),
            "role_v9_compat_eq_v9": int((both["role_v9_compat"] == both["v9_role"]).sum()),
            "role_v9_chain_only_eq_v9": int((both["role_v9_chain_only"] == both["v9_role"]).sum()),
            "role_v9_compat_src": both["role_v9_compat_src"].value_counts().to_dict(),
            "enrich_stats": stats}
    summ["role_v9_chain_only_eq_v9_share"] = round(summ["role_v9_chain_only_eq_v9"] / max(len(both), 1), 6)
    summ["role_eq_v9_share"] = round(summ["role_eq_v9"] / max(len(both), 1), 6)
    summ["role_v9_compat_eq_v9_share"] = round(summ["role_v9_compat_eq_v9"] / max(len(both), 1), 6)
    d = (both[both["role_v9_compat"] != both["v9_role"]].groupby(["v9_role", "role_v9_compat", "speaker_label_raw"])
         .size().rename("n").reset_index().sort_values("n", ascending=False))
    return summ, d


def evaluate():
    import pandas as pd
    import roles
    rep = {}
    # ---------------- XLSX production path (build_turns -> enrich), v9 compat check
    summ, d = _xlsx_pipeline_eval()
    if summ is not None:
        rep["xlsx_pipeline"] = summ
        d.to_csv(os.path.join(OUT, "xlsx_pipeline_v9compat_mismatches.csv"), index=False)
        print("xlsx_pipeline", json.dumps(summ, ensure_ascii=False, default=str), flush=True)
    # ---------------- XLSX
    for with_mid in (False, True):
        f = _xlsx_eval_frame(with_mid)
        tag = "with_mid" if with_mid else "no_mid"
        f.to_parquet(os.path.join(OUT, f"eval_xlsx_{tag}.parquet"), index=False)
        n = f["n"].sum()
        agree = f.loc[f["role_v10"] == f["role"], "n"].sum()
        f["_agree_n"] = f["n"].where(f["role_v10"] == f["role"], 0)
        byrole = f.groupby("role").agg(n=("n", "sum"), agree=("_agree_n", "sum"))
        f = f.drop(columns="_agree_n")
        byrole["share"] = byrole["agree"] / byrole["n"]
        cw = f.pivot_table(index="role_v10", columns="role", values="n", aggfunc="sum", fill_value=0)
        cw.to_csv(os.path.join(OUT, f"crosswalk_xlsx_v10_x_v9_{tag}.csv"))
        v9c = f.loc[f["role_v9_compat"] == f["role"], "n"].sum()
        v9ch = f.loc[f["role_v9_chain_only"] == f["role"], "n"].sum()
        # affiliation_raw vs v9 (spaces removed on both sides; v9 '' and null both count as null)
        a9 = f["affiliation_raw"].fillna("").astype(str).str.replace(r"\s+", "", regex=True)
        a10 = f["affiliation_v10"].fillna("").astype(str).str.replace(r"\s+", "", regex=True)
        f["_aff_eq"] = f["n"].where(a9 == a10, 0)
        aff_by_role = f.groupby("role").agg(n=("n", "sum"), aff_equal=("_aff_eq", "sum"))
        aff_by_role["share"] = (aff_by_role["aff_equal"] / aff_by_role["n"]).round(6)
        aff_by_role.to_csv(os.path.join(OUT, f"affiliation_vs_v9_by_role_{tag}.csv"))
        f = f.drop(columns="_aff_eq")
        dis = (f[f["role_v10"] != f["role"]]
               .groupby(["role", "role_v10", "role_rule"])
               .apply(lambda d: pd.Series({"n": int(d["n"].sum()), "n_pos": d["pos_hangul"].nunique(),
                                           "examples": " | ".join(d.groupby("pos_hangul")["n"].sum()
                                                                  .sort_values(ascending=False).head(6).index)}),
                      include_groups=False)
               .reset_index().sort_values("n", ascending=False))
        dis.to_csv(os.path.join(OUT, f"disagreements_xlsx_{tag}.csv"), index=False)
        rep[tag] = {"rows": int(n), "agree_rows": int(agree), "agree_share": round(agree / n, 6),
                    "v9_compat_reproduces_v9_rows": int(v9c), "v9_compat_share": round(v9c / n, 6),
                    "v9_compat_note": "in-sample: the lookup is the v9 XLSX speaker table itself",
                    "v9_chain_only_reproduces_v9_rows": int(v9ch), "v9_chain_only_share": round(v9ch / n, 6),
                    "affiliation_equal_v9_rows": int(aff_by_role["aff_equal"].sum()),
                    "affiliation_equal_v9_share": round(float(aff_by_role["aff_equal"].sum()) / n, 6),
                    "n_disagreement_classes": int(len(dis)),
                    "by_v9_role": byrole.round(6).astype(object).to_dict(orient="index")}
        byrole.to_csv(os.path.join(OUT, f"agreement_by_role_xlsx_{tag}.csv"))
    # ---------------- XML
    e, n_miss = _xml_eval_frame()
    e.drop(columns=["snippet"]).to_parquet(os.path.join(OUT, "eval_xml_turns.parquet"), index=False)
    rep["xml"] = {"turns": int(len(e)), "pages": int(e["conf_num"].nunique()), "pages_not_in_universe": n_miss,
                  "role_counts": e["role"].value_counts().to_dict(),
                  "role_group_counts": e["role_group"].value_counts().to_dict(),
                  "pos_fix_counts": e["pos_fix"].value_counts().to_dict(),
                  "v9compat_src": e["role_v9_compat_src"].value_counts().to_dict(),
                  "agree_with_v9compat": round(float((e["role"] == e["role_v9_compat"]).mean()), 6)}
    cwx = pd.crosstab(e["role"], e["role_v9_compat"])
    cwx.to_csv(os.path.join(OUT, "crosswalk_xml_v10_x_v9compat.csv"))
    rules = e.groupby(["role", "role_rule"]).size().rename("n").reset_index().sort_values("n", ascending=False)
    rules.to_csv(os.path.join(OUT, "rule_counts_xml.csv"), index=False)
    # ---------------- HWP (18대 files crawled so far)
    if os.path.exists(os.path.join(OUT, "hwp_turn_speakers.parquet")):
        h, n_miss_h = _hwp_eval_frame()
        h.to_parquet(os.path.join(OUT, "eval_hwp_turns.parquet"), index=False)
        rep["hwp"] = {"turns": int(len(h)), "files": int(h["conf_num"].nunique()), "files_not_in_universe": n_miss_h,
                      "role_counts": h["role"].value_counts().to_dict(),
                      "role_group_counts": h["role_group"].value_counts().to_dict(),
                      "pos_fix_counts": h["pos_fix"].value_counts().to_dict(),
                      "class_counts": h["class_name"].value_counts(dropna=False).to_dict(),
                      "agree_with_v9compat": round(float((h["role"] == h["role_v9_compat"]).mean()), 6)}
        pd.crosstab(h["role"], h["role_v9_compat"]).to_csv(os.path.join(OUT, "crosswalk_hwp_v10_x_v9compat.csv"))
        (h.groupby(["role", "role_rule"]).size().rename("n").reset_index().sort_values("n", ascending=False)
          .to_csv(os.path.join(OUT, "rule_counts_hwp.csv"), index=False))
    else:
        h = None
    # ---------------- position inventory (distinct printed positions, all sources)
    fx = pd.read_parquet(os.path.join(OUT, "eval_xlsx_no_mid.parquet"))
    a = (fx.groupby(["pos_printed", "pos_hangul", "role_v10", "role"], dropna=False)["n"].sum().reset_index()
           .rename(columns={"pos_printed": "pos", "role_v10": "role_v10", "role": "role_v9"}))
    a["source"] = "xlsx_v9"
    b = (e.groupby(["speaker_pos", "pos_hangul", "role", "role_v9_compat"], dropna=False).size().rename("n").reset_index()
           .rename(columns={"speaker_pos": "pos", "role": "role_v10", "role_v9_compat": "role_v9"}))
    b["source"] = "xml"
    parts = [a, b]
    if h is not None:
        c = (h.groupby(["speaker_pos", "pos_hangul", "role", "role_v9_compat"], dropna=False).size().rename("n")
               .reset_index().rename(columns={"speaker_pos": "pos", "role": "role_v10", "role_v9_compat": "role_v9"}))
        c["source"] = "hwp"
        parts.append(c)
    inv = pd.concat(parts, ignore_index=True)
    inv.to_parquet(os.path.join(OUT, "pos_inventory.parquet"), index=False)
    rep["inventory"] = {"xlsx_distinct_pos": int(a["pos"].nunique()), "xml_distinct_pos": int(b["pos"].nunique()),
                        "union_distinct_pos_hangul": int(inv["pos_hangul"].nunique())}
    rep["load_status"] = roles.load_status()
    with open(os.path.join(OUT, "eval_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(rep, fh, ensure_ascii=False, indent=1, default=str)
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "by_v9_role"} for k, v in rep.items()},
                     ensure_ascii=False, indent=1, default=str))


# --------------------------------------------------------------------------- audit sample

def audit_sample(n_total=300, seed=8374, min_per_role=4):
    """Stratified random sample of distinct Hangul position strings (pos_hangul) from the
    union inventory. Strata = v10 role (majority role of the string, frequency-weighted).
    Allocation: min(min_per_role, stratum size) per role, the rest proportional to the
    square root of the number of distinct strings in the stratum (largest remainder).
    Strings are drawn without replacement with numpy default_rng(seed)."""
    import numpy as np
    import pandas as pd
    inv = pd.read_parquet(os.path.join(OUT, "pos_inventory.parquet"))
    inv = inv[inv["pos_hangul"].notna() & (inv["pos_hangul"] != "")]
    g = inv.groupby(["pos_hangul", "role_v10"])["n"].sum().reset_index()
    g = g.sort_values(["pos_hangul", "n", "role_v10"], ascending=[True, False, True])
    tot = inv.groupby("pos_hangul")["n"].sum().rename("n_total")
    nroles = inv.groupby("pos_hangul")["role_v10"].nunique().rename("n_roles_v10")
    src = inv.groupby("pos_hangul")["source"].agg(lambda s: "+".join(sorted(set(s)))).rename("sources")
    v9 = (inv.groupby(["pos_hangul", "role_v9"])["n"].sum().reset_index()
             .sort_values(["pos_hangul", "n", "role_v9"], ascending=[True, False, True])
             .drop_duplicates("pos_hangul").set_index("pos_hangul")["role_v9"].rename("role_v9_major"))
    d = g.drop_duplicates("pos_hangul").set_index("pos_hangul")[["role_v10"]]
    d = d.join(tot).join(nroles).join(src).join(v9).reset_index()
    sizes = d.groupby("role_v10").size()
    base = np.minimum(sizes, min_per_role)
    rest = n_total - int(base.sum())
    w = np.sqrt(sizes - base).astype(float)
    quota = (w / w.sum() * rest) if w.sum() > 0 else w * 0
    alloc = base + np.floor(quota).astype(int)
    rem = (quota - np.floor(quota)).sort_values(ascending=False)
    for r in rem.index[: n_total - int(alloc.sum())]:
        alloc[r] += 1
    alloc = np.minimum(alloc, sizes)
    rng = np.random.default_rng(seed)
    picks = []
    for role in sorted(alloc.index):
        pool = d[d["role_v10"] == role].sort_values("pos_hangul")
        k = int(alloc[role])
        idx = rng.choice(len(pool), size=k, replace=False)
        picks.append(pool.iloc[np.sort(idx)])
    smp = pd.concat(picks, ignore_index=True)
    # an example printed form and rule for each sampled string
    ex = (inv.sort_values("n", ascending=False).drop_duplicates("pos_hangul").set_index("pos_hangul")["pos"])
    smp["pos_example"] = smp["pos_hangul"].map(ex)
    smp["stratum_size"] = smp["role_v10"].map(sizes)
    smp.to_csv(os.path.join(OUT, "audit_sample_300.csv"), index=False)
    print(json.dumps({"n": len(smp), "strata": alloc.to_dict(), "universe_strings": int(len(d))}, ensure_ascii=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["xml-inventory", "hwp-inventory", "v9-groups", "evaluate", "audit-sample"])
    a = ap.parse_args()
    if a.cmd == "xml-inventory":
        xml_inventory()
    elif a.cmd == "hwp-inventory":
        hwp_inventory()
    elif a.cmd == "v9-groups":
        v9_groups()
    elif a.cmd == "evaluate":
        evaluate()
    else:
        audit_sample()
