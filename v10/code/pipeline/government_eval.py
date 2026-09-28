"""Evaluation driver for government.py (component 'government').

Subcommands
-----------
panel-audit   Audit minister_panel_comprehensive.csv; writes panel_audit.csv.
v9            v9 XLSX-era (상임위원회, 국정감사) rows, aggregated with duckdb (no text).
              A: rows with v9 roles minister / minister_acting / minister_nominee / prime_minister:
                 v10 linkage and admin versus the stored v9 values and versus a recomputation of
                 the v9 cascade (legacy_rules.link_minister_panel_v9); window sensitivity;
                 ministry coverage for every v9 non-legislator role.
              B: the same rows re-roled with roles.py the way production XLSX turns are
                 (build_turns.split_xlsx_label, no mem_id): coverage and link rates by v10 role.
              C: transcript evidence on panel dates (panel_date_evidence).
xml, hwp      Locally available view pages (raw/samples, raw/samples_random, crawled pages with
              fetch.status='ok') or crawled HWP files, parsed with the build_turns adapters,
              roled with roles.enrich: ministry coverage by role, link methods, Hanja name
              resolution, admin by presidency_state.
evidence      Pools the transcript observations of all runs into panel_date_evidence.csv and
              ministers_not_in_panel.csv.
v2            Panel v2 (minister-data v2.0.0 snapshot) versus legacy_296 over the release turns
              (build/release/turns/tNN, by term, in meeting chunks, duckdb memory_limit 8GB): runs
              government.enrich in both panels, checks that legacy_296 reproduces the stored release columns,
              link rates by term x role, unlinked residual classes, and a comparison with the read-only
              prototype verify_minister_v2.py (stored rc2 check, and rerun on rc3 in-process).
              Outputs go to v10/interim/pipeline/government_v2/.

Outputs go to v10/interim/pipeline/government/ (v2: government_v2/). No network access.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import re
import sqlite3
import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
V10 = os.path.abspath(os.path.join(HERE, "..", ".."))
REPO = os.path.abspath(os.path.join(V10, ".."))
OUT = os.path.join(V10, "interim", "pipeline", "government")
sys.path.insert(0, os.path.join(V10, "code"))
sys.path.insert(0, HERE)

import government as G  # noqa: E402
import legacy_rules as L  # noqa: E402

V9 = os.path.join(REPO, "data", "all_speeches_16_22_v9.parquet")

# Source files whose content determines each output. The parse cache is keyed by PARSE_FILES;
# every eval JSON records the sha1 of CODE_FILES so a number can be traced to the code version.
PARSE_FILES = {"view": ["build_turns.py", "../parse_viewer.py", "../legacy_rules.py"],
               "hwp": ["build_turns.py", "hwp_parser.py", "../legacy_rules.py"]}
CODE_FILES = ["government.py", "government_eval.py", "roles.py", "build_turns.py", "hwp_parser.py",
              "../parse_viewer.py", "../legacy_rules.py"]


def _sha(rel):
    import hashlib
    path = os.path.normpath(os.path.join(HERE, rel))
    if not os.path.exists(path):
        return "missing"
    with open(path, "rb") as fh:
        return hashlib.sha1(fh.read()).hexdigest()[:12]


def parse_fingerprint(kind):
    return ";".join(f"{os.path.basename(f)}={_sha(f)}" for f in PARSE_FILES[kind])


def code_fingerprint():
    return {"files": {os.path.basename(f): _sha(f) for f in CODE_FILES},
            "run_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


def _duck():
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    return con


def _w(df, col, weight="n"):
    return df.groupby(col, dropna=False)[weight].sum().sort_values(ascending=False)


# --------------------------------------------------------------------------- panel audit
def panel_audit():
    os.makedirs(OUT, exist_ok=True)
    p = G.load_panel()
    a = G.audit_panel(p)
    a.to_csv(os.path.join(OUT, "panel_audit.csv"), index=False)
    res = {
        "n_rows": int(len(p)),
        "n_names": int(p["name"].nunique()),
        "n_ministries": int(p["ministry"].nunique()),
        "issues": a["issue"].value_counts().to_dict(),
        "valid_dates": int(p["valid_dates"].sum()),
        "end_imputed": p.loc[p["end_imputed"], ["name", "ministry", "start", "end_eff"]].astype(str).values.tolist(),
        "invalid": p.loc[~p["valid_dates"], ["name", "ministry", "start", "end", "issues"]].astype(str).values.tolist(),
        "prime_minister_rows": int((p["ministry"] == "국무총리").sum()),
        "notes_date_approx": int(p["notes"].fillna("").str.contains("approx").sum()),
        "end_equals_admin_boundary": {d: int((p["end"] == d).sum()) for d in
                                      ["2008-02-24", "2013-02-24", "2017-03-10", "2022-05-09", "2025-06-03"]},
    }
    print(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(a[~a["issue"].isin(["name_in_several_rows"])].to_string())
    return res


# --------------------------------------------------------------------------- v9
LEG = ("legislator", "chair")


def _v10_role_keys(con):
    """v10 role for every v9 XLSX-era (speaker, term, hearing_type, committee) key, computed
    the way the production path does it: build_turns.split_xlsx_label -> roles.classify with no
    mem_id (XLSX turns carry speaker_mem_id = null)."""
    import roles
    import build_turns as bt
    keys = con.execute(f"""select distinct speaker, term, hearing_type, committee from '{V9}'
        where hearing_type in ('상임위원회', '국정감사')""").fetchdf()
    res = []
    for sp, term, ht, com in keys.itertuples(index=False):
        pos, name, how = bt.split_xlsx_label(sp)
        r = roles.classify(pos, name, label_raw=sp, mem_id=None, term=int(term), class_name=ht,
                           hearing_type=ht, is_subcommittee=False, committee_raw=com, subcommittee=None)
        res.append((pos, name, how, r.role, r.role_rule))
    keys["speaker_pos"] = [r[0] for r in res]
    keys["speaker_name"] = [r[1] for r in res]
    keys["split_how"] = [r[2] for r in res]
    keys["role_v10"] = [r[3] for r in res]
    keys["role_rule_v10"] = [r[4] for r in res]
    return keys


def _v9_groups(con, keys):
    """v9 XLSX-era rows grouped (no text) for every key that is non-legislator under v9 OR v10."""
    k = keys[~(keys["role_v10"].isin(LEG))][["speaker", "term", "hearing_type", "committee"]].copy()
    con.register("k10", k)
    q = f"""
      select v.hearing_type, v.term, v.committee, v.date, v.speaker, v.person_name, v.role,
             v.ministry_normalized, v.dual_office, v."admin", v.admin_ideology, count(*)::BIGINT n
      from '{V9}' v
      left join k10 on v.speaker is not distinct from k10.speaker and v.term = k10.term
                   and v.hearing_type = k10.hearing_type and v.committee is not distinct from k10.committee
      where v.hearing_type in ('상임위원회', '국정감사')
        and (v.role not in ('legislator', 'chair') or k10.speaker is not null)
      group by all"""
    g = con.execute(q).fetchdf()
    g = g.merge(keys, on=["speaker", "term", "hearing_type", "committee"], how="left", validate="many_to_one")
    return g


def _legacy_candidates(panel):
    """panel rows by name in file order, in the dict shape legacy_rules expects (build_v9.py:320-364)."""
    by = {}
    for r in panel.itertuples(index=False):
        sd = pd.Timestamp(r.start) if isinstance(r.start, str) else None
        ed = pd.Timestamp(r.end) if isinstance(r.end, str) else pd.Timestamp("2099-12-31")
        by.setdefault(r.name, []).append({
            "ministry": r.ministry, "start_dt": sd, "end_dt": ed, "dual_office": bool(r.dual_office),
            "admin": r.admin, "admin_ideology": r.admin_ideology, "panel_row": r.panel_row,
            "minister_panel_id": r.minister_panel_id})
    return by


def _v9_link(row, by):
    if row.role not in L.PANEL_LINK_ROLES:     # v9 linked and dated only these three roles
        return None, "v9_role_not_linked", None, None, None
    d = pd.Timestamp(row.date) if isinstance(row.date, str) and row.date else None
    mn = row.ministry_normalized_v9 if isinstance(row.ministry_normalized_v9, str) else None
    c, rule = L.link_minister_panel_v9(row.person_name, mn, d, by.get(row.person_name, []))
    if c is None:
        adm, ide = L.infer_admin_from_date(row.date, L.ADMIN_TERMS_V9)
        return None, rule, None, adm, ide
    return c["minister_panel_id"], rule, c["dual_office"], c["admin"], c["admin_ideology"]


def _admin_change_reason(r):
    """Why v10 admin / ideology differ from v9 for one aggregated row."""
    v9a, v10a = r.admin_v9, r.admin
    v9i, v10i = r.admin_ideology_v9, r.admin_ideology
    if (v9a == v10a) or (pd.isna(v9a) and pd.isna(v10a)):
        if v9i == v10i or (pd.isna(v9i) and pd.isna(v10i)):
            return "same"
        if v10a == "김대중":
            return "ideology_only:김대중_Conservative_bug"
        return "ideology_only:other"
    rule = r.rule_v9_recomputed
    date_admin_v9, _ = L.infer_admin_from_date(r.date, L.ADMIN_TERMS_V9)
    if pd.isna(v9a):
        if rule == "v9_role_not_linked":
            return f"v9_no_admin_for_role:{r.role}"
        if date_admin_v9 is None:
            return f"v9_calendar_gap:{r.presidency_state}"
        return f"v9_null_admin_other:{r.presidency_state}"
    if isinstance(rule, str) and rule.startswith("fallback_2"):
        tag = "fallback_2_name_ministry_anydate"
    elif isinstance(rule, str) and rule.startswith("fallback_3"):
        tag = "fallback_3_single_entry"
    elif isinstance(rule, str) and rule.startswith("fallback_1"):
        tag = "fallback_1_name_date"
    elif rule == "exact_name_ministry_date":
        tag = "exact_panel_admin"
    else:
        tag = "date_inferred"
    if date_admin_v9 is None:
        return f"v9_panel_admin_in_v9_calendar_gap:{tag}"
    if v9a != date_admin_v9:
        return f"v9_panel_admin_outside_date:{tag}"
    if r.presidency_state == "acting":          # no 'vacant' state (R2: every removal has an acting president)
        return f"calendar_acting_window:{tag}"
    if r.presidency_state == "suspended":
        return f"calendar_suspended_window:{tag}"
    return f"calendar_boundary:{tag}"


def _by_role(df, flag, weight="n"):
    return df.groupby("role").apply(lambda x: pd.Series({
        "rows": int(x[weight].sum()), "linked": int(x.loc[x[flag], weight].sum()),
        "rate": round(float(x.loc[x[flag], weight].sum() / max(1, x[weight].sum())), 4)}),
        include_groups=False).to_dict(orient="index")


def panel_date_evidence(obs: pd.DataFrame, idx=None) -> tuple:
    """Transcript evidence on panel dates. `obs`: rows of role 'minister' or 'prime_minister'
    (sitting office holders; not acting / nominee, and not a bare '국무총리직무대행', see
    _evidence_obs) with speaker_name, ministry_normalized, speech_date, n, source (role optional).
    Each observed (name, ministry, date) is assigned to the compatible valid panel row of that
    name (exact ministry or same cabinet lineage) at the smallest distance from its
    [start, end_eff] window (0 inside). Returns (per_panel_row, not_in_panel) frames.
    Nothing here changes a link; it documents where panel dates disagree with the minutes."""
    idx = G._default_index() if idx is None else idx
    rows, missing = [], []
    for r in obs.itertuples(index=False):
        nm, _ = idx.resolve_name(r.speaker_name)
        d = G._to_date(r.speech_date)
        if nm is None or d is None or r.ministry_normalized is None or pd.isna(r.ministry_normalized):
            continue
        lin = set(G.ministry_lineage(r.ministry_normalized))
        cands = [c for c in idx.by_name.get(nm, []) if c.valid_dates and
                 (c.ministry == r.ministry_normalized or lin & set(G.ministry_lineage(c.ministry)))]
        if not cands:
            missing.append((nm, r.ministry_normalized, d, r.n, r.source,
                            "name_not_in_panel" if nm not in idx.by_name else "no_compatible_valid_row"))
            continue
        best = None
        for c in cands:
            k = 0 if c.start_d <= d <= c.end_eff else ((c.start_d - d).days if d < c.start_d else -(d - c.end_eff).days)
            if best is None or abs(k) < abs(best[0]):
                best = (k, c)
        rows.append((best[1].panel_row, d, best[0], r.n, r.source))
    a = pd.DataFrame(rows, columns=["panel_row", "date", "days", "n", "source"])
    panel = idx.panel
    out = []
    for pr, g in a.groupby("panel_row"):
        p = panel.loc[panel["panel_row"] == pr].iloc[0]
        before = g[g["days"] > G.BUFFER_DAYS]
        after = g[g["days"] < -G.BUFFER_DAYS]
        out.append({
            "panel_row": int(pr), "minister_panel_id": p["minister_panel_id"], "name": p["name"],
            "ministry": p["ministry"], "start": p["start"], "end": p["end"], "end_eff": p["end_eff"],
            "notes_date_approx": "approx" in str(p["notes"]),
            "obs_first": g["date"].min(), "obs_last": g["date"].max(), "obs_dates": int(g["date"].nunique()),
            "obs_rows": int(g["n"].sum()),
            "rows_before_start_beyond_buffer": int(before["n"].sum()),
            "dates_before_start_beyond_buffer": int(before["date"].nunique()),
            "max_days_before_start": int(before["days"].max()) if len(before) else 0,
            "rows_after_end_beyond_buffer": int(after["n"].sum()),
            "dates_after_end_beyond_buffer": int(after["date"].nunique()),
            "max_days_after_end": int(-after["days"].min()) if len(after) else 0,
            "sources": ",".join(sorted(g["source"].unique())),
        })
    per = pd.DataFrame(out).sort_values("panel_row") if out else pd.DataFrame()
    m = pd.DataFrame(missing, columns=["name", "ministry", "date", "n", "source", "reason"])
    if len(m):
        m = (m.groupby(["name", "ministry", "reason"])
             .agg(first=("date", "min"), last=("date", "max"), dates=("date", "nunique"), rows=("n", "sum"),
                  sources=("source", lambda s: ",".join(sorted(set(s)))))
             .reset_index().sort_values("rows", ascending=False))
        m["admin_first"] = m["first"].map(lambda d: G.admin_for_date(d)[0])
        m["admin_last"] = m["last"].map(lambda d: G.admin_for_date(d)[0])
    return per, m


EVIDENCE_ROLES = ("minister", "prime_minister")


def _evidence_obs(df, source, weight=None):
    """Panel-date observations: role minister or prime_minister, excluding prime_minister turns
    whose title is a bare '국무총리직무대행' (someone standing in for the PM; linked as acting)."""
    keep = df["role"].isin(EVIDENCE_ROLES)
    bare = (df["role"] == "prime_minister") & df["speaker_pos"].map(G.is_bare_acting_pm).astype(bool)
    o = df.loc[keep & ~bare, ["speaker_name", "ministry_normalized", "speech_date", "role"]
               + ([weight] if weight else [])].copy()
    if not weight:
        o["n"] = 1
    return o.assign(source=source), int((keep & bare).sum() if not weight else df.loc[keep & bare, weight].sum())


def v9_eval(write=True):
    os.makedirs(OUT, exist_ok=True)
    con = _duck()
    keys = _v10_role_keys(con)
    grp = _v9_groups(con, keys)
    res = {"code_fingerprint": code_fingerprint(),
           "v9_xlsx_rows_nonleg_v9_or_v10": int(grp["n"].sum()), "v9_groups": int(len(grp)),
           "v9_keys": int(len(keys)), "split_how_keys": keys["split_how"].value_counts().to_dict()}
    grp["speech_date"] = grp["date"]
    t = grp.rename(columns={"dual_office": "dual_office_v9", "admin": "admin_v9",
                            "admin_ideology": "admin_ideology_v9",
                            "ministry_normalized": "ministry_normalized_v9"})

    # ---- A. v9 roles: comparison with the stored v9 values
    a = t[~t["role"].isin(LEG)].copy()
    out = G.enrich(a, None)
    res["A_enrich_diag_v9_roles"] = out.attrs["government"]
    cov = out.assign(has=out["ministry_normalized"].notna(), v9has=out["ministry_normalized_v9"].notna(),
                     agree=(out["ministry_normalized"] == out["ministry_normalized_v9"]).fillna(False))
    res["A_ministry_coverage_by_v9_role"] = cov.groupby("role").apply(lambda x: pd.Series({
        "rows": int(x["n"].sum()),
        "v10_coverage": round(float(x.loc[x["has"], "n"].sum() / x["n"].sum()), 4),
        "v9_coverage": round(float(x.loc[x["v9has"], "n"].sum() / x["n"].sum()), 4),
        "agree_with_v9_among_both": round(float(x.loc[x["has"] & x["v9has"] & x["agree"], "n"].sum()
                                                / max(1, x.loc[x["has"] & x["v9has"], "n"].sum())), 4),
    }), include_groups=False).sort_values("rows", ascending=False).to_dict(orient="index")
    res["A_ministry_rule_rows"] = _w(out.assign(rule0=out["ministry_rule"].str.replace(r"^.*\+", "", regex=True)),
                                     "rule0").to_dict()
    gov = out[out["role"].isin(L.GOVT_ROLES)]
    dis = gov[gov["ministry_normalized"].notna() & gov["ministry_normalized_v9"].notna()
              & (gov["ministry_normalized"] != gov["ministry_normalized_v9"])]
    res["A_govt_roles_ministry_differs_from_v9_rows"] = int(dis["n"].sum())
    res["A_govt_roles_ministry_differs_top"] = (
        dis.groupby(["ministry_normalized_v9", "ministry_normalized"])["n"].sum()
        .sort_values(ascending=False).head(30).reset_index().values.tolist())

    lk = out[out["role"].isin(G.LINK_ROLES)].copy()
    panel = G.load_panel()
    by = _legacy_candidates(panel)
    rec = [_v9_link(r, by) for r in lk.itertuples(index=False)]
    lk["panel_id_v9_recomputed"] = [r[0] for r in rec]
    lk["rule_v9_recomputed"] = [r[1] for r in rec]
    lk["dual_v9_recomputed"] = [r[2] for r in rec]
    lk["admin_v9_recomputed"] = [r[3] for r in rec]
    lk["ideology_v9_recomputed"] = [r[4] for r in rec]
    same_admin = (lk["admin_v9_recomputed"].fillna("<NA>") == lk["admin_v9"].fillna("<NA>"))
    same_dual = (lk["dual_v9_recomputed"].astype("object").where(lk["dual_v9_recomputed"].notna(), "<NA>").astype(str)
                 == lk["dual_office_v9"].astype("object").where(lk["dual_office_v9"].notna(), "<NA>").astype(str))
    res["A_v9_recompute_agreement"] = {"rows": int(lk["n"].sum()),
                                       "admin_equal_rows": int(lk.loc[same_admin, "n"].sum()),
                                       "dual_office_equal_rows": int(lk.loc[same_dual, "n"].sum())}
    res["A_rows_by_v9_role"] = _w(lk, "role").to_dict()
    res["A_v9_rule_rows"] = _w(lk, "rule_v9_recomputed").to_dict()
    res["A_v10_link_method_rows"] = _w(lk, "link_method").to_dict()
    lk["linked_v9"] = lk["panel_id_v9_recomputed"].notna()
    lk["linked_v10"] = lk["minister_panel_id"].notna()
    res["A_link_rate_by_v9_role"] = lk.groupby("role").apply(lambda x: pd.Series({
        "rows": int(x["n"].sum()),
        "v9_linked": int(x.loc[x["linked_v9"], "n"].sum()),
        "v10_linked": int(x.loc[x["linked_v10"], "n"].sum()),
        "v9_rate": round(float(x.loc[x["linked_v9"], "n"].sum() / x["n"].sum()), 4),
        "v10_rate": round(float(x.loc[x["linked_v10"], "n"].sum() / x["n"].sum()), 4),
    }), include_groups=False).to_dict(orient="index")

    def _cmp(r):
        if not r.linked_v9 and not r.linked_v10:
            return "both_unlinked"
        if r.linked_v9 and not r.linked_v10:
            return "v9_only"
        if r.linked_v10 and not r.linked_v9:
            return "v10_only"
        return "same_row" if r.panel_id_v9_recomputed == r.minister_panel_id else "different_row"
    lk["link_cmp"] = [_cmp(r) for r in lk.itertuples(index=False)]
    res["A_v9_rule_x_comparison"] = lk.groupby(["rule_v9_recomputed", "link_cmp"])["n"].sum().unstack(fill_value=0).to_dict(orient="index")
    res["A_v9_rule_x_v10_method"] = (lk.groupby(["rule_v9_recomputed", "link_method"], dropna=False)["n"].sum()
                                     .reset_index().sort_values("n", ascending=False).values.tolist())
    fb = lk[lk["rule_v9_recomputed"].isin(["fallback_2_name_ministry_anydate", "fallback_3_single_entry",
                                          "fallback_1_name_date"])]
    res["A_v9_fallback_rows_by_role"] = fb.groupby(["rule_v9_recomputed", "role"])["n"].sum().reset_index().values.tolist()
    res["A_v9_fallback_x_v10_outcome"] = (fb.groupby(["rule_v9_recomputed", "link_cmp"])["n"].sum()
                                          .reset_index().values.tolist())
    res["A_v9_only_top"] = (lk[lk["link_cmp"] == "v9_only"]
                            .groupby(["role", "speaker_pos", "speaker_name", "rule_v9_recomputed", "link_method"])["n"]
                            .sum().sort_values(ascending=False).head(40).reset_index().values.tolist())
    res["A_v10_only_top"] = (lk[lk["link_cmp"] == "v10_only"]
                             .groupby(["role", "speaker_pos", "speaker_name", "rule_v9_recomputed", "link_method"])["n"]
                             .sum().sort_values(ascending=False).head(20).reset_index().values.tolist())
    res["A_different_row_dual_office"] = (lk[lk["link_cmp"] == "different_row"]
                                          .assign(ch=lambda x: x["dual_v9_recomputed"].astype(str) + "->" + x["dual_office"].astype(str))
                                          .groupby("ch")["n"].sum().to_dict())
    lk["reason"] = [_admin_change_reason(r) for r in lk.itertuples(index=False)]
    res["A_admin_change_reasons"] = _w(lk, "reason").to_dict()
    res["A_admin_changed_rows"] = int(lk.loc[lk["admin_v9"].fillna("<NA>") != lk["admin"].fillna("<NA>"), "n"].sum())
    res["A_ideology_changed_rows"] = int(lk.loc[lk["admin_ideology_v9"].fillna("<NA>") != lk["admin_ideology"].fillna("<NA>"), "n"].sum())
    res["A_admin_transition_top"] = (lk[lk["reason"] != "same"]
                                     .groupby(["admin_v9", "admin_ideology_v9", "admin", "admin_ideology", "reason"], dropna=False)["n"]
                                     .sum().sort_values(ascending=False).head(40).reset_index().values.tolist())
    res["A_presidency_state_rows"] = _w(lk, "presidency_state").to_dict()

    # window sensitivity (v9 roles)
    sens = []
    base = lk[["speaker_pos", "speaker_name", "speech_date", "role", "n"]]
    for b in (0, 3, 7, 14, 30, 60):
        o = G.enrich(base, None, panel_index=G.PanelIndex(panel, buffer_days=b))
        sens.append({"buffer_days": b, **{r: int(o.loc[(o["role"] == r) & o["minister_panel_id"].notna(), "n"].sum())
                                          for r in G.LINK_ROLES}})
    for nd in (14, 30, 60, 90, 180):
        for npost in (0, 30, 60, 90):
            o = G.enrich(base, None, panel_index=G.PanelIndex(panel, buffer_days=G.BUFFER_DAYS,
                                                               nominee_pre_days=nd, nominee_post_days=npost))
            nom = o[o["role"] == "minister_nominee"]
            sens.append({"nominee_pre_days": nd, "nominee_post_days": npost,
                         "minister_nominee_linked": int(nom.loc[nom["minister_panel_id"].notna(), "n"].sum()),
                         "of_which_nominee_in_tenure": int(nom.loc[nom["link_method"].fillna("").str.startswith("nominee_in_tenure"), "n"].sum())})
    res["A_window_sensitivity"] = sens

    # ---- B. v10 roles (production path for XLSX turns)
    b = t[~t["role_v10"].isin(LEG)].copy()
    b["role_v9"] = b["role"]
    b["role"] = b["role_v10"]
    outb = G.enrich(b, None)
    res["B_enrich_diag_v10_roles"] = outb.attrs["government"]
    res["B_ministry_coverage_by_v10_role"] = outb.assign(has=outb["ministry_normalized"].notna()).groupby("role").apply(
        lambda x: pd.Series({"rows": int(x["n"].sum()),
                             "coverage": round(float(x.loc[x["has"], "n"].sum() / x["n"].sum()), 4)}),
        include_groups=False).sort_values("rows", ascending=False).to_dict(orient="index")
    res["B_ministry_rule_rows"] = _w(outb, "ministry_rule").to_dict()
    lkb = outb[outb["role"].isin(G.LINK_ROLES)].copy()
    lkb["linked"] = lkb["minister_panel_id"].notna()
    res["B_link_rate_by_v10_role"] = _by_role(lkb, "linked")
    res["B_link_method_by_v10_role"] = (lkb.groupby(["role", "link_method"], dropna=False)["n"].sum()
                                        .reset_index().sort_values(["role", "n"], ascending=[True, False]).values.tolist())
    res["B_v9_role_x_v10_role_link_roles"] = (outb[outb["role"].isin(G.LINK_ROLES) | outb["role_v9"].isin(G.LINK_ROLES)]
                                             .groupby(["role_v9", "role"])["n"].sum().reset_index()
                                             .sort_values("n", ascending=False).values.tolist())
    res["B_admin_by_presidency_state"] = (outb.groupby(["presidency_state", "admin", "admin_ideology"], dropna=False)["n"]
                                          .sum().reset_index().values.tolist())
    susp = outb[outb["presidency_state"] == "suspended"]
    res["B_rows_in_suspended_windows_by_admin"] = _w(susp, "admin").to_dict()
    res["B_unlinked_top"] = (lkb[~lkb["linked"]].groupby(["role", "speaker_pos", "speaker_name", "link_method"])["n"]
                             .sum().sort_values(ascending=False).head(40).reset_index().values.tolist())

    # ---- C. transcript evidence on panel dates (v10 roles minister and prime_minister, XLSX era)
    obs, n_bare = _evidence_obs(outb, "xlsx", weight="n")
    res["C_obs_rows_by_role"] = obs.groupby("role")["n"].sum().astype(int).to_dict()
    res["C_prime_minister_bare_acting_rows_excluded"] = n_bare
    per, miss = panel_date_evidence(obs)
    res["C_panel_rows_observed"] = int(len(per))
    res["C_panel_rows_with_rows_before_start_beyond_buffer"] = int((per["rows_before_start_beyond_buffer"] > 0).sum())
    res["C_panel_rows_with_rows_after_end_beyond_buffer"] = int((per["rows_after_end_beyond_buffer"] > 0).sum())
    res["C_rows_before_start_beyond_buffer"] = int(per["rows_before_start_beyond_buffer"].sum())
    res["C_rows_after_end_beyond_buffer"] = int(per["rows_after_end_beyond_buffer"].sum())
    res["C_contradicted_rows_by_notes_date_approx"] = per.assign(
        c=(per["rows_before_start_beyond_buffer"] + per["rows_after_end_beyond_buffer"]) > 0).groupby(
        ["notes_date_approx", "c"]).size().reset_index().values.tolist()
    res["C_not_in_panel_rows"] = int(miss["rows"].sum()) if len(miss) else 0
    res["C_not_in_panel_by_admin_first"] = miss.groupby("admin_first")["rows"].sum().to_dict() if len(miss) else {}
    res["C_not_in_panel_by_reason"] = (miss.groupby("reason").agg(names=("name", "nunique"), rows=("rows", "sum"))
                                       .astype(int).to_dict(orient="index") if len(miss) else {})
    if write:
        keep = ["hearing_type", "term", "committee", "date", "speaker", "role", "speaker_pos", "speaker_name", "n",
                "ministry_normalized", "ministry_family", "ministry_rule", "ministry_normalized_v9",
                "minister_panel_id", "dual_office", "link_method", "gov_link_name", "admin", "admin_ideology",
                "presidency_state", "panel_id_v9_recomputed", "rule_v9_recomputed", "dual_office_v9",
                "admin_v9", "admin_ideology_v9", "link_cmp", "reason", "role_v10"]
        lk[keep].to_parquet(os.path.join(OUT, "v9_xlsx_link_roles_eval.parquet"), index=False)
        keepb = ["hearing_type", "term", "committee", "date", "speaker", "role_v9", "role", "speaker_pos", "speaker_name",
                 "n", "ministry_normalized", "ministry_family", "ministry_rule", "minister_panel_id", "dual_office",
                 "link_method", "gov_link_name", "admin", "admin_ideology", "presidency_state"]
        outb[keepb].to_parquet(os.path.join(OUT, "v9_xlsx_v10roles_government.parquet"), index=False)
        obs.to_parquet(os.path.join(OUT, "panel_evidence_obs_xlsx.parquet"), index=False)
        per.to_csv(os.path.join(OUT, "panel_date_evidence_xlsx.csv"), index=False)
        miss.to_csv(os.path.join(OUT, "ministers_not_in_panel_xlsx.csv"), index=False)
        with open(os.path.join(OUT, "v9_eval.json"), "w") as f:
            json.dump(res, f, ensure_ascii=False, indent=1, default=str)
    return res


# --------------------------------------------------------------------------- xml / hwp turns
def _raw_paths(kind):
    """{conf_num: path} for kind 'view' (crawled + discovery samples) or 'hwp' (crawled)."""
    paths = {}
    if kind == "view":
        pats = [
            (os.path.join(V10, "raw", "samples", "*_view.html.gz"), r"(\d+)_view\.html\.gz$"),
            (os.path.join(V10, "raw", "samples_random", "*_view.html.gz"), r"(\d+)_view\.html\.gz$"),
        ]
        for pat, rx in pats:
            for p in glob.glob(pat):
                m = re.search(rx, os.path.basename(p))
                if m:
                    paths.setdefault(int(m.group(1)), p)
    db = os.path.join(V10, "interim", "crawl_state.sqlite")
    ext = "html.gz" if kind == "view" else "hwp"
    sub = ("viewer", "view") if kind == "view" else ("hwp",)
    if os.path.exists(db):
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        ok = [r[0] for r in con.execute("select conf_num from fetch where kind=? and status='ok'", (kind,))]
        con.close()
        for c in ok:
            p = os.path.join(V10, "raw", *sub, f"{c // 1000:03d}", f"{c}.{ext}")
            if not os.path.exists(p):
                cands = glob.glob(os.path.join(V10, "raw", *sub, "*", f"{c}.{ext}"))
                p = cands[0] if cands else None
            if p:
                paths[c] = p
    return paths


TURN_COLS = ["conf_num", "turn_seq", "source", "speaker_label_raw", "speaker_pos", "speaker_name",
             "speaker_mem_id", "speech_date"]


def _extract_one(args):
    conf_num, path, kind, term = args
    import build_turns as bt
    with open(path, "rb") as fh:
        data = fh.read()
    try:
        r = bt.xml_extract(conf_num, data, term=term) if kind == "view" else bt.hwp_extract(conf_num, data, term=term)
    except Exception as e:  # noqa: BLE001
        return conf_num, "error:" + type(e).__name__, []
    rows = [tuple(t.get(c) for c in TURN_COLS) for t in r["tables"].get("turns", [])]
    return conf_num, str(r["status"]), rows


def _universe_meetings():
    import build_turns as bt
    u = pd.read_parquet(os.path.join(V10, "interim", "meeting_universe_api.parquet"),
                        columns=["CONFER_NUM", "DAE_NUM", "CLASS_NAME_unified", "COMM_NAME", "is_subcommittee_name",
                                 "CONF_DATE"])
    comm = u["COMM_NAME"].fillna("")
    m = pd.DataFrame({
        "conf_num": u["CONFER_NUM"].astype("int64"), "term": u["DAE_NUM"],
        "class_name": u["CLASS_NAME_unified"],
        "is_subcommittee": u["is_subcommittee_name"].fillna(False) | comm.str.contains("조정위원회"),
        "committee_raw": comm.str.split().str[0].where(comm != "", None),
        "subcommittee": comm.str.split().str[1], "date": u["CONF_DATE"]})
    m["hearing_type"] = [bt.hearing_type_for(c, n) for c, n in zip(m["class_name"], u["COMM_NAME"])]
    return m


def turns_eval(kind="view", write=True, workers=4, limit=None):
    """Parse every locally available view page (kind='view') or HWP file (kind='hwp') with the
    build_turns adapters, add roles (roles.enrich) and government columns, and summarise."""
    import roles
    os.makedirs(OUT, exist_ok=True)
    tag = "xml" if kind == "view" else "hwp"
    meetings = _universe_meetings()
    terms = dict(zip(meetings["conf_num"], meetings["term"]))
    paths = _raw_paths(kind)
    if limit:
        paths = dict(sorted(paths.items())[:limit])
    # parse cache (speaker fields only, no text). It is valid only for the parser fingerprint it
    # was written with (sha1 of build_turns.py, the source parser and legacy_rules.py): any parser
    # edit invalidates the whole cache. Error statuses are never reused (re-parsed every run).
    cache_t = os.path.join(OUT, f"{tag}_parsed_turns.parquet")
    cache_s = os.path.join(OUT, f"{tag}_parsed_status.json")
    use_cache = write and not limit
    fp0 = parse_fingerprint(kind)
    old_t, old_s = pd.DataFrame(columns=TURN_COLS), {}
    cache_state = "absent"
    if use_cache and os.path.exists(cache_t) and os.path.exists(cache_s):
        with open(cache_s) as f:
            meta = json.load(f)
        if isinstance(meta, dict) and meta.get("fingerprint") == fp0 and isinstance(meta.get("status"), dict):
            old_t = pd.read_parquet(cache_t)
            old_s = {int(k): v for k, v in meta["status"].items() if not str(v).startswith("error")}
            old_t = old_t[old_t["conf_num"].isin(list(old_s))]
            cache_state = "reused"
        else:
            cache_state = "invalidated:fingerprint_changed"
    todo = {c: p for c, p in paths.items() if c not in old_s}
    tasks = [(c, p, kind, (int(terms[c]) if c in terms and pd.notna(terms[c]) else None)) for c, p in sorted(todo.items())]
    done = {}
    pool_failed = False
    try:
        if workers > 1 and tasks:
            with ProcessPoolExecutor(max_workers=workers) as ex:
                for c, st, rr in ex.map(_extract_one, tasks, chunksize=4):
                    done[c] = (st, rr)
    except BrokenProcessPool:   # a worker was killed (shared machine): finish the rest serially
        pool_failed = True
    for task in tasks:
        if task[0] not in done:
            c, st, rr = _extract_one(task)
            done[c] = (st, rr)
    new_rows = [r for c in sorted(done) for r in done[c][1]]
    new_t = pd.DataFrame(new_rows, columns=TURN_COLS)
    all_s = {**old_s, **{c: st for c, (st, _) in done.items()}}
    t = pd.concat([x for x in (old_t, new_t) if len(x)], ignore_index=True) if (len(old_t) or len(new_t)) \
        else pd.DataFrame(columns=TURN_COLS)
    t = t[t["conf_num"].isin(list(paths))].sort_values(["conf_num", "turn_seq"]).reset_index(drop=True)
    fp1 = parse_fingerprint(kind)
    if use_cache:
        t.to_parquet(cache_t, index=False)
        with open(cache_s, "w") as f:   # a parser edit during the run leaves the cache unusable
            json.dump({"fingerprint": fp0 if fp1 == fp0 else "changed_during_run",
                       "status": {str(k): v for k, v in all_s.items()}}, f)
    status = {}
    for c in paths:
        st = all_s.get(c, "missing")
        status[st] = status.get(st, 0) + 1
    t["speaker_mem_id"] = pd.array(t["speaker_mem_id"], dtype="Int64")
    res = {"code_fingerprint": code_fingerprint(), "parse_fingerprint": fp0,
           "parser_changed_during_run": fp1 != fp0, "parse_cache": cache_state,
           "files": len(paths), "files_parsed_this_run": len(tasks), "files_from_cache": len(paths) - len(tasks),
           "process_pool_failed": pool_failed, "status": status, "turns": int(len(t)), "meetings": int(t["conf_num"].nunique()),
           "meetings_not_in_universe": int(len(set(t["conf_num"]) - set(meetings["conf_num"]))),
           "turns_null_speech_date": int(t["speech_date"].isna().sum())}
    t = roles.enrich(t, meetings)
    res["role_source"] = "roles.enrich"
    out = G.enrich(t, meetings)
    assert len(out) == len(t)
    gdiag = out.attrs["government"]         # merge() drops attrs: keep the diagnostics here
    res["enrich_diag"] = gdiag
    out = out.merge(meetings[["conf_num", "term", "hearing_type"]], on="conf_num", how="left", validate="many_to_one")
    res["turns_by_term_hearing_type"] = out.groupby(["term", "hearing_type"], dropna=False).size().reset_index().values.tolist()
    nonleg = out[out["role_group"] != "legislator"]
    res["ministry_coverage_by_role"] = nonleg.groupby("role").apply(lambda x: pd.Series({
        "turns": int(len(x)), "coverage": round(float(x["ministry_normalized"].notna().mean()), 4)}),
        include_groups=False).sort_values("turns", ascending=False).to_dict(orient="index")
    res["ministry_rule_turns"] = nonleg["ministry_rule"].value_counts().to_dict()
    lk = out[out["role"].isin(G.LINK_ROLES)]
    res["link_method_by_role"] = lk.groupby(["role", "link_method"], dropna=False).size().reset_index().values.tolist()
    res["link_rate_by_role"] = lk.groupby("role")["minister_panel_id"].apply(
        lambda s: [int(len(s)), int(s.notna().sum()), round(float(s.notna().mean()), 4)]).to_dict()
    res["link_rate_by_year"] = lk.assign(y=lk["speech_date"].str[:4]).groupby("y")["minister_panel_id"].agg(
        ["size", lambda s: int(s.notna().sum())]).reset_index().values.tolist()
    hz = lk[lk["speaker_name"].fillna("").map(lambda s: bool(G.HANJA_RE.search(s)))]
    res["hanja_name_link_roles"] = {"turns": int(len(hz)), "linked": int(hz["minister_panel_id"].notna().sum()),
                                    "name_resolution": gdiag.get("link_name_resolution")}
    res["hanja_unlinked_top"] = (hz[hz["minister_panel_id"].isna()]
                                 .groupby(["speaker_pos", "speaker_name", "gov_link_name", "link_method"], dropna=False).size()
                                 .sort_values(ascending=False).head(25).reset_index().values.tolist())
    nip = lk[lk["link_method"] == "unmatched:name_not_in_panel"]
    res["name_not_in_panel_by_role_admin"] = (nip.groupby(["role", "admin"], dropna=False).size()
                                              .reset_index().values.tolist())
    res["admin_by_state"] = out.groupby(["presidency_state", "admin", "admin_ideology"], dropna=False).size().reset_index().values.tolist()
    res["unlinked_top"] = (lk[lk["minister_panel_id"].isna()]
                           .groupby(["role", "speaker_pos", "speaker_name", "link_method"], dropna=False).size()
                           .sort_values(ascending=False).head(40).reset_index().values.tolist())
    res["uncovered_positions_top"] = (nonleg[nonleg["ministry_normalized"].isna() & nonleg["role"].isin(list(L.GOVT_ROLES))]
                                      .groupby(["role", "speaker_pos", "ministry_rule"], dropna=False).size()
                                      .sort_values(ascending=False).head(40).reset_index().values.tolist())
    obs, n_bare = _evidence_obs(out, tag)
    res["panel_evidence_obs_by_role"] = obs.groupby("role")["n"].sum().astype(int).to_dict()
    res["panel_evidence_prime_minister_bare_acting_excluded"] = n_bare
    per, miss = panel_date_evidence(obs)
    res["panel_evidence_rows_before_start_beyond_buffer"] = int(per["rows_before_start_beyond_buffer"].sum()) if len(per) else 0
    res["panel_evidence_rows_after_end_beyond_buffer"] = int(per["rows_after_end_beyond_buffer"].sum()) if len(per) else 0
    if write:
        cols = ["conf_num", "turn_seq", "source", "speaker_label_raw", "speaker_pos", "speaker_name", "speaker_mem_id",
                "speech_date", "term", "hearing_type", "role", "role_group", "role_rule", *G.ADDED_COLUMNS,
                "presidency_state"]
        out[[c for c in cols if c in out.columns]].to_parquet(os.path.join(OUT, f"{tag}_turns_government.parquet"), index=False)
        obs.groupby(["speaker_name", "ministry_normalized", "speech_date", "role", "source"], dropna=False)["n"].sum().reset_index() \
            .to_parquet(os.path.join(OUT, f"panel_evidence_obs_{tag}.parquet"), index=False)
        with open(os.path.join(OUT, f"{tag}_eval.json"), "w") as f:
            json.dump(res, f, ensure_ascii=False, indent=1, default=str)
    return res


def seed_cache(tag):
    """Seed the parse cache from an earlier {tag}_turns_government.parquet (same TURN_COLS, no
    text). Meetings present there are marked 'ok'; every other file is parsed on the next run."""
    src = os.path.join(OUT, f"{tag}_turns_government.parquet")
    t = pd.read_parquet(src, columns=TURN_COLS)
    t.to_parquet(os.path.join(OUT, f"{tag}_parsed_turns.parquet"), index=False)
    with open(os.path.join(OUT, f"{tag}_parsed_status.json"), "w") as f:   # fingerprint unknown: reparsed next run
        json.dump({"fingerprint": "seeded_unknown", "status": {str(int(c)): "ok" for c in t["conf_num"].unique()}}, f)
    return {"turns": int(len(t)), "meetings": int(t["conf_num"].nunique())}


def evidence_all(write=True):
    """Pool the transcript observations of all sources into one panel-date evidence table."""
    parts = [pd.read_parquet(p) for p in sorted(glob.glob(os.path.join(OUT, "panel_evidence_obs_*.parquet")))]
    obs = pd.concat(parts, ignore_index=True)
    per, miss = panel_date_evidence(obs)
    if write:
        per.to_csv(os.path.join(OUT, "panel_date_evidence.csv"), index=False)
        miss.to_csv(os.path.join(OUT, "ministers_not_in_panel.csv"), index=False)
    bad = (per["dates_before_start_beyond_buffer"] + per["dates_after_end_beyond_buffer"]) if len(per) else pd.Series(dtype=int)
    res = {"code_fingerprint": code_fingerprint(),
           "note": "row counts add sources that overlap (XLSX and HWP both hold the 2,433 18대 XLSX meetings; "
                    "XLSX and XML both hold crawled 16-17/19-22대 committee meetings); distinct-date counts do not",
            "obs_rows": int(obs["n"].sum()), "by_source": obs.groupby("source")["n"].sum().to_dict(),
            "panel_rows_with_2plus_distinct_dates_outside_window": int((bad >= 2).sum()),
            "distinct_dates_before_start_beyond_buffer": int(per["dates_before_start_beyond_buffer"].sum()) if len(per) else 0,
            "distinct_dates_after_end_beyond_buffer": int(per["dates_after_end_beyond_buffer"].sum()) if len(per) else 0,
            "contradicted_rows_flagged_date_approx": int(((bad > 0) & per["notes_date_approx"]).sum()) if len(per) else 0,
            "panel_rows_observed": int(len(per)),
            "panel_rows_contradicted": int(((per["rows_before_start_beyond_buffer"] + per["rows_after_end_beyond_buffer"]) > 0).sum()),
            "rows_before_start_beyond_buffer": int(per["rows_before_start_beyond_buffer"].sum()),
            "rows_after_end_beyond_buffer": int(per["rows_after_end_beyond_buffer"].sum()),
            "not_in_panel_rows": int(miss["rows"].sum()) if len(miss) else 0,
            "not_in_panel_people": int(miss["name"].nunique()) if len(miss) else 0,
            "not_in_panel_by_reason": (miss.groupby("reason").agg(names=("name", "nunique"), rows=("rows", "sum"))
                                       .astype(int).to_dict(orient="index") if len(miss) else {}),
            "obs_rows_by_role": obs.groupby("role")["n"].sum().astype(int).to_dict() if "role" in obs else {},
            "panel_rows_observed_by_ministry_pm": {
                "prime_minister": int((per["ministry"] == "국무총리").sum()) if len(per) else 0,
                "other": int((per["ministry"] != "국무총리").sum()) if len(per) else 0}}
    if write:
        with open(os.path.join(OUT, "evidence_eval.json"), "w") as f:
            json.dump(res, f, ensure_ascii=False, indent=1, default=str)
    return res


# --------------------------------------------------------------------------- v2 (minister-data v2.0.0)
OUT_V2 = os.path.join(V10, "interim", "pipeline", "government_v2")
RELEASE = os.path.join(V10, "build", "release")
V2_EVAL_ROLES = ("minister", "minister_acting", "minister_nominee", "prime_minister", "nominee")
# columns government.enrich reads (run_all.py hands it the post-party_timeline frame; these are the same values)
V2_IN_COLS = ("conf_num", "turn_seq", "speaker_pos", "speaker_name", "speech_date", "role", "role_group",
              "label_confidence", "label_inconsistent_in_meeting", "is_former_title", "label_repaired",
              "label_meeting_majority", "presidency_state")
# stored release columns compared with the legacy_296 rerun
V2_STORED = ("ministry_normalized", "ministry_family", "ministry_rule", "minister_panel_id", "dual_office",
             "link_method", "gov_link_name", "admin", "admin_ideology", "gov_date_source")


def _progress(msg):
    os.makedirs(OUT_V2, exist_ok=True)
    with open(os.path.join(OUT_V2, "PROGRESS.txt"), "a", encoding="utf-8") as fh:
        fh.write(msg.rstrip() + "\n")


def _linked(meth: pd.Series, panel: str) -> pd.Series:
    return meth.fillna("").str.startswith(G.LINKED_PREFIXES[panel])


def _eq(a: pd.Series, b: pd.Series) -> pd.Series:
    """Element-wise equality with null == null (values compared as strings)."""
    a, b = a.astype("string").reset_index(drop=True), b.astype("string").reset_index(drop=True)
    return (a.fillna("<NA>") == b.fillna("<NA>"))


def v2_eval(chunk_turns: int = 500_000, memory: str = "8GB", terms=None, prototype_rc3: bool = True) -> dict:
    """See the module docstring (subcommand v2). Returns the summary written to v2_summary.json."""
    import duckdb
    import time
    os.makedirs(OUT_V2, exist_ok=True)
    t_start = time.time()
    v2 = G._default_index(panel="v2")
    legacy = G._default_index(panel="legacy_296")          # the windows of config.yaml (7 / 60 / 60)
    meetings = pd.read_parquet(os.path.join(RELEASE, "meetings.parquet"), columns=["conf_num", "date"])
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory}'")
    con.execute("SET threads=4")
    terms = terms or sorted(d[1:] for d in os.listdir(os.path.join(RELEASE, "turns")) if d.startswith("t"))
    sel = ", ".join([f'"{c}"' for c in V2_IN_COLS] + [f'"{c}" AS "rel_{c}"' for c in V2_STORED])
    keep, repro, admin_diff, n_all = [], {}, {}, 0
    for tk in terms:
        src = f"read_parquet('{os.path.join(RELEASE, 'turns', 't' + tk, '*.parquet')}')"
        sizes = con.execute(f"SELECT conf_num, count(*) n FROM {src} GROUP BY 1 ORDER BY 1").fetchall()
        chunks, cur, n = [], [], 0
        for cn, k in sizes:
            if cur and n + k > chunk_turns:
                chunks.append(cur)
                cur, n = [], 0
            cur.append(cn)
            n += k
        if cur:
            chunks.append(cur)
        for ch in chunks:
            df = con.execute(f"SELECT {sel} FROM {src} WHERE conf_num BETWEEN ? AND ? ORDER BY conf_num, turn_seq",
                             [ch[0], ch[-1]]).df()
            n_all += len(df)
            inp = df[list(V2_IN_COLS)]
            mt = meetings[meetings.conf_num.isin(ch)]
            a = G.enrich(inp, mt, panel_index=v2)
            b = G.enrich(inp, mt, panel_index=legacy)
            for c in V2_STORED:                                   # legacy_296 rerun == stored release
                repro[c] = repro.get(c, 0) + int((~_eq(b[c], df["rel_" + c])).sum())
            for c in ("admin", "admin_ideology", "ministry_normalized", "ministry_family", "gov_date_source"):
                admin_diff[c] = admin_diff.get(c, 0) + int((~_eq(a[c], df["rel_" + c])).sum())
            m = df["role"].isin(V2_EVAL_ROLES).to_numpy()
            k = pd.DataFrame({
                "conf_num": df.loc[m, "conf_num"].to_numpy(), "turn_seq": df.loc[m, "turn_seq"].to_numpy(),
                "term": int(tk), "role": df.loc[m, "role"].to_numpy(),
                "speaker_name": df.loc[m, "speaker_name"].to_numpy(), "speaker_pos": df.loc[m, "speaker_pos"].to_numpy(),
                "speech_date": df.loc[m, "speech_date"].to_numpy(),
                "ministry_normalized": a.loc[m, "ministry_normalized"].to_numpy(),
                "legacy_link_method": b.loc[m, "link_method"].to_numpy(),
                "legacy_panel_id": b.loc[m, "minister_panel_id"].to_numpy(),
                "legacy_dual_office": b.loc[m, "dual_office"].to_numpy(),
                "link_method": a.loc[m, "link_method"].to_numpy(),
                "minister_panel_id": a.loc[m, "minister_panel_id"].to_numpy(),
                "dual_office": a.loc[m, "dual_office"].to_numpy(),
                **{c: a.loc[m, c].to_numpy() for c in G.V2_LINK_COLUMNS},
                "gov_link_name": a.loc[m, "gov_link_name"].to_numpy(),
            })
            keep.append(k)
            del df, inp, a, b
        _progress(f"  v2 eval term {tk}: {sum(len(x) for x in keep)} government turns so far, "
                  f"{n_all} turns enriched, {round(time.time() - t_start)} s")
    con.close()
    tl = pd.concat(keep, ignore_index=True)
    for c in ("dual_office", "legacy_dual_office"):
        tl[c] = tl[c].astype("boolean")
    tl["legacy_linked"] = _linked(tl["legacy_link_method"], "legacy_296")
    tl["v2_linked"] = _linked(tl["link_method"], "v2")
    # role 'nominee' counts only when in the v2 link scope (cabinet nominee title)
    tl["role_eval"] = tl["role"].where(tl["role"] != "nominee", "nominee_cabinet_title")
    tl = tl[(tl["role"] != "nominee") | tl["link_method"].notna()].reset_index(drop=True)
    tl.to_parquet(os.path.join(OUT_V2, "turn_links_v2.parquet"), index=False)

    def rates(g):
        lm = g["link_method"].fillna("")
        return pd.Series({
            "turns": len(g), "legacy_linked": int(g["legacy_linked"].sum()),
            "legacy_rate": round(g["legacy_linked"].mean(), 4) if len(g) else None,
            "v2_linked": int(g["v2_linked"].sum()), "v2_rate": round(g["v2_linked"].mean(), 4) if len(g) else None,
            "v2_spell_exact": int((lm == "spell:exact").sum()), "v2_spell_buffer": int((lm == "spell:buffer").sum()),
            "v2_nomination": int(lm.str.startswith("nomination:").sum()),
            "v2_acting_head": int(lm.str.startswith("acting_head:").sum()),
            "v2_unlinked": int((~g["v2_linked"]).sum())})
    by_tr = tl.groupby(["term", "role_eval"]).apply(rates, include_groups=False).reset_index()
    by_r = tl.groupby("role_eval").apply(rates, include_groups=False).reset_index()
    for f in (by_tr, by_r):
        for c in f.columns:
            if c not in ("role_eval", "legacy_rate", "v2_rate"):
                f[c] = f[c].astype(int)
    by_tr.to_csv(os.path.join(OUT_V2, "rates_term_role.csv"), index=False)
    by_r.to_csv(os.path.join(OUT_V2, "rates_role.csv"), index=False)
    meth = (tl.groupby(["role_eval", "link_method"], dropna=False).size().rename("turns").reset_index()
            .sort_values(["role_eval", "turns"], ascending=[True, False]))
    meth.to_csv(os.path.join(OUT_V2, "v2_link_methods.csv"), index=False)
    lmeth = (tl.groupby(["role_eval", "legacy_link_method"], dropna=False).size().rename("turns").reset_index()
             .sort_values(["role_eval", "turns"], ascending=[True, False]))
    lmeth.to_csv(os.path.join(OUT_V2, "legacy_link_methods.csv"), index=False)

    # unlinked residuals (v2), grouped; detail from SpellIndex.link (nearest spell, hearing dates, acting rows)
    res = tl[~tl["v2_linked"]].copy()
    det = {}
    for r in res[["speaker_name", "speaker_pos", "ministry_normalized", "speech_date", "role"]].drop_duplicates() \
            .itertuples(index=False):
        t = tuple(G._nv(x) for x in r)
        det[t] = v2.link(*t).detail
    res["detail"] = [det.get(tuple(G._nv(x) for x in r)) for r in
                     res[["speaker_name", "speaker_pos", "ministry_normalized", "speech_date", "role"]].itertuples(index=False)]
    resid = (res.groupby(["role_eval", "link_method", "speaker_name", "speaker_pos", "minister_lineage"], dropna=False)
             .agg(n_turns=("turn_seq", "size"), first_date=("speech_date", "min"), last_date=("speech_date", "max"),
                  n_meetings=("conf_num", "nunique"),
                  conf_nums=("conf_num", lambda s: ",".join(map(str, sorted(set(s))[:8]))),
                  detail=("detail", "first"), legacy_link=("legacy_link_method", lambda s: ",".join(sorted(set(map(str, s)))[:3])))
             .reset_index().sort_values(["role_eval", "n_turns"], ascending=[True, False]))
    resid.to_csv(os.path.join(OUT_V2, "v2_residuals.csv"), index=False)
    resid_cls = (res.groupby(["role_eval", "link_method"]).agg(n_turns=("turn_seq", "size"),
                                                               n_speakers=("speaker_name", "nunique"))
                 .reset_index().sort_values(["role_eval", "n_turns"], ascending=[True, False]))
    resid_cls.to_csv(os.path.join(OUT_V2, "v2_residual_classes.csv"), index=False)

    # agreement of linked ids with the legacy panel is not comparable (different keys); v2 dual_office vs legacy
    both = tl["v2_linked"] & tl["legacy_linked"]
    dual_x = pd.crosstab(tl.loc[both, "legacy_dual_office"].astype(str), tl.loc[both, "dual_office"].astype(str))
    dual_x.to_csv(os.path.join(OUT_V2, "dual_office_legacy_vs_v2.csv"))

    summary = {"run_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "code": code_fingerprint()["files"], "release": v2.release_dir.replace(V10 + os.sep, ""),
               "release_version": v2.version, "turns_enriched": int(n_all), "government_turns": int(len(tl)),
               "legacy_reproduces_release_mismatches": repro, "v2_vs_release_admin_ministry_mismatches": admin_diff,
               "rates_role": by_r.to_dict(orient="records"),
               "v2_residual_classes": resid_cls.to_dict(orient="records")}
    summary.update(_v2_prototype_compare(tl, prototype_rc3))
    summary["seconds"] = round(time.time() - t_start, 1)
    with open(os.path.join(OUT_V2, "v2_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=1, default=str)
    return summary


def _v2_proto_linked(status: pd.Series) -> pd.Series:
    return status.isin(["spell", "nomination_hearing", "acting_head"])


def _v2_prototype_compare(tl: pd.DataFrame, rerun_rc3: bool) -> dict:
    """verify_minister_v2.py (read-only prototype) versus enrich v2, turn by turn on (conf_num, turn_seq).
    rc2: the stored check (interim/external/minister_data_v2.0.0-rc2_check/turn_links.parquet, run on the
    release of 2026-09-28 02:41). rc3: the prototype rerun in-process on the rc3 snapshot and the current
    release, its outputs redirected to government_v2/prototype_rc3/ (the prototype file is not changed)."""
    out = {}
    runs = {"rc2": os.path.join(V10, "interim", "external", "minister_data_v2.0.0-rc2_check", "turn_links.parquet")}
    if rerun_rc3:
        import importlib
        os.environ["MINISTER_RELEASE"] = "minister_data_v2.0.0-rc3"
        sys.path.insert(0, os.path.join(V10, "code"))
        import verify_minister_v2 as VM
        VM = importlib.reload(VM)
        from pathlib import Path
        VM.OUT = Path(OUT_V2) / "prototype_rc3"
        VM.main()
        runs["rc3"] = str(VM.OUT / "turn_links.parquet")
    for tag, path in runs.items():
        if not os.path.exists(path):
            out[f"prototype_{tag}"] = "missing"
            continue
        p = pd.read_parquet(path, columns=["conf_num", "turn_seq", "role", "new_status", "new_spell_id",
                                           "new_nomination_id"])
        p["proto_linked"] = _v2_proto_linked(p["new_status"])
        p["proto_id"] = p["new_spell_id"].where(p["new_spell_id"].notna(), p["new_nomination_id"])
        m = tl.merge(p.rename(columns={"role": "proto_role"}), on=["conf_num", "turn_seq"], how="outer", indicator=True)
        m["side"] = m["_merge"].astype(str)
        for c in ("v2_linked", "proto_linked"):
            m[c] = m[c].astype("boolean").fillna(False).astype(bool)
        m["id_v2"] = m["minister_spell_id"].where(m["minister_spell_id"].notna(),
                                                  m["minister_acting_id"].where(m["minister_acting_id"].notna(),
                                                                                m["minister_nomination_id"]))
        both = m["side"] == "both"
        x = (m[both].groupby(["role_eval", "new_status", "link_method"], dropna=False).size().rename("turns")
             .reset_index().sort_values("turns", ascending=False))
        x.to_csv(os.path.join(OUT_V2, f"compare_prototype_{tag}_status.csv"), index=False)
        ids = m[both & m["proto_linked"] & m["v2_linked"]]
        diff_id = ids[ids["proto_id"].astype("string") != ids["id_v2"].astype("string")]
        diff_id[["conf_num", "turn_seq", "role", "speaker_name", "speaker_pos", "speech_date", "new_status", "proto_id",
                 "link_method", "id_v2"]].to_csv(os.path.join(OUT_V2, f"compare_prototype_{tag}_id_diff.csv"), index=False)
        flips = m[both & (m["proto_linked"] != m["v2_linked"])]
        flips[["conf_num", "turn_seq", "role", "speaker_name", "speaker_pos", "speech_date", "new_status", "proto_id",
               "link_method", "id_v2"]].to_csv(os.path.join(OUT_V2, f"compare_prototype_{tag}_flips.csv"), index=False)
        role_totals = {}
        for role, g in m[m["proto_role"].notna()].groupby("proto_role"):
            role_totals[role] = {"proto_turns": int(len(g)), "proto_linked": int(g["proto_linked"].sum())}
        out[f"prototype_{tag}"] = {
            "turns_proto": int(m["proto_role"].notna().sum()),
            "only_in_prototype": int((m["side"] == "right_only").sum()),
            "only_in_v2_eval": int((m["side"] == "left_only").sum()),
            "only_in_v2_eval_by_role": m[m["side"] == "left_only"].groupby("role_eval").size().astype(int).to_dict(),
            "only_in_prototype_by_role": m[m["side"] == "right_only"].groupby("proto_role").size().astype(int).to_dict(),
            "by_role": role_totals,
            "linked_both": int((both & m["proto_linked"] & m["v2_linked"]).sum()),
            "linked_proto_only": int((both & m["proto_linked"] & ~m["v2_linked"]).sum()),
            "linked_v2_only": int((both & ~m["proto_linked"] & m["v2_linked"]).sum()),
            "same_id_when_both_linked": int(len(ids) - len(diff_id)), "different_id_when_both_linked": int(len(diff_id)),
            "flip_classes": {f"{a} -> {b}": int(v) for (a, b), v in
                             flips.groupby(["new_status", "link_method"], dropna=False).size().items()},
        }
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["panel-audit", "v9", "xml", "hwp", "evidence", "all", "seed-cache-xml",
                                    "seed-cache-hwp", "v2"])
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--terms", default=None, help="v2: comma-separated terms (default all)")
    ap.add_argument("--no-prototype-rc3", action="store_true", help="v2: skip the in-process prototype rerun on rc3")
    a = ap.parse_args(argv)
    if a.cmd == "v2":
        r = v2_eval(terms=a.terms.split(",") if a.terms else None, prototype_rc3=not a.no_prototype_rc3)
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str)[:30000])
        return
    if a.cmd.startswith("seed-cache-"):
        print(seed_cache(a.cmd.rsplit("-", 1)[1]))
        return
    if a.cmd in ("panel-audit", "all"):
        panel_audit()
    if a.cmd in ("v9", "all"):
        r = v9_eval()
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str)[:20000])
    if a.cmd in ("xml", "all"):
        r = turns_eval("view", workers=a.workers)
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str)[:20000])
    if a.cmd in ("hwp", "all"):
        r = turns_eval("hwp", workers=a.workers)
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str)[:20000])
    if a.cmd in ("evidence", "all"):
        print(json.dumps(evidence_all(), ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()
