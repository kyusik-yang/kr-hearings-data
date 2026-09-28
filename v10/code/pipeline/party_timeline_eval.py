"""Evaluation of party_timeline outputs (writes v10/interim/pipeline/party_timeline/eval/).

    python party_timeline_eval.py            # all checks
    python party_timeline_eval.py spot       # only the 30-event spot check

Checks
  1. spells per term: members, spells, members with >1 spell, basis mix, daily seated count
     against [seats-10, seats].
  2. end-of-term party (the member's last seated day) against ALLNAMEMBER (party_allnamember), the
     의원이력 term party (party_term_api), the record system (party_record) and assemblykor
     legislators.rda (20-22대 'party'); compared at organisation level on the end date.
  3. share of legislator turns coded by person spell vs label-lineage fallback, on
       a) v9 XLSX rows (role legislator/chair, v9 naas_cd, meetings whose v9_source is xlsx),
       b) the legislators component's XLSX keys (v10 naas_cd, n_rows weights),
       c) crawled XML turns and d) HWP turns as enriched by the legislators component.
     Inputs are aggregated to unique (naas_cd, date, term) with duckdb before enrich().
  4. ruling_status by term x presidency_state (same inputs), and v9 ruling_status vs v10 on (a).
  5. 30 random applied party-change events (seed 8374) checked against the minutes text.
Numbers only; no interpretation."""
from __future__ import annotations

import datetime as dt
import gzip
import json
import random
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import party_timeline as pt  # noqa: E402

EVAL = pt.OUT / "eval"
REPO = pt.REPO
LEG_EVAL = pt.INTERIM / "pipeline" / "legislators" / "eval"
SEED = 8374


def _duck():
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET threads=4")
    return con


# ----------------------------------------------------------------------------- 1. spells

def spell_table(sp: pd.DataFrame):
    rows = []
    for t in range(16, 23):
        s = sp[sp.term == t]
        end = min(pt.TERM_END[t], dt.date.today().isoformat())
        days = pd.date_range(pt.TERM_START[t], end, freq="D")
        st = pd.to_datetime(s.start).values
        en = pd.to_datetime(s.end).values
        cnt = np.array([((st <= d) & (en >= d)).sum() for d in days.values])
        seats = pt.TERM_SEATS[t]
        per = s.groupby("naas_cd").size()
        rows.append({"term": t, "seats": seats, "members": s.naas_cd.nunique(), "spells": len(s),
                     "members_multi_spell": int((per > 1).sum()),
                     "seated_min": int(cnt.min()), "seated_min_day": str(days[cnt.argmin()].date()),
                     "seated_max": int(cnt.max()), "days": len(days),
                     "days_above_seats": int((cnt > seats).sum()),
                     "days_below_seats_minus_10": int((cnt < seats - 10).sum())})
    return pd.DataFrame(rows)


BASIS_GROUP = [
    ("recorded_event", r"^(start_roster|roster|join|leave|switch|enter_notice|party_merge|party_rename|group_rename"
                       r"|group_rename_caucus_to_party|registration_notice_representative|join_forming_caucus)$"),
    ("relabel", r"^(lineage_relabel|notice_relabel)$"),
    ("inferred", r"^(inferred_|speaker_nonpartisan_by_law)"),
    ("election_party", r"^election_party"),
]


def basis_group(b):
    for g, rx in BASIS_GROUP:
        if re.search(rx, str(b)):
            return g
    return "other"


def member_days_by_basis(sp: pd.DataFrame):
    s = sp.copy()
    s["days"] = (pd.to_datetime(s.end) - pd.to_datetime(s.start)).dt.days + 1
    s["basis_group"] = s.basis.map(basis_group)
    out = s.groupby(["term", "basis_group"]).days.sum().unstack(fill_value=0)
    out["total"] = out.sum(axis=1)
    return out.reset_index()


# ----------------------------------------------------------------------------- 2. end of term

def end_of_term_agreement(sp: pd.DataFrame, rs: pt.Resolver):
    """Agreement of the member's last spell party with end-of-term snapshots. A snapshot label is
    read as the formal label on the spell's end date (renames / mergers forward, a later rename
    backward); agreement is strict: the same label or the same organisation on that date. Labels
    that only share a lineage family (e.g. 새천년민주당 vs 열린우리당, 미래통합당 vs 국민의당) are
    counted separately as family_only and are not agreement."""
    ptm = pd.read_parquet(pt.INTERIM / "pipeline" / "legislators" / "person_terms.parquet")
    try:
        import pyreadr
        ak = pyreadr.read_r(str(REPO.parent / "assemblykor" / "data" / "legislators.rda"))["legislators"]
        ak = ak.rename(columns={"member_id": "naas_cd", "assembly": "term", "party": "party_assemblykor"})
        ak["term"] = ak.term.astype(int)
    except Exception as e:  # noqa: BLE001
        ak = pd.DataFrame(columns=["naas_cd", "term", "party_assemblykor"])
        print("assemblykor not read:", e)
    last = sp.sort_values("start").groupby(["naas_cd", "term"]).tail(1)[["naas_cd", "term", "party", "end", "name", "basis"]]
    m = last.merge(ptm[["naas_cd", "term", "stint", "party_allnamember", "party_term_api", "party_record"]]
                   .sort_values("stint").groupby(["naas_cd", "term"]).tail(1), on=["naas_cd", "term"], how="left")
    m = m.merge(ak[["naas_cd", "term", "party_assemblykor"]].drop_duplicates(["naas_cd", "term"]),
                on=["naas_cd", "term"], how="left")
    rows, detail = [], []
    for src in ("party_allnamember", "party_term_api", "party_record", "party_assemblykor"):
        for r in m.itertuples():
            snap = getattr(r, src)
            if not isinstance(snap, str) or not snap.strip():
                continue
            snap = snap.split("/")[-1].strip()           # ALLNAMEMBER lists parties per term with '/'
            q, how = rs.lin.label_at(snap, r.end)
            k = rs.lin.key(q, r.end, historic=True)
            born = (rs.lin.rows[k]["label_from"] or "0000-01-01") if k else "0000-01-01"
            sf = pt.norm_party(rs.formal_on(q, min(born, r.end), r.end))
            ours = pt.norm_party(r.party)
            strict = sf == ours or (ours != pt.INDEP and sf != pt.INDEP and rs.lin.same_party(sf, ours, r.end))
            fam = (not strict) and ours not in ("", pt.INDEP) and sf not in ("", pt.INDEP) and \
                rs.family(ours, r.end) == rs.family(sf, r.end) and rs.family(ours, r.end) not in (None, "")
            rows.append({"source": src, "term": r.term, "agree": bool(strict), "family_only": bool(fam)})
            if not strict:
                detail.append({"source": src, "term": r.term, "naas_cd": r.naas_cd, "name": r.name,
                               "end": r.end, "party_end_spell": r.party, "basis_last_spell": r.basis,
                               "snapshot_raw": getattr(r, src), "snapshot_on_end": sf, "family_only": bool(fam)})
    agg = pd.DataFrame(rows).groupby(["source", "term"]).agg(agree=("agree", "sum"), compared=("agree", "size"),
                                                           family_only=("family_only", "sum")).reset_index()
    agg["share"] = (agg.agree / agg.compared).round(4)
    return agg, pd.DataFrame(detail)


# ----------------------------------------------------------------------------- 3/4. coverage

def _proxy_keys(con):
    """Unique (source, naas_cd, date, term) with row weights for the four inputs."""
    parts = {}
    cw = pt.INTERIM / "v9_to_api_crosswalk.parquet"
    v9 = REPO / "data" / "all_speeches_16_22_v9.parquet"
    parts["v9_xlsx_rows"] = con.execute(f"""
        select s.naas_cd, s.date, cast(s.term as integer) as term, count(*) as n,
               sum(case when s.ruling_status='ruling' then 1 else 0 end) as v9_ruling,
               sum(case when s.ruling_status='opposition' then 1 else 0 end) as v9_opposition,
               sum(case when s.ruling_status='independent' then 1 else 0 end) as v9_independent
        from read_parquet('{v9}') s
        join (select distinct meeting_id from read_parquet('{cw}') where v9_source='xlsx') x
          on s.meeting_id = x.meeting_id
        where s.role in ('legislator','chair')
        group by 1,2,3""").fetchdf()
    xk = LEG_EVAL / "xlsx_enriched_keys.parquet"
    if xk.exists():
        parts["xlsx_keys_v10_ids"] = con.execute(f"""
            select naas_cd, coalesce(speech_date, date) as date, cast(term as integer) as term, sum(n_rows) as n
            from read_parquet('{xk}') where role_group='legislator' group by 1,2,3""").fetchdf()
    for nm, f in (("xml_turns", LEG_EVAL / "xml_enriched.parquet"), ("hwp_turns", LEG_EVAL / "hwp_enriched.parquet")):
        if f.exists():
            parts[nm] = con.execute(f"""
                select naas_cd, speech_date as date, cast(term as integer) as term, count(*) as n
                from read_parquet('{f}') where role_group='legislator' group by 1,2,3""").fetchdf()
    return parts


def coverage(rs: pt.Resolver):
    con = _duck()
    parts = _proxy_keys(con)
    cov, rul, v9cmp, meta = [], [], [], {}
    for src, k in parts.items():
        k = k.copy()
        k["date"] = k.date.astype(str).str[:10].where(k.date.notna(), None)
        k = k.reset_index(drop=True)
        t = pd.DataFrame({"conf_num": 0, "turn_seq": range(len(k)), "speech_date": k.date,
                          "naas_cd": k.naas_cd, "role_group": "legislator", "term": k.term})
        e = pt.enrich(t, None, resolver=rs)
        k["party_method"] = e.party_method.values
        k["ruling_status"] = e.ruling_status.values
        k["presidency_state"] = e.presidency_state.values
        k["ruling_null_reason"] = e.ruling_null_reason.values
        k["party_basis"] = e.party_basis.values
        k["party_uncertain"] = e.party_uncertain.fillna(False).astype(bool).values
        k["party_unconfirmed_after_gap"] = e.party_unconfirmed_after_gap.fillna(False).astype(bool).values
        k["method"] = [m_ if isinstance(m_, str) else f"null:{r_}" for m_, r_ in zip(k.party_method, k.ruling_null_reason)]
        k["method"] = np.where(k.party_basis.astype(str).str.startswith("nearest_spell"), "person_spell(nearest)", k.method)
        meta[src] = {"keys": len(k), "rows": int(k.n.sum()), "rows_party_uncertain": int(k.n[k.party_uncertain].sum()),
                     "rows_unconfirmed_after_gap": int(k.n[k.party_unconfirmed_after_gap].sum()),
                     "rows_unconfirmed_after_gap_only": int(k.n[k.party_unconfirmed_after_gap & ~k.party_uncertain].sum())}
        g = k.groupby(["term", "method"], dropna=False).n.sum().unstack(fill_value=0)
        g["total"] = g.sum(axis=1)
        g = g.reset_index()
        g.insert(0, "source", src)
        cov.append(g)
        r = k.groupby(["term", "presidency_state", "ruling_status", "party_uncertain", "party_unconfirmed_after_gap"],
                      dropna=False).n.sum().reset_index()
        r.insert(0, "source", src)
        rul.append(r)
        if src == "v9_xlsx_rows":
            k["v10"] = k.ruling_status.fillna("NULL")
            for lab in ("ruling", "opposition", "independent"):
                x = k.groupby(["term", "v10"])[f"v9_{lab}"].sum().reset_index().rename(columns={f"v9_{lab}": "n"})
                x["v9"] = lab
                v9cmp.append(x)
    cov = pd.concat(cov, ignore_index=True).fillna(0)
    rul = pd.concat(rul, ignore_index=True)
    v9 = pd.concat(v9cmp, ignore_index=True) if v9cmp else pd.DataFrame()
    return cov, rul, v9, meta


# ----------------------------------------------------------------------------- 5. spot check

def _page_text(conf_num):
    kind, data, path = pt.load_page(int(conf_num))
    if kind == "xml":
        from lxml import html as LH
        t = LH.fromstring(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data).text_content()
    elif kind == "hwp":
        paras, _ = pt._hwp_paragraphs(data)
        t = "\n".join(p.get("text") or "" for p in paras)
    else:
        t = ""
    return kind, path, t


def _date_forms(d):
    y, m, dd = d.split("-")
    mi, di = int(m), int(dd)
    return [rf"{y}\s*\.\s*{mi}\s*\.\s*{di}(?!\d)", rf"{y}\s*년\s*{mi}\s*월\s*{di}\s*일", rf"(?<!\d){mi}\s*월\s*{di}\s*일",
            rf"{y}\s+{mi}\s*\.\s*{di}(?!\d)", rf"{y}\s*\.\s*0{mi}\s*\.\s*0?{di}(?!\d)"]


WIN = [150, 250, 300, 250]   # characters before / after the name searched for the date, then for the labels
APPLIED = ["applied", "applied_from_is_party_before_recent_exit", "applied_conflict_from_mismatch",
           "applied_conflict_state_not_group", "applied_conflict_from_independent_state_party",
           "applied_inferred_exit_from_observation", "applied_backfilled_from_party", "applied_registration_then_switch"]


def _sections(hg, caption_norm):
    """[(start, end)] of the report item(s) whose heading is `caption_norm` in the de-spaced,
    hangulized report text: from the heading to the next ◯/○ heading (at most 6,000 characters)."""
    out = []
    if not caption_norm:
        return out
    for m in re.finditer(re.escape(caption_norm), hg):
        nx = [x for x in (hg.find("◯", m.end()), hg.find("○", m.end())) if x >= 0]
        out.append((m.start(), min(nx + [m.end() + 6000])))
    return out


def spot_check(n=30, seed=SEED):
    """Sample applied member-level events and check them against the page text they came from.
    The report text is de-spaced; the check is confined to the report item with the event's own
    heading (e.g. '의원 당적 변경', '교섭단체 가입', '통지'), so a name in another item (a roll of
    attendees, a committee table) cannot pass. Within that item the name must occur, the event
    date must occur within 150 characters before / 250 after the name, and the party label within
    300 before / 250 after (group-first layouts and rowspan cells print the group / labels before
    the names). For a switch the 'from' label must be followed by the 'to' label within 60
    characters (adjacent cells of one row) inside that window. For a roster the 교섭단체 label of the
    name's block (the last one printed before the name, or for names-first tables the first one
    after it) must be the event's party and the date must follow that label within the item.
    pass_nearest_label is stricter for leave / join / switch: the item label nearest to the name
    (within 30 characters after it, else before it) must be the event's (see _row_association);
    flattened rowspan / names-first tables fail it although the event is right, so it is reported
    next to pass, with negative controls for both."""
    log = pd.read_parquet(pt.OUT / "event_log.parquet")
    rec = pd.read_parquet(pt.OUT / "report_records.parquet")
    sp = pd.read_parquet(pt.OUT / "party_spells.parquet")
    names = sp.drop_duplicates("naas_cd").set_index("naas_cd").name.to_dict()
    ev = log[log.status.isin(APPLIED) & log.kind.isin(["leave", "join", "switch", "roster"])
             & log.naas_cd.notna()].reset_index(drop=True)
    rng = random.Random(seed)
    idx = sorted(rng.sample(range(len(ev)), min(n, len(ev))))
    return check_events(ev.loc[idx], rec, names)


DATE_ANY = re.compile(r"\d{4}\.\d{1,2}\.\d{1,2}|\d{1,2}월\d{1,2}일")


def _row_association(sec, p, pl, fl, labels, after=30, before=300):
    """(party_ok, order_ok) for a leave / join / switch row of the name at p in the de-spaced item
    text. The label that belongs to the name is the nearest item label (any group / from / to label
    of the item's records) starting within `after` characters after the name (row layout: name,
    district, label), else the nearest one before it within `before` characters (rowspan cells and
    group-first text print the label once before a block of names). For a switch that label must be
    the 'to' label preceded within 40 characters by the 'from' label. party_ok: the label is the
    event's."""
    occ = sorted({(m.start(), y) for y in (labels | set(pl) | set(fl or [])) for m in re.finditer(re.escape(y), sec)})
    if fl:
        # a switch row prints 'from' then 'to': candidate positions are the 'to' labels
        pairs = [(q, y) for q, y in occ if y in pl and any(0 < q - f <= 40 for f, z in occ if z in fl)]
        others = [(q, y) for q, y in occ if y not in pl and y not in fl]
        occ = sorted(pairs + others + [(q, y) for q, y in occ if y in pl and (q, y) not in pairs])
    nxt = [x for x in occ if p < x[0] <= p + after]
    prv = [x for x in occ if p - before <= x[0] < p]
    cand = nxt[0] if nxt else (prv[-1] if prv else None)
    if cand is None:
        return False, not fl
    ok = cand[1] in pl
    if fl:
        return ok and any(0 < cand[0] - f <= 40 for f, z in occ if z in fl), ok
    return ok, True


def check_events(ev: pd.DataFrame, rec: pd.DataFrame, names: dict, perturb=None):
    """The spot-check rule applied to given event_log rows (see spot_check). perturb (negative
    control): 'date' moves the event date by 400 days, 'party' replaces the party label with the
    next sampled event's different label, 'item' looks in the wrong report item."""
    out = []
    ev = ev.copy()
    if perturb == "date":
        ev["date"] = [(dt.date.fromisoformat(d) + dt.timedelta(days=400)).isoformat() for d in ev.date]
    swap = {}
    if perturb == "party":
        labs = []
        for _, e in ev.iterrows():
            r = rec[(rec.conf_num == int(e.conf_num)) & (rec.item_seq.astype(str) == str(e.item_seq))
                    & (rec.row_idx.astype(str) == str(e.row_idx)) & (rec.kind == e.kind)]
            labs.append((r.iloc[0].to_party if e.kind == "switch" else r.iloc[0].group) if len(r) else None)
        for i, a in enumerate(labs):
            swap[i] = next((b for b in labs[i + 1:] + labs[:i] if b and pt.norm_party(b) != pt.norm_party(a or "")), None)
    for j, (_, e) in enumerate(ev.iterrows()):
        r = rec[(rec.conf_num == int(e.conf_num)) & (rec.item_seq.astype(str) == str(e.item_seq))
                & (rec.row_idx.astype(str) == str(e.row_idx)) & (rec.kind == e.kind)]
        r = r[r.name_raw == e.name_raw] if len(r) > 1 else r
        r = r.iloc[0] if len(r) else None
        party = frm = None
        caption = None
        if r is not None:
            party = r.to_party if e.kind == "switch" else r.group
            frm = r.from_party if e.kind == "switch" else None
            caption = r.caption
        if perturb == "party":
            party = swap.get(j)
        if perturb == "item":
            caption = {"leave": "교섭단체가입", "join": "교섭단체소속의원제적", "roster": "의원당적변경",
                       "switch": "교섭단체소속의원명부제출"}.get(e.kind)
        it = rec[(rec.conf_num == int(e.conf_num)) & (rec.item_seq.astype(str) == str(e.item_seq))]
        labels = {y for c in ("group", "from_party", "to_party") for g in it[c].dropna().unique()
                  for y in (pt.nows(g), pt.norm_party(g)) if y and y != pt.NON_GROUP}
        groups = set()
        if e.kind == "roster":
            groups = {y for g in it[it.kind == "roster"].group.dropna().unique() for y in (pt.nows(g), pt.norm_party(g)) if y}
        kind, path, text = _page_text(e.conf_num)
        rep = text[text.find("보고사항") if "보고사항" in text else text.find("報告事項"):] if text else ""
        raw_ns = pt.nows(rep)          # NFKC: compatibility ideographs (U+F9E1 李) as in the parsed names
        hg = "".join(pt.HANJA_KO.get(ch, ch) for ch in raw_ns)     # same length, aligned with raw_ns
        secs = _sections(hg, pt.norm_caption(caption or ""))
        nm_raw = pt.nows(e.name_raw or "")
        nm_hg = names.get(e.naas_cd) or ""
        dforms = [re.sub(r"\\s[*+]", "", f) for f in _date_forms(e.date)]
        lab = lambda x: [y for y in {pt.nows(x or ""), pt.norm_party(x or "")} if y]  # noqa: E731
        pl, fl = lab(party), lab(frm)
        best = {"name_found": False, "date_ok": False, "party_ok": False, "order_ok": e.kind != "switch", "passage": None}
        for a, b in secs:
            sec = raw_ns[a:b]
            pos = [m.start() for m in re.finditer(re.escape(nm_raw), sec)] if nm_raw else []
            if not pos and nm_hg:
                pos = [m.start() for m in re.finditer(re.escape(pt.nows(nm_hg)), sec)]
            for p in pos:
                best["name_found"] = True
                win_d = sec[max(0, p - WIN[0]): p + WIN[1]]
                d_ok = any(re.search(f, win_d) for f in dforms)
                strict = None
                if e.kind in ("switch", "leave", "join"):
                    strict = _row_association(sec, p, pl, fl if e.kind == "switch" else None, labels)[0]
                if e.kind == "switch":
                    # the row's 'from' cell then its 'to' cell, adjacent (<= 60 characters apart);
                    # rowspan cells are printed once for a block of rows, so they may precede the name
                    win = sec[max(0, p - WIN[2]): p + WIN[3]]
                    tps = [m.start() for x in pl for m in re.finditer(re.escape(x), win)]
                    fps = [m.start() for x in fl for m in re.finditer(re.escape(x), win)] if fl else []
                    p_ok = bool(tps)
                    o_ok = p_ok and (not fl or any(0 < t - f <= 60 for t in tps for f in fps))
                elif e.kind == "roster" and groups:
                    # a roster lists each 교섭단체 label once before its (long) member list and the
                    # date once: the last group label of the item before the name must be the
                    # event's party, and the date must follow that label within the item
                    gp = sorted((m.start(), g) for g in groups for m in re.finditer(re.escape(g), sec))
                    before = [x for x in gp if x[0] < p]
                    after = [x for x in gp if x[0] > p]
                    # group-first layout: the last label before the name; names-first layout (header
                    # '의원명 교섭단체 연월일', rowspan): the first label after the name
                    cand = ([before[-1]] if before else []) + ([after[0]] if after else [])
                    hit = [x for x in cand if x[1] in pl]
                    p_ok = bool(hit)
                    d_ok = d_ok or any(re.search(f, sec[x[0]:]) for x in hit for f in dforms)
                    o_ok = True
                else:
                    win_p = sec[max(0, p - WIN[2]): p + WIN[3]]
                    p_ok = bool(pl) and any(x in win_p for x in pl)
                    o_ok = True
                if strict is None:
                    strict = p_ok
                best["nearest_label_ok"] = best.get("nearest_label_ok", False) or bool(d_ok and strict)
                if d_ok and (p_ok or not pl) and o_ok:
                    best.update(date_ok=True, party_ok=p_ok, order_ok=True, passage=sec[max(0, p - 60): p + 160])
                    break
                best["date_ok"] |= d_ok
                best["party_ok"] |= p_ok
                best["passage"] = best["passage"] or sec[max(0, p - 60): p + 160]
            if best["passage"] and best["date_ok"] and best["order_ok"] and (best["party_ok"] or not pl):
                break
        ok = best["name_found"] and best["date_ok"] and (best["party_ok"] or not pl) and best["order_ok"]
        out.append({"conf_num": int(e.conf_num), "source": kind, "path": path, "term": int(e.term), "kind": e.kind,
                    "date": e.date, "name_raw": e.name_raw, "naas_cd": e.naas_cd, "name": nm_hg, "party": party,
                    "from_party": frm, "status": e.status, "item_caption": caption, "item_found": bool(secs),
                    "name_found_in_item": best["name_found"], "date_found_near_name": best["date_ok"],
                    "party_found_near_name": best["party_ok"], "switch_order_ok": best["order_ok"],
                    "pass": bool(ok), "pass_nearest_label": bool(ok and best.get("nearest_label_ok")),
                    "passage": best["passage"]})
    return pd.DataFrame(out)


# ----------------------------------------------------------------------------- main

def evaluate(write=True, parts=("spells", "snapshots", "coverage", "spot")):
    EVAL.mkdir(parents=True, exist_ok=True)
    rs = pt.Resolver()
    sp = rs.spells
    res = {}
    if "spells" in parts:
        res["spell_table"] = spell_table(sp)
        res["member_days_by_basis"] = member_days_by_basis(sp)
        res["spell_basis_counts"] = sp.groupby(["term", "basis"]).size().rename("n").reset_index()
        log = pd.read_parquet(pt.OUT / "event_log.parquet")
        res["event_status_counts"] = log.groupby(["term", "status"]).size().rename("n").reset_index()
        res["calendar_checks"] = pt.verify_calendar()
        tr = pd.read_parquet(pt.OUT / "party_transitions.parquet")
        nt = tr.rename(columns={"old": "old_party", "new": "new_party"}).assign(raw=lambda d: d.old_party + " -> " + d.new_party)
        res["lineage_checks"] = pt.verify_lineage(notices=nt)
    if "snapshots" in parts:
        agg, det = end_of_term_agreement(sp, rs)
        res["end_of_term_agreement"], res["end_of_term_disagreements"] = agg, det
    if "coverage" in parts:
        cov, rul, v9, meta = coverage(rs)
        res["coverage_by_method"], res["ruling_distribution"], res["v9_vs_v10_ruling"] = cov, rul, v9
        res["coverage_meta"] = pd.DataFrame([{"source": k, **v} for k, v in meta.items()])
    if "spot" in parts:
        res["spot_check"] = spot_check()
        res["spot_check_n100"] = spot_check(n=100)
        # negative controls on the same sample: each must fail when the date, the party or the
        # report item is wrong
        log = pd.read_parquet(pt.OUT / "event_log.parquet")
        rec = pd.read_parquet(pt.OUT / "report_records.parquet")
        names = sp.drop_duplicates("naas_cd").set_index("naas_cd").name.to_dict()
        evs = log[log.status.isin(APPLIED) & log.kind.isin(["leave", "join", "switch", "roster"])
                  & log.naas_cd.notna()].reset_index(drop=True)
        nc = []
        for n in (30, 100):
            idx = sorted(random.Random(SEED).sample(range(len(evs)), min(n, len(evs))))
            for pb in ("date", "party", "item"):
                c = check_events(evs.loc[idx], rec, names, perturb=pb)
                nc.append({"sample": n, "perturbation": pb, "checked": len(c), "passed": int(c["pass"].sum()),
                           "passed_nearest_label": int(c["pass_nearest_label"].sum())})
        res["spot_check_negative_control"] = pd.DataFrame(nc)
    if write:
        for k, v in res.items():
            v.to_csv(EVAL / f"{k}.csv", index=False)
        (EVAL / "eval_run.json").write_text(json.dumps(
            {"run_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "parts": list(parts),
             "inputs_mtime": {str(p.relative_to(pt.V10)): dt.datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds")
                              for p in [pt.OUT / "party_spells.parquet", LEG_EVAL / "xml_enriched.parquet",
                                        LEG_EVAL / "hwp_enriched.parquet", LEG_EVAL / "xlsx_enriched_keys.parquet"] if p.exists()}},
            indent=1))
    return res


if __name__ == "__main__":
    which = tuple(sys.argv[1:]) or ("spells", "snapshots", "coverage", "spot")
    r = evaluate(parts=which)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 40)
    pd.set_option("display.max_rows", 400)
    for k, v in r.items():
        if k in ("calendar_checks", "lineage_checks", "spell_basis_counts", "event_status_counts", "end_of_term_disagreements",
                 "spot_check", "spot_check_n100"):
            print(f"== {k}: {len(v)} rows (csv)")
            continue
        print(f"== {k}")
        print(v.to_string())
