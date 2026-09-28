"""Link-rate comparison: old minister panel (build/turns link columns) vs minister-data v2.0.0-rc1.

Read-only analysis for the minister-data session. The build is not changed.

New-panel rule (agreed interface, 2026-09-26/27):
  person   speaker_name (NFKC, spaces removed) equals spells.name or spells.name_hanja, or a
           person_name_variants.variant_string valid for that lineage and date
  lineage  the printed title (speaker_pos) or ministry_normalized resolved through
           ministry_alias.csv (alias valid on the speech date); '국무총리' -> pm
  date     spell_start - 1 day <= speech_date <= (spell_end or cutoff) + 1 day
  nominees nominations.csv (nominee, lineage, speech_date within 1 day of a hearing date)
  minister_acting turns are reported against acting_heads.csv, never linked to spells

Outputs: interim/external/minister_data_v2.0.0-rc1_check/
  turn_links.parquet   one row per government turn with old and new link results
  misses.csv           unlinked minister/pm/nominee turns grouped by person, title and reason
  rates.csv            old vs new link rates by term x role
"""
from __future__ import annotations

import datetime as dt
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

V10 = Path(__file__).resolve().parent.parent
import os
EXT = V10 / "interim" / "external" / os.environ.get("MINISTER_RELEASE", "minister_data_v2.0.0-rc1")
OUT = EXT.parent / (EXT.name + "_check")
CUTOFF = dt.date(2026, 9, 24)
# release layout since 2026-09-28 (build/release/turns); the earlier layout had build/turns
TURNS = str(V10 / "build" / "release" / "turns" / "*" / "*.parquet") if (V10 / "build" / "release" / "turns").exists() \
    else str(V10 / "build" / "turns" / "*" / "*.parquet")
ROLES = ("minister", "minister_acting", "minister_nominee", "prime_minister")
TITLE_STRIP = ("후보자", "직무대행", "직무대리", "候補者", "職務代行", "職務代理")


def nk(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return "".join(unicodedata.normalize("NFKC", str(s)).split())


def d(s):
    try:
        return dt.date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def load():
    sp = pd.read_csv(EXT / "spells.csv", dtype=str)
    al = pd.read_csv(EXT / "ministry_alias.csv", dtype=str)
    nv = pd.read_csv(EXT / "person_name_variants.csv", dtype=str)
    nm = pd.read_csv(EXT / "nominations.csv", dtype=str)
    ah = pd.read_csv(EXT / "acting_heads.csv", dtype=str)
    return sp, al, nv, nm, ah


def build_alias(al):
    idx = {}
    scoped = "person_id" in al.columns
    for r in al.itertuples(index=False):
        pid = getattr(r, "person_id", None) if scoped else None
        pid = pid if isinstance(pid, str) and pid else None
        idx.setdefault(nk(r.alias_string), []).append((d(r.valid_from) or dt.date.min, d(r.valid_to) or dt.date.max,
                                                       r.lineage, r.alias_type, pid))
    return idx


def resolve_lineage(idx, title, ministry, date, role, person_ids=frozenset()):
    if role == "prime_minister" or nk(title).startswith("국무총리") or nk(title).startswith("國務總理"):
        return "pm", "pm_title"
    cands = [nk(title)]
    t = nk(title)
    for suf in TITLE_STRIP:
        if t.endswith(nk(suf)):
            cands.append(t[: -len(nk(suf))])
    cands.append(nk(ministry))
    for c in cands:
        for lo, hi, lin, typ, pid in idx.get(c, []):
            if pid is not None and pid not in person_ids:
                continue                          # person-scoped alias (rc2): only for that person
            if lo <= date <= hi and lin and lin != "out_of_scope":
                return lin, f"alias:{typ}"
    return None, "lineage_unresolved"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    sp, al, nv, nm, ah = load()
    aidx = build_alias(al)
    sp["s"] = sp.spell_start.map(d)
    sp["e"] = sp.spell_end.map(d)
    by_name = {}
    for r in sp.itertuples(index=False):
        for key in {nk(r.name), nk(r.name_hanja)} - {""}:
            by_name.setdefault(key, []).append(r)
    var = {}
    for r in nv.itertuples(index=False):
        var.setdefault(nk(r.variant_string), []).append((r.lineage, d(r.valid_from) or dt.date.min,
                                                         d(r.valid_to) or dt.date.max, nk(r.name)))
    pids_by_name = {}
    for r in sp.itertuples(index=False):
        for key in {nk(r.name), nk(r.name_hanja)} - {""}:
            pids_by_name.setdefault(key, set()).add(r.person_id)
    acting_pm = []
    if "acting_for_lineage" in ah.columns:
        for r in ah[ah.acting_for_lineage == "pm"].to_dict("records"):
            acting_pm.append((nk(r["name"]), nk(r.get("name_hanja")), d(r["from"]), d(r["to"]), r["acting_id"]))
    noms = {}
    for r in nm.itertuples(index=False):
        hd = [d(x) for x in str(r.hearing_dates or "").replace(";", ",").replace("|", ",").split(",") if d(x)]
        noms.setdefault((nk(r.nominee), r.lineage), []).append((hd, r.nomination_id, r.spell_id, r.outcome))

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    t = con.execute(f"""SELECT conf_num, turn_seq, term, source, role, speaker_name, speaker_pos, speaker_label_raw,
        ministry_normalized, ministry_family, speech_date, minister_panel_id, link_method
        FROM read_parquet('{TURNS}') WHERE role IN {ROLES}""").df()
    out = []
    one_day = dt.timedelta(days=1)
    for r in t.itertuples(index=False):
        date = d(r.speech_date)
        name = nk(r.speaker_name)
        pids = frozenset(pids_by_name.get(name, set()))
        lin, how = resolve_lineage(aidx, r.speaker_pos, r.ministry_normalized, date, r.role, pids) if date else (None, "no_date")
        res = {"new_spell_id": None, "new_nomination_id": None, "new_status": None, "new_lineage": lin, "lineage_how": how,
               "nearest_spell": None, "days_outside": None}
        names = {name}
        for lin_v, lo, hi, canon in var.get(name, []):
            if (lin is None or lin_v == lin) and date and lo <= date <= hi:
                names.add(canon)
        acting_hit = None
        if r.role == "prime_minister" and "직무대행" in nk(r.speaker_pos) and date:
            for an, ahj, lo, hi, aid in acting_pm:
                if (name in (an, ahj)) and lo and lo <= date <= (hi or CUTOFF):
                    acting_hit = aid
        if acting_hit:
            res.update(new_status="acting_head", new_spell_id=acting_hit)
        elif r.role == "minister_acting":
            res["new_status"] = "acting_not_linked"
        elif r.role == "minister_nominee":
            hit = None
            for nmn in names:
                for hd, nid, sid, outc in noms.get((nmn, lin), []):
                    if any(abs((date - h).days) <= 1 for h in hd):
                        hit = (nid, sid)
            if hit:
                res.update(new_nomination_id=hit[0], new_spell_id=hit[1], new_status="nomination_hearing")
            else:
                res["new_status"] = "nominee_unlinked" if lin else "lineage_unresolved"
        else:
            cands = [s for nmn in names for s in by_name.get(nmn, [])]
            if not cands:
                res["new_status"] = "name_unknown"
            elif lin is None:
                res["new_status"] = "lineage_unresolved"
            else:
                same = [s for s in cands if s.lineage == lin]
                if not same:
                    res["new_status"] = "person_in_other_lineage"
                    res["nearest_spell"] = ";".join(sorted({f"{s.spell_id}[{s.spell_start}..{s.spell_end}]" for s in cands}))
                else:
                    inside = [s for s in same if s.s and s.s - one_day <= date <= ((s.e or CUTOFF) + one_day)]
                    if inside:
                        res.update(new_spell_id=inside[0].spell_id, new_status="spell")
                    else:
                        best = min(same, key=lambda s: min(abs((date - (s.s or date)).days), abs((date - (s.e or CUTOFF)).days)))
                        dd = (best.s - date).days if best.s and date < best.s else (date - (best.e or CUTOFF)).days
                        res.update(new_status="outside_spell", nearest_spell=f"{best.spell_id}[{best.spell_start}..{best.spell_end}]",
                                   days_outside=dd)
        out.append({**r._asdict(), **res})
    o = pd.DataFrame(out)
    o["old_linked"] = o.link_method.fillna("").str.split(":").str[0].isin(["tenure", "buffer", "nominee", "nominee_in_tenure"])
    o["new_linked"] = o.new_status.isin(["spell", "nomination_hearing", "acting_head"])
    o.to_parquet(OUT / "turn_links.parquet", index=False)
    rates = (o.groupby(["term", "role"]).agg(turns=("role", "size"), old_linked=("old_linked", "sum"), new_linked=("new_linked", "sum"))
             .reset_index())
    rates["old_rate"] = (rates.old_linked / rates.turns).round(4)
    rates["new_rate"] = (rates.new_linked / rates.turns).round(4)
    rates.to_csv(OUT / "rates.csv", index=False)
    miss = o[~o.new_linked & (o.role != "minister_acting")]
    g = (miss.groupby(["role", "new_status", "speaker_name", "speaker_pos", "new_lineage"], dropna=False)
         .agg(n_turns=("turn_seq", "size"), first_date=("speech_date", "min"), last_date=("speech_date", "max"),
              n_meetings=("conf_num", "nunique"), conf_nums=("conf_num", lambda s: ",".join(map(str, sorted(set(s))[:8]))),
              nearest_spell=("nearest_spell", "first"), max_days_outside=("days_outside", "max"),
              old_link=("link_method", lambda s: ",".join(sorted(set(map(str, s)))[:3])))
         .reset_index().sort_values("n_turns", ascending=False))
    g.to_csv(OUT / "misses.csv", index=False)
    acting = o[o.role == "minister_acting"].groupby(["speaker_name", "speaker_pos"]).agg(
        n_turns=("turn_seq", "size"), first_date=("speech_date", "min"), last_date=("speech_date", "max"),
        conf_nums=("conf_num", lambda s: ",".join(map(str, sorted(set(s))[:8])))).reset_index()
    acting.to_csv(OUT / "acting_turns.csv", index=False)
    print(rates.to_string())
    print(o.groupby(["role", "new_status"]).size().to_string())
    print("miss groups", len(g), "acting groups", len(acting))


if __name__ == "__main__":
    main()
