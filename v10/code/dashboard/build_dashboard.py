"""Build the kr-hearings-data summary dashboard (docs/index.html).

The page is a summary of one release: counts and compositions of meetings, speaker turns and
legislator-witness dyads, plus browsable lists of meetings, legislators, government bodies,
ministers and confirmation hearings. It holds no speech text. Each meeting links to the
official minutes of the National Assembly.

Inputs: the release tables (meetings.parquet, turns/tNN/*.parquet, dyads/tNN/*.parquet,
MANIFEST.json, validation_report.json) and the minister-data v2 tables spells.csv and
nominations.csv (for minister names, offices and nomination outcomes).

Usage (from the repository root):
    python3 v10/code/dashboard/build_dashboard.py --version v10.2
        [--release v10/build/release] [--minister v10/interim/external/minister_data_v2.0.0]
        [--out docs/index.html]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
TYPE_ORDER = ["상임위원회", "국정감사", "특별위원회", "예산결산특별위원회", "인사청문특별위원회", "국정조사",
              "국회본회의", "전원위원회"]
VIEWER = "https://record.assembly.go.kr/assembly/viewer/minutes/xml.do?id={}&type=view"
PDF = "https://record.assembly.go.kr/assembly/viewer/minutes/download/pdf.do?id={}"


def _records(df: pd.DataFrame) -> list:
    """Rows as lists, with NaN / NA written as null."""
    out = df.astype(object).where(df.notna(), None)
    return out.values.tolist()


def build(release: Path, minister: Path, version: str) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    con.execute(f"CREATE VIEW m AS SELECT * FROM read_parquet('{release / 'meetings.parquet'}')")
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{release}/turns/*/*.parquet', union_by_name=true)")
    con.execute(f"CREATE VIEW d AS SELECT * FROM read_parquet('{release}/dyads/*/*.parquet', union_by_name=true)")
    manifest = json.loads((release / "MANIFEST.json").read_text(encoding="utf-8"))
    validation = json.loads((release / "validation_report.json").read_text(encoding="utf-8"))
    run = manifest.get("run") or {}

    # meetings, with their turn count
    con.execute("CREATE TEMP TABLE nt AS SELECT conf_num, count(*) n FROM t GROUP BY 1")
    meet = con.execute("""
        SELECT m.conf_num, m.date, m.term, m.hearing_type, m.committee_raw, m.subcommittee, m.is_subcommittee,
               m.audit_team, m.is_confirmation_hearing, m.source, m.title, m.duplicate_of, coalesce(nt.n, 0) n_turns
        FROM m LEFT JOIN nt USING (conf_num) ORDER BY m.date, m.conf_num""").df()
    types = [x for x in TYPE_ORDER if x in set(meet.hearing_type)] + \
        sorted(set(meet.hearing_type.dropna()) - set(TYPE_ORDER))
    comms = sorted(set(meet.committee_raw.dropna()))
    t_idx = {x: i for i, x in enumerate(types)}
    c_idx = {x: i for i, x in enumerate(comms)}
    meet["title_short"] = meet.title.fillna("").str.replace(r"^제\d+대국회\s*", "", regex=True)
    meetings = [[int(r.conf_num), r.date, int(r.term), t_idx.get(r.hearing_type),
                 c_idx.get(r.committee_raw), r.subcommittee if isinstance(r.subcommittee, str) else None,
                 r.audit_team if isinstance(r.audit_team, str) else None,
                 1 if r.is_confirmation_hearing else 0, int(r.n_turns),
                 "x" if r.source == "xml" else "h", r.title_short,
                 1 if isinstance(r.duplicate_of, (int, float)) and pd.notna(r.duplicate_of) else 0]
                for r in meet.itertuples(index=False)]

    by_type = con.execute("""
        WITH mm AS (SELECT term, hearing_type, count(*) meetings FROM m GROUP BY ALL),
             tt AS (SELECT m.term, m.hearing_type, count(*) turns FROM t JOIN m USING (conf_num) GROUP BY ALL),
             dd AS (SELECT term, hearing_type, count(*) dyads FROM d GROUP BY ALL)
        SELECT mm.term, mm.hearing_type, mm.meetings, coalesce(tt.turns, 0) turns, coalesce(dd.dyads, 0) dyads
        FROM mm LEFT JOIN tt USING (term, hearing_type) LEFT JOIN dd USING (term, hearing_type)
        ORDER BY 1, 2""").df()
    monthly = con.execute("""SELECT substr(date, 1, 7) ym, hearing_type, count(*) n FROM m
                             WHERE date IS NOT NULL GROUP BY ALL ORDER BY 1, 2""").df()
    groups = con.execute("""SELECT term, role_group, count(*) n FROM t GROUP BY ALL ORDER BY 1, 2""").df()
    ruling = con.execute("""SELECT term, coalesce(ruling_status, 'unknown') status, count(*) n FROM t
                            WHERE role_group = 'legislator' GROUP BY ALL ORDER BY 1, 2""").df()
    roles = con.execute("""SELECT role_group, role, count(*) n FROM t GROUP BY ALL ORDER BY n DESC""").df()

    # legislators (legislator-role turns with a linked member code)
    leg = con.execute("""
        WITH x AS (SELECT t.naas_cd, t.leg_name_hangul, t.term, t.conf_num, t.party, t.speech_date, m.committee_key
                   FROM t JOIN m USING (conf_num) WHERE t.role_group = 'legislator' AND t.naas_cd IS NOT NULL),
             base AS (SELECT naas_cd, mode(leg_name_hangul) AS leg_name, list(DISTINCT term ORDER BY term) AS terms,
                             count(*) AS turns, count(DISTINCT conf_num) AS meetings, arg_max(party, speech_date) AS last_party
                      FROM x GROUP BY 1),
             ck AS (SELECT naas_cd, committee_key, count(*) n,
                           row_number() OVER (PARTITION BY naas_cd ORDER BY count(*) DESC, committee_key) rk
                    FROM x WHERE committee_key IS NOT NULL GROUP BY 1, 2),
             tops AS (SELECT naas_cd, string_agg(committee_key, ' · ' ORDER BY rk) AS comms FROM ck WHERE rk <= 3 GROUP BY 1),
             dy AS (SELECT leg_naas_cd naas_cd, count(*) dyads FROM d WHERE leg_naas_cd IS NOT NULL GROUP BY 1)
        SELECT base.*, tops.comms, coalesce(dy.dyads, 0) AS dyads FROM base LEFT JOIN tops USING (naas_cd)
        LEFT JOIN dy USING (naas_cd) ORDER BY turns DESC""").df()
    legislators = [[r.naas_cd, r.leg_name, "·".join(str(int(x)) for x in r.terms), int(r.turns), int(r.meetings),
                    int(r.dyads), r.last_party if isinstance(r.last_party, str) else None,
                    r.comms if isinstance(r.comms, str) else None] for r in leg.itertuples(index=False)]

    # government bodies (non-legislator turns with a normalized organisation)
    org = con.execute("""
        WITH x AS (SELECT ministry_normalized org, count(*) turns, count(DISTINCT conf_num) meetings,
                          min(substr(speech_date, 1, 4)) y0, max(substr(speech_date, 1, 4)) y1
                   FROM t WHERE role_group = 'nonlegislator' AND ministry_normalized IS NOT NULL GROUP BY 1),
             dy AS (SELECT wit_ministry_normalized org, count(*) dyads FROM d
                    WHERE wit_ministry_normalized IS NOT NULL GROUP BY 1)
        SELECT x.*, coalesce(dy.dyads, 0) dyads FROM x LEFT JOIN dy USING (org) ORDER BY turns DESC""").df()
    orgs = _records(org[["org", "turns", "meetings", "dyads", "y0", "y1"]])

    # ministers and prime ministers (turns linked to a minister-data appointment spell)
    spells = pd.read_csv(minister / "spells.csv", dtype=str)
    noms = pd.read_csv(minister / "nominations.csv", dtype=str)
    mn = con.execute("""
        SELECT minister_spell_id spell_id, count(*) turns, count(DISTINCT conf_num) meetings,
               min(speech_date) d0, max(speech_date) d1, sum(CASE WHEN dual_office THEN 1 ELSE 0 END) dual_turns
        FROM t WHERE role IN ('minister', 'prime_minister') AND minister_spell_id IS NOT NULL
        GROUP BY 1""").df()
    mn = mn.merge(spells[["spell_id", "name", "office_title", "spell_start", "spell_end"]], on="spell_id", how="left")
    mn = mn.sort_values("turns", ascending=False)
    mn["dual_turns"] = mn["dual_turns"].fillna(0).astype(int)
    ministers = _records(mn[["name", "office_title", "spell_start", "spell_end", "turns", "meetings", "dual_turns",
                             "d0", "d1"]])

    # confirmation hearings
    hr = con.execute("""
        WITH nomi AS (SELECT conf_num, mode(coalesce(gov_link_name, speaker_name)) nominee,
                             mode(minister_nomination_id) nid
                      FROM t WHERE role IN ('nominee', 'minister_nominee') GROUP BY 1)
        SELECT m.conf_num, m.date, m.term, m.hearing_type, m.committee_raw, nomi.nominee, nomi.nid,
               coalesce(nt.n, 0) n_turns
        FROM m LEFT JOIN nomi USING (conf_num) LEFT JOIN nt USING (conf_num)
        WHERE m.is_confirmation_hearing ORDER BY m.date, m.conf_num""").df()
    hr = hr.merge(noms[["nomination_id", "office_title", "outcome"]].rename(columns={"nomination_id": "nid"}),
                  on="nid", how="left")
    hearings = _records(hr[["conf_num", "date", "term", "hearing_type", "committee_raw", "nominee",
                            "office_title", "outcome", "n_turns"]])

    # committee_key -> the latest name of its standing (non-subcommittee) meetings, for display
    key_labels = dict(con.execute("""SELECT committee_key, arg_max(committee_raw, date) FROM m
        WHERE committee_key IS NOT NULL AND NOT coalesce(is_subcommittee, false) GROUP BY 1""").fetchall())

    # 국정감사: meetings by term and committee, and team sittings
    audit = con.execute("""SELECT term, committee_key, count(*) n, count(audit_team) teams FROM m
                           WHERE hearing_type = '국정감사' GROUP BY ALL ORDER BY 1, 2""").df()

    dates = meet.date.dropna()
    counts = {"meetings": int(len(meet)), "turns": int(manifest["row_counts"]["turns"]),
              "dyads": int(manifest["row_counts"]["dyads"]), "legislators": int(len(leg)),
              "orgs": int(len(org)), "ministers": int(len(mn)), "hearings": int(len(hr)),
              "date_min": dates.min(), "date_max": dates.max()}
    return {
        "meta": {"version": version, "run_id": run.get("run_id"), "built": manifest.get("generated_at"),
                 "validation": validation.get("summary"), "counts": counts,
                 "viewer": VIEWER, "pdf": PDF},
        "types": types, "comms": comms, "key_labels": key_labels,
        "by_type": _records(by_type), "monthly": _records(monthly), "groups": _records(groups),
        "ruling": _records(ruling), "roles": _records(roles),
        "meetings": meetings, "legislators": legislators, "orgs": orgs, "ministers": ministers,
        "hearings": hearings, "audit": _records(audit),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, help="release tag, e.g. v10.2")
    ap.add_argument("--release", type=Path, default=REPO / "v10" / "build" / "release")
    ap.add_argument("--minister", type=Path, default=REPO / "v10" / "interim" / "external" / "minister_data_v2.0.0")
    ap.add_argument("--out", type=Path, default=REPO / "docs" / "index.html")
    a = ap.parse_args()
    data = build(a.release, a.minister, a.version)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    html = (HERE / "template.html").read_text(encoding="utf-8").replace("/*__DATA__*/null", payload)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(html, encoding="utf-8")
    c = data["meta"]["counts"]
    print(f"{a.out}: {len(html.encode('utf-8')) / 1e6:.1f} MB, {c['meetings']:,} meetings, "
          f"{len(data['legislators']):,} legislators, {len(data['orgs']):,} bodies, {len(data['ministers']):,} ministers, "
          f"{len(data['hearings']):,} hearings")


if __name__ == "__main__":
    main()
