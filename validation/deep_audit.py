"""
Deep audit of kr-hearings-data beyond the 50-check validation suite.

Designed for maximum rigor before public release.
Memory-conscious: loads only needed columns per phase.

Usage:
    python3 validation/deep_audit.py --phase 1
    python3 validation/deep_audit.py --phase all
"""

import argparse
import gc
import json
import re
import sys
import warnings
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

warnings.filterwarnings("ignore", category=FutureWarning)

DATA_DIR = Path("/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed")
SPEECH_FILE = DATA_DIR / "all_speeches_16_22_v3.parquet"
DYAD_FILE = DATA_DIR / "dyads_16_22_v3.parquet"
REPORT_FILE = Path(__file__).parent / "deep_audit_report.json"

VALID_TERMS = {16, 17, 18, 19, 20, 21, 22}
LEG_ROLES = {"legislator", "chair"}
NONLEG_ROLES = {
    "minister", "minister_nominee", "minister_acting", "vice_minister",
    "prime_minister", "witness", "testifier", "expert_witness",
    "senior_bureaucrat", "other_official", "local_gov_head",
    "agency_head", "public_corp_head", "org_head", "mid_bureaucrat",
    "nominee", "military", "police", "financial_regulator",
    "audit_official", "election_official", "constitutional_court",
    "assembly_official", "independent_official", "private_sector",
    "research_head", "cultural_institution_head", "broadcasting",
    "cooperative_head",
}
EXCLUDED_ROLES = {"committee_staff", "other", "unknown"}

# Known ruling parties per term (majority party in National Assembly)
RULING_PARTIES_BY_TERM = {
    16: ["한나라당"],  # approximate
    17: ["열린우리당", "한나라당"],
    18: ["한나라당"],
    19: ["새누리당"],
    20: ["더불어민주당", "자유한국당"],
    21: ["더불어민주당"],
    22: ["국민의힘"],
}

TERM_DATE_RANGES = {
    16: ("2000-05-30", "2004-05-29"),
    17: ("2004-05-30", "2008-05-29"),
    18: ("2008-05-30", "2012-05-29"),
    19: ("2012-05-30", "2016-05-29"),
    20: ("2016-05-30", "2020-05-29"),
    21: ("2020-05-30", "2024-05-29"),
    22: ("2024-05-30", "2028-05-29"),
}


class AuditResult:
    def __init__(self):
        self.findings = []
        self.phase_name = ""

    def set_phase(self, name):
        self.phase_name = name
        print(f"\n{'=' * 70}")
        print(f"  PHASE: {name}")
        print(f"{'=' * 70}")

    def add(self, check_id, severity, title, detail=None, data=None):
        """severity: OK, INFO, WARN, ISSUE, CRITICAL"""
        icon = {
            "OK": "[OK]", "INFO": "[ii]", "WARN": "[!!]",
            "ISSUE": "[XX]", "CRITICAL": "[!!XX!!]",
        }[severity]
        entry = {
            "phase": self.phase_name,
            "check_id": check_id,
            "severity": severity,
            "title": title,
            "detail": detail,
        }
        if data is not None:
            entry["data"] = data
        self.findings.append(entry)
        print(f"  {icon} [{severity}] {check_id}: {title}")
        if detail:
            for line in detail.split("\n"):
                print(f"       {line}")

    def summary(self):
        counts = Counter(f["severity"] for f in self.findings)
        return dict(counts)

    def save(self, path):
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": self.summary(),
            "total_checks": len(self.findings),
            "findings": self.findings,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f"\nReport saved to {path}")


def load_speech_columns(cols):
    """Load specific columns from speeches parquet."""
    return pq.read_table(SPEECH_FILE, columns=cols).to_pandas()


def load_dyad_columns(cols):
    """Load specific columns from dyads parquet."""
    return pq.read_table(DYAD_FILE, columns=cols).to_pandas()


def parse_korean_date(d):
    """Parse Korean date strings with day-of-week suffixes."""
    d = str(d).strip()
    # Strip parenthetical suffix: (월), (화), ..., (月), (火), ...
    d = re.sub(r"\([^)]*\)$", "", d).strip()
    for fmt in ["%Y년%m월%d일", "%Y年%m月%d日", "%Y-%m-%d", "%Y%m%d"]:
        try:
            return datetime.strptime(d, fmt)
        except (ValueError, TypeError):
            continue
    return None


# ══════════════════════════════════════════════════════════════════════
# PHASE 1: Date Integrity (Full)
# ══════════════════════════════════════════════════════════════════════

def phase1_date_integrity(result):
    result.set_phase("1. Date Integrity (Full)")

    df = load_speech_columns(["meeting_id", "term", "date", "hearing_type"])
    n = len(df)

    # 1.1 Parse ALL dates (not sampled)
    print("  Parsing all dates...")
    df["parsed_date"] = df["date"].apply(parse_korean_date)
    parse_rate = df["parsed_date"].notna().mean() * 100
    unparsed = df[df["parsed_date"].isna()]
    n_unparsed = len(unparsed)

    if parse_rate == 100:
        result.add("date_parse_rate", "OK", f"100% dates parsed ({n:,} rows)")
    elif parse_rate >= 99:
        examples = unparsed["date"].value_counts().head(10)
        result.add("date_parse_rate", "WARN",
                    f"{parse_rate:.2f}% parsed, {n_unparsed:,} unparsed",
                    data={str(k): int(v) for k, v in examples.items()})
    else:
        examples = unparsed["date"].value_counts().head(20)
        result.add("date_parse_rate", "ISSUE",
                    f"Only {parse_rate:.2f}% parsed, {n_unparsed:,} unparsed",
                    data={str(k): int(v) for k, v in examples.items()})

    # 1.2 Date-term alignment (ALL rows)
    print("  Checking date-term alignment...")
    valid = df[df["parsed_date"].notna()].copy()
    valid["year"] = valid["parsed_date"].apply(lambda d: d.year)
    valid["month"] = valid["parsed_date"].apply(lambda d: d.month)

    out_of_range = []
    for term in VALID_TERMS:
        if term not in TERM_DATE_RANGES:
            continue
        start_str, end_str = TERM_DATE_RANGES[term]
        start_dt = datetime.strptime(start_str, "%Y-%m-%d")
        end_dt = datetime.strptime(end_str, "%Y-%m-%d")
        term_rows = valid[valid["term"] == term]
        # Allow 30-day buffer for edge cases (transition periods)
        from datetime import timedelta
        oor = term_rows[
            (term_rows["parsed_date"] < start_dt - timedelta(days=30)) |
            (term_rows["parsed_date"] > end_dt + timedelta(days=30))
        ]
        if len(oor) > 0:
            out_of_range.append({
                "term": term,
                "n_out": len(oor),
                "date_range": f"{oor['parsed_date'].min()} ~ {oor['parsed_date'].max()}",
                "expected": f"{start_str} ~ {end_str}",
                "examples": oor["date"].head(5).tolist(),
            })

    if not out_of_range:
        result.add("date_term_alignment", "OK",
                    f"All dates fall within expected term ranges (30-day buffer)")
    else:
        total_oor = sum(x["n_out"] for x in out_of_range)
        result.add("date_term_alignment", "ISSUE",
                    f"{total_oor:,} rows with dates outside expected term range",
                    data=out_of_range)

    # 1.3 Null/missing dates
    null_dates = df["date"].isna() | (df["date"].astype(str).str.strip() == "")
    n_null = null_dates.sum()
    if n_null == 0:
        result.add("date_null_check", "OK", "No null/empty dates")
    else:
        result.add("date_null_check", "ISSUE", f"{n_null:,} null/empty dates")

    # 1.4 Temporal coverage - monthly meeting counts
    print("  Analyzing temporal coverage...")
    if len(valid) > 0:
        valid["ym"] = valid["parsed_date"].apply(lambda d: f"{d.year}-{d.month:02d}")
        monthly = valid.groupby("ym")["meeting_id"].nunique().sort_index()

        # Check for long gaps (> 3 consecutive months with 0 meetings)
        all_months = pd.date_range(
            valid["parsed_date"].min().replace(day=1),
            valid["parsed_date"].max(),
            freq="MS",
        )
        all_ym = [f"{d.year}-{d.month:02d}" for d in all_months]
        gaps = []
        consecutive_empty = 0
        gap_start = None
        for ym in all_ym:
            if ym not in monthly.index or monthly[ym] == 0:
                if consecutive_empty == 0:
                    gap_start = ym
                consecutive_empty += 1
            else:
                if consecutive_empty >= 3:
                    gaps.append({"from": gap_start, "to": ym, "months": consecutive_empty})
                consecutive_empty = 0
                gap_start = None

        if not gaps:
            result.add("date_temporal_gaps", "OK",
                        "No gaps > 3 months in temporal coverage")
        else:
            result.add("date_temporal_gaps", "WARN",
                        f"{len(gaps)} temporal gaps > 3 months found",
                        data=gaps)

        # 1.5 Per-term date range check
        term_date_summary = []
        for term in sorted(VALID_TERMS):
            trows = valid[valid["term"] == term]
            if len(trows) > 0:
                term_date_summary.append({
                    "term": int(term),
                    "n_speeches": len(trows),
                    "n_meetings": int(trows["meeting_id"].nunique()),
                    "date_min": str(trows["parsed_date"].min().date()),
                    "date_max": str(trows["parsed_date"].max().date()),
                })
        result.add("date_term_summary", "INFO",
                    "Per-term date ranges",
                    data=term_date_summary)

    # 1.6 Duplicate dates for same meeting
    meeting_dates = df.groupby("meeting_id")["date"].nunique()
    multi_date = meeting_dates[meeting_dates > 1]
    if len(multi_date) == 0:
        result.add("date_meeting_consistency", "OK",
                    "Each meeting has exactly one date")
    else:
        examples = {}
        for mid in multi_date.index[:10]:
            dates = df[df["meeting_id"] == mid]["date"].unique().tolist()
            examples[str(mid)] = dates
        result.add("date_meeting_consistency", "ISSUE",
                    f"{len(multi_date):,} meetings have multiple dates",
                    data=examples)

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 2: member_id & Speaker Identity Integrity
# ══════════════════════════════════════════════════════════════════════

def phase2_identity_integrity(result):
    result.set_phase("2. member_id & Speaker Identity Integrity")

    df = load_speech_columns(["member_id", "person_name", "speaker", "role", "term", "meeting_id"])
    n = len(df)

    # Clean member_id
    df["mid_clean"] = df["member_id"].astype(str).str.strip()
    df.loc[df["mid_clean"].isin(["", "nan", "None", "NaN"]), "mid_clean"] = None
    has_mid = df["mid_clean"].notna()

    # 2.1 member_id format validation
    print("  Validating member_id format...")
    mid_vals = df.loc[has_mid, "mid_clean"].unique()
    non_numeric = [m for m in mid_vals if not re.match(r"^\d+(\.\d+)?$", str(m))]
    if not non_numeric:
        result.add("mid_format", "OK",
                    f"All {len(mid_vals):,} unique member_ids are numeric")
    else:
        result.add("mid_format", "WARN",
                    f"{len(non_numeric)} non-numeric member_ids found",
                    data=non_numeric[:20])

    # 2.2 member_id -> person_name consistency
    print("  Checking member_id -> person_name consistency...")
    mid_names = df[has_mid].groupby("mid_clean")["person_name"].apply(
        lambda x: list(x.dropna().unique())
    )
    inconsistent_mid = mid_names[mid_names.apply(len) > 1]
    if len(inconsistent_mid) == 0:
        result.add("mid_name_consistency", "OK",
                    f"All {len(mid_names):,} member_ids map to exactly one person_name")
    else:
        # Check if differences are trivial (spacing, etc.)
        truly_different = {}
        for mid, names in inconsistent_mid.items():
            # Normalize: strip whitespace
            normalized = set(n.strip() for n in names if n and str(n).strip())
            if len(normalized) > 1:
                truly_different[mid] = list(normalized)
        if not truly_different:
            result.add("mid_name_consistency", "OK",
                        f"All member_ids consistent (minor whitespace differences only)")
        else:
            result.add("mid_name_consistency", "ISSUE",
                        f"{len(truly_different)} member_ids map to multiple person_names",
                        data=dict(list(truly_different.items())[:30]))

    # 2.3 person_name -> member_id mapping (detect homonyms)
    print("  Checking person_name -> member_id mapping (homonym detection)...")
    name_mids = df[has_mid].groupby("person_name")["mid_clean"].apply(
        lambda x: sorted(set(x.dropna()))
    )
    multi_mid_names = name_mids[name_mids.apply(len) > 1]
    if len(multi_mid_names) == 0:
        result.add("name_mid_uniqueness", "OK",
                    "Each person_name maps to exactly one member_id")
    else:
        # This is expected (e.g., 김영환 as both legislator and minister)
        result.add("name_mid_uniqueness", "INFO",
                    f"{len(multi_mid_names)} names map to multiple member_ids (homonyms expected)",
                    data={k: v for k, v in list(multi_mid_names.items())[:20]})

    # 2.4 person_name extraction accuracy (sample check)
    print("  Spot-checking person_name extraction from speaker field...")
    sample = df.sample(min(2000, n), random_state=42)
    extraction_issues = []
    for _, row in sample.iterrows():
        speaker = str(row["speaker"])
        pname = str(row["person_name"]) if pd.notna(row["person_name"]) else ""
        if not pname or pname == "nan":
            continue
        # Person name should appear in the speaker field
        if pname not in speaker:
            extraction_issues.append({
                "speaker": speaker,
                "person_name": pname,
                "role": row["role"],
            })

    if len(extraction_issues) == 0:
        result.add("name_extraction_accuracy", "OK",
                    "All 2000 sampled person_names found in speaker field")
    elif len(extraction_issues) < 10:
        result.add("name_extraction_accuracy", "WARN",
                    f"{len(extraction_issues)}/2000 samples: person_name not in speaker field",
                    data=extraction_issues[:10])
    else:
        result.add("name_extraction_accuracy", "ISSUE",
                    f"{len(extraction_issues)}/2000 samples: person_name not in speaker field",
                    data=extraction_issues[:20])

    # 2.5 Empty person_name analysis
    empty_pname = df["person_name"].isna() | (df["person_name"].astype(str).str.strip().isin(["", "nan"]))
    n_empty = empty_pname.sum()
    if n_empty > 0:
        empty_df = df[empty_pname]
        speakers = empty_df["speaker"].value_counts().head(20)
        result.add("empty_person_name_detail", "WARN" if n_empty < 100 else "ISSUE",
                    f"{n_empty:,} rows with empty person_name",
                    data={str(k): int(v) for k, v in speakers.items()})
    else:
        result.add("empty_person_name_detail", "OK", "No empty person_names")

    # 2.6 Legislator cross-term tracking
    print("  Tracking legislators across terms...")
    leg_df = df[has_mid & df["role"].isin(LEG_ROLES)]
    mid_terms = leg_df.groupby("mid_clean")["term"].apply(lambda x: sorted(set(x)))

    # How many legislators span multiple terms?
    multi_term = mid_terms[mid_terms.apply(len) > 1]
    single_term = mid_terms[mid_terms.apply(len) == 1]
    max_terms = mid_terms.apply(len).max() if len(mid_terms) > 0 else 0

    result.add("legislator_cross_term", "INFO",
                f"{len(multi_term):,} legislators span multiple terms, "
                f"{len(single_term):,} single-term, max terms: {max_terms}",
                data={
                    "term_span_distribution": dict(mid_terms.apply(len).value_counts().sort_index()),
                    "total_unique_legislators": len(mid_terms),
                })

    # 2.7 Same member_id with different roles across meetings (should always be leg/chair)
    mid_roles = df[has_mid].groupby("mid_clean")["role"].apply(lambda x: set(x))
    non_leg_roles_by_mid = {}
    for mid, roles in mid_roles.items():
        non_leg = roles - LEG_ROLES
        if non_leg:
            non_leg_roles_by_mid[mid] = {
                "leg_roles": sorted(roles & LEG_ROLES),
                "non_leg_roles": sorted(non_leg),
            }
    if not non_leg_roles_by_mid:
        result.add("mid_role_consistency", "OK",
                    "All member_ids only have legislator/chair roles")
    else:
        result.add("mid_role_consistency", "ISSUE",
                    f"{len(non_leg_roles_by_mid)} member_ids have non-legislator roles",
                    data=dict(list(non_leg_roles_by_mid.items())[:20]))

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 3: Role Classification Deep Audit
# ══════════════════════════════════════════════════════════════════════

def phase3_role_classification(result):
    result.set_phase("3. Role Classification Deep Audit")

    df = load_speech_columns(["speaker", "role", "member_id", "person_name", "term", "meeting_id", "hearing_type"])
    n = len(df)

    # 3.1 "other" role deep analysis
    print("  Analyzing 'other' role classifications...")
    other_df = df[df["role"] == "other"]
    n_other = len(other_df)

    if n_other > 0:
        # Pattern analysis of "other" speakers
        speaker_patterns = other_df["speaker"].value_counts()
        top_50 = {str(k): int(v) for k, v in speaker_patterns.head(50).items()}

        # Could any be reclassified?
        reclassifiable = []
        for speaker, count in speaker_patterns.head(100).items():
            spk = str(speaker)
            candidate_role = None
            if "장관" in spk and "후보자" not in spk:
                candidate_role = "minister"
            elif "위원장" in spk:
                candidate_role = "chair"
            elif "차관" in spk:
                candidate_role = "vice_minister"
            elif "청장" in spk:
                candidate_role = "agency_head"
            elif "원장" in spk or "회장" in spk:
                candidate_role = "org_head"
            elif "사장" in spk:
                candidate_role = "public_corp_head"
            elif "교수" in spk:
                candidate_role = "expert_witness"
            elif spk.endswith("위원"):
                # Check if they have member_id
                has_mid = other_df[other_df["speaker"] == speaker]["member_id"].notna().any()
                candidate_role = "legislator" if has_mid else "expert_witness"
            elif "이사" in spk:
                candidate_role = "org_head"
            elif "감독" in spk or "선수" in spk:
                candidate_role = "private_sector"
            elif "국장" in spk or "실장" in spk:
                candidate_role = "senior_bureaucrat"

            if candidate_role:
                reclassifiable.append({
                    "speaker": spk,
                    "count": int(count),
                    "suggested_role": candidate_role,
                })

        result.add("other_role_analysis", "WARN",
                    f"{n_other:,} speeches classified as 'other'",
                    detail=f"Top 50 speakers listed. {len(reclassifiable)} potentially reclassifiable.",
                    data={"top_50_speakers": top_50, "reclassifiable": reclassifiable})
    else:
        result.add("other_role_analysis", "OK", "No 'other' role speeches")

    # 3.2 Role stability per speaker across meetings
    print("  Checking role stability per speaker...")
    # For non-legislator speakers, same person should usually have the same role
    nonleg_df = df[df["role"].isin(NONLEG_ROLES)]
    speaker_roles = nonleg_df.groupby("person_name")["role"].apply(lambda x: set(x))
    multi_role = speaker_roles[speaker_roles.apply(len) > 1]

    # Filter to frequent speakers only (>= 10 speeches)
    speaker_counts = nonleg_df["person_name"].value_counts()
    frequent_multi = {
        k: {"roles": sorted(v), "count": int(speaker_counts.get(k, 0))}
        for k, v in multi_role.items()
        if speaker_counts.get(k, 0) >= 10 and pd.notna(k) and str(k).strip()
    }

    if len(frequent_multi) == 0:
        result.add("role_stability_nonleg", "OK",
                    "All frequent non-legislator speakers have consistent roles")
    else:
        # Many of these are expected (e.g., 차관 promoted to 장관)
        result.add("role_stability_nonleg", "INFO",
                    f"{len(frequent_multi)} frequent non-legislator speakers have multiple roles",
                    detail="Expected for promotions (차관->장관), career changes, or homonyms",
                    data=dict(list(frequent_multi.items())[:30]))

    # 3.3 Chair vs legislator distribution per term
    print("  Checking chair/legislator ratio per term...")
    leg_all = df[df["role"].isin(LEG_ROLES)]
    chair_ratio = leg_all.groupby("term").apply(
        lambda x: (x["role"] == "chair").mean()
    )
    chair_data = {str(k): round(float(v), 4) for k, v in chair_ratio.items()}
    # Chairs are typically 10-20% of legislator speeches
    abnormal_terms = {k: v for k, v in chair_data.items() if v < 0.05 or v > 0.35}
    if not abnormal_terms:
        result.add("chair_ratio_per_term", "OK",
                    "Chair/legislator ratio is consistent across terms",
                    data=chair_data)
    else:
        result.add("chair_ratio_per_term", "WARN",
                    "Some terms have unusual chair/legislator ratios",
                    data={"ratios": chair_data, "abnormal": abnormal_terms})

    # 3.4 Role distribution per hearing type
    print("  Checking role distribution by hearing type...")
    ht_roles = df.groupby(["hearing_type", "role"]).size().unstack(fill_value=0)
    # Certain roles should appear more in audit (국정감사): minister, agency_head
    # Standing committee: more chair, committee_staff
    if "국정감사" in ht_roles.index and "상임위원회" in ht_roles.index:
        audit_pct = ht_roles.loc["국정감사"] / ht_roles.loc["국정감사"].sum()
        standing_pct = ht_roles.loc["상임위원회"] / ht_roles.loc["상임위원회"].sum()
        comparison = {}
        for role in ["minister", "chair", "committee_staff", "witness", "expert_witness"]:
            if role in audit_pct.index:
                comparison[role] = {
                    "audit_pct": round(float(audit_pct.get(role, 0)) * 100, 2),
                    "standing_pct": round(float(standing_pct.get(role, 0)) * 100, 2),
                }
        result.add("role_by_hearing_type", "INFO",
                    "Role distribution by hearing type",
                    data=comparison)

    # 3.5 Minister names validation (sample)
    print("  Validating minister speaker field patterns...")
    ministers = df[df["role"] == "minister"]
    # Ministers should have format like "국방부장관 이종섭"
    minister_patterns = Counter()
    minister_issues = []
    for _, row in ministers.sample(min(2000, len(ministers)), random_state=42).iterrows():
        spk = str(row["speaker"])
        if re.match(r".*장관\s+\S+", spk):
            minister_patterns["standard (부처장관 이름)"] += 1
        elif re.match(r".*장관$", spk):
            minister_patterns["no_name (부처장관)"] += 1
        elif "장관" in spk:
            minister_patterns["other_pattern"] += 1
            if len(minister_issues) < 10:
                minister_issues.append(spk)
        else:
            minister_patterns["no_장관_keyword"] += 1
            if len(minister_issues) < 10:
                minister_issues.append(spk)

    result.add("minister_pattern_check", "OK" if not minister_issues else "WARN",
                f"Minister speaker patterns (sample of {min(2000, len(ministers))})",
                data={"patterns": dict(minister_patterns), "issues": minister_issues})

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 4: Meeting-level Integrity (Full)
# ══════════════════════════════════════════════════════════════════════

def phase4_meeting_integrity(result):
    result.set_phase("4. Meeting-level Integrity (Exhaustive)")

    df = load_speech_columns([
        "meeting_id", "term", "committee", "committee_key",
        "hearing_type", "date", "speech_order", "role",
    ])

    meetings = df.groupby("meeting_id")

    # 4.1 Full speech_order gap check (ALL meetings)
    print("  Checking speech_order gaps in ALL meetings...")
    gap_meetings = []
    single_speech = []
    meeting_sizes = []

    for mid, group in meetings:
        size = len(group)
        meeting_sizes.append(size)

        if size == 1:
            single_speech.append(mid)
            continue

        try:
            orders = sorted(group["speech_order"].astype(int))
        except (ValueError, TypeError):
            continue

        expected = list(range(orders[0], orders[0] + len(orders)))
        if orders != expected:
            # Characterize the gap
            missing = set(expected) - set(orders)
            extra = set(orders) - set(expected)
            gap_meetings.append({
                "meeting_id": str(mid),
                "n_speeches": size,
                "n_missing": len(missing),
                "n_extra": len(extra),
                "order_range": f"{orders[0]}-{orders[-1]}",
            })

    n_total_meetings = len(meeting_sizes)
    result.add("meeting_order_gaps_full", "OK" if not gap_meetings else "WARN",
                f"{len(gap_meetings)}/{n_total_meetings:,} meetings have speech_order gaps",
                data=gap_meetings[:20] if gap_meetings else None)

    # 4.2 Single-speech meetings
    if single_speech:
        result.add("single_speech_meetings", "WARN" if len(single_speech) < 100 else "ISSUE",
                    f"{len(single_speech):,} meetings with only 1 speech",
                    data={"meeting_ids": [str(m) for m in single_speech[:20]]})
    else:
        result.add("single_speech_meetings", "OK", "No single-speech meetings")

    # 4.3 Meeting size distribution
    sizes = np.array(meeting_sizes)
    size_stats = {
        "min": int(sizes.min()),
        "q1": int(np.percentile(sizes, 25)),
        "median": int(np.median(sizes)),
        "q3": int(np.percentile(sizes, 75)),
        "max": int(sizes.max()),
        "mean": round(float(sizes.mean()), 1),
        "std": round(float(sizes.std()), 1),
    }
    # Flag extreme outliers (> Q3 + 3*IQR)
    iqr = size_stats["q3"] - size_stats["q1"]
    upper_fence = size_stats["q3"] + 3 * iqr
    outlier_meetings = []
    for mid, group in meetings:
        if len(group) > upper_fence:
            outlier_meetings.append({
                "meeting_id": str(mid),
                "size": len(group),
                "term": int(group["term"].iloc[0]),
                "committee": str(group["committee"].iloc[0]),
            })

    result.add("meeting_size_distribution", "INFO",
                f"Meeting size stats: median={size_stats['median']}, "
                f"max={size_stats['max']}, {len(outlier_meetings)} outliers (>{upper_fence:.0f})",
                data={"stats": size_stats, "outliers": outlier_meetings[:10]})

    # 4.4 Metadata consistency within meetings
    print("  Checking metadata consistency within meetings...")
    inconsistent = {"term": 0, "committee": 0, "committee_key": 0, "hearing_type": 0}
    inconsistent_examples = defaultdict(list)

    for field in ["term", "committee", "committee_key", "hearing_type"]:
        nunique = meetings[field].nunique()
        bad = nunique[nunique > 1]
        inconsistent[field] = len(bad)
        if len(bad) > 0:
            for mid in bad.index[:5]:
                vals = df[df["meeting_id"] == mid][field].unique().tolist()
                inconsistent_examples[field].append({"meeting_id": str(mid), "values": vals})

    all_consistent = all(v == 0 for v in inconsistent.values())
    if all_consistent:
        result.add("meeting_metadata_consistency", "OK",
                    "All meetings have consistent term/committee/hearing_type")
    else:
        result.add("meeting_metadata_consistency", "ISSUE",
                    f"Inconsistent metadata within meetings: {inconsistent}",
                    data=dict(inconsistent_examples))

    # 4.5 One-sided meetings (only leg or only non-leg speeches)
    print("  Checking for one-sided meetings...")
    def check_sides(group):
        roles = set(group["role"])
        has_leg = bool(roles & LEG_ROLES)
        has_nonleg = bool(roles & NONLEG_ROLES)
        return has_leg, has_nonleg

    one_sided = {"leg_only": 0, "nonleg_only": 0, "both": 0, "neither": 0}
    one_sided_examples = {"leg_only": [], "nonleg_only": []}
    for mid, group in meetings:
        has_leg, has_nonleg = check_sides(group)
        if has_leg and has_nonleg:
            one_sided["both"] += 1
        elif has_leg:
            one_sided["leg_only"] += 1
            if len(one_sided_examples["leg_only"]) < 5:
                one_sided_examples["leg_only"].append(str(mid))
        elif has_nonleg:
            one_sided["nonleg_only"] += 1
            if len(one_sided_examples["nonleg_only"]) < 5:
                one_sided_examples["nonleg_only"].append(str(mid))
        else:
            one_sided["neither"] += 1

    result.add("meeting_sidedness", "INFO",
                f"Both sides: {one_sided['both']:,}, "
                f"Leg only: {one_sided['leg_only']:,}, "
                f"Non-leg only: {one_sided['nonleg_only']:,}",
                data={"counts": one_sided, "examples": one_sided_examples})

    # 4.6 Per-term meeting counts
    term_meetings = df.groupby("term")["meeting_id"].nunique()
    term_data = {str(k): int(v) for k, v in term_meetings.items()}
    result.add("meetings_per_term", "INFO", "Meeting counts per term", data=term_data)

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 5: Text Quality Deep Dive
# ══════════════════════════════════════════════════════════════════════

def phase5_text_quality(result):
    result.set_phase("5. Text Quality Deep Dive")

    # Load text + role + meeting_id for analysis
    df = load_speech_columns(["speech_text", "role", "meeting_id", "speech_order", "term"])
    n = len(df)

    # 5.1 Encoding issues detection
    print("  Scanning for encoding issues...")
    # Common mojibake patterns
    mojibake_patterns = [
        r"â€™",  # UTF-8 interpreted as Latin-1
        r"â€œ",
        r"â€\x9d",
        r"Ã¤|Ã¶|Ã¼",  # German umlauts (unlikely in Korean text)
        r"\x00",  # null bytes
        r"\\u[0-9a-f]{4}",  # escaped unicode
        r"\ufffd",  # replacement character
    ]
    encoding_issues = {}
    text_series = df["speech_text"].astype(str)
    for pattern in mojibake_patterns:
        try:
            matches = text_series.str.contains(pattern, regex=True, na=False)
            count = matches.sum()
            if count > 0:
                encoding_issues[pattern] = int(count)
        except re.error:
            pass

    if not encoding_issues:
        result.add("text_encoding", "OK", "No mojibake or encoding issues detected")
    else:
        result.add("text_encoding", "ISSUE",
                    f"Encoding issues found in {sum(encoding_issues.values()):,} rows",
                    data=encoding_issues)

    # 5.2 HTML/XML remnants
    print("  Checking for HTML/XML remnants...")
    html_pattern = r"<[a-zA-Z/][^>]*>"
    html_matches = text_series.str.contains(html_pattern, regex=True, na=False)
    n_html = html_matches.sum()
    if n_html == 0:
        result.add("text_html_remnants", "OK", "No HTML/XML tags found in text")
    else:
        examples = df.loc[html_matches, "speech_text"].head(5).tolist()
        examples = [str(e)[:200] for e in examples]
        result.add("text_html_remnants", "WARN",
                    f"{n_html:,} speeches contain HTML/XML tags",
                    data={"examples": examples})

    # 5.3 Text length distribution by role
    print("  Analyzing text length distribution by role...")
    df["text_len"] = text_series.str.len()
    role_stats = df.groupby("role")["text_len"].agg(["mean", "median", "min", "max", "count"])
    role_stats_dict = {}
    for role, row in role_stats.iterrows():
        role_stats_dict[str(role)] = {
            "mean": round(float(row["mean"]), 1),
            "median": float(row["median"]),
            "min": int(row["min"]),
            "max": int(row["max"]),
            "count": int(row["count"]),
        }
    result.add("text_length_by_role", "INFO",
                "Text length statistics by role",
                data=role_stats_dict)

    # 5.4 Extremely long texts (possible concatenation errors)
    print("  Checking for extremely long texts...")
    p999 = df["text_len"].quantile(0.999)
    very_long = df[df["text_len"] > p999]
    max_len = df["text_len"].max()
    result.add("text_extreme_length", "INFO",
                f"99.9th percentile: {p999:.0f} chars, max: {max_len:,} chars, "
                f"{len(very_long):,} texts above 99.9th pct",
                data={
                    "top_10_lengths": df.nlargest(10, "text_len")[
                        ["meeting_id", "speech_order", "role", "text_len"]
                    ].to_dict("records"),
                })

    # 5.5 Suspiciously short texts analysis
    print("  Analyzing short texts...")
    short = df[df["text_len"] < 10]
    if len(short) > 0:
        short_values = short["speech_text"].value_counts().head(30)
        short_by_role = short.groupby("role").size().to_dict()
        result.add("text_short_analysis", "INFO",
                    f"{len(short):,} texts under 10 chars",
                    data={
                        "top_30_short_texts": {str(k): int(v) for k, v in short_values.items()},
                        "by_role": {str(k): int(v) for k, v in short_by_role.items()},
                    })

    # 5.6 Duplicate texts within same meeting
    print("  Checking for duplicate texts within meetings...")
    # This would flag copy-paste or processing errors
    meeting_text_dups = 0
    dup_examples = []
    for mid, group in df.groupby("meeting_id"):
        texts = group["speech_text"].astype(str)
        # Only check non-trivial texts (> 30 chars)
        long_texts = texts[texts.str.len() > 30]
        if len(long_texts) != len(long_texts.drop_duplicates()):
            dup_count = len(long_texts) - len(long_texts.drop_duplicates())
            meeting_text_dups += 1
            if len(dup_examples) < 10:
                dup_text_vals = long_texts[long_texts.duplicated(keep=False)].value_counts().head(3)
                dup_examples.append({
                    "meeting_id": str(mid),
                    "n_dup_texts": dup_count,
                    "examples": [str(t)[:100] for t in dup_text_vals.index],
                })

    if meeting_text_dups == 0:
        result.add("text_intra_meeting_dups", "OK",
                    "No duplicate texts (>30 chars) within any meeting")
    else:
        result.add("text_intra_meeting_dups", "WARN",
                    f"{meeting_text_dups:,} meetings have duplicate texts (>30 chars)",
                    data=dup_examples)

    # 5.7 Concatenation artifact detection
    print("  Checking for concatenation artifacts...")
    # The original data has 발언내용1-7 concatenated. Check for:
    # - Double spaces (join artifact)
    # - Text starting with space
    double_space = text_series.str.contains(r"  ", regex=False, na=False)
    starts_space = text_series.str.startswith(" ", na=False)
    n_double = double_space.sum()
    n_starts = starts_space.sum()

    result.add("text_concat_artifacts", "WARN" if n_double > n * 0.01 else "OK",
                f"Double spaces: {n_double:,} ({n_double/n*100:.2f}%), "
                f"Leading spaces: {n_starts:,} ({n_starts/n*100:.2f}%)")

    # 5.8 Non-Korean character analysis
    print("  Checking for non-Korean text content...")
    # Sample-based check for texts that are predominantly non-Korean
    sample = df.sample(min(5000, n), random_state=42)
    non_korean = []
    for _, row in sample.iterrows():
        text = str(row["speech_text"])
        if len(text) < 5:
            continue
        # Count Korean characters
        korean_chars = len(re.findall(r"[가-힣]", text))
        total_chars = len(text.strip())
        if total_chars > 20 and korean_chars / total_chars < 0.1:
            non_korean.append({
                "meeting_id": str(row["meeting_id"]),
                "text_preview": text[:150],
                "korean_ratio": round(korean_chars / total_chars, 3),
            })

    if not non_korean:
        result.add("text_language_check", "OK",
                    "All sampled texts contain substantial Korean content")
    else:
        result.add("text_language_check", "WARN",
                    f"{len(non_korean)}/5000 sampled texts are predominantly non-Korean",
                    data=non_korean[:10])

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 6: Committee Harmonization Deep Check
# ══════════════════════════════════════════════════════════════════════

def phase6_committee_harmonization(result):
    result.set_phase("6. Committee Harmonization Deep Check")

    df = load_speech_columns(["committee", "committee_key", "term", "hearing_type", "meeting_id"])

    # 6.1 Full raw -> key mapping table
    print("  Building complete committee mapping table...")
    mapping = df.groupby(["committee", "committee_key"]).size().reset_index(name="count")
    mapping_dict = {}
    for _, row in mapping.iterrows():
        key = str(row["committee_key"])
        raw = str(row["committee"])
        if key not in mapping_dict:
            mapping_dict[key] = {}
        mapping_dict[key][raw] = int(row["count"])

    result.add("committee_full_mapping", "INFO",
                f"{len(mapping)} unique (raw, key) pairs across {len(mapping_dict)} keys",
                data=mapping_dict)

    # 6.2 Committee presence by term
    print("  Checking committee presence by term...")
    term_comms = df.groupby(["term", "committee_key"])["meeting_id"].nunique().unstack(fill_value=0)
    term_comm_dict = {}
    for term in sorted(term_comms.index):
        active = term_comms.loc[term]
        active_keys = sorted(active[active > 0].index.tolist())
        term_comm_dict[str(term)] = {
            "n_committees": len(active_keys),
            "committees": active_keys,
        }

    result.add("committee_by_term", "INFO",
                "Committee presence by term",
                data=term_comm_dict)

    # 6.3 Committees that span unexpected terms
    # education_science should only be in terms where that committee existed (18-19)
    # education_culture only in 20+
    expected_term_ranges = {
        "education_science": {18, 19},
        "education_culture": {20, 21, 22},
        "culture_media": {18, 19},
    }
    term_issues = []
    for key, expected_terms in expected_term_ranges.items():
        if key in term_comms.columns:
            actual_terms = set(term_comms.index[term_comms[key] > 0].astype(int))
            unexpected = actual_terms - expected_terms
            if unexpected:
                term_issues.append({
                    "committee_key": key,
                    "expected_terms": sorted(expected_terms),
                    "actual_terms": sorted(actual_terms),
                    "unexpected": sorted(unexpected),
                })

    if not term_issues:
        result.add("committee_term_range", "OK",
                    "Term-specific committees appear only in expected terms")
    else:
        result.add("committee_term_range", "WARN",
                    f"{len(term_issues)} committees appear in unexpected terms",
                    data=term_issues)

    # 6.4 Subcommittee handling
    print("  Checking subcommittee handling...")
    subcomm = df[df["committee"].astype(str).str.contains("소위|예결|법안심사|인사청문", na=False)]
    n_sub = len(subcomm)
    sub_keys = subcomm["committee_key"].value_counts()
    result.add("subcommittee_mapping", "INFO",
                f"{n_sub:,} subcommittee speeches mapped to {len(sub_keys)} keys",
                data={str(k): int(v) for k, v in sub_keys.items()})

    # 6.5 Committee-hearing_type cross-tab
    print("  Building committee-hearing_type cross-tab...")
    ht_cross = df.groupby(["committee_key", "hearing_type"])["meeting_id"].nunique().unstack(fill_value=0)
    cross_dict = {}
    for key in ht_cross.index:
        cross_dict[str(key)] = {
            str(col): int(ht_cross.loc[key, col]) for col in ht_cross.columns
        }
    result.add("committee_hearing_type_cross", "INFO",
                "Committee x hearing_type meeting counts",
                data=cross_dict)

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 7: Dyad Formation Exhaustive Check
# ══════════════════════════════════════════════════════════════════════

def phase7_dyad_exhaustive(result):
    result.set_phase("7. Dyad Formation Exhaustive Check")

    # Load speech metadata (no text to save memory)
    print("  Loading speech metadata for dyad verification...")
    speeches = load_speech_columns([
        "meeting_id", "speech_order", "role", "person_name", "speaker",
    ])

    print("  Loading dyad metadata...")
    dyads = load_dyad_columns([
        "meeting_id", "leg_name", "witness_name", "witness_role",
        "direction", "leg_speaker_raw", "witness_speaker_raw",
    ])

    # 7.1 Full dyad formation spot-check (ALL meetings)
    print("  Full dyad formation spot-check (ALL meetings)...")
    speech_meetings = speeches.groupby("meeting_id")
    dyad_counts_by_meeting = dyads.groupby("meeting_id").size()

    mismatches = []
    n_checked = 0
    for mid, group in speech_meetings:
        n_checked += 1
        group = group.copy()
        group["_so_num"] = pd.to_numeric(group["speech_order"], errors="coerce")
        group = group.dropna(subset=["_so_num"]).sort_values("_so_num")
        rows = group.to_dict("records")

        expected = 0
        for i in range(len(rows) - 1):
            cr = rows[i]["role"]
            nr = rows[i + 1]["role"]
            if (cr in LEG_ROLES and nr in NONLEG_ROLES) or \
               (cr in NONLEG_ROLES and nr in LEG_ROLES):
                expected += 1

        actual = dyad_counts_by_meeting.get(mid, 0)
        if expected != actual:
            mismatches.append({
                "meeting_id": str(mid),
                "expected": expected,
                "actual": int(actual),
                "diff": int(actual) - expected,
                "n_speeches": len(group),
            })

        if n_checked % 2000 == 0:
            print(f"    Checked {n_checked:,} meetings...")

    mismatch_rate = len(mismatches) / n_checked * 100 if n_checked > 0 else 0
    if len(mismatches) == 0:
        result.add("dyad_formation_full_check", "OK",
                    f"ALL {n_checked:,} meetings pass dyad formation check")
    elif mismatch_rate < 1:
        result.add("dyad_formation_full_check", "WARN",
                    f"{len(mismatches)}/{n_checked:,} meetings ({mismatch_rate:.2f}%) "
                    f"have dyad count mismatch",
                    data=mismatches[:20])
    else:
        result.add("dyad_formation_full_check", "ISSUE",
                    f"{len(mismatches)}/{n_checked:,} meetings ({mismatch_rate:.2f}%) "
                    f"have dyad count mismatch",
                    data=mismatches[:30])

    # 7.2 Self-pairing deep analysis
    print("  Analyzing self-pairing cases...")
    self_pairs = dyads[dyads["leg_name"] == dyads["witness_name"]]
    n_self = len(self_pairs)

    if n_self > 0:
        # Are these truly same-person or homonyms?
        self_detail = []
        for _, row in self_pairs.head(20).iterrows():
            self_detail.append({
                "meeting_id": str(row["meeting_id"]),
                "name": str(row["leg_name"]),
                "witness_role": str(row["witness_role"]),
                "leg_speaker_raw": str(row["leg_speaker_raw"]),
                "witness_speaker_raw": str(row["witness_speaker_raw"]),
            })

        # Check if same raw speaker fields (truly same person) or different
        truly_same = self_pairs["leg_speaker_raw"] == self_pairs["witness_speaker_raw"]
        result.add("self_pairing_analysis", "WARN",
                    f"{n_self:,} self-pairings: {truly_same.sum()} truly same speaker, "
                    f"{(~truly_same).sum()} different speakers (homonyms)",
                    data=self_detail)
    else:
        result.add("self_pairing_analysis", "OK", "No self-pairing dyads")

    # 7.3 Direction balance per meeting
    print("  Checking direction balance per meeting...")
    dir_balance = dyads.groupby("meeting_id")["direction"].value_counts().unstack(fill_value=0)
    if "question" in dir_balance.columns and "answer" in dir_balance.columns:
        dir_balance["ratio"] = dir_balance["question"] / (dir_balance["question"] + dir_balance["answer"])
        extreme = dir_balance[(dir_balance["ratio"] < 0.2) | (dir_balance["ratio"] > 0.8)]
        # Filter to meetings with at least 10 dyads
        total_dyads = dir_balance["question"] + dir_balance["answer"]
        extreme = extreme[total_dyads[extreme.index] >= 10]

        if len(extreme) == 0:
            result.add("dyad_direction_balance_per_meeting", "OK",
                        "No meetings with extreme direction imbalance (>10 dyads)")
        else:
            result.add("dyad_direction_balance_per_meeting", "INFO",
                        f"{len(extreme):,} meetings with extreme Q/A imbalance "
                        f"(ratio <0.2 or >0.8, >=10 dyads)")

    # 7.4 Dyad meetings without speeches
    dyad_meeting_ids = set(dyads["meeting_id"].unique())
    speech_meeting_ids = set(speeches["meeting_id"].unique())
    orphan_dyads = dyad_meeting_ids - speech_meeting_ids
    if not orphan_dyads:
        result.add("dyad_orphan_meetings", "OK",
                    "All dyad meeting_ids have corresponding speeches")
    else:
        result.add("dyad_orphan_meetings", "CRITICAL",
                    f"{len(orphan_dyads)} dyad meeting_ids not in speeches",
                    data=list(str(x) for x in list(orphan_dyads)[:20]))

    # 7.5 Witness role distribution in dyads vs speeches
    print("  Comparing witness role distributions...")
    dyad_roles = dyads["witness_role"].value_counts()
    speech_nonleg = speeches[speeches["role"].isin(NONLEG_ROLES)]["role"].value_counts()

    role_comparison = {}
    for role in sorted(set(dyad_roles.index) | set(speech_nonleg.index)):
        d_count = int(dyad_roles.get(role, 0))
        s_count = int(speech_nonleg.get(role, 0))
        if s_count > 0:
            ratio = d_count / s_count
        else:
            ratio = float("inf") if d_count > 0 else 0
        role_comparison[str(role)] = {
            "dyad_count": d_count,
            "speech_count": s_count,
            "ratio": round(ratio, 3) if ratio != float("inf") else "inf",
        }

    result.add("dyad_vs_speech_role_distribution", "INFO",
                "Witness role: dyad count vs speech count",
                data=role_comparison)

    del speeches, dyads
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 8: Extra Columns Validation
# ══════════════════════════════════════════════════════════════════════

def phase8_extra_columns(result):
    result.set_phase("8. Extra Columns Validation (party, gender, seniority, etc.)")

    # Check what extra columns exist
    schema = pq.read_schema(SPEECH_FILE)
    all_cols = [f.name for f in schema]
    extra_cols = [c for c in all_cols if c in [
        "gender", "party", "ruling_status", "seniority",
        "naas_cd", "name_clean", "session", "sub_session",
    ]]
    result.add("extra_cols_present", "INFO",
                f"Extra columns found: {extra_cols}")

    if not extra_cols:
        return

    df = load_speech_columns(extra_cols + ["member_id", "role", "term", "meeting_id"])

    # Clean member_id
    df["mid_clean"] = df["member_id"].astype(str).str.strip()
    df.loc[df["mid_clean"].isin(["", "nan", "None", "NaN"]), "mid_clean"] = None
    has_mid = df["mid_clean"].notna()

    # 8.1 Gender validation
    if "gender" in df.columns:
        print("  Validating gender column...")
        gender_vals = df["gender"].value_counts(dropna=False)
        gender_dict = {str(k): int(v) for k, v in gender_vals.items()}
        result.add("gender_values", "INFO",
                    f"Gender value distribution",
                    data=gender_dict)

        # Gender consistency per member_id
        if has_mid.any():
            mid_gender = df[has_mid].groupby("mid_clean")["gender"].apply(
                lambda x: set(x.dropna().unique())
            )
            multi_gender = mid_gender[mid_gender.apply(len) > 1]
            if len(multi_gender) == 0:
                result.add("gender_consistency", "OK",
                            "Gender is consistent per member_id")
            else:
                result.add("gender_consistency", "ISSUE",
                            f"{len(multi_gender)} member_ids have inconsistent gender",
                            data={k: sorted(v) for k, v in list(multi_gender.items())[:10]})

    # 8.2 Party validation
    if "party" in df.columns:
        print("  Validating party column...")
        party_vals = df[has_mid]["party"].value_counts(dropna=False).head(30)
        party_dict = {str(k): int(v) for k, v in party_vals.items()}
        result.add("party_values", "INFO",
                    f"Top 30 party values (for legislator speeches)",
                    data=party_dict)

        # Party should only be filled for legislators
        nonleg_with_party = df[~df["role"].isin(LEG_ROLES) & df["party"].notna() &
                                (df["party"].astype(str).str.strip() != "")]
        if len(nonleg_with_party) == 0:
            result.add("party_leg_only", "OK",
                        "Party is only filled for legislator/chair roles")
        else:
            n_nonleg_party = len(nonleg_with_party)
            result.add("party_leg_only", "WARN",
                        f"{n_nonleg_party:,} non-legislator rows have party values",
                        data=nonleg_with_party["role"].value_counts().head(10).to_dict())

    # 8.3 Ruling status validation
    if "ruling_status" in df.columns:
        print("  Validating ruling_status column...")
        rs_vals = df["ruling_status"].value_counts(dropna=False)
        rs_dict = {str(k): int(v) for k, v in rs_vals.items()}
        result.add("ruling_status_values", "INFO",
                    "Ruling status distribution",
                    data=rs_dict)

    # 8.4 Seniority validation
    if "seniority" in df.columns:
        print("  Validating seniority column...")
        sen = df[has_mid]["seniority"]
        sen_clean = pd.to_numeric(sen, errors="coerce")
        sen_stats = {
            "min": float(sen_clean.min()) if sen_clean.notna().any() else None,
            "max": float(sen_clean.max()) if sen_clean.notna().any() else None,
            "mean": round(float(sen_clean.mean()), 2) if sen_clean.notna().any() else None,
            "null_pct": round(float(sen_clean.isna().mean()) * 100, 2),
        }
        # Seniority should be 1-10ish
        if sen_stats["max"] and sen_stats["max"] > 15:
            result.add("seniority_range", "WARN",
                        f"Seniority range seems high: {sen_stats}",
                        data=sen_stats)
        else:
            result.add("seniority_range", "OK",
                        f"Seniority range valid: {sen_stats}",
                        data=sen_stats)

    # 8.5 Session/sub_session validation
    if "session" in df.columns:
        print("  Validating session column...")
        sess_vals = df.groupby("term")["session"].agg(["min", "max", "nunique"])
        sess_dict = {}
        for term, row in sess_vals.iterrows():
            sess_dict[str(term)] = {
                "min": str(row["min"]),
                "max": str(row["max"]),
                "n_unique": int(row["nunique"]),
            }
        result.add("session_per_term", "INFO",
                    "Session range per term",
                    data=sess_dict)

    # 8.6 naas_cd validation (National Assembly member code)
    if "naas_cd" in df.columns:
        print("  Validating naas_cd column...")
        naas = df[has_mid]["naas_cd"]
        naas_clean = naas.dropna().astype(str).str.strip()
        naas_clean = naas_clean[naas_clean != ""]
        n_unique_naas = naas_clean.nunique()
        n_unique_mid = df[has_mid]["mid_clean"].nunique()

        # naas_cd should be consistent with member_id
        mid_naas = df[has_mid & naas.notna()].groupby("mid_clean")["naas_cd"].apply(
            lambda x: set(x.dropna().unique())
        )
        multi_naas = mid_naas[mid_naas.apply(len) > 1]
        if len(multi_naas) == 0:
            result.add("naas_cd_consistency", "OK",
                        f"naas_cd consistent per member_id ({n_unique_naas} unique codes)")
        else:
            result.add("naas_cd_consistency", "WARN",
                        f"{len(multi_naas)} member_ids have multiple naas_cd values",
                        data={k: sorted(str(x) for x in v) for k, v in list(multi_naas.items())[:10]})

    del df
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 9: Dyad Text Alignment
# ══════════════════════════════════════════════════════════════════════

def phase9_dyad_text_alignment(result):
    result.set_phase("9. Dyad Text Alignment Verification")

    # This is memory-intensive. Load in batches.
    print("  Loading speech texts for alignment check...")
    speeches = load_speech_columns([
        "meeting_id", "speech_order", "speech_text", "role", "person_name",
    ])

    print("  Loading dyad texts...")
    dyads = load_dyad_columns([
        "meeting_id", "leg_name", "witness_name", "leg_speech",
        "witness_speech", "direction",
    ])

    # Sample-based alignment check (full check would be too expensive)
    print("  Running alignment check on 500 sampled meetings...")
    meeting_ids = dyads["meeting_id"].unique()
    rng = np.random.RandomState(42)
    sample_mids = rng.choice(meeting_ids, min(500, len(meeting_ids)), replace=False)

    alignment_ok = 0
    alignment_fail = 0
    fail_examples = []

    for mid in sample_mids:
        m_speeches = speeches[speeches["meeting_id"] == mid].copy()
        m_speeches["_so_num"] = pd.to_numeric(m_speeches["speech_order"], errors="coerce")
        m_speeches = m_speeches.dropna(subset=["_so_num"]).sort_values("_so_num")
        m_dyads = dyads[dyads["meeting_id"] == mid]

        if len(m_dyads) == 0:
            continue

        # Rebuild expected dyads from speeches
        rows = m_speeches.to_dict("records")
        expected_dyads = []
        for i in range(len(rows) - 1):
            curr = rows[i]
            nxt = rows[i + 1]
            cr = curr["role"]
            nr = nxt["role"]
            if cr in LEG_ROLES and nr in NONLEG_ROLES:
                expected_dyads.append({
                    "leg_text": str(curr["speech_text"]),
                    "wit_text": str(nxt["speech_text"]),
                    "direction": "question",
                })
            elif cr in NONLEG_ROLES and nr in LEG_ROLES:
                expected_dyads.append({
                    "leg_text": str(nxt["speech_text"]),
                    "wit_text": str(curr["speech_text"]),
                    "direction": "answer",
                })

        # Compare
        if len(expected_dyads) != len(m_dyads):
            # Count mismatch - already checked in phase 7
            continue

        # The dyad file preserves insertion order (built sequentially per meeting)
        # Reset index and use positional order
        actual_rows = m_dyads.reset_index(drop=True).head(5).to_dict("records")
        for j, (exp, act) in enumerate(zip(expected_dyads[:5], actual_rows)):
            leg_match = str(act["leg_speech"]).strip() == exp["leg_text"].strip()
            wit_match = str(act["witness_speech"]).strip() == exp["wit_text"].strip()
            dir_match = act["direction"] == exp["direction"]

            if leg_match and wit_match and dir_match:
                alignment_ok += 1
            else:
                alignment_fail += 1
                if len(fail_examples) < 10:
                    fail_examples.append({
                        "meeting_id": str(mid),
                        "dyad_index": j,
                        "leg_match": leg_match,
                        "wit_match": wit_match,
                        "dir_match": dir_match,
                        "dir_expected": exp["direction"],
                        "dir_actual": act["direction"],
                        "leg_text_exp_head": exp["leg_text"][:80],
                        "leg_text_act_head": str(act["leg_speech"])[:80],
                    })

    total_checked = alignment_ok + alignment_fail
    if alignment_fail == 0:
        result.add("dyad_text_alignment", "OK",
                    f"{alignment_ok}/{total_checked} sampled dyad texts align perfectly")
    elif alignment_fail < total_checked * 0.01:
        result.add("dyad_text_alignment", "WARN",
                    f"{alignment_fail}/{total_checked} dyads have text misalignment",
                    data=fail_examples)
    else:
        result.add("dyad_text_alignment", "ISSUE",
                    f"{alignment_fail}/{total_checked} dyads have text misalignment",
                    data=fail_examples)

    del speeches, dyads
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# PHASE 10: Statistical Sanity Checks
# ══════════════════════════════════════════════════════════════════════

def phase10_statistical_sanity(result):
    result.set_phase("10. Statistical Sanity Checks")

    df = load_speech_columns([
        "meeting_id", "term", "committee_key", "hearing_type",
        "role", "date", "speech_order",
    ])

    # 10.1 Benford's law on meeting_id first digits
    print("  Benford's law check on meeting_id...")
    first_digits = df["meeting_id"].astype(str).str[0]
    fd_dist = first_digits.value_counts(normalize=True).sort_index()
    benford_expected = {str(d): np.log10(1 + 1/d) for d in range(1, 10)}

    benford_comparison = {}
    for d in range(1, 10):
        d_str = str(d)
        observed = float(fd_dist.get(d_str, 0))
        expected = benford_expected[d_str]
        benford_comparison[d_str] = {
            "observed": round(observed, 4),
            "expected": round(expected, 4),
            "diff": round(observed - expected, 4),
        }

    result.add("benford_meeting_id", "INFO",
                "Benford's law check on meeting_id first digits",
                detail="Meeting IDs are assigned sequentially, so deviations are expected",
                data=benford_comparison)

    # 10.2 Speeches per legislator distribution
    print("  Speeches per legislator distribution...")
    leg_df = df[df["role"].isin(LEG_ROLES)]
    # Approximate: use person_name since we don't have member_id loaded
    # Actually, let's load member_id
    del df
    gc.collect()

    df2 = load_speech_columns(["member_id", "role", "term", "meeting_id"])
    df2["mid_clean"] = df2["member_id"].astype(str).str.strip()
    df2.loc[df2["mid_clean"].isin(["", "nan", "None", "NaN"]), "mid_clean"] = None
    has_mid = df2["mid_clean"].notna()

    leg_speeches_per_member = df2[has_mid].groupby("mid_clean").size()
    if len(leg_speeches_per_member) > 0:
        stats = {
            "n_legislators": len(leg_speeches_per_member),
            "min": int(leg_speeches_per_member.min()),
            "q1": int(leg_speeches_per_member.quantile(0.25)),
            "median": int(leg_speeches_per_member.median()),
            "q3": int(leg_speeches_per_member.quantile(0.75)),
            "max": int(leg_speeches_per_member.max()),
            "mean": round(float(leg_speeches_per_member.mean()), 1),
        }
        # Top 10 most active
        top10 = leg_speeches_per_member.nlargest(10)
        stats["top_10"] = {str(k): int(v) for k, v in top10.items()}

        result.add("speeches_per_legislator", "INFO",
                    f"Speeches per legislator: median={stats['median']}, max={stats['max']}",
                    data=stats)

    # 10.3 Meetings per term-committee (detect sparse cells)
    print("  Checking for sparse term-committee cells...")
    term_comm = df2.groupby(["term", "meeting_id"]).size().reset_index()
    meetings_per_tc = term_comm.groupby("term")["meeting_id"].nunique()
    result.add("meetings_per_term_check", "INFO",
                "Meetings per term",
                data={str(k): int(v) for k, v in meetings_per_tc.items()})

    # 10.4 22nd Assembly completeness
    print("  Checking 22nd Assembly completeness...")
    t22 = df2[df2["term"] == 22]
    n_t22 = len(t22)
    n_meetings_t22 = t22["meeting_id"].nunique()
    # 22nd Assembly started 2024-05-30, data should cover up to late 2024
    result.add("term22_completeness", "INFO",
                f"22nd Assembly: {n_t22:,} speeches, {n_meetings_t22:,} meetings",
                detail="22nd Assembly started 2024-05-30. Check if coverage matches expectation.")

    # 10.5 Intelligence committee size check
    del df2
    gc.collect()

    df3 = load_speech_columns(["committee_key", "term", "meeting_id"])
    intel = df3[df3["committee_key"] == "intelligence"]
    n_intel = len(intel)
    n_intel_meetings = intel["meeting_id"].nunique()
    result.add("intelligence_committee", "INFO",
                f"Intelligence committee: {n_intel:,} speeches, {n_intel_meetings:,} meetings",
                detail="정보위원회 is typically smaller due to classified proceedings")

    # 10.6 Hearing type ratio per term
    ht_ratio = df3.groupby("term")["committee_key"].apply(lambda x: len(x))
    # Actually need hearing_type
    del df3
    gc.collect()

    df4 = load_speech_columns(["term", "hearing_type"])
    ht_term = df4.groupby(["term", "hearing_type"]).size().unstack(fill_value=0)
    if "국정감사" in ht_term.columns and "상임위원회" in ht_term.columns:
        ht_term["audit_ratio"] = ht_term["국정감사"] / (ht_term["국정감사"] + ht_term["상임위원회"])
        ht_ratio_dict = {str(k): round(float(v), 4) for k, v in ht_term["audit_ratio"].items()}
        result.add("audit_ratio_per_term", "INFO",
                    "국정감사 ratio per term",
                    data=ht_ratio_dict)

    del df4
    gc.collect()


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

PHASES = {
    1: ("Date Integrity", phase1_date_integrity),
    2: ("Identity Integrity", phase2_identity_integrity),
    3: ("Role Classification", phase3_role_classification),
    4: ("Meeting Integrity", phase4_meeting_integrity),
    5: ("Text Quality", phase5_text_quality),
    6: ("Committee Harmonization", phase6_committee_harmonization),
    7: ("Dyad Formation", phase7_dyad_exhaustive),
    8: ("Extra Columns", phase8_extra_columns),
    9: ("Dyad Text Alignment", phase9_dyad_text_alignment),
    10: ("Statistical Sanity", phase10_statistical_sanity),
}


def main():
    parser = argparse.ArgumentParser(description="Deep audit of kr-hearings-data")
    parser.add_argument("--phase", type=str, default="all",
                        help="Phase number (1-10) or 'all'")
    args = parser.parse_args()

    result = AuditResult()

    print("=" * 70)
    print("  DEEP AUDIT: kr-hearings-data")
    print(f"  Speeches: {SPEECH_FILE}")
    print(f"  Dyads: {DYAD_FILE}")
    print(f"  Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    if args.phase == "all":
        phases_to_run = sorted(PHASES.keys())
    else:
        phases_to_run = [int(p.strip()) for p in args.phase.split(",")]

    for p in phases_to_run:
        if p not in PHASES:
            print(f"Unknown phase: {p}")
            continue
        name, func = PHASES[p]
        try:
            func(result)
        except Exception as e:
            result.add(f"phase{p}_error", "CRITICAL",
                        f"Phase {p} ({name}) failed: {e}")
            import traceback
            traceback.print_exc()
        gc.collect()

    # Final summary
    summary = result.summary()
    print(f"\n{'=' * 70}")
    print(f"  DEEP AUDIT SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Total checks:  {len(result.findings)}")
    for sev in ["OK", "INFO", "WARN", "ISSUE", "CRITICAL"]:
        count = summary.get(sev, 0)
        if count > 0:
            print(f"  {sev:>10}: {count}")

    result.save(REPORT_FILE)

    # Return exit code
    if summary.get("CRITICAL", 0) > 0:
        sys.exit(2)
    elif summary.get("ISSUE", 0) > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
