"""
Comprehensive validation of the Korean National Assembly hearings dataset.

Checks:
1. Schema & types
2. Row counts & completeness
3. Speaker classification integrity
4. Dyad formation correctness
5. Committee harmonization coverage
6. Date integrity
7. Meeting-level consistency
8. Text quality
9. Duplicates
10. Cross-dataset consistency (speeches <-> dyads)

Usage:
    python validation/validate_dataset.py --data-dir /path/to/processed/
    python validation/validate_dataset.py --data-dir /path/to/processed/ --report validation/report.json
"""

import argparse
import json
import sys
import warnings
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ── Expected schema ──
SPEECH_REQUIRED_COLS = [
    "meeting_id",
    "term",
    "committee",
    "committee_key",
    "hearing_type",
    "date",
    "agenda",
    "speaker",
    "member_id",
    "speech_order",
    "role",
    "person_name",
    "affiliation_raw",
    "speech_text",
]

DYAD_REQUIRED_COLS = [
    "meeting_id",
    "term",
    "committee",
    "committee_key",
    "hearing_type",
    "date",
    "agenda",
    "leg_name",
    "leg_speaker_raw",
    "witness_name",
    "witness_speaker_raw",
    "witness_role",
    "witness_affiliation",
    "direction",
    "leg_speech",
    "witness_speech",
]

# ── Valid values ──
VALID_TERMS = {16, 17, 18, 19, 20, 21, 22}
VALID_HEARING_TYPES = {"상임위원회", "국정감사"}
VALID_DIRECTIONS = {"question", "answer"}

LEGISLATOR_ROLES = {"legislator", "chair"}
NONLEGISLATOR_ROLES = {
    "minister",
    "minister_nominee",
    "minister_acting",
    "vice_minister",
    "prime_minister",
    "witness",
    "testifier",
    "expert_witness",
    "senior_bureaucrat",
    "other_official",
    "local_gov_head",
    "agency_head",
    "public_corp_head",
    "org_head",
    "mid_bureaucrat",
    "nominee",
    "military",
    "police",
    "financial_regulator",
    "audit_official",
    "election_official",
    "constitutional_court",
    "assembly_official",
    "independent_official",
    "private_sector",
    "research_head",
    "cultural_institution_head",
    "broadcasting",
    "cooperative_head",
}
ALL_KNOWN_ROLES = LEGISLATOR_ROLES | NONLEGISLATOR_ROLES | {
    "committee_staff",
    "other",
    "unknown",
    "mid_bureaucrat",
}

VALID_COMMITTEE_KEYS = {
    "foreign_affairs",
    "defense",
    "finance",
    "education",
    "education_science",
    "education_culture",
    "science_ict",
    "agriculture",
    "industry",
    "health_welfare",
    "environment_labor",
    "land_transport",
    "public_admin",
    "judiciary",
    "political_affairs",
    "assembly_operations",
    "intelligence",
    "gender_family",
    "culture",
    "culture_media",
    "other",
}

# ── Term date ranges (approximate) ──
TERM_YEAR_RANGES = {
    16: (2000, 2004),
    17: (2004, 2008),
    18: (2008, 2012),
    19: (2012, 2016),
    20: (2016, 2020),
    21: (2020, 2024),
    22: (2024, 2028),
}


class ValidationResult:
    """Collect pass/fail/warn results."""

    def __init__(self):
        self.checks = []

    def add(self, name, status, message, details=None):
        """status: PASS, FAIL, WARN"""
        entry = {"name": name, "status": status, "message": message}
        if details:
            entry["details"] = details
        self.checks.append(entry)
        icon = {"PASS": "\u2705", "FAIL": "\u274c", "WARN": "\u26a0\ufe0f"}[status]
        print(f"  {icon} [{status}] {name}: {message}")

    def summary(self):
        counts = Counter(c["status"] for c in self.checks)
        return {
            "total": len(self.checks),
            "pass": counts.get("PASS", 0),
            "fail": counts.get("FAIL", 0),
            "warn": counts.get("WARN", 0),
        }

    def to_dict(self):
        return {"summary": self.summary(), "checks": self.checks}


def validate_schema(df, required_cols, name, result):
    """Check required columns exist."""
    missing = set(required_cols) - set(df.columns)
    extra = set(df.columns) - set(required_cols)
    if missing:
        result.add(
            f"{name}_schema_missing",
            "FAIL",
            f"Missing columns: {sorted(missing)}",
        )
    else:
        result.add(f"{name}_schema_required", "PASS", f"All {len(required_cols)} required columns present")
    if extra:
        result.add(
            f"{name}_schema_extra",
            "WARN",
            f"{len(extra)} extra columns: {sorted(extra)}",
        )


def validate_speeches(speeches, result):
    """Validate the all_speeches parquet."""
    print("\n=== SPEECH DATASET VALIDATION ===")
    n = len(speeches)
    result.add("speech_row_count", "PASS" if n > 0 else "FAIL", f"{n:,} rows")

    # 1. Schema
    validate_schema(speeches, SPEECH_REQUIRED_COLS, "speech", result)

    # 2. Terms
    terms = set(speeches["term"].dropna().unique())
    # term may be string
    terms_int = set()
    for t in terms:
        try:
            terms_int.add(int(t))
        except (ValueError, TypeError):
            pass
    missing_terms = VALID_TERMS - terms_int
    if missing_terms:
        result.add("speech_terms_missing", "FAIL", f"Missing terms: {sorted(missing_terms)}")
    else:
        result.add("speech_terms_complete", "PASS", f"All 7 terms present (16-22)")

    # Per-term counts
    term_counts = speeches.groupby("term").size()
    details = {str(k): int(v) for k, v in term_counts.items()}
    result.add("speech_term_distribution", "PASS", f"Per-term counts available", details)

    # 3. Hearing types
    ht = set(speeches["hearing_type"].dropna().unique())
    if ht == VALID_HEARING_TYPES:
        result.add("speech_hearing_types", "PASS", f"Both hearing types present")
    else:
        result.add("speech_hearing_types", "WARN", f"Hearing types found: {ht}")

    ht_counts = speeches["hearing_type"].value_counts().to_dict()
    result.add(
        "speech_hearing_distribution",
        "PASS",
        f"Standing: {ht_counts.get('상임위원회', 0):,}, Audit: {ht_counts.get('국정감사', 0):,}",
    )

    # 4. Speaker roles
    roles = set(speeches["role"].dropna().unique())
    unknown_roles = roles - ALL_KNOWN_ROLES
    if unknown_roles:
        result.add("speech_roles_unknown", "WARN", f"Unknown roles: {sorted(unknown_roles)}")
    else:
        result.add("speech_roles_valid", "PASS", f"All {len(roles)} roles are known categories")

    role_counts = speeches["role"].value_counts()
    leg_count = role_counts.get("legislator", 0) + role_counts.get("chair", 0)
    nonleg_count = n - leg_count
    leg_pct = leg_count / n * 100
    result.add(
        "speech_role_balance",
        "PASS" if 30 < leg_pct < 70 else "WARN",
        f"Legislators: {leg_count:,} ({leg_pct:.1f}%), Non-legislators: {nonleg_count:,} ({100 - leg_pct:.1f}%)",
    )

    # 5. Committee keys
    comm_keys = set(speeches["committee_key"].dropna().unique())
    unknown_comms = comm_keys - VALID_COMMITTEE_KEYS
    if unknown_comms:
        result.add("speech_committees_unknown", "WARN", f"Unknown committee keys: {sorted(unknown_comms)}")
    else:
        result.add("speech_committees_valid", "PASS", f"All {len(comm_keys)} committee keys are valid")

    other_count = (speeches["committee_key"] == "other").sum()
    other_pct = other_count / n * 100
    if other_pct > 5:
        result.add("speech_committees_other", "WARN", f"{other_count:,} speeches ({other_pct:.1f}%) mapped to 'other'")
    else:
        result.add("speech_committees_other", "PASS", f"Only {other_count:,} ({other_pct:.1f}%) mapped to 'other'")

    # 6. member_id vs role consistency
    has_mid = speeches["member_id"].notna() & (speeches["member_id"].astype(str).str.strip() != "") & (speeches["member_id"].astype(str) != "nan")
    mid_but_nonleg = has_mid & (~speeches["role"].isin(LEGISLATOR_ROLES))
    if mid_but_nonleg.sum() > 0:
        examples = speeches.loc[mid_but_nonleg, ["speaker", "member_id", "role"]].head(10).to_dict("records")
        result.add(
            "speech_memberid_role_mismatch",
            "FAIL",
            f"{mid_but_nonleg.sum():,} rows have member_id but non-legislator role",
            details=examples,
        )
    else:
        result.add("speech_memberid_role_consistency", "PASS", "All rows with member_id are classified as legislator/chair")

    no_mid_but_leg = ~has_mid & speeches["role"].isin(LEGISLATOR_ROLES)
    no_mid_leg_count = no_mid_but_leg.sum()
    if no_mid_leg_count > 0:
        # This is expected: some legislators are identified by title suffix (위원)
        pct = no_mid_leg_count / leg_count * 100
        result.add(
            "speech_leg_without_memberid",
            "WARN" if pct > 30 else "PASS",
            f"{no_mid_leg_count:,} legislators ({pct:.1f}%) without member_id (title-based classification)",
        )

    # 7. Date validation
    def parse_date(d):
        """Attempt to parse date strings in various Korean formats."""
        d = str(d).strip()
        for fmt in ["%Y년%m월%d일", "%Y年%m月%d日", "%Y-%m-%d", "%Y%m%d"]:
            try:
                return datetime.strptime(d, fmt)
            except (ValueError, TypeError):
                continue
        return None

    date_sample = speeches["date"].dropna().sample(min(10000, len(speeches)), random_state=42)
    parsed = date_sample.apply(parse_date)
    parse_rate = parsed.notna().mean() * 100
    if parse_rate < 80:
        unparsed = date_sample[parsed.isna()].head(10).tolist()
        result.add(
            "speech_date_parsing",
            "WARN",
            f"Only {parse_rate:.1f}% of sampled dates parse correctly",
            details={"unparsed_examples": unparsed},
        )
    else:
        result.add("speech_date_parsing", "PASS", f"{parse_rate:.1f}% of sampled dates parse correctly")

    # Check dates fall within term ranges
    valid_dates = parsed.dropna()
    if len(valid_dates) > 0:
        years = valid_dates.apply(lambda d: d.year)
        date_df = pd.DataFrame({"term": speeches.loc[date_sample.index, "term"], "year": years}).dropna()
        out_of_range = 0
        for _, row in date_df.iterrows():
            try:
                t = int(row["term"])
            except (ValueError, TypeError):
                continue
            yr = int(row["year"])
            if t in TERM_YEAR_RANGES:
                lo, hi = TERM_YEAR_RANGES[t]
                if yr < lo - 1 or yr > hi + 1:
                    out_of_range += 1
        oor_pct = out_of_range / len(date_df) * 100
        if oor_pct > 5:
            result.add("speech_date_term_range", "WARN", f"{out_of_range} ({oor_pct:.1f}%) dates outside expected term range")
        else:
            result.add("speech_date_term_range", "PASS", f"Only {out_of_range} ({oor_pct:.1f}%) dates outside expected range")

    # 8. Empty/missing text
    empty_text = speeches["speech_text"].isna() | (speeches["speech_text"].astype(str).str.strip() == "")
    empty_count = empty_text.sum()
    empty_pct = empty_count / n * 100
    result.add(
        "speech_empty_text",
        "WARN" if empty_pct > 1 else "PASS",
        f"{empty_count:,} ({empty_pct:.2f}%) speeches with empty text",
    )

    # Very short speeches (< 10 chars)
    short = speeches["speech_text"].astype(str).str.len() < 10
    short_count = short.sum()
    short_pct = short_count / n * 100
    result.add(
        "speech_short_text",
        "WARN" if short_pct > 10 else "PASS",
        f"{short_count:,} ({short_pct:.1f}%) speeches under 10 characters",
    )

    # 9. Duplicates
    dup_cols = ["meeting_id", "speech_order"]
    dup_cols = [c for c in dup_cols if c in speeches.columns]
    if len(dup_cols) == 2:
        dups = speeches.duplicated(subset=dup_cols, keep=False)
        dup_count = dups.sum()
        if dup_count > 0:
            dup_pct = dup_count / n * 100
            result.add(
                "speech_duplicates",
                "WARN" if dup_pct < 1 else "FAIL",
                f"{dup_count:,} ({dup_pct:.2f}%) duplicate (meeting_id, speech_order) pairs",
            )
        else:
            result.add("speech_duplicates", "PASS", "No duplicate (meeting_id, speech_order) pairs")

    # 10. Speech order continuity within meetings
    # Sample meetings to check
    meeting_sample = speeches["meeting_id"].dropna().unique()
    if len(meeting_sample) > 500:
        rng = np.random.RandomState(42)
        meeting_sample = rng.choice(meeting_sample, 500, replace=False)

    gaps_found = 0
    for mid in meeting_sample:
        mdf = speeches[speeches["meeting_id"] == mid].copy()
        try:
            orders = sorted(mdf["speech_order"].astype(int))
        except (ValueError, TypeError):
            continue
        if len(orders) > 1:
            expected = list(range(orders[0], orders[0] + len(orders)))
            if orders != expected:
                gaps_found += 1

    gap_pct = gaps_found / len(meeting_sample) * 100
    result.add(
        "speech_order_continuity",
        "WARN" if gap_pct > 20 else "PASS",
        f"{gaps_found}/{len(meeting_sample)} sampled meetings ({gap_pct:.1f}%) have speech_order gaps",
    )

    # 11. Empty person_name
    empty_name = speeches["person_name"].isna() | (speeches["person_name"].astype(str).str.strip() == "")
    empty_name_count = empty_name.sum()
    result.add(
        "speech_empty_person_name",
        "WARN" if empty_name_count / n > 0.01 else "PASS",
        f"{empty_name_count:,} ({empty_name_count / n * 100:.2f}%) speeches with empty person_name",
    )

    return result


def validate_dyads(dyads, result):
    """Validate the dyads parquet."""
    print("\n=== DYAD DATASET VALIDATION ===")
    n = len(dyads)
    result.add("dyad_row_count", "PASS" if n > 0 else "FAIL", f"{n:,} rows")

    # 1. Schema
    validate_schema(dyads, DYAD_REQUIRED_COLS, "dyad", result)

    # 2. Direction values
    dirs = set(dyads["direction"].dropna().unique())
    if dirs == VALID_DIRECTIONS:
        result.add("dyad_directions", "PASS", "Both 'question' and 'answer' present")
    else:
        result.add("dyad_directions", "FAIL", f"Unexpected directions: {dirs}")

    dir_counts = dyads["direction"].value_counts()
    q_count = dir_counts.get("question", 0)
    a_count = dir_counts.get("answer", 0)
    balance = min(q_count, a_count) / max(q_count, a_count) * 100
    result.add(
        "dyad_direction_balance",
        "PASS" if balance > 80 else "WARN",
        f"Question: {q_count:,}, Answer: {a_count:,} (balance: {balance:.1f}%)",
    )

    # 3. Witness roles must be non-legislator
    witness_roles = set(dyads["witness_role"].dropna().unique())
    leg_in_witness = witness_roles & LEGISLATOR_ROLES
    if leg_in_witness:
        count = dyads["witness_role"].isin(LEGISLATOR_ROLES).sum()
        result.add(
            "dyad_witness_not_legislator",
            "FAIL",
            f"{count:,} dyads have legislator role as witness: {leg_in_witness}",
        )
    else:
        result.add("dyad_witness_not_legislator", "PASS", "No dyads have legislator/chair as witness role")

    unknown_witness_roles = witness_roles - NONLEGISLATOR_ROLES
    if unknown_witness_roles:
        counts = dyads[dyads["witness_role"].isin(unknown_witness_roles)]["witness_role"].value_counts().to_dict()
        result.add(
            "dyad_witness_roles_unknown",
            "WARN",
            f"Unknown witness roles: {counts}",
        )
    else:
        result.add("dyad_witness_roles_valid", "PASS", f"All {len(witness_roles)} witness roles are valid non-legislator roles")

    # 4. Witness role distribution
    wr_counts = dyads["witness_role"].value_counts()
    top_roles = {str(k): int(v) for k, v in wr_counts.head(10).items()}
    result.add("dyad_witness_role_distribution", "PASS", f"Top 10 witness roles", details=top_roles)

    # 5. Terms
    terms_int = set()
    for t in dyads["term"].dropna().unique():
        try:
            terms_int.add(int(t))
        except (ValueError, TypeError):
            pass
    missing = VALID_TERMS - terms_int
    if missing:
        result.add("dyad_terms_missing", "FAIL", f"Missing terms: {sorted(missing)}")
    else:
        result.add("dyad_terms_complete", "PASS", "All 7 terms present")

    # 6. Empty text
    empty_leg = dyads["leg_speech"].isna() | (dyads["leg_speech"].astype(str).str.strip() == "")
    empty_wit = dyads["witness_speech"].isna() | (dyads["witness_speech"].astype(str).str.strip() == "")
    result.add(
        "dyad_empty_leg_speech",
        "WARN" if empty_leg.sum() / n > 0.01 else "PASS",
        f"{empty_leg.sum():,} ({empty_leg.sum() / n * 100:.2f}%) dyads with empty legislator speech",
    )
    result.add(
        "dyad_empty_witness_speech",
        "WARN" if empty_wit.sum() / n > 0.01 else "PASS",
        f"{empty_wit.sum():,} ({empty_wit.sum() / n * 100:.2f}%) dyads with empty witness speech",
    )

    # 7. Empty names
    empty_leg_name = dyads["leg_name"].isna() | (dyads["leg_name"].astype(str).str.strip() == "")
    empty_wit_name = dyads["witness_name"].isna() | (dyads["witness_name"].astype(str).str.strip() == "")
    result.add(
        "dyad_empty_leg_name",
        "WARN" if empty_leg_name.sum() > 0 else "PASS",
        f"{empty_leg_name.sum():,} dyads with empty legislator name",
    )
    result.add(
        "dyad_empty_witness_name",
        "WARN" if empty_wit_name.sum() > 0 else "PASS",
        f"{empty_wit_name.sum():,} dyads with empty witness name",
    )

    # 8. Self-dyads (same person on both sides)
    self_dyads = dyads["leg_name"] == dyads["witness_name"]
    self_count = self_dyads.sum()
    if self_count > 0:
        examples = dyads.loc[self_dyads, ["meeting_id", "leg_name", "witness_name", "witness_role"]].head(5).to_dict("records")
        result.add(
            "dyad_self_pairing",
            "WARN",
            f"{self_count:,} dyads where legislator == witness name",
            details=examples,
        )
    else:
        result.add("dyad_self_pairing", "PASS", "No self-pairing dyads")

    # 9. Committee key coverage
    comm_keys = set(dyads["committee_key"].dropna().unique())
    unknown_comms = comm_keys - VALID_COMMITTEE_KEYS
    if unknown_comms:
        result.add("dyad_committees_unknown", "WARN", f"Unknown committee keys: {sorted(unknown_comms)}")
    else:
        result.add("dyad_committees_valid", "PASS", f"All {len(comm_keys)} committee keys valid")

    # 10. Per-term dyad counts
    term_counts = dyads.groupby("term").size()
    details = {str(k): int(v) for k, v in term_counts.items()}
    result.add("dyad_term_distribution", "PASS", "Per-term counts", details=details)

    return result


def validate_cross_consistency(speeches, dyads, result):
    """Cross-validate speeches and dyads."""
    print("\n=== CROSS-DATASET CONSISTENCY ===")

    # 1. Dyad count should be less than speech count
    if len(dyads) < len(speeches):
        ratio = len(dyads) / len(speeches) * 100
        result.add(
            "cross_dyad_lt_speech",
            "PASS",
            f"Dyads ({len(dyads):,}) < Speeches ({len(speeches):,}), ratio: {ratio:.1f}%",
        )
    else:
        result.add("cross_dyad_lt_speech", "FAIL", f"Dyads ({len(dyads):,}) >= Speeches ({len(speeches):,})")

    # 2. All dyad meeting_ids should exist in speeches
    dyad_meetings = set(dyads["meeting_id"].dropna().unique())
    speech_meetings = set(speeches["meeting_id"].dropna().unique())
    missing_meetings = dyad_meetings - speech_meetings
    if missing_meetings:
        result.add(
            "cross_dyad_meetings_in_speeches",
            "FAIL",
            f"{len(missing_meetings)} dyad meeting_ids not found in speeches",
        )
    else:
        result.add(
            "cross_dyad_meetings_in_speeches",
            "PASS",
            f"All {len(dyad_meetings):,} dyad meeting_ids exist in speeches",
        )

    # 3. Per-term ratio consistency
    speech_term = speeches.groupby("term").size()
    dyad_term = dyads.groupby("term").size()
    term_ratios = {}
    for t in speech_term.index:
        if t in dyad_term.index and speech_term[t] > 0:
            ratio = dyad_term[t] / speech_term[t]
            term_ratios[str(t)] = round(ratio, 3)
    result.add("cross_term_ratios", "PASS", "Dyad/speech ratio per term", details=term_ratios)

    # Check ratio consistency
    if term_ratios:
        ratios = list(term_ratios.values())
        if max(ratios) - min(ratios) > 0.3:
            result.add(
                "cross_term_ratio_variance",
                "WARN",
                f"High variance in dyad/speech ratios: min={min(ratios):.3f}, max={max(ratios):.3f}",
            )
        else:
            result.add(
                "cross_term_ratio_variance",
                "PASS",
                f"Dyad/speech ratios consistent: min={min(ratios):.3f}, max={max(ratios):.3f}",
            )

    # 4. Spot-check: sample meetings and verify dyad formation
    print("\n  Spot-checking dyad formation in 100 meetings...")
    meeting_sample = speeches["meeting_id"].dropna().unique()
    rng = np.random.RandomState(42)
    if len(meeting_sample) > 100:
        meeting_sample = rng.choice(meeting_sample, 100, replace=False)

    mismatches = 0
    mismatch_examples = []
    for mid in meeting_sample:
        mdf = speeches[speeches["meeting_id"] == mid].copy()
        try:
            mdf["_so_num"] = pd.to_numeric(mdf["speech_order"], errors="coerce")
            mdf = mdf.dropna(subset=["_so_num"]).sort_values("_so_num")
        except Exception:
            continue

        # Recreate dyads locally
        rows = mdf.to_dict("records")
        expected_dyads = 0
        for i in range(len(rows) - 1):
            curr_role = rows[i]["role"]
            nxt_role = rows[i + 1]["role"]
            if curr_role in LEGISLATOR_ROLES and nxt_role in NONLEGISLATOR_ROLES:
                expected_dyads += 1
            elif curr_role in NONLEGISLATOR_ROLES and nxt_role in LEGISLATOR_ROLES:
                expected_dyads += 1

        actual_dyads = len(dyads[dyads["meeting_id"] == mid])
        if expected_dyads != actual_dyads:
            mismatches += 1
            if len(mismatch_examples) < 5:
                mismatch_examples.append({
                    "meeting_id": str(mid),
                    "expected": expected_dyads,
                    "actual": actual_dyads,
                    "speeches": len(mdf),
                })

    if mismatches > 0:
        result.add(
            "cross_dyad_formation_spot_check",
            "WARN" if mismatches < 10 else "FAIL",
            f"{mismatches}/100 meetings have dyad count mismatch",
            details=mismatch_examples,
        )
    else:
        result.add("cross_dyad_formation_spot_check", "PASS", "100/100 sampled meetings pass dyad formation check")

    # 5. Hearing type distribution match
    speech_ht = speeches["hearing_type"].value_counts(normalize=True).to_dict()
    dyad_ht = dyads["hearing_type"].value_counts(normalize=True).to_dict()
    result.add(
        "cross_hearing_type_distribution",
        "PASS",
        f"Speech: {speech_ht}, Dyad: {dyad_ht}",
    )

    return result


def validate_speaker_classification(speeches, result):
    """Deep validation of speaker classification rules."""
    print("\n=== SPEAKER CLASSIFICATION VALIDATION ===")

    # 1. chair vs legislator: chairs should have 위원장 in speaker field
    chairs = speeches[speeches["role"] == "chair"]
    chair_without_keyword = chairs[~chairs["speaker"].astype(str).str.contains("위원장", na=False)]
    if len(chair_without_keyword) > 0:
        examples = chair_without_keyword["speaker"].head(10).tolist()
        result.add(
            "classify_chair_keyword",
            "WARN",
            f"{len(chair_without_keyword):,} chairs without '위원장' in speaker field",
            details=examples,
        )
    else:
        result.add("classify_chair_keyword", "PASS", f"All {len(chairs):,} chairs have '위원장' in speaker field")

    # 2. minister should have 장관 in speaker
    ministers = speeches[speeches["role"] == "minister"]
    minister_without_kw = ministers[~ministers["speaker"].astype(str).str.contains("장관", na=False)]
    if len(minister_without_kw) > 0:
        examples = minister_without_kw["speaker"].head(10).tolist()
        result.add(
            "classify_minister_keyword",
            "WARN",
            f"{len(minister_without_kw):,} ministers without '장관' in speaker field",
            details=examples,
        )
    else:
        result.add("classify_minister_keyword", "PASS", f"All {len(ministers):,} ministers have '장관' in speaker field")

    # 3. v2 fix check: no government chairs misclassified as legislative chairs
    # Government org heads ending with 장 but in chair role
    gov_chair_patterns = ["공사장", "공단장", "청장"]
    suspicious_chairs = chairs[
        chairs["speaker"].astype(str).apply(
            lambda s: any(p in s for p in gov_chair_patterns)
        )
    ]
    if len(suspicious_chairs) > 0:
        examples = suspicious_chairs["speaker"].head(10).tolist()
        result.add(
            "classify_gov_chair_leak",
            "WARN",
            f"{len(suspicious_chairs):,} potential government chairs in legislator chair role",
            details=examples,
        )
    else:
        result.add("classify_gov_chair_leak", "PASS", "No government chairs misclassified as legislative chairs")

    # 4. committee_staff should not form dyads (they are excluded from both leg and nonleg roles)
    staff = speeches[speeches["role"] == "committee_staff"]
    result.add(
        "classify_staff_count",
        "PASS",
        f"{len(staff):,} committee_staff speeches (excluded from dyads)",
    )

    # 5. 'other' and 'unknown' role counts
    other_count = (speeches["role"] == "other").sum()
    unknown_count = (speeches["role"] == "unknown").sum()
    total_unclass = other_count + unknown_count
    unclass_pct = total_unclass / len(speeches) * 100
    result.add(
        "classify_unclassified",
        "WARN" if unclass_pct > 5 else "PASS",
        f"other: {other_count:,}, unknown: {unknown_count:,} ({unclass_pct:.1f}% unclassified)",
    )

    # 6. Sample unclassified speakers for review
    if total_unclass > 0:
        unclass = speeches[speeches["role"].isin(["other", "unknown"])]
        sample_speakers = unclass["speaker"].value_counts().head(20)
        details = {str(k): int(v) for k, v in sample_speakers.items()}
        result.add(
            "classify_unclassified_top20",
            "WARN",
            f"Top 20 unclassified speakers (for manual review)",
            details=details,
        )

    return result


def validate_committee_harmonization(speeches, result):
    """Validate committee name harmonization."""
    print("\n=== COMMITTEE HARMONIZATION VALIDATION ===")

    # 1. Raw committee name -> key mapping coverage
    raw_comms = speeches["committee"].dropna().unique()
    mapped = speeches.groupby("committee")["committee_key"].first()
    other_comms = mapped[mapped == "other"].index.tolist()

    result.add(
        "committee_raw_count",
        "PASS",
        f"{len(raw_comms)} unique raw committee names",
    )

    if other_comms:
        other_speech_counts = speeches[speeches["committee"].isin(other_comms)].groupby("committee").size()
        details = {str(k): int(v) for k, v in other_speech_counts.head(20).items()}
        total_other = other_speech_counts.sum()
        result.add(
            "committee_unmapped",
            "WARN",
            f"{len(other_comms)} raw names mapped to 'other' ({total_other:,} speeches)",
            details=details,
        )
    else:
        result.add("committee_unmapped", "PASS", "All raw committee names mapped to valid keys")

    # 2. Per-key speech counts
    key_counts = speeches["committee_key"].value_counts()
    details = {str(k): int(v) for k, v in key_counts.items()}
    result.add("committee_key_distribution", "PASS", f"{len(key_counts)} committee keys", details=details)

    # 3. Check that committee keys are consistent within meetings
    meeting_comm = speeches.groupby("meeting_id")["committee_key"].nunique()
    multi_comm = (meeting_comm > 1).sum()
    if multi_comm > 0:
        examples = meeting_comm[meeting_comm > 1].head(5).to_dict()
        result.add(
            "committee_meeting_consistency",
            "WARN",
            f"{multi_comm:,} meetings have multiple committee keys",
            details={str(k): int(v) for k, v in examples.items()},
        )
    else:
        result.add("committee_meeting_consistency", "PASS", "Each meeting has exactly one committee key")

    return result


def main():
    parser = argparse.ArgumentParser(description="Validate Korean National Assembly hearings dataset")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed",
        help="Path to processed data directory",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Path to save JSON report",
    )
    parser.add_argument(
        "--speeches-file",
        type=str,
        default="all_speeches_16_22_v2.parquet",
        help="Speech parquet filename",
    )
    parser.add_argument(
        "--dyads-file",
        type=str,
        default="dyads_16_22.parquet",
        help="Dyad parquet filename",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    speech_path = data_dir / args.speeches_file
    dyad_path = data_dir / args.dyads_file

    result = ValidationResult()

    # Load speeches
    print(f"Loading speeches from {speech_path}...")
    if not speech_path.exists():
        print(f"ERROR: {speech_path} not found")
        sys.exit(1)
    speeches = pd.read_parquet(speech_path)
    print(f"  Loaded {len(speeches):,} rows, {len(speeches.columns)} columns")

    # Load dyads
    print(f"Loading dyads from {dyad_path}...")
    if not dyad_path.exists():
        print(f"ERROR: {dyad_path} not found")
        sys.exit(1)
    dyads = pd.read_parquet(dyad_path)
    print(f"  Loaded {len(dyads):,} rows, {len(dyads.columns)} columns")

    # Run validations
    validate_speeches(speeches, result)
    validate_dyads(dyads, result)
    validate_speaker_classification(speeches, result)
    validate_committee_harmonization(speeches, result)
    validate_cross_consistency(speeches, dyads, result)

    # Summary
    summary = result.summary()
    print(f"\n{'=' * 60}")
    print(f"VALIDATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total checks:  {summary['total']}")
    print(f"  \u2705 PASS:       {summary['pass']}")
    print(f"  \u274c FAIL:       {summary['fail']}")
    print(f"  \u26a0\ufe0f  WARN:       {summary['warn']}")

    # Save report
    report_path = args.report or str(data_dir.parent.parent / "validation" / "report.json")
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report = result.to_dict()
    report["metadata"] = {
        "timestamp": datetime.now().isoformat(),
        "speech_file": str(speech_path),
        "dyad_file": str(dyad_path),
        "speech_rows": len(speeches),
        "dyad_rows": len(dyads),
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nReport saved to {report_path}")

    sys.exit(1 if summary["fail"] > 0 else 0)


if __name__ == "__main__":
    main()
