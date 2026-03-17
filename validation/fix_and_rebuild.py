"""
Fix validated issues and rebuild dyads.

Fixes:
1. Reclassify 소위원장/위원장직무대행/위원장대리 with member_id back to 'chair'
2. Remove exact duplicate (meeting_id, speech_order) rows
3. Rebuild dyads from corrected speeches

Usage:
    python3 validation/fix_and_rebuild.py [--dry-run]
"""

import argparse
import os
import sys
import time

import pandas as pd
import numpy as np

DATA_DIR = "/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed"
SPEECH_V2 = os.path.join(DATA_DIR, "all_speeches_16_22_v2.parquet")
DYADS_OLD = os.path.join(DATA_DIR, "dyads_16_22.parquet")

# Output files
SPEECH_V3 = os.path.join(DATA_DIR, "all_speeches_16_22_v3.parquet")
DYADS_V3 = os.path.join(DATA_DIR, "dyads_16_22_v3.parquet")

# Roles for dyad formation
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

# Legislative chair patterns that should remain as 'chair'
LEGISLATIVE_CHAIR_PATTERNS = [
    "소위원장",
    "위원장직무대행",
    "위원장대리",
    "조정위원장",
]


def fix_speaker_classification(speeches):
    """Fix 1: Reclassify legislative chairs that were over-corrected in v2."""
    print("\n--- FIX 1: Speaker classification ---")
    has_mid = speeches["member_id"].notna() & (
        speeches["member_id"].astype(str).str.strip().isin(["", "nan"]) == False
    )
    not_leg = ~speeches["role"].isin(LEG_ROLES)

    # Rows with member_id but non-legislator role
    mismatch = has_mid & not_leg
    n_before = mismatch.sum()
    print(f"  Rows with member_id but non-legislator role: {n_before:,}")

    # Check which are legitimate legislative chairs
    speaker_str = speeches["speaker"].astype(str)
    is_leg_chair = speaker_str.apply(
        lambda s: any(p in s for p in LEGISLATIVE_CHAIR_PATTERNS)
    )

    # Fix: reclassify to 'chair' if they match legislative chair patterns and have member_id
    fix_mask = mismatch & is_leg_chair
    n_fix_chair = fix_mask.sum()
    print(f"  Legislative chairs to reclassify: {n_fix_chair:,}")
    speeches.loc[fix_mask, "role"] = "chair"

    # Remaining mismatches: these have member_id but don't match chair patterns
    # They should be reclassified as 'legislator' (member_id is the strongest signal)
    remaining = has_mid & ~speeches["role"].isin(LEG_ROLES)
    n_remaining = remaining.sum()
    if n_remaining > 0:
        # Show what these are
        remaining_roles = speeches.loc[remaining, "role"].value_counts()
        remaining_speakers = speeches.loc[remaining, "speaker"].value_counts().head(10)
        print(f"  Remaining mismatches: {n_remaining:,}")
        print(f"    Roles: {dict(remaining_roles)}")
        print(f"    Top speakers: {dict(remaining_speakers.head(5))}")

        # Reclassify based on speaker field
        for idx in speeches.index[remaining]:
            spk = str(speeches.at[idx, "speaker"])
            if "위원장" in spk:
                speeches.at[idx, "role"] = "chair"
            elif spk.endswith("위원"):
                speeches.at[idx, "role"] = "legislator"
            else:
                # member_id is strongest signal
                speeches.at[idx, "role"] = "legislator"

    # Verify fix
    still_mismatch = has_mid & ~speeches["role"].isin(LEG_ROLES)
    print(f"  After fix: {still_mismatch.sum():,} remaining mismatches (should be 0)")

    return speeches


def deduplicate(speeches):
    """Fix 2: Remove exact duplicate rows."""
    print("\n--- FIX 2: Deduplication ---")
    n_before = len(speeches)

    # Check for duplicates on (meeting_id, speech_order)
    dup_mask = speeches.duplicated(subset=["meeting_id", "speech_order"], keep="first")
    n_dups = dup_mask.sum()
    print(f"  Duplicate rows found: {n_dups:,}")

    if n_dups > 0:
        # Distribution by term
        dup_terms = speeches.loc[dup_mask, "term"].value_counts()
        print(f"  By term: {dict(dup_terms)}")

        speeches = speeches[~dup_mask].copy()
        print(f"  Rows: {n_before:,} -> {len(speeches):,} (removed {n_dups:,})")

    return speeches


def build_dyads(df):
    """
    Build legislator-witness dyads from consecutive speeches.
    Identical logic to 01_build_speech_dataset.py.
    """
    dyads = []

    for meeting_id, group in df.groupby("meeting_id"):
        group = group.sort_values("speech_order")
        rows = group.to_dict("records")

        for i in range(len(rows) - 1):
            curr = rows[i]
            nxt = rows[i + 1]

            if curr["role"] in LEG_ROLES and nxt["role"] in NONLEG_ROLES:
                dyads.append({
                    "meeting_id": meeting_id,
                    "term": curr["term"],
                    "committee": curr["committee"],
                    "committee_key": curr["committee_key"],
                    "hearing_type": curr.get("hearing_type", "상임위원회"),
                    "date": curr["date"],
                    "agenda": curr["agenda"],
                    "leg_name": curr["person_name"],
                    "leg_speaker_raw": curr["speaker"],
                    "witness_name": nxt["person_name"],
                    "witness_speaker_raw": nxt["speaker"],
                    "witness_role": nxt["role"],
                    "witness_affiliation": nxt["affiliation_raw"],
                    "direction": "question",
                    "leg_speech": curr["speech_text"],
                    "witness_speech": nxt["speech_text"],
                })
            elif curr["role"] in NONLEG_ROLES and nxt["role"] in LEG_ROLES:
                dyads.append({
                    "meeting_id": meeting_id,
                    "term": curr["term"],
                    "committee": curr["committee"],
                    "committee_key": curr["committee_key"],
                    "hearing_type": curr.get("hearing_type", "상임위원회"),
                    "date": curr["date"],
                    "agenda": curr["agenda"],
                    "leg_name": nxt["person_name"],
                    "leg_speaker_raw": nxt["speaker"],
                    "witness_name": curr["person_name"],
                    "witness_speaker_raw": curr["speaker"],
                    "witness_role": curr["role"],
                    "witness_affiliation": curr["affiliation_raw"],
                    "direction": "answer",
                    "leg_speech": nxt["speech_text"],
                    "witness_speech": curr["speech_text"],
                })

    return pd.DataFrame(dyads)


def rebuild_dyads(speeches):
    """Fix 3: Rebuild dyads from corrected speeches."""
    print("\n--- FIX 3: Rebuild dyads ---")
    t0 = time.time()

    # Only keep columns needed for dyad building
    needed_cols = [
        "meeting_id", "term", "committee", "committee_key", "hearing_type",
        "date", "agenda", "speaker", "speech_order", "role",
        "person_name", "affiliation_raw", "speech_text",
    ]
    df = speeches[[c for c in needed_cols if c in speeches.columns]].copy()

    # Ensure speech_order is numeric for sorting
    df["speech_order"] = pd.to_numeric(df["speech_order"], errors="coerce")
    df = df.dropna(subset=["speech_order"])

    n_meetings = df["meeting_id"].nunique()
    print(f"  Building dyads from {len(df):,} speeches across {n_meetings:,} meetings...")

    dyads = build_dyads(df)
    elapsed = time.time() - t0
    print(f"  Built {len(dyads):,} dyads in {elapsed:.0f}s")

    return dyads


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Analyze without saving")
    args = parser.parse_args()

    print("=" * 60)
    print("FIX AND REBUILD PIPELINE")
    print("=" * 60)

    # Load
    print(f"\nLoading {SPEECH_V2}...")
    speeches = pd.read_parquet(SPEECH_V2)
    print(f"  Loaded {len(speeches):,} rows")

    # Fix 1: Speaker classification
    speeches = fix_speaker_classification(speeches)

    # Fix 2: Deduplication
    speeches = deduplicate(speeches)

    # Fix 3: Rebuild dyads
    dyads = rebuild_dyads(speeches)

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"  Speeches: {len(speeches):,}")
    print(f"  Dyads: {len(dyads):,}")
    print(f"  Dyad/Speech ratio: {len(dyads) / len(speeches):.3f}")

    # Role distribution
    print(f"\n  Speaker role distribution:")
    role_counts = speeches["role"].value_counts()
    for role, count in role_counts.head(15).items():
        print(f"    {role:<25} {count:>10,} ({count / len(speeches) * 100:5.1f}%)")

    # Dyad direction balance
    dir_counts = dyads["direction"].value_counts()
    print(f"\n  Dyad directions:")
    for d, c in dir_counts.items():
        print(f"    {d}: {c:,}")

    if args.dry_run:
        print("\n  [DRY RUN] No files saved.")
        return

    # Save
    print(f"\nSaving...")
    speeches.to_parquet(SPEECH_V3, index=False)
    sz_s = os.path.getsize(SPEECH_V3) / 1024 / 1024
    print(f"  {SPEECH_V3} ({sz_s:.0f} MB)")

    dyads.to_parquet(DYADS_V3, index=False)
    sz_d = os.path.getsize(DYADS_V3) / 1024 / 1024
    print(f"  {DYADS_V3} ({sz_d:.0f} MB)")

    print("\nDone. Run validate_dataset.py with --speeches-file all_speeches_16_22_v3.parquet --dyads-file dyads_16_22_v3.parquet")


if __name__ == "__main__":
    main()
