"""
Build v8 dataset from v7 by adding 국정조사, 예산결산특별위원회, 국회본회의.

Changes from v7:
1. Add 3 new hearing types: 국정조사 (191 meetings), 예산결산특별위원회 (832),
   국회본회의 (1,058) - totaling 2,081 new meetings (1,165,665 speeches)
2. hearing_type gains three new values
3. committee_key gains three new values: investigation, budget_special, plenary
4. Total: 9,906,444 speeches across 16,830 meetings in 6 hearing types

Source: assembly_hearing_pipeline/data/ (per-meeting directories with minutes.csv)
Method: Hybrid XML viewer scraping (19-21대) + PDF parsing via PyMuPDF (16-18대, 22대)
Legislator metadata enriched via mp_metadata (party match rate 99.8%)

Usage:
    python3 validation/build_v8.py [--dry-run]

NOTE: This script reconstructs v8 from v7 + pipeline source data.
      The original build was done interactively on 2026-03-19.
"""

import argparse
import gc
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# -- Paths --
KR_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SPEECH_V7 = KR_DATA_DIR / "all_speeches_16_22_v7.parquet"
SPEECH_V8 = KR_DATA_DIR / "all_speeches_16_22_v8.parquet"
DYADS_V6 = KR_DATA_DIR / "dyads_16_22_v6.parquet"
DYADS_V8 = KR_DATA_DIR / "dyads_16_22_v8.parquet"

# Pipeline source data (per-meeting directories with minutes.csv)
PIPELINE_DATA = Path("/Users/kyusik/Desktop/kyusik-claude/projects/assembly_hearing_pipeline/data")
PIPELINE_MASTER = Path("/Users/kyusik/Desktop/kyusik-claude/projects/assembly_hearing_pipeline/hearings_master_all.csv")

# MP metadata for legislator enrichment
MP_META = Path("/Users/kyusik/Desktop/kyusik-claude/projects/committee-witnesses-korea/data/processed/mp_metadata_16_22.csv")

# Hearing type -> committee_key mapping for new types
NEW_HEARING_TYPES = {
    "국정조사": "investigation",
    "예산결산특별위원회": "budget_special",
    "국회본회의": "plenary",
}

# -- Role definitions (from build_v6.py) --
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


def build_dyads_for_meetings(df, meeting_ids=None):
    """Build dyads for specific meetings (incremental build)."""
    if meeting_ids is not None:
        df = df[df["meeting_id"].isin(meeting_ids)]

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
                    "hearing_type": curr.get("hearing_type"),
                    "date": curr["date"],
                    "agenda": curr["agenda"],
                    "leg_name": curr["person_name"],
                    "leg_speaker_raw": curr["speaker"],
                    "leg_member_uid": curr.get("member_uid", curr.get("member_id")),
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
                    "hearing_type": curr.get("hearing_type"),
                    "date": curr["date"],
                    "agenda": curr["agenda"],
                    "leg_name": nxt["person_name"],
                    "leg_speaker_raw": nxt["speaker"],
                    "leg_member_uid": nxt.get("member_uid", nxt.get("member_id")),
                    "witness_name": curr["person_name"],
                    "witness_speaker_raw": curr["speaker"],
                    "witness_role": curr["role"],
                    "witness_affiliation": curr["affiliation_raw"],
                    "direction": "answer",
                    "leg_speech": nxt["speech_text"],
                    "witness_speech": curr["speech_text"],
                })

    return pd.DataFrame(dyads)


def verify_v8(dry_run=False):
    """Verify existing v8 against v7 (no rebuild needed if v8 exists)."""
    if not SPEECH_V8.exists():
        return False

    print("=== VERIFICATION MODE: v8 already exists ===\n")
    v7 = pd.read_parquet(SPEECH_V7, columns=["meeting_id", "hearing_type"])
    v8 = pd.read_parquet(SPEECH_V8, columns=["meeting_id", "hearing_type"])

    v7_mids = set(v7.meeting_id.unique())
    v8_mids = set(v8.meeting_id.unique())
    new_mids = v8_mids - v7_mids
    lost_mids = v7_mids - v8_mids

    print(f"v7: {len(v7):,} speeches, {len(v7_mids):,} meetings")
    print(f"v8: {len(v8):,} speeches, {len(v8_mids):,} meetings")
    print(f"New meetings in v8: {len(new_mids):,}")
    if lost_mids:
        print(f"WARNING: {len(lost_mids)} meetings in v7 missing from v8!")

    # Check new meetings by hearing type
    new_speeches = v8[v8.meeting_id.isin(new_mids)]
    print(f"\nNew speeches by hearing_type:")
    for ht, cnt in new_speeches.hearing_type.value_counts().items():
        n_meetings = new_speeches[new_speeches.hearing_type == ht].meeting_id.nunique()
        print(f"  {ht}: {n_meetings} meetings, {cnt:,} speeches")

    del v7, v8
    gc.collect()
    return True


def main(dry_run=False):
    t0 = time.time()

    # Check if v8 already exists - verify instead of rebuild
    if SPEECH_V8.exists() and not dry_run:
        verified = verify_v8()
        if verified:
            print("\nv8 verified. To force rebuild, delete v8 first.")
            print(f"  rm {SPEECH_V8}")
            return

    # -- Load v7 --
    print(f"Loading {SPEECH_V7}...")
    v7 = pd.read_parquet(SPEECH_V7)
    print(f"  v7: {len(v7):,} speeches, {v7.meeting_id.nunique():,} meetings")
    v7_cols = v7.columns.tolist()

    # -- Load pipeline source data --
    print(f"\nLoading pipeline source from {PIPELINE_DATA}...")
    if not PIPELINE_DATA.exists():
        print(f"  ERROR: Pipeline data not found at {PIPELINE_DATA}")
        print("  Cannot rebuild v8 without source data.")
        sys.exit(1)

    all_new = []
    meeting_dirs = sorted(d for d in PIPELINE_DATA.iterdir() if d.is_dir())
    print(f"  Found {len(meeting_dirs)} meeting directories")

    for mdir in meeting_dirs:
        minutes_path = mdir / "minutes.csv"
        if not minutes_path.exists():
            continue
        try:
            df = pd.read_csv(minutes_path, encoding="utf-8-sig")
            df["meeting_id"] = mdir.name
            all_new.append(df)
        except Exception as e:
            print(f"  WARNING: Failed to read {minutes_path}: {e}")

    if not all_new:
        print("  ERROR: No minutes.csv files found in pipeline data")
        sys.exit(1)

    new_speeches = pd.concat(all_new, ignore_index=True)
    print(f"  Loaded: {len(new_speeches):,} speeches from {new_speeches.meeting_id.nunique()} meetings")
    del all_new
    gc.collect()

    # -- Remove any overlap with v7 --
    v7_mids = set(v7.meeting_id.unique())
    overlap = set(new_speeches.meeting_id.unique()) & v7_mids
    if overlap:
        print(f"  Removing {len(overlap)} overlapping meetings")
        new_speeches = new_speeches[~new_speeches.meeting_id.isin(overlap)]

    # -- Align schema to v7 --
    print("\nAligning schema...")
    for col in v7_cols:
        if col not in new_speeches.columns:
            new_speeches[col] = pd.NA

    new_speeches["term"] = new_speeches["term"].astype("Int64")
    new_speeches["seniority"] = pd.to_numeric(new_speeches.get("seniority"), errors="coerce")
    new_speeches = new_speeches[v7_cols]

    if dry_run:
        print(f"\n[DRY RUN] Would append {len(new_speeches):,} speeches to v7")
        print(f"Hearing types: {new_speeches.hearing_type.value_counts().to_dict()}")
        return

    # -- Concatenate --
    print("\nConcatenating...")
    v8 = pd.concat([v7, new_speeches], ignore_index=True)
    new_mids = set(new_speeches.meeting_id.unique())
    del v7
    gc.collect()

    print(f"  v8: {len(v8):,} speeches, {v8.meeting_id.nunique():,} meetings")

    # -- Save speeches --
    print(f"\nSaving {SPEECH_V8}...")
    v8.to_parquet(SPEECH_V8, compression="zstd", index=False)
    speech_size = os.path.getsize(SPEECH_V8) / (1024 * 1024)
    print(f"  Saved: {speech_size:.1f} MB")

    # -- Build dyads for new meetings --
    print(f"\n=== Building dyads for {len(new_mids)} new meetings ===")
    new_dyads = build_dyads_for_meetings(v8, meeting_ids=new_mids)
    print(f"  New dyads: {len(new_dyads):,}")
    if len(new_dyads) > 0:
        print(f"  Direction: {new_dyads.direction.value_counts().to_dict()}")

    # -- Merge with existing dyads --
    print(f"\nLoading {DYADS_V6}...")
    v6_dyads = pd.read_parquet(DYADS_V6)
    print(f"  v6 dyads: {len(v6_dyads):,}")

    v8_dyads = pd.concat([v6_dyads, new_dyads], ignore_index=True)
    del v6_dyads
    gc.collect()

    print(f"  v8 dyads: {len(v8_dyads):,}")

    print(f"\nSaving {DYADS_V8}...")
    v8_dyads.to_parquet(DYADS_V8, index=False, compression="zstd")
    dyad_size = os.path.getsize(DYADS_V8) / (1024 * 1024)
    print(f"  Saved: {dyad_size:.1f} MB")

    # -- Summary --
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"BUILD V8 COMPLETE ({elapsed:.1f}s)")
    print(f"{'=' * 60}")
    print(f"Speeches: v7 -> v8 = {len(v8):,} (+{len(new_speeches):,})")
    print(f"Meetings: {v8.meeting_id.nunique():,}")
    print(f"Dyads: {len(v8_dyads):,}")

    print(f"\nBy hearing_type:")
    for ht, grp in v8.groupby("hearing_type"):
        print(f"  {ht}: {grp.meeting_id.nunique()} meetings, {len(grp):,} speeches")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing files")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
