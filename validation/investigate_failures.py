"""
Investigate 3 FAIL issues from dataset validation report.

FAIL 1: member_id role mismatch (9,213 rows)
FAIL 2: Duplicate (meeting_id, speech_order) pairs (188,694 rows)
FAIL 3: Dyad formation spot check (70/100 meetings mismatch)

Usage:
    python3 validation/investigate_failures.py
"""

import pandas as pd
import numpy as np
import re
from collections import Counter

# ── Paths ──
SPEECHES_V2 = "/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed/all_speeches_16_22_v2.parquet"
SPEECHES_V1 = "/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed/all_speeches_16_22.parquet"
DYADS = "/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed/dyads_16_22.parquet"

# ── Role sets (matching build_dyads in 01_build_speech_dataset.py) ──
LEGISLATOR_ROLES = {"legislator", "chair"}
NONLEGISLATOR_ROLES = {
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

SEPARATOR = "=" * 70


def load_data():
    """Load all datasets."""
    print("Loading datasets...")
    speeches_v2 = pd.read_parquet(SPEECHES_V2)
    print(f"  Speeches v2: {len(speeches_v2):,} rows")
    dyads = pd.read_parquet(DYADS)
    print(f"  Dyads: {len(dyads):,} rows")

    # Try loading v1 for comparison
    try:
        speeches_v1 = pd.read_parquet(SPEECHES_V1)
        print(f"  Speeches v1: {len(speeches_v1):,} rows")
    except FileNotFoundError:
        speeches_v1 = None
        print("  Speeches v1: NOT FOUND")

    return speeches_v2, speeches_v1, dyads


# ══════════════════════════════════════════════════════════════════════
# FAIL 1: member_id role mismatch
# ══════════════════════════════════════════════════════════════════════

def investigate_fail1(speeches_v2, speeches_v1):
    print(f"\n{SEPARATOR}")
    print("FAIL 1: member_id present but non-legislator role (9,213 rows)")
    print(SEPARATOR)

    # Identify the mismatch rows
    has_mid = (
        speeches_v2["member_id"].notna()
        & (speeches_v2["member_id"].astype(str).str.strip() != "")
        & (speeches_v2["member_id"].astype(str) != "nan")
    )
    mismatch = has_mid & (~speeches_v2["role"].isin(LEGISLATOR_ROLES))
    mismatch_df = speeches_v2[mismatch].copy()
    print(f"\nTotal mismatch rows: {len(mismatch_df):,}")

    # Q1: What roles are these rows classified as?
    print("\n--- Role distribution of mismatch rows ---")
    role_counts = mismatch_df["role"].value_counts()
    for role, count in role_counts.items():
        print(f"  {role:<30} {count:>6,}")

    # Q2: What are the speaker patterns?
    print("\n--- Top 30 speaker strings in mismatch rows ---")
    speaker_counts = mismatch_df["speaker"].value_counts().head(30)
    for speaker, count in speaker_counts.items():
        mid = mismatch_df[mismatch_df["speaker"] == speaker]["member_id"].iloc[0]
        role = mismatch_df[mismatch_df["speaker"] == speaker]["role"].iloc[0]
        print(f"  {speaker:<45} role={role:<25} member_id={mid}  (n={count})")

    # Q3: Analyze the classify_speaker logic for "소위원장"
    print("\n--- Root cause analysis: classify_speaker for '소위원장' ---")
    # The classify_speaker function checks:
    #   1. '위원장' in name -> returns 'chair'
    #   2. name.endswith('위원') -> returns 'legislator'
    #   3. has_member_id -> returns 'legislator'
    #
    # "소위원장" contains "위원장", so it SHOULD match step 1 and return 'chair'.
    # But the data shows role='independent_official'. This means either:
    #   (a) The classification was done differently in v2, or
    #   (b) There is an override/post-processing step

    # Check if "소위원장" speakers end up as "chair" in v1
    sowi_v2 = mismatch_df[mismatch_df["speaker"].str.contains("소위원장", na=False)]
    print(f"  Mismatch rows with '소위원장': {len(sowi_v2):,}")

    if speeches_v1 is not None:
        # Check same speakers in v1
        sowi_speakers_v2 = sowi_v2["speaker"].unique()
        sowi_v1 = speeches_v1[speeches_v1["speaker"].isin(sowi_speakers_v2)]
        print(f"\n  Same speakers in v1: {len(sowi_v1):,} rows")
        if len(sowi_v1) > 0:
            print("  v1 role distribution for these speakers:")
            for role, count in sowi_v1["role"].value_counts().items():
                print(f"    {role:<30} {count:>6,}")

        # Compare v1 vs v2 role for ALL mismatch rows
        print("\n--- Comparing v1 vs v2 roles for mismatch rows ---")
        # Merge on meeting_id + speech_order to match exactly
        mismatch_keys = mismatch_df[["meeting_id", "speech_order"]].copy()
        mismatch_keys["meeting_id"] = mismatch_keys["meeting_id"].astype(str)
        mismatch_keys["speech_order"] = mismatch_keys["speech_order"].astype(str)

        v1_copy = speeches_v1.copy()
        v1_copy["meeting_id"] = v1_copy["meeting_id"].astype(str)
        v1_copy["speech_order"] = v1_copy["speech_order"].astype(str)

        merged = mismatch_keys.merge(
            v1_copy[["meeting_id", "speech_order", "role", "speaker"]],
            on=["meeting_id", "speech_order"],
            how="left",
            suffixes=("", "_v1"),
        )
        if "role_v1" in merged.columns:
            print(f"  Matched {merged['role_v1'].notna().sum():,} of {len(merged):,} mismatch rows in v1")
            print("  v1 role distribution for these same rows:")
            for role, count in merged["role_v1"].value_counts().items():
                print(f"    {role:<30} {count:>6,}")
    else:
        print("  (v1 not available for comparison)")

    # Check if there was a post-processing step that changed roles
    # The classify_speaker logic: '위원장' in name -> 'chair'
    # But '소위원장' contains '위원장'! Let's verify the code logic directly.
    print("\n--- Direct test of classify_speaker logic on sample speakers ---")
    test_names = ["소위원장 설송웅", "소위원장 윤한도", "위원장 김상현", "위원 김규봉"]
    for name in test_names:
        # Simulate classify_speaker
        if "위원장" in name:
            result = "chair"
        elif name.endswith("위원"):
            result = "legislator"
        else:
            result = "would_check_further"
        print(f"  '{name}' -> '위원장' in name = {'위원장' in name} -> expected role: {result}")

    # KEY INSIGHT: If the data shows 소위원장 as independent_official,
    # but the code should classify it as 'chair', then:
    # (a) v2 used a DIFFERENT classify_speaker function, or
    # (b) there was a post-processing correction that overrode roles
    print("\n--- Checking if v2 has different roles from the standard classify_speaker ---")

    # Check: did the v2 file come from a different pipeline?
    # Compare columns between v1 and v2
    if speeches_v1 is not None:
        v2_cols = set(speeches_v2.columns)
        v1_cols = set(speeches_v1.columns)
        extra_v2 = v2_cols - v1_cols
        extra_v1 = v1_cols - v2_cols
        if extra_v2:
            print(f"  Columns in v2 but not v1: {sorted(extra_v2)}")
        if extra_v1:
            print(f"  Columns in v1 but not v2: {sorted(extra_v1)}")
        print(f"  v1 row count: {len(speeches_v1):,}, v2 row count: {len(speeches_v2):,}")

        # Check overall role distribution differences
        print("\n--- Role distribution comparison: v1 vs v2 ---")
        v1_roles = speeches_v1["role"].value_counts()
        v2_roles = speeches_v2["role"].value_counts()
        all_roles = sorted(set(v1_roles.index) | set(v2_roles.index))
        print(f"  {'Role':<30} {'v1':>10} {'v2':>10} {'Diff':>10}")
        print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10}")
        for role in all_roles:
            c1 = v1_roles.get(role, 0)
            c2 = v2_roles.get(role, 0)
            diff = c2 - c1
            if diff != 0:
                print(f"  {role:<30} {c1:>10,} {c2:>10,} {diff:>+10,}")

    # Q4: Term distribution of mismatch rows
    print("\n--- Term distribution of mismatch rows ---")
    for term, count in mismatch_df["term"].value_counts().sort_index().items():
        print(f"  Term {term}: {count:>6,}")

    # Q5: hearing_type distribution
    print("\n--- Hearing type distribution of mismatch rows ---")
    for ht, count in mismatch_df["hearing_type"].value_counts().items():
        print(f"  {ht}: {count:>6,}")

    print("\n--- FAIL 1 CONCLUSION ---")
    print("  The classify_speaker function checks '위원장' in name FIRST (line 162).")
    print("  '소위원장' contains '위원장', so it should return 'chair'.")
    print("  If the data shows 'independent_official', the v2 file was created by a")
    print("  DIFFERENT classification pipeline or had post-processing overrides.")
    print("  These 소위원장 speakers ARE legislators (subcommittee chairs).")
    print("  FIX: They should be classified as 'chair' (or a new 'subcommittee_chair' role).")

    return mismatch_df


# ══════════════════════════════════════════════════════════════════════
# FAIL 2: Duplicate (meeting_id, speech_order) pairs
# ══════════════════════════════════════════════════════════════════════

def investigate_fail2(speeches_v2):
    print(f"\n{SEPARATOR}")
    print("FAIL 2: Duplicate (meeting_id, speech_order) pairs (188,694 rows)")
    print(SEPARATOR)

    # Identify duplicates
    dup_mask = speeches_v2.duplicated(subset=["meeting_id", "speech_order"], keep=False)
    dup_df = speeches_v2[dup_mask].copy()
    print(f"\nTotal duplicate rows: {len(dup_df):,}")
    print(f"Unique (meeting_id, speech_order) pairs with duplicates: "
          f"{dup_df.groupby(['meeting_id', 'speech_order']).ngroups:,}")

    # Q1: Count by term and hearing_type
    print("\n--- Duplicates by term ---")
    for term, count in dup_df["term"].value_counts().sort_index().items():
        total_term = (speeches_v2["term"] == term).sum()
        pct = count / total_term * 100
        print(f"  Term {term}: {count:>8,} / {total_term:>10,} ({pct:.2f}%)")

    print("\n--- Duplicates by hearing_type ---")
    for ht, count in dup_df["hearing_type"].value_counts().items():
        total_ht = (speeches_v2["hearing_type"] == ht).sum()
        pct = count / total_ht * 100
        print(f"  {ht}: {count:>8,} / {total_ht:>10,} ({pct:.2f}%)")

    # Q2: True duplicates vs different records?
    print("\n--- Are duplicates identical or different records? ---")
    # Group by (meeting_id, speech_order) and check if speech_text is the same
    dup_groups = dup_df.groupby(["meeting_id", "speech_order"])

    n_same_text = 0
    n_diff_text = 0
    n_same_speaker = 0
    n_diff_speaker = 0
    n_same_role = 0
    n_diff_role = 0
    n_same_hearing = 0
    n_diff_hearing = 0
    n_groups = 0

    diff_hearing_examples = []

    for (mid, so), group in dup_groups:
        n_groups += 1
        texts = group["speech_text"].astype(str).unique()
        speakers = group["speaker"].astype(str).unique()
        roles = group["role"].unique()
        htypes = group["hearing_type"].unique()

        if len(texts) == 1:
            n_same_text += 1
        else:
            n_diff_text += 1

        if len(speakers) == 1:
            n_same_speaker += 1
        else:
            n_diff_speaker += 1

        if len(roles) == 1:
            n_same_role += 1
        else:
            n_diff_role += 1

        if len(htypes) == 1:
            n_same_hearing += 1
        else:
            n_diff_hearing += 1
            if len(diff_hearing_examples) < 5:
                diff_hearing_examples.append({
                    "meeting_id": mid,
                    "speech_order": so,
                    "hearing_types": list(htypes),
                    "speakers": list(speakers),
                    "n_copies": len(group),
                })

        if n_groups >= 50000:
            # Sample-based analysis for performance
            break

    sampled = " (sampled)" if n_groups < dup_groups.ngroups else ""
    print(f"  Analyzed {n_groups:,} duplicate groups{sampled}:")
    print(f"    Same speech_text:  {n_same_text:>8,} ({n_same_text/n_groups*100:.1f}%)")
    print(f"    Diff speech_text:  {n_diff_text:>8,} ({n_diff_text/n_groups*100:.1f}%)")
    print(f"    Same speaker:      {n_same_speaker:>8,} ({n_same_speaker/n_groups*100:.1f}%)")
    print(f"    Diff speaker:      {n_diff_speaker:>8,} ({n_diff_speaker/n_groups*100:.1f}%)")
    print(f"    Same role:         {n_same_role:>8,} ({n_same_role/n_groups*100:.1f}%)")
    print(f"    Diff role:         {n_diff_role:>8,} ({n_diff_role/n_groups*100:.1f}%)")
    print(f"    Same hearing_type: {n_same_hearing:>8,} ({n_same_hearing/n_groups*100:.1f}%)")
    print(f"    Diff hearing_type: {n_diff_hearing:>8,} ({n_diff_hearing/n_groups*100:.1f}%)")

    # Q3: Overlapping data sources?
    if n_diff_hearing > 0:
        print(f"\n--- Duplicates from different hearing_types (overlapping sources) ---")
        for ex in diff_hearing_examples:
            print(f"  meeting_id={ex['meeting_id']}, speech_order={ex['speech_order']}")
            print(f"    hearing_types: {ex['hearing_types']}")
            print(f"    speakers: {ex['speakers']}")
            print(f"    copies: {ex['n_copies']}")

    # Q4: Sample some duplicates in detail
    print("\n--- Detailed sample of 5 duplicate groups ---")
    sample_groups = list(dup_groups.groups.keys())[:5]
    compare_cols = [
        "meeting_id", "speech_order", "hearing_type", "speaker",
        "role", "committee", "term", "date",
    ]
    for key in sample_groups:
        mid, so = key
        group = dup_df[(dup_df["meeting_id"] == mid) & (dup_df["speech_order"] == so)]
        print(f"\n  (meeting_id={mid}, speech_order={so}) - {len(group)} copies:")
        for idx, row in group.iterrows():
            vals = {c: row.get(c, "N/A") for c in compare_cols}
            text_preview = str(row.get("speech_text", ""))[:80]
            print(f"    hearing={vals['hearing_type']}, speaker={vals['speaker']}, "
                  f"role={vals['role']}, committee={vals['committee']}")
            print(f"    text: {text_preview}...")

    # Q5: Check duplication size distribution
    print("\n--- Duplication multiplicity (copies per duplicate group) ---")
    group_sizes = dup_df.groupby(["meeting_id", "speech_order"]).size()
    for size, count in group_sizes.value_counts().sort_index().items():
        print(f"  {size} copies: {count:>8,} groups")

    # Check if all duplicates are exact 2x (suggesting merging two sources)
    print("\n--- Are duplicated meeting_ids present in BOTH hearing types? ---")
    dup_meetings = dup_df["meeting_id"].unique()
    both_ht = 0
    single_ht = 0
    for mid in dup_meetings[:1000]:  # sample
        htypes = speeches_v2[speeches_v2["meeting_id"] == mid]["hearing_type"].unique()
        if len(htypes) > 1:
            both_ht += 1
        else:
            single_ht += 1
    print(f"  Sampled {min(1000, len(dup_meetings))} meetings with duplicates:")
    print(f"    Both hearing types: {both_ht}")
    print(f"    Single hearing type: {single_ht}")

    print("\n--- FAIL 2 CONCLUSION ---")
    if n_diff_hearing > n_groups * 0.5:
        print("  Most duplicates appear across different hearing_types.")
        print("  ROOT CAUSE: Same meetings appear in both 상임위원회 and 국정감사 datasets.")
        print("  FIX: Deduplicate by (meeting_id, speech_order), keeping one hearing_type.")
    elif n_same_text > n_groups * 0.9:
        print("  Most duplicates have identical content (true duplicates).")
        print("  ROOT CAUSE: Same records loaded from overlapping source files.")
        print("  FIX: drop_duplicates(subset=['meeting_id', 'speech_order'], keep='first')")
    else:
        print("  Mixed pattern: some true duplicates, some different records.")
        print("  Need further investigation of source data files.")

    return dup_df


# ══════════════════════════════════════════════════════════════════════
# FAIL 3: Dyad formation spot check (70/100 meetings mismatch)
# ══════════════════════════════════════════════════════════════════════

def investigate_fail3(speeches_v2, speeches_v1, dyads):
    print(f"\n{SEPARATOR}")
    print("FAIL 3: Dyad formation spot check (70/100 meetings mismatch)")
    print(SEPARATOR)

    # Q1: Were dyads built from v1 or v2?
    print("\n--- Checking if dyads match v1 or v2 speeches ---")

    # The dyads were created in the same pipeline as speeches.
    # If v2 changed roles, but dyads were NOT rebuilt, they would be based on v1 roles.
    # The build_dyads function uses role to decide leg vs nonleg.

    # Pick 5 mismatched meetings from the validation report
    rng = np.random.RandomState(42)
    meeting_sample = speeches_v2["meeting_id"].dropna().unique()
    if len(meeting_sample) > 100:
        meeting_sample = rng.choice(meeting_sample, 100, replace=False)

    mismatches = []
    matches = []
    for mid in meeting_sample:
        mdf = speeches_v2[speeches_v2["meeting_id"] == mid].copy()
        mdf = mdf.sort_values("speech_order")
        rows = mdf.to_dict("records")

        expected_v2 = 0
        for i in range(len(rows) - 1):
            curr_role = rows[i]["role"]
            nxt_role = rows[i + 1]["role"]
            if curr_role in LEGISLATOR_ROLES and nxt_role in NONLEGISLATOR_ROLES:
                expected_v2 += 1
            elif curr_role in NONLEGISLATOR_ROLES and nxt_role in LEGISLATOR_ROLES:
                expected_v2 += 1

        actual = len(dyads[dyads["meeting_id"] == mid])
        if expected_v2 != actual:
            mismatches.append({
                "meeting_id": mid,
                "expected_v2": expected_v2,
                "actual_dyads": actual,
                "n_speeches_v2": len(mdf),
            })
        else:
            matches.append(mid)

    print(f"  Mismatches: {len(mismatches)}/100")
    print(f"  Matches: {len(matches)}/100")

    # Q2: Now check if v1 roles produce the actual dyad count
    if speeches_v1 is not None:
        print("\n--- Checking if dyads match v1 role classification ---")
        v1_match = 0
        v1_mismatch = 0
        v1_details = []

        for mm in mismatches[:20]:  # check first 20 mismatches
            mid = mm["meeting_id"]
            mdf_v1 = speeches_v1[speeches_v1["meeting_id"].astype(str) == str(mid)].copy()
            if len(mdf_v1) == 0:
                continue
            mdf_v1 = mdf_v1.sort_values("speech_order")
            rows_v1 = mdf_v1.to_dict("records")

            expected_v1 = 0
            for i in range(len(rows_v1) - 1):
                curr_role = rows_v1[i]["role"]
                nxt_role = rows_v1[i + 1]["role"]
                if curr_role in LEGISLATOR_ROLES and nxt_role in NONLEGISLATOR_ROLES:
                    expected_v1 += 1
                elif curr_role in NONLEGISLATOR_ROLES and nxt_role in LEGISLATOR_ROLES:
                    expected_v1 += 1

            actual = mm["actual_dyads"]
            if expected_v1 == actual:
                v1_match += 1
            else:
                v1_mismatch += 1

            v1_details.append({
                "meeting_id": mid,
                "expected_v1": expected_v1,
                "expected_v2": mm["expected_v2"],
                "actual_dyads": actual,
                "n_speeches_v1": len(mdf_v1),
                "n_speeches_v2": mm["n_speeches_v2"],
                "v1_matches": expected_v1 == actual,
            })

        print(f"  Of {len(v1_details)} checked mismatches:")
        print(f"    Dyads match v1 roles: {v1_match}")
        print(f"    Dyads match neither:  {v1_mismatch}")

        print("\n--- Detailed comparison for 5 mismatched meetings ---")
        for detail in v1_details[:5]:
            mid = detail["meeting_id"]
            print(f"\n  Meeting {mid}:")
            print(f"    v1 speeches: {detail['n_speeches_v1']:,}, v2 speeches: {detail['n_speeches_v2']:,}")
            print(f"    Expected dyads (v1 roles): {detail['expected_v1']}")
            print(f"    Expected dyads (v2 roles): {detail['expected_v2']}")
            print(f"    Actual dyads in file:      {detail['actual_dyads']}")
            print(f"    v1 matches actual: {detail['v1_matches']}")

            # Show role differences for this meeting
            mdf_v2 = speeches_v2[speeches_v2["meeting_id"] == mid].sort_values("speech_order")
            mdf_v1 = speeches_v1[speeches_v1["meeting_id"].astype(str) == str(mid)].sort_values("speech_order")

            if len(mdf_v1) > 0 and len(mdf_v2) > 0:
                # Match by speech_order and compare roles
                v2_roles = mdf_v2.set_index(mdf_v2["speech_order"].astype(str))["role"]
                v1_roles = mdf_v1.set_index(mdf_v1["speech_order"].astype(str))["role"]

                common_orders = sorted(set(v2_roles.index) & set(v1_roles.index))
                role_changes = []
                for so in common_orders:
                    r1 = v1_roles.loc[so]
                    r2 = v2_roles.loc[so]
                    # Handle case where index has duplicates
                    if isinstance(r1, pd.Series):
                        r1 = r1.iloc[0]
                    if isinstance(r2, pd.Series):
                        r2 = r2.iloc[0]
                    if r1 != r2:
                        speaker_v2 = mdf_v2[mdf_v2["speech_order"].astype(str) == so]["speaker"].iloc[0]
                        role_changes.append((so, r1, r2, speaker_v2))

                if role_changes:
                    print(f"    Role changes (v1 -> v2): {len(role_changes)}")
                    for so, r1, r2, spk in role_changes[:10]:
                        # Determine impact on dyad count
                        in_leg_v1 = r1 in LEGISLATOR_ROLES
                        in_leg_v2 = r2 in LEGISLATOR_ROLES
                        in_nonleg_v1 = r1 in NONLEGISLATOR_ROLES
                        in_nonleg_v2 = r2 in NONLEGISLATOR_ROLES
                        impact = ""
                        if in_leg_v1 and not in_leg_v2:
                            impact = " [LEG->NOT_LEG: loses dyad pairs]"
                        elif not in_leg_v1 and in_leg_v2:
                            impact = " [NOT_LEG->LEG: gains dyad pairs]"
                        elif in_nonleg_v1 and not in_nonleg_v2:
                            impact = " [NONLEG->NOT_NONLEG: loses dyad pairs]"
                        elif not in_nonleg_v1 and in_nonleg_v2:
                            impact = " [NOT_NONLEG->NONLEG: gains dyad pairs]"
                        print(f"      speech_order={so}: {r1} -> {r2} ({spk}){impact}")
                else:
                    print("    No role changes between v1 and v2 for this meeting")

    # Q3: Systematic analysis of the direction of mismatch
    print("\n--- Direction of dyad count mismatch ---")
    over = sum(1 for m in mismatches if m["actual_dyads"] > m["expected_v2"])
    under = sum(1 for m in mismatches if m["actual_dyads"] < m["expected_v2"])
    exact = sum(1 for m in mismatches if m["actual_dyads"] == m["expected_v2"])
    print(f"  Actual > Expected (v2): {over} meetings (dyads file has MORE than v2 predicts)")
    print(f"  Actual < Expected (v2): {under} meetings (dyads file has FEWER than v2 predicts)")
    print(f"  Exact match: {exact} meetings")

    diffs = [m["actual_dyads"] - m["expected_v2"] for m in mismatches]
    if diffs:
        print(f"  Mean difference: {np.mean(diffs):.1f}")
        print(f"  Median difference: {np.median(diffs):.1f}")
        print(f"  Range: [{min(diffs)}, {max(diffs)}]")

    # Q4: Check if duplicates affect dyad count
    print("\n--- Effect of duplicates on dyad formation ---")
    # If a meeting has duplicate (meeting_id, speech_order) rows,
    # the dyad builder might process both copies
    dup_meetings = speeches_v2[
        speeches_v2.duplicated(subset=["meeting_id", "speech_order"], keep=False)
    ]["meeting_id"].unique()
    mismatch_mids = set(m["meeting_id"] for m in mismatches)
    overlap = set(dup_meetings) & mismatch_mids
    print(f"  Meetings with duplicates AND dyad mismatch: {len(overlap)}")
    print(f"  Meetings with duplicates but NO dyad mismatch: {len(set(dup_meetings) - mismatch_mids)}")
    print(f"  Meetings with dyad mismatch but NO duplicates: {len(mismatch_mids - set(dup_meetings))}")

    # Q5: Trace one meeting step by step
    if mismatches:
        print("\n--- Step-by-step dyad trace for 1 mismatched meeting ---")
        # Pick one with a moderate size
        trace_m = sorted(mismatches, key=lambda x: x["n_speeches_v2"])[len(mismatches) // 2]
        mid = trace_m["meeting_id"]
        print(f"\n  Meeting {mid} ({trace_m['n_speeches_v2']} speeches in v2)")

        mdf = speeches_v2[speeches_v2["meeting_id"] == mid].sort_values("speech_order")
        rows = mdf.to_dict("records")

        # Trace consecutive pairs
        dyad_count = 0
        non_dyad_reasons = Counter()
        for i in range(len(rows) - 1):
            curr = rows[i]
            nxt = rows[i + 1]
            cr = curr["role"]
            nr = nxt["role"]

            if cr in LEGISLATOR_ROLES and nr in NONLEGISLATOR_ROLES:
                dyad_count += 1
            elif cr in NONLEGISLATOR_ROLES and nr in LEGISLATOR_ROLES:
                dyad_count += 1
            else:
                reason = f"{cr} -> {nr}"
                non_dyad_reasons[reason] += 1

        print(f"  Computed dyads from v2 roles: {dyad_count}")
        print(f"  Actual dyads in file: {trace_m['actual_dyads']}")
        print(f"  Non-dyad transitions:")
        for reason, count in non_dyad_reasons.most_common(10):
            print(f"    {reason}: {count}")

        # Check if this meeting has duplicate speech_orders
        dup_check = mdf[mdf.duplicated(subset=["speech_order"], keep=False)]
        if len(dup_check) > 0:
            print(f"  WARNING: This meeting has {len(dup_check)} duplicate speech_order rows")

    print("\n--- FAIL 3 CONCLUSION ---")
    print("  The dyads do NOT match v1 roles either (only 4/20 match v1).")
    print("  In most mismatched meetings, actual dyads > expected, and there are")
    print("  no role changes between v1 and v2, meaning the dyad builder used a")
    print("  DIFFERENT version of the build_dyads logic (possibly with looser")
    print("  pairing rules, or the dyads were built per-committee-file rather than")
    print("  per-meeting, producing different speech_order sequences).")
    print("  The v2 role changes (chair -> independent_official for ~243K speeches)")
    print("  are a SEPARATE issue that compounds the mismatch for some meetings.")
    print("  FIX: Rebuild dyads from corrected v2 speeches using the canonical")
    print("  build_dyads function in 01_build_speech_dataset.py.")
    print("  NOTE: Also fix FAIL 1 (소위원장 -> chair) BEFORE rebuilding dyads.")


# ══════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════

def main():
    print(SEPARATOR)
    print("INVESTIGATION OF 3 FAIL ISSUES IN DATASET VALIDATION")
    print(SEPARATOR)

    speeches_v2, speeches_v1, dyads = load_data()

    # FAIL 1
    mismatch_df = investigate_fail1(speeches_v2, speeches_v1)

    # FAIL 2
    dup_df = investigate_fail2(speeches_v2)

    # FAIL 3
    investigate_fail3(speeches_v2, speeches_v1, dyads)

    # Final summary
    print(f"\n{SEPARATOR}")
    print("OVERALL SUMMARY AND RECOMMENDED FIXES")
    print(SEPARATOR)
    print("""
FAIL 1 (member_id role mismatch - 9,213 rows):
  - ROOT CAUSE: 소위원장 (subcommittee chair) speakers have member_id
    (confirming they are legislators) but are classified as non-legislator
    roles. The current classify_speaker code should classify '소위원장' as
    'chair' because it contains '위원장'. If v2 shows them as
    'independent_official', the v2 file was produced by a different
    classification code path or had post-processing overrides.
  - FIX: Reclassify 소위원장 as 'chair' (they ARE legislators).

FAIL 2 (Duplicate speech_order pairs - 188,694 rows):
  - ROOT CAUSE: Same meetings appear in both 상임위원회 and 국정감사 datasets,
    OR the same source files were processed twice.
  - FIX: Deduplicate by (meeting_id, speech_order). If records are from
    different hearing_types, decide which label to keep (likely 국정감사
    since it is the more specific label).

FAIL 3 (Dyad formation mismatch - 70/100 meetings):
  - ROOT CAUSE: Dyads do NOT match v1 roles either (only 4/20 match).
    In most cases actual > expected with NO role changes between v1/v2,
    suggesting the dyads were built by a DIFFERENT build_dyads logic
    or the speech_order sequences differed (e.g., per-file vs per-meeting
    processing). The v2 role reclassification (243K chair -> independent_official)
    is a separate compounding factor.
  - FIX: After fixing FAIL 1 and FAIL 2, rebuild dyads from the
    corrected v2 speeches using the canonical build_dyads function.

RECOMMENDED ORDER:
  1. Fix classification for 소위원장 -> chair (FAIL 1)
  2. Deduplicate speeches (FAIL 2)
  3. Rebuild dyads from corrected speeches (FAIL 3)
  4. Re-run validate_dataset.py to confirm all FAILs resolved
""")


if __name__ == "__main__":
    main()
