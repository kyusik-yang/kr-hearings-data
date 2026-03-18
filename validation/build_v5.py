"""
Build v5 dataset from v4 with comprehensive data integrity fixes.

Fixes:
1. member_id null representation: "nan"/empty → proper NA
2. person_title contamination: affiliation prefixes → null + affiliation_raw
3. Empty person_name: parse from speaker field (25 rows)
4. Homonymous member_id disambiguation: add member_uid column
5. minister 직무대리 → minister_acting (2,357 rows)
6. Additional 'other' role reclassification
7. Non-legislator person_name cleanup (affiliation prefix leakage)
8. Rebuild dyads from corrected speeches

Usage:
    python3 validation/build_v5.py [--dry-run]
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

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SPEECH_V4 = DATA_DIR / "all_speeches_16_22_v4.parquet"
DYADS_V4 = DATA_DIR / "dyads_16_22_v4.parquet"

SPEECH_V5 = DATA_DIR / "all_speeches_16_22_v5.parquet"
DYADS_V5 = DATA_DIR / "dyads_16_22_v5.parquet"

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

# ── Valid person_title values ──
VALID_PERSON_TITLES = {
    "대리", "반장", "직무대리", "직무대행",
    "반장대리", "반장직무대행", "반장직무대리",
    "위원장대리", "위원당대리",
}

# ── Homonymous member_id disambiguation ──
# These member_ids represent two different legislators across terms.
# naas_cd (국회의원 고유코드) disambiguates them.
HOMONYM_MEMBER_IDS = {
    "7407": {  # 김영주
        "E6S73230": "7407_A",   # 남, 자유선진당, term 19
        "0W194007": "7407_B",   # 여, 열린우리당/더불어민주당, terms 17, 20
    },
    "6182": {  # 최경환
        "FJ03481D": "6182_A",   # terms 17-19
        "KA04352K": "6182_B",   # term 20
    },
    "806": {  # 김선동
        "ZC87486D": "806_A",    # term 18
        "DTG4846A": "806_B",    # term 20
    },
    "878": {  # 김성태
        "BQS2021C": "878_A",    # terms 18-19
        "9UW75767": "878_B",    # term 20
    },
}

# ── Additional 'other' role reclassification rules (v5) ──
# Extends the v4 rules for remaining 8,692 'other' speeches
OTHER_RECLASS_RULES_V5 = [
    # Military/security
    (re.compile(r"기무사령부|기무부대"), "military"),
    (re.compile(r"군사보좌관"), "military"),

    # Financial
    (re.compile(r"금융통화위원"), "financial_regulator"),

    # Broadcasting
    (re.compile(r"한국정책방송원|KBS|방송통신위원회"), "broadcasting"),
    (re.compile(r"방송위원회"), "broadcasting"),

    # Local government / education
    (re.compile(r"교육장$|교육장\s"), "local_gov_head"),
    (re.compile(r"학교장\s|학교장$"), "org_head"),
    (re.compile(r"맹학교장|고등학교장|중학교장"), "org_head"),

    # Government advisors/spokespersons
    (re.compile(r"정책보좌관|정치자문역|자문역"), "other_official"),
    (re.compile(r"보좌관\s|보좌관$"), "other_official"),
    (re.compile(r"홍보관\s|홍보관$"), "other_official"),
    (re.compile(r"대변인\s|대변인$"), "other_official"),

    # Organization heads
    (re.compile(r"제작소장|자원관장"), "org_head"),
    (re.compile(r"공사.*팀\s|공사.*부\s"), "org_head"),
    (re.compile(r"워킹그룹.*관\s|워킹그룹.*관$"), "other_official"),

    # Research/expert
    (re.compile(r"연구부\s|연구부$"), "expert_witness"),
    (re.compile(r"교수\s|교수$"), "expert_witness"),

    # Bureaucrats
    (re.compile(r"안전평가관\s|안전평가관$"), "mid_bureaucrat"),
    (re.compile(r"어업자원관\s|어업자원관$"), "mid_bureaucrat"),
    (re.compile(r"노사협력관\s|노사협력관$"), "mid_bureaucrat"),
    (re.compile(r"선진화관\s|선진화관$"), "mid_bureaucrat"),
    (re.compile(r"정책과\s|정책과$"), "mid_bureaucrat"),
    (re.compile(r"TF장\s|TF장$"), "mid_bureaucrat"),

    # Police/fire
    (re.compile(r"소방서|안전센터"), "police"),

    # Assembly official
    (re.compile(r"대통령직인수위원회위원"), "assembly_official"),

    # Private sector
    (re.compile(r"㈜|주식회사|\(재\)|\(사\)"), "private_sector"),
    (re.compile(r"선수단감독|감독\s"), "private_sector"),
    (re.compile(r"철인3종|트라이애슬론"), "private_sector"),
]


# ── Korean name extraction ──
# Korean person names are typically 2-4 syllables (most commonly 3)
KOREAN_NAME_RE = re.compile(r"([가-힣]{2,4})$")
# Names with party disambiguation suffix: e.g., 최경환(국), 이수진(비)
KOREAN_NAME_PAREN_RE = re.compile(r"([가-힣]{2,4}\([가-힣새한비국평]\))$")


def fix_member_id_nulls(speeches):
    """FIX 1: Convert member_id 'nan'/'' to proper pandas NA."""
    print("\n--- FIX 1: member_id null representation ---")
    mid = speeches["member_id"]
    n_nan_str = (mid == "nan").sum()
    n_empty = (mid.astype(str).str.strip() == "").sum()

    # Convert to proper nulls
    speeches["member_id"] = speeches["member_id"].replace(
        {"nan": pd.NA, "": pd.NA, "None": pd.NA, "NaN": pd.NA}
    )
    # Also handle whitespace-only
    mask_ws = speeches["member_id"].notna() & (
        speeches["member_id"].astype(str).str.strip() == ""
    )
    speeches.loc[mask_ws, "member_id"] = pd.NA

    n_after = speeches["member_id"].isna().sum()
    n_valid = speeches["member_id"].notna().sum()
    print(f"  Converted: 'nan' strings={n_nan_str:,}, empty strings={n_empty:,}")
    print(f"  After: {n_valid:,} valid, {n_after:,} null")
    return speeches


def fix_person_title_contamination(speeches):
    """FIX 2: Move non-title values from person_title to affiliation_raw."""
    print("\n--- FIX 2: person_title contamination ---")
    has_title = speeches["person_title"].notna()
    invalid_mask = has_title & ~speeches["person_title"].isin(VALID_PERSON_TITLES)
    n_invalid = invalid_mask.sum()

    if n_invalid > 0:
        # For each invalid title, prepend it to affiliation_raw and clear title
        for idx in speeches.index[invalid_mask]:
            bad_title = speeches.at[idx, "person_title"]
            old_aff = speeches.at[idx, "affiliation_raw"]
            if pd.notna(old_aff) and str(old_aff).strip():
                speeches.at[idx, "affiliation_raw"] = f"{bad_title} {old_aff}"
            else:
                speeches.at[idx, "affiliation_raw"] = bad_title
            speeches.at[idx, "person_title"] = pd.NA

    n_valid_after = speeches["person_title"].notna().sum()
    print(f"  Fixed {n_invalid} contaminated entries")
    print(f"  Valid person_title remaining: {n_valid_after:,}")
    return speeches


def fix_empty_person_name(speeches):
    """FIX 3: Parse person_name from speaker field for 25 empty rows."""
    print("\n--- FIX 3: empty person_name ---")
    empty_mask = (
        speeches["person_name"].isna()
        | (speeches["person_name"].astype(str).str.strip() == "")
        | (speeches["person_name"].astype(str) == "nan")
    )
    n_empty = empty_mask.sum()

    fixed = 0
    for idx in speeches.index[empty_mask]:
        speaker = str(speeches.at[idx, "speaker"]).strip()
        # Try to extract name: "김부겸 위원장" → "김부겸"
        # Pattern 1: Name + title suffix
        m = re.match(r"^([가-힣]{2,4})\s+(위원장|위원|의원|장관)", speaker)
        if m:
            speeches.at[idx, "person_name"] = m.group(1)
            fixed += 1
            continue

        # Pattern 2: Title + name: "여성가족부 장관" → no name available
        # Pattern 3: Org + title: "산업통상자원부 제1차관" → no name
        # Leave these empty - no name to extract

    print(f"  Empty rows: {n_empty}, Fixed: {fixed}")
    remaining = (
        speeches["person_name"].isna()
        | (speeches["person_name"].astype(str).str.strip() == "")
        | (speeches["person_name"].astype(str) == "nan")
    ).sum()
    print(f"  Remaining empty: {remaining}")
    return speeches


def add_member_uid(speeches):
    """FIX 4: Add member_uid column to disambiguate homonymous legislators."""
    print("\n--- FIX 4: Homonymous member_id disambiguation ---")
    # Start with member_id as the base UID
    speeches["member_uid"] = speeches["member_id"].copy()

    total_fixed = 0
    for mid, naas_map in HOMONYM_MEMBER_IDS.items():
        mask = speeches["member_id"] == mid
        n_rows = mask.sum()
        if n_rows == 0:
            continue

        for naas_val, uid in naas_map.items():
            sub_mask = mask & (speeches["naas_cd"] == naas_val)
            n_sub = sub_mask.sum()
            speeches.loc[sub_mask, "member_uid"] = uid
            total_fixed += n_sub

        # Rows with NA naas_cd but matching member_id - assign by term
        na_naas = mask & speeches["naas_cd"].isna()
        if na_naas.sum() > 0:
            # Use term to disambiguate
            for idx in speeches.index[na_naas]:
                term = speeches.at[idx, "term"]
                # Assign based on which naas_cd appears in that term
                term_mask = mask & (speeches["term"] == term) & speeches["naas_cd"].notna()
                if term_mask.sum() > 0:
                    dominant_naas = speeches.loc[term_mask, "naas_cd"].mode()
                    if len(dominant_naas) > 0:
                        uid = naas_map.get(dominant_naas.iloc[0], mid)
                        speeches.at[idx, "member_uid"] = uid

        print(f"  member_id {mid}: {n_rows:,} rows disambiguated")

    print(f"  Total rows with new UIDs: {total_fixed:,}")
    return speeches


def fix_minister_acting(speeches):
    """FIX 5a: Reclassify minister 직무대리 → minister_acting."""
    print("\n--- FIX 5a: minister 직무대리 → minister_acting ---")
    mask = (
        (speeches["role"] == "minister")
        & speeches["speaker"].astype(str).str.contains("장관직무대리", na=False)
    )
    n = mask.sum()
    speeches.loc[mask, "role"] = "minister_acting"
    print(f"  Reclassified {n:,} rows to minister_acting")
    return speeches


def reclassify_other_v5(speeches):
    """FIX 5b: Additional 'other' role reclassification (vectorized)."""
    print("\n--- FIX 5b: Additional 'other' reclassification ---")
    other_mask = speeches["role"] == "other"
    n_before = other_mask.sum()

    # Vectorized: apply rules in priority order
    other_speakers = speeches.loc[other_mask, "speaker"].astype(str)
    new_roles = pd.Series(pd.NA, index=other_speakers.index, dtype="object")

    for pattern, new_role in OTHER_RECLASS_RULES_V5:
        still_other = new_roles.isna()
        if not still_other.any():
            break
        matches = other_speakers[still_other].str.contains(pattern, na=False)
        new_roles.loc[matches[matches].index] = new_role

    changed = new_roles.notna()
    n_changed = changed.sum()
    speeches.loc[changed[changed].index, "role"] = new_roles[changed]

    n_after = (speeches["role"] == "other").sum()
    print(f"  Before: {n_before:,} 'other'")
    print(f"  Reclassified: {n_changed:,}")
    print(f"  After: {n_after:,} 'other'")

    if n_changed > 0:
        reclass_dist = new_roles[changed].value_counts()
        for role, count in reclass_dist.items():
            print(f"    -> {role}: {count:,}")

    return speeches


def clean_nonleg_person_name(speeches):
    """FIX 6: Clean person_name for non-legislators with affiliation prefix leakage (vectorized)."""
    print("\n--- FIX 6: Non-legislator person_name cleanup ---")
    # Only process rows without member_id (non-legislators)
    no_mid = speeches["member_id"].isna()

    names = speeches.loc[no_mid, "person_name"].astype(str)

    # Pure Korean name: 2-4 hangul syllables, optionally with party suffix
    pure_name = names.str.match(r"^[가-힣]{2,4}(\([가-힣새한비국평]\))?$", na=False)
    empty_name = (names.str.strip() == "") | (names == "nan") | (names == "None")
    # Anonymized or foreign names - skip these
    anon_name = names.str.match(r"^[가-힣]?0+$|^O+$", na=False)
    foreign_name = names.str.contains(r"[a-zA-Z]", na=False, regex=True)
    skip = pure_name | empty_name | anon_name | foreign_name

    # Rows that need cleanup
    fix_idx = names.index[~skip]
    n_needs_fix = len(fix_idx)
    print(f"  Non-legislator rows: {no_mid.sum():,}")
    print(f"  Already clean/skip: {skip.sum():,}")
    print(f"  Needs fixing: {n_needs_fix:,}")

    if n_needs_fix > 0:
        fix_names = names.loc[fix_idx]

        # Vectorized extraction: try to get Korean name (2-4 syllables) from end
        # First try with party paren suffix, then without
        extracted_paren = fix_names.str.extract(
            r"(.+?)\s*([가-힣]{2,4}\([가-힣새한비국평]\))$", expand=True
        )
        extracted_plain = fix_names.str.extract(
            r"(.+?)\s*([가-힣]{2,4})$", expand=True
        )

        # Use paren match if available, else plain match
        prefix = extracted_paren[0].fillna(extracted_plain[0])
        extracted = extracted_paren[1].fillna(extracted_plain[1])

        # Only fix where we got a valid extraction
        valid = extracted.notna() & (extracted.str.len() >= 2)
        valid_idx = valid[valid].index

        # Update person_name
        speeches.loc[valid_idx, "person_name"] = extracted.loc[valid_idx]

        # Update affiliation_raw with the prefix
        has_prefix = prefix.loc[valid_idx].notna() & (prefix.loc[valid_idx].str.strip() != "")
        prefix_idx = has_prefix[has_prefix].index

        if len(prefix_idx) > 0:
            old_aff = speeches.loc[prefix_idx, "affiliation_raw"].astype(str)
            needs_aff = (old_aff.str.strip().isin(["", "nan", "None"])) | speeches.loc[prefix_idx, "affiliation_raw"].isna()
            # Set affiliation where it's currently empty
            set_idx = needs_aff[needs_aff].index
            speeches.loc[set_idx, "affiliation_raw"] = prefix.loc[set_idx].str.strip()
            # Prepend to existing affiliation
            prepend_idx = needs_aff[~needs_aff].index
            if len(prepend_idx) > 0:
                speeches.loc[prepend_idx, "affiliation_raw"] = (
                    prefix.loc[prepend_idx].str.strip() + " " +
                    speeches.loc[prepend_idx, "affiliation_raw"].astype(str)
                )

        print(f"  Fixed: {len(valid_idx):,}")
    else:
        print(f"  Fixed: 0")

    # Verify
    names_after = speeches.loc[no_mid, "person_name"].astype(str)
    pure_after = names_after.str.match(r"^[가-힣]{2,4}(\([가-힣새한비국평]\))?$", na=False)
    print(f"  Clean names after: {pure_after.sum():,} (was {pure_name.sum():,})")
    return speeches


def fix_gender_naas_consistency(speeches):
    """FIX 4b: Fix gender/metadata consistency for homonymous member_ids."""
    print("\n--- FIX 4b: Gender/metadata consistency ---")
    # For homonymous member_ids, the metadata (gender, party, etc.) should
    # be consistent within each member_uid group.
    for mid, naas_map in HOMONYM_MEMBER_IDS.items():
        for naas_val, uid in naas_map.items():
            mask = speeches["member_uid"] == uid
            if mask.sum() == 0:
                continue

            # Fix gender: use the mode within this uid group
            for col in ["gender", "party", "ruling_status"]:
                vals = speeches.loc[mask, col].dropna()
                if len(vals) == 0:
                    continue
                mode_val = vals.mode()
                if len(mode_val) > 0:
                    inconsistent = mask & speeches[col].notna() & (speeches[col] != mode_val.iloc[0])
                    n_fix = inconsistent.sum()
                    if n_fix > 0:
                        speeches.loc[inconsistent, col] = mode_val.iloc[0]
                        print(f"  {uid} ({col}): fixed {n_fix} inconsistent rows")

    return speeches


def build_dyads(df):
    """Build legislator-witness dyads from consecutive speeches (fully vectorized)."""
    # Pre-sort by meeting_id and speech_order
    df = df.copy()
    df["_so_num"] = pd.to_numeric(df["speech_order"], errors="coerce")
    df = df.dropna(subset=["_so_num"]).sort_values(["meeting_id", "_so_num"])
    df = df.reset_index(drop=True)

    n = len(df)
    has_uid = "member_uid" in df.columns

    # Classify each row's side
    is_leg = df["role"].isin(LEG_ROLES).values
    is_nonleg = df["role"].isin(NONLEG_ROLES).values

    # Same-meeting consecutive pair masks
    same_meeting = df["meeting_id"].values[:-1] == df["meeting_id"].values[1:]

    # Question: legislator[i] -> non-legislator[i+1]
    q_mask = same_meeting & is_leg[:-1] & is_nonleg[1:]
    # Answer: non-legislator[i] -> legislator[i+1]
    a_mask = same_meeting & is_nonleg[:-1] & is_leg[1:]

    q_pos = np.where(q_mask)[0]  # positions where curr is at q_pos, next at q_pos+1
    a_pos = np.where(a_mask)[0]

    print(f"    Question pairs: {len(q_pos):,}, Answer pairs: {len(a_pos):,}")

    # For questions: leg=curr(q_pos), wit=next(q_pos+1)
    # For answers: leg=next(a_pos+1), wit=curr(a_pos)
    q_leg_idx = q_pos
    q_wit_idx = q_pos + 1
    a_leg_idx = a_pos + 1
    a_wit_idx = a_pos

    # Combine
    all_leg_idx = np.concatenate([q_leg_idx, a_leg_idx])
    all_wit_idx = np.concatenate([q_wit_idx, a_wit_idx])
    all_direction = np.array(
        ["question"] * len(q_pos) + ["answer"] * len(a_pos)
    )

    # Build result using iloc for speed
    result = pd.DataFrame({
        "meeting_id": df["meeting_id"].values[all_leg_idx],
        "term": df["term"].values[all_leg_idx],
        "committee": df["committee"].values[all_leg_idx],
        "committee_key": df["committee_key"].values[all_leg_idx],
        "hearing_type": df["hearing_type"].values[all_leg_idx],
        "date": df["date"].values[all_leg_idx],
        "agenda": df["agenda"].values[all_leg_idx],
        "leg_name": df["person_name"].values[all_leg_idx],
        "leg_speaker_raw": df["speaker"].values[all_leg_idx],
        "leg_member_uid": df["member_uid"].values[all_leg_idx] if has_uid else pd.NA,
        "witness_name": df["person_name"].values[all_wit_idx],
        "witness_speaker_raw": df["speaker"].values[all_wit_idx],
        "witness_role": df["role"].values[all_wit_idx],
        "witness_affiliation": df["affiliation_raw"].values[all_wit_idx],
        "direction": all_direction,
        "leg_speech": df["speech_text"].values[all_leg_idx],
        "witness_speech": df["speech_text"].values[all_wit_idx],
    })

    # Sort by meeting_id
    result = result.sort_values("meeting_id").reset_index(drop=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("BUILD v5 DATASET")
    print("=" * 60)

    # ── Load v4 ──
    print(f"\nLoading {SPEECH_V4}...")
    speeches = pd.read_parquet(SPEECH_V4)
    n_v4 = len(speeches)
    print(f"  Loaded {n_v4:,} rows, {len(speeches.columns)} columns")

    # ── Apply fixes ──
    speeches = fix_member_id_nulls(speeches)        # FIX 1
    speeches = fix_person_title_contamination(speeches)  # FIX 2
    speeches = fix_empty_person_name(speeches)       # FIX 3
    speeches = add_member_uid(speeches)              # FIX 4a
    speeches = fix_gender_naas_consistency(speeches)  # FIX 4b
    speeches = fix_minister_acting(speeches)         # FIX 5a
    speeches = reclassify_other_v5(speeches)         # FIX 5b
    speeches = clean_nonleg_person_name(speeches)    # FIX 6

    # ── FIX 7: Rebuild dyads ──
    print("\n--- FIX 7: Rebuild dyads ---")
    t0 = time.time()
    n_meetings = speeches["meeting_id"].nunique()
    print(f"  Building dyads from {len(speeches):,} speeches, "
          f"{n_meetings:,} meetings...")
    dyads = build_dyads(speeches)
    print(f"  Built {len(dyads):,} dyads ({time.time()-t0:.0f}s)")

    # ── Column ordering ──
    # Place member_uid after member_id
    speech_cols = list(speeches.columns)
    if "member_uid" in speech_cols:
        speech_cols.remove("member_uid")
        mid_idx = speech_cols.index("member_id")
        speech_cols.insert(mid_idx + 1, "member_uid")
        speeches = speeches[speech_cols]

    # ── Summary ──
    print("\n" + "=" * 60)
    print("v4 -> v5 CHANGE SUMMARY")
    print("=" * 60)

    # Load v4 dyad count for comparison
    n_dyads_v4 = len(pd.read_parquet(DYADS_V4, columns=["meeting_id"]))

    # Role changes
    v4_speeches = pd.read_parquet(SPEECH_V4, columns=["role"])
    v4_roles = v4_speeches["role"].value_counts()
    v5_roles = speeches["role"].value_counts()
    del v4_speeches

    n_other_v4 = v4_roles.get("other", 0)
    n_other_v5 = v5_roles.get("other", 0)
    n_ma_v4 = v4_roles.get("minister_acting", 0)
    n_ma_v5 = v5_roles.get("minister_acting", 0)

    print(f"\n  {'Metric':<40} {'v4':>12} {'v5':>12} {'Delta':>10}")
    print(f"  {'-'*40} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'Speeches':<40} {n_v4:>12,} {len(speeches):>12,} {len(speeches)-n_v4:>+10,}")
    print(f"  {'Dyads':<40} {n_dyads_v4:>12,} {len(dyads):>12,} {len(dyads)-n_dyads_v4:>+10,}")
    print(f"  {'Dyad/Speech ratio':<40} {n_dyads_v4/n_v4:>12.4f} {len(dyads)/len(speeches):>12.4f}")
    print(f"  {'other role':<40} {n_other_v4:>12,} {n_other_v5:>12,} {n_other_v5-n_other_v4:>+10,}")
    print(f"  {'minister_acting role':<40} {n_ma_v4:>12,} {n_ma_v5:>12,} {n_ma_v5-n_ma_v4:>+10,}")

    # member_id null fix
    n_mid_valid = speeches["member_id"].notna().sum()
    n_mid_null = speeches["member_id"].isna().sum()
    print(f"  {'member_id valid/null':<40} {'':>12} {n_mid_valid:>6,}/{n_mid_null:>6,}")

    # person_title
    n_pt = speeches["person_title"].notna().sum()
    print(f"  {'person_title filled':<40} {'':>12} {n_pt:>12,}")

    # Empty person_name
    empty_pn = (
        speeches["person_name"].isna()
        | (speeches["person_name"].astype(str).str.strip() == "")
        | (speeches["person_name"].astype(str) == "nan")
    ).sum()
    print(f"  {'Empty person_name':<40} {'25':>12} {empty_pn:>12,}")

    # New column
    print(f"  {'New: member_uid column':<40} {'No':>12} {'Yes':>12}")
    n_disambig = (speeches["member_uid"] != speeches["member_id"]).sum()
    print(f"  {'Disambiguated member rows':<40} {'':>12} {n_disambig:>12,}")

    # Role distribution changes
    print(f"\n  Role distribution changes:")
    for role in sorted(set(v4_roles.index) | set(v5_roles.index)):
        c4 = v4_roles.get(role, 0)
        c5 = v5_roles.get(role, 0)
        if c4 != c5:
            print(f"    {role:<25} {c4:>10,} -> {c5:>10,} ({c5-c4:>+8,})")

    if args.dry_run:
        print("\n  [DRY RUN] No files saved.")
        return

    # ── Save ──
    print(f"\nSaving...")
    speeches.to_parquet(SPEECH_V5, index=False)
    sz_s = os.path.getsize(SPEECH_V5) / 1024 / 1024
    print(f"  {SPEECH_V5} ({sz_s:.0f} MB)")

    dyads.to_parquet(DYADS_V5, index=False)
    sz_d = os.path.getsize(DYADS_V5) / 1024 / 1024
    print(f"  {DYADS_V5} ({sz_d:.0f} MB)")

    print(f"\nDone. Run validation:")
    print(f"  python3 validation/validate_dataset.py \\")
    print(f"    --data-dir data/ \\")
    print(f"    --speeches-file all_speeches_16_22_v5.parquet \\")
    print(f"    --dyads-file dyads_16_22_v5.parquet \\")
    print(f"    --report validation/report_v5.json")


if __name__ == "__main__":
    main()
