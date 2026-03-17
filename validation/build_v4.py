"""
Build v4 dataset from v3 with the following improvements:

1. person_title column: Extract acting/deputy titles from person_name
2. person_name cleanup: Remove prefix/suffix, keep core name only
3. 'other' role reclassification: ~26K speeches to proper roles
4. speech_text normalization: Collapse double spaces
5. Date normalization: Parse all date formats into YYYY-MM-DD
6. Rebuild dyads from corrected speeches

Usage:
    python3 validation/build_v4.py [--dry-run]
"""

import argparse
import gc
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime

import numpy as np
import pandas as pd

DATA_DIR = "/Volumes/kyusik-ssd/kyusik-research/projects/committee-witnesses-korea/data/processed"
SPEECH_V3 = os.path.join(DATA_DIR, "all_speeches_16_22_v3.parquet")
DYADS_V3 = os.path.join(DATA_DIR, "dyads_16_22_v3.parquet")

SPEECH_V4 = os.path.join(DATA_DIR, "all_speeches_16_22_v4.parquet")
DYADS_V4 = os.path.join(DATA_DIR, "dyads_16_22_v4.parquet")

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

# ── Person title extraction patterns ──
# Ordered by specificity (longest first to avoid partial matches)
PERSON_TITLE_PREFIXES = [
    ("반장직무대행", "반장직무대행"),
    ("반장직무대리", "반장직무대리"),
    ("반장대리", "반장대리"),
    ("직무대행", "직무대행"),
    ("직무대리", "직무대리"),
    ("위원당대리", "위원당대리"),
    ("위윈장대리", "위원장대리"),  # typo in original data
    ("대리위", "대리"),  # parsing artifact
    ("대리", "대리"),
    ("반장", "반장"),
]

PERSON_TITLE_SUFFIXES = [
    (" 의원", ""),
    ("의원", ""),
    (" 위원님", ""),
    ("위원님", ""),
]

# Parenthetical party markers to preserve in name (not titles)
# e.g., 최경환(국), 이수진(비) - these disambiguate homonymous legislators
PARTY_PAREN_PATTERN = re.compile(r"\([가-힣새한비국평]\)$")


# ── 'other' role reclassification rules ──
# Based on deep audit analysis of 26,461 'other' speeches
OTHER_RECLASS_RULES = [
    # Pattern in speaker field -> new role
    # Order matters: more specific patterns first
    (re.compile(r"사관학교장"), "military"),
    (re.compile(r"사령관|참모총장|참모차장"), "military"),
    (re.compile(r"경찰청|경찰서"), "police"),
    (re.compile(r"교수|연구위원|연구원.*센터"), "expert_witness"),
    (re.compile(r"감독$|선수단"), "private_sector"),
    (re.compile(r"예술감독"), "private_sector"),
    (re.compile(r"국장$|국장\s"), "senior_bureaucrat"),
    (re.compile(r"실장$|실장\s"), "senior_bureaucrat"),
    (re.compile(r"정책관$|정책관\s"), "mid_bureaucrat"),
    (re.compile(r"감사관$|감사관\s"), "mid_bureaucrat"),
    (re.compile(r"교육장\s|교육장$"), "local_gov_head"),
    (re.compile(r"관장\s|관장$"), "org_head"),
    (re.compile(r"소장\s|소장$"), "org_head"),
    (re.compile(r"상임위원\s|상임위원$"), "org_head"),
    (re.compile(r"전무이사|상무이사|기획이사|사업이사|관리이사|운영이사|기금이사|보험관리이사|업무이사|업무상임이사|유통이사|유통담당이사|검정이사|자격검정이사|기반조성본부이사|유지관리본부이사|기획운영이사|선임비상임이사|관리상임이사"), "org_head"),
    (re.compile(r"이사\s|이사$"), "org_head"),
    (re.compile(r"감사\s|감사$"), "org_head"),
    (re.compile(r"통제소장"), "senior_bureaucrat"),
]


def extract_person_title(person_name_str):
    """Extract acting/deputy title from person_name, return (clean_name, title)."""
    if not person_name_str or pd.isna(person_name_str):
        return person_name_str, None

    name = str(person_name_str).strip()
    title = None

    # 1. Check prefixes (longest first)
    for pattern, title_label in PERSON_TITLE_PREFIXES:
        if name.startswith(pattern):
            remainder = name[len(pattern):].strip()
            if remainder:  # Make sure there's still a name left
                name = remainder
                title = title_label
                break

    # 2. Check suffixes
    for pattern, replacement in PERSON_TITLE_SUFFIXES:
        if name.endswith(pattern):
            remainder = name[:-len(pattern)].strip() if pattern else name
            if remainder:
                name = remainder
                break

    return name.strip(), title


def reclassify_other(speaker_str):
    """Try to reclassify an 'other' role based on speaker field patterns."""
    if not speaker_str or pd.isna(speaker_str):
        return None
    spk = str(speaker_str)
    for pattern, new_role in OTHER_RECLASS_RULES:
        if pattern.search(spk):
            return new_role
    return None


def parse_korean_date(d):
    """Parse Korean date strings, handling all known formats."""
    d = str(d).strip()
    # Strip parenthetical day-of-week suffix
    d = re.sub(r"\([^)]*\)$", "", d).strip()
    # Handle spaces in Japanese kanji format: "2001年 9月 14日"
    d = d.replace(" ", "")

    for fmt in ["%Y년%m월%d일", "%Y年%m月%d日", "%Y-%m-%d", "%Y%m%d"]:
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    return None


def normalize_text(text):
    """Normalize speech text: collapse multiple spaces."""
    if pd.isna(text):
        return text
    t = str(text)
    # Collapse multiple spaces to single space
    t = re.sub(r"  +", " ", t)
    return t.strip()


def build_dyads(df):
    """Build legislator-witness dyads from consecutive speeches."""
    dyads = []
    for meeting_id, group in df.groupby("meeting_id"):
        group = group.copy()
        group["_so"] = pd.to_numeric(group["speech_order"], errors="coerce")
        group = group.dropna(subset=["_so"]).sort_values("_so")
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("BUILD v4 DATASET")
    print("=" * 60)

    # ── Load v3 ──
    print(f"\nLoading {SPEECH_V3}...")
    speeches = pd.read_parquet(SPEECH_V3)
    n_v3 = len(speeches)
    print(f"  Loaded {n_v3:,} rows, {len(speeches.columns)} columns")

    # ── FIX 1: person_title extraction & person_name cleanup ──
    print("\n--- FIX 1: person_title extraction ---")
    t0 = time.time()
    results = speeches["person_name"].apply(extract_person_title)
    speeches["person_name"] = results.apply(lambda x: x[0])
    speeches["person_title"] = results.apply(lambda x: x[1])

    n_with_title = speeches["person_title"].notna().sum()
    title_dist = speeches["person_title"].value_counts()
    print(f"  Extracted titles for {n_with_title:,} rows ({time.time()-t0:.1f}s)")
    for title, count in title_dist.items():
        print(f"    {title}: {count:,}")

    # ── Additional person_name cleanup for remaining edge cases ──
    mid_clean = speeches["member_id"].astype(str).str.strip()
    mid_clean = mid_clean.replace({"": None, "nan": None, "None": None, "NaN": None})
    has_mid = mid_clean.notna()

    # Fix 1a: For rows with member_id, if person_name still contains a long prefix
    # (e.g., "대통령직인수위원회정부혁신ㆍ규제개혁T/F팀장 박재완"),
    # and another row with same member_id has the clean name,
    # use the clean name (shortest non-empty variant per member_id)
    print("  Fixing remaining multi-name edge cases...")
    mid_groups = speeches[has_mid].groupby(mid_clean[has_mid])["person_name"]
    name_map = {}
    for mid_val, names_series in mid_groups:
        unique_names = sorted(set(
            n for n in names_series.dropna().astype(str).str.strip()
            if n and n != "nan"
        ))
        if len(unique_names) > 1:
            # Pick shortest non-empty name as canonical
            canonical = min(unique_names, key=len)
            if canonical:
                name_map[mid_val] = canonical

    if name_map:
        for mid_val, canonical_name in name_map.items():
            mask = has_mid & (mid_clean == mid_val) & (
                speeches["person_name"].astype(str).str.strip() != canonical_name
            )
            n_fixed = mask.sum()
            if n_fixed > 0:
                # Extract title from the long variant before overwriting
                old_names = speeches.loc[mask, "person_name"].astype(str)
                for idx in old_names.index:
                    old = old_names[idx].strip()
                    if old.endswith(canonical_name) and len(old) > len(canonical_name):
                        prefix = old[:-len(canonical_name)].strip()
                        if prefix and pd.isna(speeches.at[idx, "person_title"]):
                            speeches.at[idx, "person_title"] = prefix
                speeches.loc[mask, "person_name"] = canonical_name

        print(f"  Fixed {len(name_map)} member_ids with multi-name variants")

    # Verify
    mid_names_after = speeches[has_mid].groupby(mid_clean[has_mid])["person_name"].apply(
        lambda x: len(set(n for n in x.dropna().astype(str).str.strip() if n and n != "nan"))
    )
    multi_after = (mid_names_after > 1).sum()
    print(f"  member_id name consistency: {multi_after} still have multiple names "
          f"(was 1,113 in v3)")

    if multi_after > 0:
        remaining = mid_names_after[mid_names_after > 1]
        print(f"  Remaining multi-name member_ids:")
        for mid_val in remaining.index[:15]:
            names = sorted(set(
                n for n in speeches[has_mid & (mid_clean == mid_val)]["person_name"].dropna().astype(str).str.strip()
                if n and n != "nan"
            ))
            print(f"    {mid_val}: {names}")

    # ── FIX 2: 'other' role reclassification ──
    print("\n--- FIX 2: 'other' role reclassification ---")
    other_mask = speeches["role"] == "other"
    n_other_before = other_mask.sum()

    reclassed = speeches.loc[other_mask, "speaker"].apply(reclassify_other)
    n_reclassed = reclassed.notna().sum()
    speeches.loc[other_mask & reclassed.notna(), "role"] = reclassed[reclassed.notna()]

    n_other_after = (speeches["role"] == "other").sum()
    reclass_dist = reclassed[reclassed.notna()].value_counts()
    print(f"  Reclassified {n_reclassed:,} / {n_other_before:,} 'other' speeches")
    print(f"  Remaining 'other': {n_other_after:,}")
    for role, count in reclass_dist.items():
        print(f"    -> {role}: {count:,}")

    # ── FIX 3: speech_text normalization ──
    print("\n--- FIX 3: speech_text normalization ---")
    t0 = time.time()
    double_before = speeches["speech_text"].astype(str).str.contains("  ", regex=False, na=False).sum()
    speeches["speech_text"] = speeches["speech_text"].apply(normalize_text)
    double_after = speeches["speech_text"].astype(str).str.contains("  ", regex=False, na=False).sum()
    print(f"  Double spaces: {double_before:,} -> {double_after:,} ({time.time()-t0:.1f}s)")

    # ── FIX 4: Date normalization ──
    print("\n--- FIX 4: Date normalization ---")
    t0 = time.time()
    date_orig = speeches["date"].copy()
    speeches["date"] = speeches["date"].apply(parse_korean_date)

    n_parsed = speeches["date"].notna().sum()
    n_failed = speeches["date"].isna().sum()
    print(f"  Parsed: {n_parsed:,} / {len(speeches):,}")
    if n_failed > 0:
        failed_orig = date_orig[speeches["date"].isna()]
        print(f"  Failed: {n_failed:,}")
        print(f"  Failed examples: {failed_orig.value_counts().head(10).to_dict()}")
        # Keep original for failed parses
        speeches.loc[speeches["date"].isna(), "date"] = date_orig[speeches["date"].isna()]
    print(f"  ({time.time()-t0:.1f}s)")

    # ── FIX 5: Rebuild dyads ──
    print("\n--- FIX 5: Rebuild dyads ---")
    t0 = time.time()
    n_meetings = speeches["meeting_id"].nunique()
    print(f"  Building dyads from {len(speeches):,} speeches, {n_meetings:,} meetings...")
    dyads = build_dyads(speeches)
    print(f"  Built {len(dyads):,} dyads ({time.time()-t0:.0f}s)")

    # ── Column ordering ──
    # Ensure person_title is placed after person_name
    speech_cols = list(speeches.columns)
    if "person_title" in speech_cols:
        speech_cols.remove("person_title")
        pn_idx = speech_cols.index("person_name")
        speech_cols.insert(pn_idx + 1, "person_title")
        speeches = speeches[speech_cols]

    # ── Summary ──
    print("\n" + "=" * 60)
    print("v3 -> v4 CHANGE SUMMARY")
    print("=" * 60)

    # Compare dyad counts
    dyads_v3 = pd.read_parquet(DYADS_V3, columns=["meeting_id"])
    n_dyads_v3 = len(dyads_v3)
    del dyads_v3

    print(f"\n  {'Metric':<35} {'v3':>12} {'v4':>12} {'Delta':>10}")
    print(f"  {'-'*35} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'Speeches':<35} {n_v3:>12,} {len(speeches):>12,} {len(speeches)-n_v3:>+10,}")
    print(f"  {'Dyads':<35} {n_dyads_v3:>12,} {len(dyads):>12,} {len(dyads)-n_dyads_v3:>+10,}")
    print(f"  {'Dyad/Speech ratio':<35} {n_dyads_v3/n_v3:>12.4f} {len(dyads)/len(speeches):>12.4f}")
    print(f"  {'other role count':<35} {n_other_before:>12,} {n_other_after:>12,} {n_other_after-n_other_before:>+10,}")
    print(f"  {'Rows with person_title':<35} {'0':>12} {n_with_title:>12,}")
    print(f"  {'Multi-name member_ids':<35} {'1,113':>12} {multi_after:>12,}")
    print(f"  {'Double-space texts':<35} {double_before:>12,} {double_after:>12,}")
    print(f"  {'Unparsed dates':<35} {'18,238':>12} {n_failed:>12,}")

    # Role distribution comparison
    print(f"\n  Role distribution changes:")
    v3_roles = pd.read_parquet(SPEECH_V3, columns=["role"])["role"].value_counts()
    v4_roles = speeches["role"].value_counts()
    for role in sorted(set(v3_roles.index) | set(v4_roles.index)):
        c3 = v3_roles.get(role, 0)
        c4 = v4_roles.get(role, 0)
        if c3 != c4:
            print(f"    {role:<25} {c3:>10,} -> {c4:>10,} ({c4-c3:>+8,})")

    if args.dry_run:
        print("\n  [DRY RUN] No files saved.")
        return

    # ── Save ──
    print(f"\nSaving...")
    speeches.to_parquet(SPEECH_V4, index=False)
    sz_s = os.path.getsize(SPEECH_V4) / 1024 / 1024
    print(f"  {SPEECH_V4} ({sz_s:.0f} MB)")

    dyads.to_parquet(DYADS_V4, index=False)
    sz_d = os.path.getsize(DYADS_V4) / 1024 / 1024
    print(f"  {DYADS_V4} ({sz_d:.0f} MB)")

    print(f"\nDone. Run validation:")
    print(f"  python3 validation/validate_dataset.py \\")
    print(f"    --speeches-file all_speeches_16_22_v4.parquet \\")
    print(f"    --dyads-file dyads_16_22_v4.parquet \\")
    print(f"    --report validation/report_v4.json")


if __name__ == "__main__":
    main()
