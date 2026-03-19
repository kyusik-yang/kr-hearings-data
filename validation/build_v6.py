"""
Build v6 dataset from v5 by adding 인사청문특별위원회 transcripts.

Changes from v5:
1. Add 52 인사청문특별위원회 meetings (55K speeches) scraped from 국회회의록시스템
2. hearing_type gains third value: '인사청문특별위원회'
3. committee_key gains new value: 'confirmation_special'
4. Rebuild dyads incrementally (new meetings only)

Usage:
    python3 validation/build_v6.py [--dry-run]
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

# ── Paths ──
KR_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SPEECH_V5 = KR_DATA_DIR / "all_speeches_16_22_v5.parquet"
DYADS_V5 = KR_DATA_DIR / "dyads_16_22_v5.parquet"
SPEECH_V6 = KR_DATA_DIR / "all_speeches_16_22_v6.parquet"
DYADS_V6 = KR_DATA_DIR / "dyads_16_22_v6.parquet"

# Scraped data location
SCRAPED_DATA = Path("/Users/kyusik/Desktop/kyusik-claude/projects/emotional-assembly/data/special_committee_speeches.parquet")

# MP metadata for legislator enrichment
MP_META = Path("/Users/kyusik/Desktop/kyusik-claude/projects/committee-witnesses-korea/data/processed/mp_metadata_16_22.csv")

# ── Role definitions (from 01_build_speech_dataset.py) ──
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


def classify_speaker(name, member_id):
    """Classify speaker into role. Copied from pipeline 01."""
    name = str(name).strip()
    if not name:
        return "unknown", "", ""

    has_member_id = pd.notna(member_id) and str(member_id).strip() not in ("", "nan", "0")

    # Chair (위원장대리 also caught here)
    if "위원장" in name:
        person = re.sub(r".*위원장\s*", "", name).strip()
        return "chair", person, name

    # Legislator by suffix
    if name.endswith("위원"):
        person = name.replace("위원", "").strip()
        return "legislator", person, name

    # Legislator by member_id
    if has_member_id:
        return "legislator", name, name

    # Minister nominee (before minister)
    if "장관후보자" in name:
        person = re.sub(r".*장관후보자\s*", "", name).strip()
        ministry = re.sub(r"장관후보자.*", "", name).strip()
        return "minister_nominee", person, ministry

    # Minister
    if "장관" in name:
        person = re.sub(r".*장관\s*", "", name).strip()
        ministry = re.sub(r"장관.*", "", name).strip()
        return "minister", person, ministry

    # Prime minister (국무총리후보자)
    if "총리" in name:
        person = re.sub(r".*총리\s*", "", name).strip()
        # Distinguish nominee vs sitting
        if "후보자" in name:
            return "nominee", person, name
        return "prime_minister", person, name

    # Vice minister
    if "차관" in name:
        person = re.sub(r".*차관\s*", "", name).strip()
        ministry = re.sub(r"차관.*", "", name).strip()
        return "vice_minister", person, ministry

    # Witness, testifier, expert witness
    if "증인" in name:
        return "witness", name.replace("증인", "").strip(), ""
    if "진술인" in name:
        return "testifier", name.replace("진술인", "").strip(), ""
    if "참고인" in name:
        return "expert_witness", name.replace("참고인", "").strip(), ""

    # Committee staff
    if "전문위원" in name or "수석전문위원" in name:
        person = re.sub(r".*(전문위원|수석전문위원)\s*", "", name).strip()
        return "committee_staff", person, name

    # Non-ministerial nominees (대법관후보자, 헌법재판소장후보자, etc.)
    if "후보자" in name:
        person = re.sub(r".*후보자\s*", "", name).strip()
        position = re.sub(r"후보자.*", "", name).strip()
        return "nominee", person, position

    # Agency heads
    if "청장" in name:
        person = re.sub(r".*청장\s*", "", name).strip()
        return "agency_head", person, name

    # Audit / Constitutional / Election
    if "감사원장" in name or "감사위원" in name:
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "audit_official", person, name
    if "헌법재판소" in name:
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "constitutional_court", person, name
    if "선거관리위원회" in name or "선관위" in name:
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "election_official", person, name

    # Assembly officials
    if any(kw in name for kw in ["국회사무", "국회도서관", "국회예산정책처", "국회입법조사처"]):
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "assembly_official", person, name

    # Senior bureaucrats
    for title in ["본부장", "처장", "사무처장", "국장", "실장", "차장", "총재", "이사장"]:
        if title in name:
            person = name.split()[-1] if len(name.split()) > 1 else name
            return "senior_bureaucrat", person, name

    # Organization heads
    if any(kw in name for kw in ["원장", "회장", "사장"]):
        person = name.split()[-1] if len(name.split()) > 1 else name
        return "org_head", person, name

    return "other", name, name


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
                    "hearing_type": curr.get("hearing_type", "상임위원회"),
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
                    "hearing_type": curr.get("hearing_type", "상임위원회"),
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


def transform_scraped_data(scraped):
    """Transform scraped special committee data to v5 schema."""
    print("\n=== PHASE 1: Transform scraped data ===")
    print(f"Input: {len(scraped):,} speeches from {scraped.meeting_id.nunique()} meetings")

    # Apply speaker classification
    print("Applying speaker classification...")
    roles, names, affiliations = [], [], []
    for _, row in scraped.iterrows():
        role, person, affil = classify_speaker(row["speaker"], row["member_id"])
        roles.append(role)
        names.append(person)
        affiliations.append(affil)

    scraped["role"] = roles
    scraped["person_name_classified"] = names
    scraped["affiliation_raw"] = affiliations

    # Use classified person_name, but keep original if better
    scraped["person_name"] = scraped.apply(
        lambda r: r["person_name"] if r["person_name"] else r["person_name_classified"],
        axis=1,
    )

    # Role distribution
    print(f"\nRole distribution:")
    for role, cnt in Counter(roles).most_common(15):
        print(f"  {role}: {cnt:,}")

    # Set committee_key
    scraped["committee_key"] = "confirmation_special"

    # Add missing columns
    scraped["member_uid"] = scraped["member_id"]  # No homonyms expected
    scraped["person_title"] = pd.NA
    scraped["name_clean"] = pd.NA
    scraped["party"] = pd.NA
    scraped["ruling_status"] = pd.NA
    scraped["seniority"] = pd.NA
    scraped["gender"] = pd.NA
    scraped["naas_cd"] = pd.NA

    # Clean member_id: "0" -> NA
    scraped.loc[scraped["member_id"] == "0", "member_id"] = pd.NA
    scraped.loc[scraped["member_uid"] == "0", "member_uid"] = pd.NA

    # Ensure term is int
    scraped["term"] = scraped["term"].astype("Int64")

    # Select and order columns to match v5 schema
    v5_cols = [
        "meeting_id", "term", "committee", "committee_key", "hearing_type",
        "session", "sub_session", "date", "agenda",
        "speaker", "member_id", "member_uid", "speech_order", "role",
        "person_name", "person_title", "affiliation_raw",
        "speech_text",
        "name_clean", "party", "ruling_status", "seniority", "gender", "naas_cd",
    ]
    for col in v5_cols:
        if col not in scraped.columns:
            scraped[col] = pd.NA

    result = scraped[v5_cols].copy()
    print(f"\nOutput: {len(result):,} speeches, {result.columns.tolist()}")
    return result


def enrich_legislators(new_speeches):
    """Enrich legislator speeches with metadata from mp_metadata."""
    print("\n=== PHASE 2: Enrich legislator metadata ===")

    if not MP_META.exists():
        print(f"  WARNING: {MP_META} not found, skipping enrichment")
        return new_speeches

    meta = pd.read_csv(MP_META)
    print(f"  MP metadata loaded: {len(meta)} records")
    print(f"  Meta columns: {meta.columns.tolist()}")

    # mp_metadata uses 'name' + 'term' as key (no member_id column)
    leg_mask = new_speeches["role"].isin(LEG_ROLES)
    n_legs = leg_mask.sum()
    print(f"  Legislator speeches to enrich: {n_legs:,}")

    # Prepare meta for merge: name + term -> party, ruling_status, etc.
    enrich_cols = ["name", "term", "party", "ruling_status", "seniority", "gender", "naas_cd"]
    meta_subset = meta[[c for c in enrich_cols if c in meta.columns]].drop_duplicates(
        subset=["name", "term"]
    )
    meta_subset = meta_subset.rename(columns={"name": "person_name_match"})
    meta_subset["term"] = meta_subset["term"].astype("Int64")

    # Rename meta columns to avoid clash
    rename_map = {c: f"{c}_meta" for c in enrich_cols if c not in ["name", "term"]}
    meta_subset = meta_subset.rename(columns=rename_map)

    # Merge on person_name + term
    merged = new_speeches.merge(
        meta_subset,
        left_on=["person_name", "term"],
        right_on=["person_name_match", "term"],
        how="left",
    )

    # Fill in metadata columns from merge
    for col in ["party", "ruling_status", "seniority", "gender", "naas_cd"]:
        meta_col = f"{col}_meta"
        if meta_col in merged.columns:
            mask = merged[col].isna() & merged[meta_col].notna()
            merged.loc[mask, col] = merged.loc[mask, meta_col]
            merged = merged.drop(columns=[meta_col])

    # Also set name_clean from person_name for matched legislators
    name_match_mask = merged["person_name_match"].notna()
    merged.loc[name_match_mask & merged["name_clean"].isna(), "name_clean"] = merged.loc[
        name_match_mask & merged["name_clean"].isna(), "person_name"
    ]

    merged = merged.drop(columns=["person_name_match"], errors="ignore")

    # Keep only original columns
    orig_cols = new_speeches.columns.tolist()
    enriched = merged[[c for c in orig_cols if c in merged.columns]].copy()

    n_enriched = enriched.loc[leg_mask, "party"].notna().sum()
    print(f"  Enriched: {n_enriched:,} / {n_legs:,} legislators with party data")

    return enriched


def main(dry_run=False):
    t0 = time.time()

    # ── Load scraped data ──
    print(f"Loading scraped data from {SCRAPED_DATA}...")
    scraped = pd.read_parquet(SCRAPED_DATA)
    print(f"  {len(scraped):,} speeches, {scraped.meeting_id.nunique()} meetings")

    # ── Transform to v5 schema ──
    new_speeches = transform_scraped_data(scraped)
    del scraped
    gc.collect()

    # ── Enrich legislators ──
    new_speeches = enrich_legislators(new_speeches)

    if dry_run:
        print(f"\n[DRY RUN] Would append {len(new_speeches):,} speeches to v5")
        print(f"Sample:\n{new_speeches.head(3).to_string()}")
        return

    # ── Load v5 speeches ──
    print(f"\n=== PHASE 3: Load v5 and append ===")
    print(f"Loading {SPEECH_V5}...")
    v5 = pd.read_parquet(SPEECH_V5)
    print(f"  v5: {len(v5):,} speeches, {v5.meeting_id.nunique()} meetings")

    # Verify no meeting_id overlap
    overlap = set(new_speeches["meeting_id"].astype(str)) & set(v5["meeting_id"].astype(str))
    if overlap:
        print(f"  WARNING: {len(overlap)} overlapping meeting_ids! Removing from new data.")
        new_speeches = new_speeches[~new_speeches["meeting_id"].astype(str).isin(overlap)]

    # Align dtypes to match v5 exactly
    print("  Aligning dtypes...")
    for col in new_speeches.columns:
        if col in v5.columns:
            v5_dtype = v5[col].dtype
            try:
                if v5_dtype == object:  # str columns
                    new_speeches[col] = new_speeches[col].astype(str).replace({"nan": pd.NA, "<NA>": pd.NA, "None": pd.NA})
                elif v5_dtype == "float64":
                    new_speeches[col] = pd.to_numeric(new_speeches[col], errors="coerce")
                elif str(v5_dtype).startswith("Int"):
                    new_speeches[col] = pd.to_numeric(new_speeches[col], errors="coerce").astype(v5_dtype)
                else:
                    new_speeches[col] = new_speeches[col].astype(v5_dtype, errors="ignore")
            except (ValueError, TypeError):
                print(f"    WARNING: Could not align {col} ({new_speeches[col].dtype} -> {v5_dtype})")

    # Append
    v6 = pd.concat([v5, new_speeches], ignore_index=True)
    del v5
    gc.collect()

    print(f"  v6: {len(v6):,} speeches, {v6.meeting_id.nunique()} meetings")
    print(f"  New hearing_type values: {v6.hearing_type.value_counts().to_dict()}")

    # ── Save v6 speeches ──
    print(f"\nSaving {SPEECH_V6}...")
    v6.to_parquet(SPEECH_V6, index=False, compression="zstd")
    speech_size = os.path.getsize(SPEECH_V6) / (1024 * 1024)
    print(f"  Saved: {speech_size:.1f} MB")

    # ── Build dyads for new meetings ──
    print(f"\n=== PHASE 4: Build dyads (incremental) ===")
    new_meeting_ids = set(new_speeches["meeting_id"])
    print(f"Building dyads for {len(new_meeting_ids)} new meetings...")

    new_dyads = build_dyads_for_meetings(v6, meeting_ids=new_meeting_ids)
    print(f"  New dyads: {len(new_dyads):,}")
    if len(new_dyads) > 0:
        print(f"  Direction: {new_dyads.direction.value_counts().to_dict()}")
        print(f"  Witness roles: {new_dyads.witness_role.value_counts().head(5).to_dict()}")

    del v6
    gc.collect()

    # ── Load v5 dyads and append ──
    print(f"\nLoading {DYADS_V5}...")
    v5_dyads = pd.read_parquet(DYADS_V5)
    print(f"  v5 dyads: {len(v5_dyads):,}")

    v6_dyads = pd.concat([v5_dyads, new_dyads], ignore_index=True)
    del v5_dyads
    gc.collect()

    print(f"  v6 dyads: {len(v6_dyads):,}")

    # ── Save v6 dyads ──
    print(f"\nSaving {DYADS_V6}...")
    v6_dyads.to_parquet(DYADS_V6, index=False, compression="zstd")
    dyad_size = os.path.getsize(DYADS_V6) / (1024 * 1024)
    print(f"  Saved: {dyad_size:.1f} MB")

    # ── Summary ──
    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"BUILD V6 COMPLETE ({elapsed:.1f}s)")
    print(f"{'='*60}")
    print(f"Speeches: v5={len(pd.read_parquet(SPEECH_V5)):,} -> v6={len(pd.read_parquet(SPEECH_V6, columns=['meeting_id'])):,}")
    print(f"Dyads: v5={len(pd.read_parquet(DYADS_V5, columns=['meeting_id'])):,} -> v6={len(pd.read_parquet(DYADS_V6, columns=['meeting_id'])):,}")
    print(f"New hearing_type: 인사청문특별위원회 ({len(new_meeting_ids)} meetings)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
