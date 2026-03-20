"""
Build v9 dataset from v8 by enriching with minister panel metadata.

Changes from v8:
1. ministry_normalized: standardized ministry name for all government officials
2. Link minister/minister_acting/minister_nominee speeches to minister_panel
3. New columns: dual_office, admin, admin_ideology
4. ruling_status empty strings cleaned to NaN
5. Full dyad rebuild from v9 speeches with enriched metadata

Source: minister-data/data/minister_panel_comprehensive.csv (296 appointments)
Method: Name + normalized ministry + date-range matching

Usage:
    python3 validation/build_v9.py [--dry-run]
"""

import argparse
import gc
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ──
KR_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SPEECH_V8 = KR_DATA_DIR / "all_speeches_16_22_v8.parquet"
SPEECH_V9 = KR_DATA_DIR / "all_speeches_16_22_v9.parquet"
DYADS_V9 = KR_DATA_DIR / "dyads_16_22_v9.parquet"

MINISTER_PANEL = Path(__file__).resolve().parent.parent.parent / "minister-data" / "data" / "minister_panel_comprehensive.csv"

# ── Role sets (from build_v8.py) ──
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

# Government roles that get ministry_normalized
GOVT_ROLES = {
    "minister", "minister_nominee", "minister_acting", "vice_minister",
    "prime_minister", "agency_head", "senior_bureaucrat", "mid_bureaucrat",
}

# Roles that get linked to minister panel
PANEL_LINK_ROLES = {"minister", "minister_acting", "minister_nominee"}

# ── Ministry name typo corrections ──
MINISTRY_TYPO_MAP = {
    "법부무": "법무부",
    "범무부": "법무부",
    "법무무": "법무부",
    "행장자치부": "행정자치부",
    "안정행정부": "안전행정부",
    "안전행전부": "안전행정부",
    "해앙수산부": "해양수산부",
    "해수부": "해양수산부",
    "정통부": "정보통신부",
    "교육적인자원부": "교육인적자원부",
    "교육인적자부": "교육인적자원부",
    "재정경재부": "재정경제부",
    "문환관광부": "문화관광부",
    "문화관부": "문화관광부",
    "교육기술부": "교육과학기술부",
    "교육과술부": "교육과학기술부",
    "교육과학기술기술부": "교육과학기술부",
    "문화체육광부": "문화체육관광부",
    "과학기술정보통부": "과학기술정보통신부",
    "농림축신식품부": "농림축산식품부",
    "농림축삭식품부": "농림축산식품부",
    "농림수산식품": "농림수산식품부",
    "산업지원부": "산업자원부",
    "산업자원통상부": "산업통상자원부",
    "여성가족주": "여성가족부",
    "통상외교부": "외교통상부",
    "해농림부": "농림부",
    "여성부가족부": "여성가족부",
    "여성부가족": "여성가족부",
    "보건복지부가족부": "보건복지부",
    "보건복지부보건복지부": "보건복지부",
    "보건복지부보": "보건복지부",
    "국방": "국방부",
    "과학기술": "과학기술부",
    "부총리겸교육적인자원부": "교육인적자원부",
    "농림수산식품부제": "농림수산식품부",
    "행정자치부장관후": "행정자치부",
    "행정자치부장관": "행정자치부",
    "재경경제부제1": "재정경제부",
    "재경부": "재정경제부",
    "재정경제": "재정경제부",
    "행전안전부제1": "행정안전부",
    "문화관광체육부제1": "문화체육관광부",
    "문화체육부제1": "문화체육관광부",
    "부총리겸재정경재부": "재정경제부",
    "부홍리겸재정경제부": "재정경제부",
}

# Historical ministry names -> panel canonical names
MINISTRY_HISTORICAL_MAP = {
    "여성부": "여성가족부",
    "보건복지가족부": "보건복지부",
    "특임": "특임장관",
}

# Person name typos in speeches (speech_name -> panel_name)
PERSON_NAME_FIXES = {
    "졍종환": "정종환",
    "백영희": "백희영",
    "윤관웅": "윤광웅",
}

# Presidential term ranges for date-based admin inference (fallback)
ADMIN_TERMS = [
    ("김대중",   "Conservative",  "1998-02-25", "2003-02-24"),
    ("노무현",   "Progressive",   "2003-02-25", "2008-02-24"),
    ("이명박",   "Conservative",  "2008-02-25", "2013-02-24"),
    ("박근혜",   "Conservative",  "2013-02-25", "2017-03-10"),
    ("문재인",   "Progressive",   "2017-05-10", "2022-05-09"),
    ("윤석열",   "Conservative",  "2022-05-10", "2025-06-03"),
    ("이재명",   "Progressive",   "2025-06-04", "2099-12-31"),
]


def normalize_ministry(raw):
    """Normalize affiliation_raw to standardized ministry name.

    Handles:
    - 부총리겸 prefix removal
    - Title+name suffix removal (장관 김현미 -> ministry only)
    - 제1/제2 vice-minister suffixes
    - "보 해양수산부" OCR artifact pattern
    - Known typos
    - Historical name mapping to panel canonical names
    - Returns None for unrecognizable patterns (garbage/misclassified)
    """
    if pd.isna(raw) or str(raw).strip() == "":
        return None

    s = str(raw).strip()

    # Early exit: known garbage patterns (misclassified non-ministry entities)
    if re.match(r"^리[공실본부팀]", s) or s.startswith("후보장") or s.startswith("후보 "):
        return None

    # Step 0: Handle "보 해양수산부" OCR artifact pattern
    # These are truncated titles like "보(좌관) 해양수산부" or "보(건복지부) 재정경제부"
    m = re.match(r"^보\s+(\S+부|국방부)$", s)
    if m:
        s = m.group(1)

    # Handle "보 산업자원부통상" -> "산업자원부"
    m = re.match(r"^보\s+(\S+부)\S*$", s)
    if m:
        s = m.group(1)

    # Step 1: Remove 부총리겸 prefix (and typo variants)
    s = re.sub(r"^부[총홍]리겸", "", s)

    # Step 2: Handle "정책보좌관 부처명" pattern -> extract ministry
    m = re.match(r"^정책보좌관\s+(\S+)$", s)
    if m:
        s = m.group(1)

    # Step 3: Remove title + person name patterns
    # "국토교통부장관 김현미" -> "국토교통부"
    # "보건복지부장관후보자 조규홍" -> "보건복지부"
    # "중소벤처기업부장관직무대리 최수규" -> "중소벤처기업부"
    # "과학기술정보통신부장관정책보좌관 진성오" -> "과학기술정보통신부"
    s = re.sub(r"장관(후보자|직무대행|직무대리|정책보좌관)?(\s+\S+)?$", "", s)

    # "보건복지부차관 권덕철" -> "보건복지부"
    s = re.sub(r"차관(보)?(\s+\S+)?$", "", s)

    # Step 4: Remove 제1/제2 suffixes (vice-minister disambiguation)
    s = re.sub(r"제[12](차관.*)?$", "", s)
    s = re.sub(r"[12]차관.*$", "", s)
    s = re.sub(r"2$", "", s)

    # Step 5: Remove trailing 차관/차관보 (without person name)
    s = re.sub(r"차관(보)?$", "", s)

    # Step 6: Handle "실*" prefix patterns from misparses
    # "실장 국무총리실장 권태신" -> 국무총리 (prime_minister role)
    # "실비서관 교육과학기술부제1" -> 교육과학기술부
    # "실특임실장 특임" -> 특임
    m = re.match(r"^실\S*\s+(.+)$", s)
    if m:
        s = m.group(1)

    # "어업자원관 농림수산식품부", "소비안전정책관 농림수산식품부" -> 농림수산식품부
    m = re.match(r"^\S+관\s+(\S+부)$", s)
    if m:
        s = m.group(1)

    # "광국장 문화관광부" -> 문화관광부
    m = re.match(r"^\S+국장\s+(\S+부)$", s)
    if m:
        s = m.group(1)

    # "국제농업국장 농림부" -> 농림부
    m = re.match(r"^\S+\s+(\S+부)$", s)
    if m and not s.endswith("부"):
        s = m.group(1)

    # Step 6b: Strip title + person name for senior_bureaucrat/agency_head
    # "국가보훈처장 박승춘" -> 국가보훈처
    # "식품의약품안전처장 정승" -> 식품의약품안전처
    s = re.sub(r"(처|청|원|실|본부)장\s+\S+$", r"\1", s)

    # "대통령비서실장 김대기" already handled above
    # "한국은행총재 이주열" -> reject (not a govt ministry)
    # "외교통상부통상교섭본부장 김종훈" -> 외교통상부 (already handled by 장관 stripping)

    # Handle regional agency prefixes
    # "서울특별시지방경찰" -> 경찰 (strip regional prefix for normalization)
    s = re.sub(r"^(서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주)\S*(지방|유역)", "", s)
    # "서울지방국세" -> 국세
    s = re.sub(r"^(서울|부산|대구|인천|광주|대전|울산|세종)\S*지방", "", s)

    s = s.strip()

    # Step 7: Fix known typos (check full string first)
    if s in MINISTRY_TYPO_MAP:
        s = MINISTRY_TYPO_MAP[s]

    # Step 8: Remove 제1/제2 again (might remain after earlier steps)
    s = re.sub(r"제[12]$", "", s).strip()

    # Step 9: Historical name mapping
    if s in MINISTRY_HISTORICAL_MAP:
        s = MINISTRY_HISTORICAL_MAP[s]

    # Step 10: Final cleanup
    # Reject remaining garbage (공사, 보험, 은행, public corp misclassifications)
    if s and re.search(r"(공사|보험|부동산원|㈜|은행총재|한국은행)", s):
        return None
    # Reject long patterns without known suffix (likely residual title+person combos)
    # but keep short agency names like 경찰, 국세, 산림 (valid abbreviations of 청)
    if s and len(s) >= 10 and not re.search(r"(부|처|청|원|총리|장관|통상|본부|실)$", s):
        return None

    return s if s else None


def infer_admin_from_date(date_str):
    """Infer administration name and ideology from date (fallback for unlinked ministers)."""
    if pd.isna(date_str) or not date_str:
        return None, None
    try:
        dt = pd.Timestamp(date_str)
    except (ValueError, TypeError):
        return None, None

    for admin, ideology, start, end in ADMIN_TERMS:
        if pd.Timestamp(start) <= dt <= pd.Timestamp(end):
            return admin, ideology
    return None, None


# ── Phase 1: Ministry name normalization ──
def phase1_normalize_ministries(df):
    """Add ministry_normalized column for all government officials."""
    print("\n" + "=" * 60)
    print("PHASE 1: Ministry name normalization")
    print("=" * 60)

    # Apply normalization to government officials
    govt_mask = df["role"].isin(GOVT_ROLES)
    n_govt = govt_mask.sum()
    print(f"Government official speeches: {n_govt:,}")

    df["ministry_normalized"] = None
    df.loc[govt_mask, "ministry_normalized"] = (
        df.loc[govt_mask, "affiliation_raw"].apply(normalize_ministry)
    )

    # Stats
    normalized = df.loc[govt_mask, "ministry_normalized"].notna().sum()
    print(f"Successfully normalized: {normalized:,} / {n_govt:,} ({100*normalized/n_govt:.1f}%)")

    # Show unresolved values
    unresolved_mask = govt_mask & df["ministry_normalized"].isna()
    if unresolved_mask.sum() > 0:
        print(f"\nUnresolved affiliation_raw ({unresolved_mask.sum():,} speeches):")
        unresolved = df.loc[unresolved_mask, "affiliation_raw"].value_counts().head(20)
        for val, ct in unresolved.items():
            print(f"  {ct:>5,} | {val}")

    # Show unique normalized values
    unique_normed = df.loc[govt_mask, "ministry_normalized"].dropna().unique()
    print(f"\nUnique ministry_normalized values: {len(unique_normed)}")

    return df


# ── Phase 2: Minister panel linkage ──
def phase2_link_minister_panel(df):
    """Link minister speeches to minister_panel_comprehensive.csv."""
    print("\n" + "=" * 60)
    print("PHASE 2: Minister panel linkage")
    print("=" * 60)

    # Load minister panel
    if not MINISTER_PANEL.exists():
        print(f"ERROR: Minister panel not found at {MINISTER_PANEL}")
        sys.exit(1)

    panel = pd.read_csv(MINISTER_PANEL)
    print(f"Minister panel: {len(panel)} appointments")

    # Parse panel dates
    panel["start_dt"] = pd.to_datetime(panel["start"], errors="coerce")
    panel["end_dt"] = pd.to_datetime(panel["end"], errors="coerce")
    # Fill missing end dates with far future
    panel["end_dt"] = panel["end_dt"].fillna(pd.Timestamp("2099-12-31"))

    # Initialize new columns
    for col in ["dual_office", "admin", "admin_ideology"]:
        df[col] = None

    # Fix known person name typos before matching
    for wrong, correct in PERSON_NAME_FIXES.items():
        mask = df["person_name"] == wrong
        if mask.sum() > 0:
            print(f"  Name fix: {wrong} -> {correct} ({mask.sum()} speeches)")
            df.loc[mask, "person_name"] = correct

    # Get minister speeches to link
    link_mask = df["role"].isin(PANEL_LINK_ROLES)
    n_to_link = link_mask.sum()
    print(f"Speeches to link: {n_to_link:,} ({df.loc[link_mask, 'role'].value_counts().to_dict()})")

    # Get unique minister-meeting combinations
    link_df = df.loc[link_mask, ["person_name", "ministry_normalized", "date", "meeting_id"]].copy()
    link_df["date_dt"] = pd.to_datetime(link_df["date"], errors="coerce")

    # Deduplicate: one row per (person_name, meeting_id)
    unique_combos = link_df.drop_duplicates(subset=["person_name", "meeting_id"])
    print(f"Unique minister-meeting combinations: {len(unique_combos):,}")

    # Build match index
    # Strategy: for each unique combo, find matching panel entry by name + ministry + date
    matched = 0
    unmatched_names = []

    # Group panel by name for fast lookup
    panel_by_name = {}
    for _, row in panel.iterrows():
        name = row["name"]
        if name not in panel_by_name:
            panel_by_name[name] = []
        panel_by_name[name].append(row)

    # Result dict: (person_name, meeting_id) -> panel row
    match_results = {}

    for _, combo in unique_combos.iterrows():
        pname = combo["person_name"]
        ministry = combo["ministry_normalized"]
        date = combo["date_dt"]
        mid = combo["meeting_id"]

        if pname not in panel_by_name:
            continue

        candidates = panel_by_name[pname]

        # Try exact match: name + ministry + date range
        best = None
        for c in candidates:
            ministry_match = (
                ministry is not None
                and c["ministry"] is not None
                and ministry == c["ministry"]
            )
            date_match = (
                pd.notna(date)
                and pd.notna(c["start_dt"])
                and c["start_dt"] <= date <= c["end_dt"]
            )

            if ministry_match and date_match:
                best = c
                break

        # Fallback 1: name + date range only (ministry might differ due to normalization)
        if best is None:
            for c in candidates:
                if pd.notna(date) and pd.notna(c["start_dt"]) and c["start_dt"] <= date <= c["end_dt"]:
                    best = c
                    break

        # Fallback 2: name + ministry only (date might be slightly off)
        if best is None:
            for c in candidates:
                if ministry is not None and c["ministry"] is not None and ministry == c["ministry"]:
                    best = c
                    break

        # Fallback 3: single panel entry for this name
        if best is None and len(candidates) == 1:
            best = candidates[0]

        if best is not None:
            match_results[(pname, mid)] = best
            matched += 1

    print(f"\nMatched: {matched:,} / {len(unique_combos):,} minister-meeting combos "
          f"({100*matched/len(unique_combos):.1f}%)")

    # Apply matches to speech dataframe
    # Create lookup arrays for vectorized assignment
    match_dual = {}
    match_admin = {}
    match_ideology = {}

    for (pname, mid), panel_row in match_results.items():
        key = (pname, mid)
        match_dual[key] = panel_row["dual_office"]
        match_admin[key] = panel_row["admin"]
        match_ideology[key] = panel_row["admin_ideology"]

    # Apply to dataframe
    print("Applying matches to speech dataframe...")
    link_indices = df.index[link_mask]

    # Build key column for lookup
    keys = list(zip(
        df.loc[link_indices, "person_name"],
        df.loc[link_indices, "meeting_id"],
    ))

    dual_vals = [match_dual.get(k) for k in keys]
    admin_vals = [match_admin.get(k) for k in keys]
    ideology_vals = [match_ideology.get(k) for k in keys]

    df.loc[link_indices, "dual_office"] = dual_vals
    df.loc[link_indices, "admin"] = admin_vals
    df.loc[link_indices, "admin_ideology"] = ideology_vals

    # Stats
    linked = df.loc[link_indices, "admin"].notna().sum()
    print(f"Speeches with panel metadata: {linked:,} / {n_to_link:,} ({100*linked/n_to_link:.1f}%)")

    # Fallback: infer admin from date for unlinked speeches
    unlinked_mask = link_mask & df["admin"].isna()
    n_unlinked = unlinked_mask.sum()
    print(f"\nUnlinked speeches: {n_unlinked:,} - applying date-based admin inference...")

    if n_unlinked > 0:
        # Get unique dates for unlinked speeches to avoid redundant computation
        unlinked_dates = df.loc[unlinked_mask, "date"].unique()
        date_admin_cache = {}
        for d in unlinked_dates:
            admin, ideology = infer_admin_from_date(d)
            date_admin_cache[d] = (admin, ideology)

        # Apply cached results
        unlinked_dates_series = df.loc[unlinked_mask, "date"]
        df.loc[unlinked_mask, "admin"] = unlinked_dates_series.map(lambda d: date_admin_cache.get(d, (None, None))[0])
        df.loc[unlinked_mask, "admin_ideology"] = unlinked_dates_series.map(lambda d: date_admin_cache.get(d, (None, None))[1])

        inferred = df.loc[unlinked_mask, "admin"].notna().sum()
        print(f"  Inferred admin for {inferred:,} / {n_unlinked:,} unlinked speeches")
        # Note: dual_office remains None for these (not in panel)

    # Show still-unlinked breakdown
    still_unlinked_mask = link_mask & df["admin"].isna()
    still_unlinked = df.loc[still_unlinked_mask]
    if len(still_unlinked) > 0:
        print(f"\nStill unlinked (no date): {len(still_unlinked):,}")

    # dual_office distribution for panel-linked speeches
    panel_linked = df.loc[link_mask & df["dual_office"].notna()]
    print(f"\nDual-office distribution (panel-linked speeches):")
    print(panel_linked["dual_office"].value_counts().to_string(header=False))

    print(f"\nAdmin distribution (all minister speeches):")
    print(df.loc[link_mask, "admin"].value_counts(dropna=False).to_string(header=False))

    return df


# ── Phase 3: Clean ruling_status ──
def phase3_clean_ruling_status(df):
    """Convert empty string ruling_status to NaN."""
    print("\n" + "=" * 60)
    print("PHASE 3: Clean ruling_status")
    print("=" * 60)

    empty_mask = df["ruling_status"] == ""
    n_empty = empty_mask.sum()
    print(f"Empty string ruling_status: {n_empty:,}")

    df.loc[empty_mask, "ruling_status"] = None

    # Also verify distribution
    leg_mask = df["role"].isin(LEG_ROLES)
    print(f"\nruling_status for legislators after cleanup:")
    print(df.loc[leg_mask, "ruling_status"].value_counts(dropna=False).to_string())

    return df


# ── Phase 4: Build dyads ──
def phase4_build_dyads(df):
    """Full rebuild of dyads from v9 speeches with enriched metadata."""
    print("\n" + "=" * 60)
    print("PHASE 4: Build dyads (full rebuild)")
    print("=" * 60)

    n_meetings = df["meeting_id"].nunique()
    print(f"Building dyads for {n_meetings:,} meetings...")

    dyads = []
    for meeting_id, group in df.groupby("meeting_id"):
        group = group.sort_values("speech_order")
        rows = group.to_dict("records")

        for i in range(len(rows) - 1):
            curr = rows[i]
            nxt = rows[i + 1]

            if curr["role"] in LEG_ROLES and nxt["role"] in NONLEG_ROLES:
                dyads.append(_make_dyad(curr, nxt, meeting_id, "question"))
            elif curr["role"] in NONLEG_ROLES and nxt["role"] in LEG_ROLES:
                dyads.append(_make_dyad(nxt, curr, meeting_id, "answer"))

    dyad_df = pd.DataFrame(dyads)
    print(f"Total dyads: {len(dyad_df):,}")
    print(f"  Direction: {dyad_df['direction'].value_counts().to_dict()}")
    print(f"  Hearing types: {dyad_df['hearing_type'].value_counts().to_dict()}")
    print(f"  Witness roles (top 5): {dyad_df['witness_role'].value_counts().head(5).to_dict()}")

    return dyad_df


def _make_dyad(leg, witness, meeting_id, direction):
    """Create a single dyad record with enriched metadata."""
    return {
        # Meeting info
        "meeting_id": meeting_id,
        "term": leg["term"],
        "committee": leg["committee"],
        "committee_key": leg["committee_key"],
        "hearing_type": leg.get("hearing_type"),
        "date": leg["date"],
        "agenda": leg["agenda"],
        # Legislator info
        "leg_name": leg["person_name"],
        "leg_speaker_raw": leg["speaker"],
        "leg_member_uid": leg.get("member_uid", leg.get("member_id")),
        "leg_party": leg.get("party"),
        "leg_ruling_status": leg.get("ruling_status"),
        "leg_seniority": leg.get("seniority"),
        "leg_gender": leg.get("gender"),
        # Witness info
        "witness_name": witness["person_name"],
        "witness_speaker_raw": witness["speaker"],
        "witness_role": witness["role"],
        "witness_affiliation": witness["affiliation_raw"],
        "witness_ministry_normalized": witness.get("ministry_normalized"),
        "witness_dual_office": witness.get("dual_office"),
        "witness_admin": witness.get("admin"),
        "witness_admin_ideology": witness.get("admin_ideology"),
        # Speech content
        "direction": direction,
        "leg_speech": leg["speech_text"],
        "witness_speech": witness["speech_text"],
    }


# ── Main ──
def main(dry_run=False):
    t0 = time.time()

    # Load v8
    print(f"Loading {SPEECH_V8}...")
    if not SPEECH_V8.exists():
        print(f"ERROR: {SPEECH_V8} not found")
        sys.exit(1)

    df = pd.read_parquet(SPEECH_V8)
    print(f"  v8: {len(df):,} speeches, {df.meeting_id.nunique():,} meetings")
    print(f"  Columns: {df.columns.tolist()}")

    # Phase 1: Ministry normalization
    df = phase1_normalize_ministries(df)

    # Phase 2: Minister panel linkage
    df = phase2_link_minister_panel(df)

    # Phase 3: Clean ruling_status
    df = phase3_clean_ruling_status(df)

    if dry_run:
        print(f"\n[DRY RUN] Would save v9 with {len(df):,} speeches")
        print(f"New columns: ministry_normalized, dual_office, admin, admin_ideology")
        elapsed = time.time() - t0
        print(f"Elapsed: {elapsed:.1f}s")
        return

    # Save v9 speeches
    print(f"\nSaving {SPEECH_V9}...")
    df.to_parquet(SPEECH_V9, compression="zstd", index=False)
    speech_size = os.path.getsize(SPEECH_V9) / (1024 * 1024)
    print(f"  Saved: {speech_size:.1f} MB")

    # Phase 4: Build dyads
    dyad_df = phase4_build_dyads(df)
    del df
    gc.collect()

    print(f"\nSaving {DYADS_V9}...")
    dyad_df.to_parquet(DYADS_V9, compression="zstd", index=False)
    dyad_size = os.path.getsize(DYADS_V9) / (1024 * 1024)
    print(f"  Saved: {dyad_size:.1f} MB")

    # Summary
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"BUILD V9 COMPLETE ({elapsed:.1f}s)")
    print(f"{'=' * 60}")
    print(f"Speeches: {SPEECH_V9.name} ({speech_size:.0f} MB)")
    print(f"Dyads: {DYADS_V9.name} ({dyad_size:.0f} MB)")
    print(f"  Total dyads: {len(dyad_df):,}")

    # Dyad enrichment stats
    minister_dyads = dyad_df[dyad_df["witness_role"].isin(PANEL_LINK_ROLES)]
    linked_dyads = minister_dyads[minister_dyads["witness_admin"].notna()]
    print(f"\nMinister dyads with panel metadata: {len(linked_dyads):,} / {len(minister_dyads):,}")

    leg_party_dyads = dyad_df[dyad_df["leg_party"].notna()]
    print(f"Dyads with legislator party: {len(leg_party_dyads):,} / {len(dyad_df):,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build v9 from v8 + minister panel")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without writing files")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
