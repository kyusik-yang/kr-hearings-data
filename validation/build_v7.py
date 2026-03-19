"""
Build v7 dataset from v6 by adding 228 PDF-parsed 인사청문특별위원회 meetings.

Changes from v6:
1. Add 228 인사청문특별위원회 meetings (111K speeches) parsed from official PDFs
2. Total 인사청문특별위원회: 42 (v6) + 228 (gap-fill) = 270 meetings
3. Apply v5_enriched flags (is_confirmation_hearing etc.) to new data

Source: emotional-assembly/data/gap_hearing_speeches.parquet
Method: PDF download from DOWN_URL -> PyMuPDF text extraction -> speaker parsing
"""

import pandas as pd
import numpy as np
from pathlib import Path

KR_DATA = Path("/Users/kyusik/Desktop/kyusik-github/kr-hearings-data/data")
GAP_DATA = Path("/Users/kyusik/Desktop/kyusik-claude/projects/emotional-assembly/data/gap_hearing_speeches.parquet")
VCONF = Path("/Users/kyusik/Desktop/kyusik-claude/projects/na-conference-details/data/vconfdetail_all.csv")

SPEECH_V6 = KR_DATA / "all_speeches_16_22_v6.parquet"
SPEECH_V7 = KR_DATA / "all_speeches_16_22_v7.parquet"
DYADS_V6 = KR_DATA / "dyads_16_22_v6.parquet"
DYADS_V7 = KR_DATA / "dyads_16_22_v7.parquet"


def main():
    # ─── Load ───
    print("Loading v6 speeches...")
    v6 = pd.read_parquet(SPEECH_V6)
    print(f"  v6: {len(v6):,} speeches, {v6.meeting_id.nunique():,} meetings")

    print("Loading gap-fill speeches...")
    gap = pd.read_parquet(GAP_DATA)
    print(f"  gap: {len(gap):,} speeches, {gap.meeting_id.nunique()} meetings")

    # ─── Validate no overlap ───
    v6_mids = set(v6.meeting_id.unique())
    gap_mids = set(gap.meeting_id.unique())
    overlap = v6_mids & gap_mids
    if overlap:
        print(f"  WARNING: {len(overlap)} overlapping meeting_ids - removing from gap")
        gap = gap[~gap.meeting_id.isin(overlap)]

    # ─── Align schemas ───
    # Ensure gap has all v6 columns in the same order
    for col in v6.columns:
        if col not in gap.columns:
            gap[col] = pd.NA

    # Ensure same dtypes
    gap["term"] = gap["term"].astype("Int64")
    gap["seniority"] = pd.to_numeric(gap.get("seniority"), errors="coerce")

    # Select only v6 columns in v6 order
    gap = gap[v6.columns]

    # ─── Concatenate ───
    print("\nConcatenating...")
    v7 = pd.concat([v6, gap], ignore_index=True)
    print(f"  v7: {len(v7):,} speeches, {v7.meeting_id.nunique():,} meetings")

    # ─── Add enrichment flags for new meetings ───
    print("\nAdding vconfdetail enrichment flags...")
    vc = pd.read_csv(VCONF)
    vc["cid_str"] = vc.CONF_ID.astype(str)
    vc_lookup = vc.set_index("cid_str")[["HR_HRG_YN", "PBHRG_YN", "HRG_YN", "SITG_YN", "BG_PTM", "ED_PTM", "DOWN_URL"]]

    # Check if enrichment columns exist (from v5_enriched -> v6)
    enrichment_cols = ["is_confirmation_hearing", "is_public_hearing",
                       "is_investigation_hearing", "is_joint_session",
                       "conf_start_time", "conf_end_time", "minutes_pdf_url"]

    has_enrichment = all(c in v7.columns for c in enrichment_cols)

    if has_enrichment:
        # Fill enrichment for new gap meetings
        new_mask = v7.meeting_id.isin(gap_mids)
        for _, row in v7[new_mask].drop_duplicates("meeting_id").iterrows():
            mid = str(row.meeting_id)
            if mid in vc_lookup.index:
                vc_row = vc_lookup.loc[mid]
                meeting_mask = v7.meeting_id == row.meeting_id
                v7.loc[meeting_mask, "is_confirmation_hearing"] = vc_row.get("HR_HRG_YN") == "Y"
                v7.loc[meeting_mask, "is_public_hearing"] = vc_row.get("PBHRG_YN") == "Y"
                v7.loc[meeting_mask, "is_investigation_hearing"] = vc_row.get("HRG_YN") == "Y"
                v7.loc[meeting_mask, "is_joint_session"] = vc_row.get("SITG_YN") == "Y"
                v7.loc[meeting_mask, "conf_start_time"] = vc_row.get("BG_PTM")
                v7.loc[meeting_mask, "conf_end_time"] = vc_row.get("ED_PTM")
                v7.loc[meeting_mask, "minutes_pdf_url"] = vc_row.get("DOWN_URL")
        print(f"  Enrichment flags filled for {new_mask.sum():,} speeches")
    else:
        print("  No enrichment columns in v6 - skipping")

    # ─── Save speeches ───
    print(f"\nSaving v7 speeches...")
    v7.to_parquet(SPEECH_V7, compression="zstd", index=False)
    print(f"  Saved: {SPEECH_V7} ({len(v7):,} rows)")

    # ─── Summary ───
    print("\n" + "=" * 60)
    print("V7 BUILD SUMMARY")
    print("=" * 60)
    print(f"Total speeches: {len(v7):,} (v6: {len(v6):,}, +{len(v7)-len(v6):,})")
    print(f"Total meetings: {v7.meeting_id.nunique():,} (v6: {v6.meeting_id.nunique():,}, +{v7.meeting_id.nunique()-v6.meeting_id.nunique()})")

    # Hearing type breakdown
    print(f"\nBy hearing_type:")
    for ht, grp in v7.groupby("hearing_type"):
        print(f"  {ht}: {grp.meeting_id.nunique()} meetings, {len(grp):,} speeches")

    # Term breakdown for 인사청문특별위원회
    special = v7[v7.hearing_type == "인사청문특별위원회"]
    print(f"\n인사청문특별위원회 by term:")
    for term in sorted(special.term.dropna().unique()):
        g = special[special.term == term]
        print(f"  {int(term)}대: {g.meeting_id.nunique()} meetings, {len(g):,} speeches")

    # Note: dyads not rebuilt (would need full pipeline)
    print(f"\nNOTE: Dyads (v7) not rebuilt. Use build_v6.py pattern to rebuild if needed.")
    print(f"  v6 dyads at: {DYADS_V6}")


if __name__ == "__main__":
    main()
