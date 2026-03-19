"""
Enrich kr-hearings-data with VCONFDETAIL meeting-level metadata.

Adds conference type flags (confirmation hearing, public hearing, etc.),
meeting times, and PDF download URLs from the National Assembly Open API.

Matching strategy:
  1. Primary: meeting_id == CONF_ID (93.4%)
  2. Fallback: date + committee (0.5% more)
  3. Unmatched (~6.1%): mostly 16th assembly gaps in VCONFDETAIL API

New columns added to speeches and dyads:
  - is_confirmation_hearing (bool): 인사청문회
  - is_public_hearing (bool): 공청회
  - is_investigation_hearing (bool): 청문회 (국정조사 등)
  - is_joint_session (bool): 연석회의
  - conf_start_time (str): 회의 시작시간
  - conf_end_time (str): 회의 종료시간
  - minutes_pdf_url (str): 회의록 PDF 다운로드 URL
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
VCONF_PATH = Path(__file__).parent.parent.parent / "kyusik-claude" / "projects" / "na-conference-details" / "data" / "vconfdetail_all.csv"

# Also accept an absolute path override
import sys
if len(sys.argv) > 1:
    VCONF_PATH = Path(sys.argv[1])


def build_meeting_lookup(vc: pd.DataFrame) -> pd.DataFrame:
    """Build a lookup table from VCONFDETAIL, keyed by CONF_ID (zero-padded)."""
    vc = vc.copy()
    vc["cid_str"] = vc.CONF_ID.astype(str).str.zfill(6)
    for col in ("BG_PTM", "ED_PTM"):
        vc[col] = vc[col].astype(str).str.strip().replace("None", pd.NA)
    return vc


def enrich(speeches_path: Path, vconf_path: Path, output_suffix: str = "_enriched") -> Path:
    print(f"Loading VCONFDETAIL from {vconf_path}...")
    vc = pd.read_csv(vconf_path)
    vc = build_meeting_lookup(vc)
    print(f"  {len(vc):,} conference records loaded")

    print(f"Loading speeches from {speeches_path}...")
    sp = pd.read_parquet(speeches_path)
    print(f"  {len(sp):,} speeches loaded")

    # Build meeting-level table
    meetings = sp[["meeting_id", "committee", "date"]].drop_duplicates("meeting_id")
    meetings["mid_str"] = meetings.meeting_id.astype(str).str.zfill(6)
    n_meetings = len(meetings)

    # --- Step 1: Direct match (meeting_id == CONF_ID) ---
    vc_lookup = vc.set_index("cid_str")[
        ["HR_HRG_YN", "PBHRG_YN", "HRG_YN", "SITG_YN", "BG_PTM", "ED_PTM", "DOWN_URL"]
    ]
    direct = meetings.merge(vc_lookup, left_on="mid_str", right_index=True, how="left")
    matched_mask = direct.HR_HRG_YN.notna()
    n_direct = matched_mask.sum()
    print(f"  Direct match (meeting_id==CONF_ID): {n_direct:,}/{n_meetings:,}")

    # --- Step 2: Fallback for unmatched (date + committee) ---
    unmatched_mids = direct.loc[~matched_mask, "meeting_id"].values
    unmatched_df = meetings[meetings.meeting_id.isin(unmatched_mids)]

    # For fallback, deduplicate VCONFDETAIL by date+committee (keep first)
    vc_fallback = vc.drop_duplicates(subset=["CONF_DT", "CMIT_NM"], keep="first")
    fallback = unmatched_df.merge(
        vc_fallback[["CONF_DT", "CMIT_NM", "HR_HRG_YN", "PBHRG_YN", "HRG_YN", "SITG_YN", "BG_PTM", "ED_PTM", "DOWN_URL"]],
        left_on=["date", "committee"],
        right_on=["CONF_DT", "CMIT_NM"],
        how="inner",
    )
    n_fallback = len(fallback)
    print(f"  Fallback match (date+committee): {n_fallback:,} additional")

    # Update direct with fallback results
    if n_fallback > 0:
        fallback_indexed = fallback.set_index("meeting_id")[
            ["HR_HRG_YN", "PBHRG_YN", "HRG_YN", "SITG_YN", "BG_PTM", "ED_PTM", "DOWN_URL"]
        ]
        direct_indexed = direct.set_index("meeting_id")
        direct_indexed.update(fallback_indexed)
        direct = direct_indexed.reset_index()

    n_total = direct.HR_HRG_YN.notna().sum()
    n_unmatched = n_meetings - n_total
    print(f"  Total matched: {n_total:,}/{n_meetings:,} ({n_total/n_meetings*100:.1f}%)")
    print(f"  Unmatched: {n_unmatched:,}")

    # --- Step 3: Create boolean flag columns ---
    flag_map = {
        "is_confirmation_hearing": "HR_HRG_YN",
        "is_public_hearing": "PBHRG_YN",
        "is_investigation_hearing": "HRG_YN",
        "is_joint_session": "SITG_YN",
    }
    meeting_enrichment = direct.set_index("meeting_id")
    for new_col, src_col in flag_map.items():
        meeting_enrichment[new_col] = meeting_enrichment[src_col] == "Y"
        # Set unmatched to False (conservative: assume not special)
        meeting_enrichment.loc[meeting_enrichment[src_col].isna(), new_col] = False

    meeting_enrichment = meeting_enrichment.rename(columns={
        "BG_PTM": "conf_start_time",
        "ED_PTM": "conf_end_time",
        "DOWN_URL": "minutes_pdf_url",
    })
    keep_cols = list(flag_map.keys()) + ["conf_start_time", "conf_end_time", "minutes_pdf_url"]
    meeting_enrichment = meeting_enrichment[keep_cols]

    # --- Step 4: Join back to speeches ---
    sp_enriched = sp.merge(meeting_enrichment, left_on="meeting_id", right_index=True, how="left")

    # Summary
    print(f"\n=== Enrichment summary ===")
    for col in flag_map:
        n = sp_enriched[col].sum()
        n_meet = sp_enriched[sp_enriched[col]].meeting_id.nunique()
        print(f"  {col}: {n:,} speeches in {n_meet} meetings")

    # Save
    out_name = speeches_path.stem + output_suffix + ".parquet"
    out_path = DATA_DIR / out_name
    sp_enriched.to_parquet(out_path, compression="zstd", index=False)
    print(f"\nSaved: {out_path} ({len(sp_enriched):,} rows)")
    return out_path


def enrich_dyads(dyads_path: Path, speeches_enriched_path: Path) -> Path:
    """Transfer flags from enriched speeches to dyads via meeting_id."""
    print(f"\nLoading enriched speeches for flag lookup...")
    sp = pd.read_parquet(speeches_enriched_path,
                         columns=["meeting_id", "is_confirmation_hearing", "is_public_hearing",
                                  "is_investigation_hearing", "is_joint_session",
                                  "conf_start_time", "conf_end_time", "minutes_pdf_url"])
    flags = sp.drop_duplicates("meeting_id").set_index("meeting_id")

    print(f"Loading dyads from {dyads_path}...")
    dy = pd.read_parquet(dyads_path)
    print(f"  {len(dy):,} dyads loaded")

    dy_enriched = dy.merge(flags, left_on="meeting_id", right_index=True, how="left")

    out_name = dyads_path.stem + "_enriched.parquet"
    out_path = DATA_DIR / out_name
    dy_enriched.to_parquet(out_path, compression="zstd", index=False)
    print(f"Saved: {out_path} ({len(dy_enriched):,} rows)")
    return out_path


if __name__ == "__main__":
    vconf_path = VCONF_PATH
    if not vconf_path.exists():
        # Try Desktop fallback
        vconf_path = Path.home() / "Desktop" / "kyusik-claude" / "projects" / "na-conference-details" / "data" / "vconfdetail_all.csv"
    if not vconf_path.exists():
        print(f"ERROR: VCONFDETAIL file not found at {vconf_path}")
        sys.exit(1)

    speeches_path = DATA_DIR / "all_speeches_16_22_v5.parquet"
    dyads_path = DATA_DIR / "dyads_16_22_v5.parquet"

    sp_out = enrich(speeches_path, vconf_path)
    enrich_dyads(dyads_path, sp_out)
