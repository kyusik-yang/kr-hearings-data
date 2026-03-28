"""Download, cache, and load kr-hearings-data parquet files."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

LATEST_VERSION = "v9"
REPO = "kyusik-yang/kr-hearings-data"
RELEASE_URL = f"https://github.com/{REPO}/releases/download"

FILES = {
    "speeches": "all_speeches_16_22_{version}.parquet",
    "dyads": "dyads_16_22_{version}.parquet",
}

CACHE_DIR = Path(os.environ.get(
    "KR_HEARINGS_CACHE",
    Path.home() / ".cache" / "kr-hearings-data",
))

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def _cache_path(dataset: str, version: str) -> Path:
    filename = FILES[dataset].format(version=version)
    return CACHE_DIR / version / filename


def _download_url(dataset: str, version: str) -> str:
    filename = FILES[dataset].format(version=version)
    return f"{RELEASE_URL}/{version}/{filename}"


def _download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    try:
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with (
            open(tmp, "wb") as f,
            tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as pbar,
        ):
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                pbar.update(len(chunk))
        tmp.rename(dest)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _ensure_cached(dataset: str, version: str) -> Path:
    path = _cache_path(dataset, version)
    if path.exists():
        return path
    url = _download_url(dataset, version)
    print(f"Downloading {dataset} ({version}) from GitHub Releases...")
    _download_file(url, path)
    print(f"Cached to {path}")
    return path


def download(version: str = LATEST_VERSION) -> dict[str, Path]:
    """Download both datasets to local cache. Returns dict of paths."""
    paths = {}
    for dataset in FILES:
        paths[dataset] = _ensure_cached(dataset, version)
    return paths


def load_speeches(
    *,
    version: str = LATEST_VERSION,
    term: int | None = None,
    hearing_type: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load speeches dataset, optionally filtered."""
    path = _ensure_cached("speeches", version)

    filters = []
    if term is not None:
        filters.append(("term", "==", term))
    if hearing_type is not None:
        filters.append(("hearing_type", "==", hearing_type))

    df = pd.read_parquet(
        path,
        columns=columns,
        filters=filters if filters else None,
    )
    return df


def load_dyads(
    *,
    version: str = LATEST_VERSION,
    term: int | None = None,
    hearing_type: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load dyads dataset, optionally filtered."""
    path = _ensure_cached("dyads", version)

    filters = []
    if term is not None:
        filters.append(("term", "==", term))
    if hearing_type is not None:
        filters.append(("hearing_type", "==", hearing_type))

    df = pd.read_parquet(
        path,
        columns=columns,
        filters=filters if filters else None,
    )
    return df


def info(version: str = LATEST_VERSION) -> None:
    """Print summary statistics for cached datasets."""
    for dataset in FILES:
        path = _cache_path(dataset, version)
        if not path.exists():
            print(f"{dataset} ({version}): not downloaded. Run `kr-hearings download`.")
            continue

        df = pd.read_parquet(path, columns=["term", "hearing_type"])
        print(f"\n{'=' * 60}")
        print(f"  {dataset} ({version})")
        print(f"  {len(df):,} rows")
        print(f"{'=' * 60}")
        print(f"\n  Terms: {sorted(df['term'].dropna().unique())}")
        print(f"  Hearing types: {sorted(df['hearing_type'].dropna().unique())}")
        print(f"\n  Rows by term:")
        for t, count in df.groupby("term", dropna=False).size().items():
            print(f"    {t}: {count:>12,}")
        print(f"\n  Rows by hearing type:")
        for ht, count in df.groupby("hearing_type", dropna=False).size().items():
            print(f"    {ht}: {count:>12,}")
