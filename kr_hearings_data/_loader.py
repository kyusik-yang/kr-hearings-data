"""Download, cache, and load kr-hearings-data parquet files.

v10 (default) is published as one asset per table, with turns and dyads split by Assembly term:
    meetings_v10.parquet, turns_t{16..22}_v10.parquet, dyads_t{16..22}_v10.parquet, and side tables
    (agenda, agenda_header, events, footer, attendance, rollcall, rollcall_groups,
    crosswalk_meetings, crosswalk_turns, duplicate_meetings).
v9 and older are no longer distributed (the v9 dyads are defective; see docs/CHANGELOG.md).
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

LATEST_VERSION = "v10"
REPO = "kyusik-yang/kr-hearings-data"
RELEASE_URL = f"https://github.com/{REPO}/releases/download"
TERMS = tuple(range(16, 23))

V10_TABLES = ("meetings", "agenda", "agenda_header", "events", "footer", "attendance", "rollcall",
              "rollcall_groups", "crosswalk_meetings", "crosswalk_turns", "duplicate_meetings")
V10_BY_TERM = ("turns", "dyads")

CACHE_DIR = Path(os.environ.get(
    "KR_HEARINGS_CACHE",
    Path.home() / ".cache" / "kr-hearings-data",
))

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def _check_version(version: str) -> None:
    try:
        major = int(version.lstrip("vV").split(".")[0])
    except ValueError:
        return
    if major < 10:
        raise ValueError(f"{version} is no longer distributed; v10 is the first available version "
                         "(the v9 dyads are defective, see docs/CHANGELOG.md)")


def _asset_name(dataset: str, version: str, term: int | None = None) -> str:
    _check_version(version)
    if dataset in V10_BY_TERM:
        if term is None:
            raise ValueError(f"{dataset} is split by term in {version}; pass term")
        return f"{dataset}_t{term}_{version}.parquet"
    if dataset not in V10_TABLES:
        raise ValueError(f"unknown table {dataset!r}; one of {V10_TABLES + V10_BY_TERM}")
    return f"{dataset}_{version}.parquet"


def _cache_path(dataset: str, version: str, term: int | None = None) -> Path:
    return CACHE_DIR / version / _asset_name(dataset, version, term)


def _download_url(dataset: str, version: str, term: int | None = None) -> str:
    return f"{RELEASE_URL}/{version}/{_asset_name(dataset, version, term)}"


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


def _ensure_cached(dataset: str, version: str, term: int | None = None) -> Path:
    path = _cache_path(dataset, version, term)
    if path.exists():
        return path
    url = _download_url(dataset, version, term)
    print(f"Downloading {path.name} from GitHub Releases...")
    _download_file(url, path)
    print(f"Cached to {path}")
    return path


def _terms(term: int | None) -> tuple[int, ...]:
    if term is None:
        return TERMS
    if term not in TERMS:
        raise ValueError(f"term must be one of {TERMS}")
    return (term,)


def download(version: str = LATEST_VERSION, *, terms: list[int] | None = None,
             tables: list[str] | None = None) -> dict[str, Path]:
    """Download datasets to the local cache and return {name: path}.

    All tables and every term by default (about 4.9 GB); restrict with `terms` and `tables`
    (e.g. tables=["meetings", "turns"], terms=[21])."""
    _check_version(version)
    paths = {}
    wanted = tables or list(V10_TABLES + V10_BY_TERM)
    for name in wanted:
        if name in V10_BY_TERM:
            for t in (terms or TERMS):
                paths[f"{name}_t{t}"] = _ensure_cached(name, version, t)
        else:
            paths[name] = _ensure_cached(name, version)
    return paths


def load_table(name: str, *, version: str = LATEST_VERSION, columns: list[str] | None = None) -> pd.DataFrame:
    """Load one v10 table that is not split by term (meetings, agenda, events, footer, attendance,
    rollcall, rollcall_groups, agenda_header, crosswalk_meetings, crosswalk_turns, duplicate_meetings)."""
    return pd.read_parquet(_ensure_cached(name, version), columns=columns)


def load_meetings(*, version: str = LATEST_VERSION, term: int | None = None,
                  hearing_type: str | None = None, columns: list[str] | None = None) -> pd.DataFrame:
    """Load the v10 meetings table (one row per meeting), optionally filtered."""
    filters = []
    if term is not None:
        filters.append(("term", "==", term))
    if hearing_type is not None:
        filters.append(("hearing_type", "==", hearing_type))
    path = _ensure_cached("meetings", version)
    return pd.read_parquet(path, columns=columns, filters=filters or None)


def _load_by_term(kind: str, version: str, term: int | None, hearing_type: str | None,
                  columns: list[str] | None) -> pd.DataFrame:
    keep = None
    if hearing_type is not None:
        m = load_meetings(version=version, term=term, hearing_type=hearing_type, columns=["conf_num"])
        keep = set(m["conf_num"].tolist())
    read_cols = columns
    if keep is not None and columns is not None and "conf_num" not in columns:
        read_cols = list(columns) + ["conf_num"]
    frames = []
    for t in _terms(term):
        df = pd.read_parquet(_ensure_cached(kind, version, t), columns=read_cols)
        if keep is not None:
            df = df[df["conf_num"].isin(keep)]
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=read_cols)
    if read_cols is not columns:
        out = out[list(columns)]
    return out


def load_turns(*, version: str = LATEST_VERSION, term: int | None = None,
               hearing_type: str | None = None, columns: list[str] | None = None) -> pd.DataFrame:
    """Load v10 speech turns (one row per merged speaker turn). Only the requested terms are
    downloaded. `hearing_type` is taken from the meetings table."""
    return _load_by_term("turns", version, term, hearing_type, columns)


def load_speeches(
    *,
    version: str = LATEST_VERSION,
    term: int | None = None,
    hearing_type: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load speeches: the same as load_turns (speech turns), kept for code written for v9."""
    return load_turns(version=version, term=term, hearing_type=hearing_type, columns=columns)


def load_dyads(
    *,
    version: str = LATEST_VERSION,
    term: int | None = None,
    hearing_type: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load legislator-witness dyads, optionally filtered. v10 dyads are numerically adjacent
    turn pairs with both turn positions (join to turns on conf_num and leg_turn_seq / wit_turn_seq)."""
    return _load_by_term("dyads", version, term, hearing_type, columns)


def info(version: str = LATEST_VERSION) -> None:
    """Print summary statistics for cached datasets."""
    path = _cache_path("meetings", version)
    if not path.exists():
        print(f"meetings ({version}): not downloaded. Run `kr-hearings download`.")
        return
    m = pd.read_parquet(path)
    released = m[m["duplicate_of"].isna()] if "duplicate_of" in m.columns else m
    print(f"\n{'=' * 60}\n  meetings ({version}): {len(m):,} rows, "
          f"{int(released['n_turns'].fillna(0).sum()):,} turns in the release "
          f"(duplicate copies excluded)\n{'=' * 60}")
    print(m.groupby(["term", "hearing_type"]).size().unstack(fill_value=0).to_string())
    cached = sorted(p.name for p in (CACHE_DIR / version).glob("*.parquet"))
    print(f"\n  Cached files: {', '.join(cached)}")
