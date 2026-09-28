"""Package the v10 release tables as GitHub Release assets.

Reads build/release/ (run_all.py output) and writes build/assets/:
  meetings_v10.parquet
  turns_t{16..22}_v10.parquet        one file per Assembly term (the per-term parts merged)
  dyads_t{16..22}_v10.parquet
  agenda_v10.parquet, agenda_header_v10.parquet, events_v10.parquet, footer_v10.parquet,
  attendance_v10.parquet, rollcall_v10.parquet, rollcall_groups_v10.parquet,
  crosswalk_meetings_v10.parquet, crosswalk_turns_v10.parquet, duplicate_meetings_v10.parquet
  MANIFEST_v10.json, validation_report_v10.json
  SHA256SUMS

Row order and values are unchanged (row counts are checked against build/release/MANIFEST.json).
Each asset stays under the 2 GB GitHub Release limit. Nothing is uploaded.

Usage: python package_release.py [--release build/release] [--out build/assets] [--version v10]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq

V10 = Path(__file__).resolve().parents[2]
SINGLE = ("meetings", "agenda", "agenda_header", "events", "footer", "attendance", "rollcall",
          "rollcall_groups", "crosswalk_meetings", "crosswalk_turns", "duplicate_meetings")
TERMS = range(16, 23)
MAX_BYTES = 2 * 1024 ** 3


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _merge(parts: list[Path], dest: Path) -> int:
    parts = sorted(parts)
    if not parts:
        raise FileNotFoundError(f"no parts for {dest.name}")
    schema = pq.read_schema(parts[0])
    n = 0
    tmp = dest.with_suffix(".tmp")
    with pq.ParquetWriter(tmp, schema, compression="zstd") as w:
        for p in parts:
            f = pq.ParquetFile(p)
            for i in range(f.num_row_groups):
                t = f.read_row_group(i)
                w.write_table(t.cast(schema))
                n += t.num_rows
    tmp.replace(dest)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", type=Path, default=V10 / "build" / "release")
    ap.add_argument("--out", type=Path, default=V10 / "build" / "assets")
    ap.add_argument("--version", default="v10")
    a = ap.parse_args()
    rel, out, ver = a.release, a.out, a.version
    out.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((rel / "MANIFEST.json").read_text(encoding="utf-8"))
    expected = manifest.get("row_counts", {})
    rows = {}
    for name in SINGLE:
        src = rel / f"{name}.parquet"
        dest = out / f"{name}_{ver}.parquet"
        shutil.copyfile(src, dest)
        rows[dest.name] = pq.ParquetFile(dest).metadata.num_rows
    for kind in ("turns", "dyads"):
        total = 0
        for t in TERMS:
            parts = list((rel / kind / f"t{t}").glob("*.parquet"))
            dest = out / f"{kind}_t{t}_{ver}.parquet"
            rows[dest.name] = _merge(parts, dest)
            total += rows[dest.name]
        if kind in expected and expected[kind] != total:
            raise SystemExit(f"{kind}: {total} rows packaged, MANIFEST says {expected[kind]}")
    for name in SINGLE:
        if name in expected and expected[name] != rows[f"{name}_{ver}.parquet"]:
            raise SystemExit(f"{name}: row count differs from MANIFEST")
    shutil.copyfile(rel / "MANIFEST.json", out / f"MANIFEST_{ver}.json")
    shutil.copyfile(rel / "validation_report.json", out / f"validation_report_{ver}.json")
    big = [p.name for p in out.iterdir() if p.stat().st_size > MAX_BYTES]
    if big:
        raise SystemExit(f"assets over 2 GB: {big}")
    lines = [f"{_sha256(p)}  {p.name}" for p in sorted(out.iterdir()) if p.name != "SHA256SUMS"]
    (out / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"assets": len(lines), "rows": rows}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
