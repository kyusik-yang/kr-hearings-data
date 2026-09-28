"""Second-stage XML-vs-HWP check for meetings flagged by xml_hwp_crosscheck.py.

Stage 1 compared turns with turns. The two sources route some printed text differently (the
viewer keeps interjections, time/video notes, agenda anchors, written answers and attendance
outside the speech turns), so stage 1 over-counts "missing" text. Stage 2 asks the question that
decides the source:

  turns_hwp_in_all_xml   share of HWP speech-turn shingles (label+text) found anywhere in the XML
                         page (turns incl. interjections and stage texts, events, agenda, agenda
                         header, footer, attendance, roll-call names)
  turns_xml_in_all_hwp   the same in the other direction
  missing_hwp_chars      normalized chars of HWP turn text in runs of >= 15 chars absent from the XML

Usage: python xml_hwp_crosscheck_full.py --from per_meeting_final.parquet [--workers 10]
"""
from __future__ import annotations

import argparse
import gzip
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from xml_hwp_crosscheck import OUT, RAW, _cmp_norm, _shingles  # noqa: E402


def _all_text(tables):
    parts = []
    for t in tables.get("turns") or []:
        parts.append((t.get("speaker_label_raw") or "") + (t.get("text_raw") or ""))
        for key in ("interjections", "stage_texts", "inline_stage_parens"):
            v = t.get(key)
            if isinstance(v, (list, tuple)):
                for x in v:
                    parts.append(" ".join(str(y) for y in (x.values() if isinstance(x, dict) else [x]) if y))
    for key in ("events", "agenda", "agenda_header", "footer", "attendance", "rollcall"):
        for r in tables.get(key) or []:
            parts.append(" ".join(str(v) for k, v in r.items()
                                  if isinstance(v, str) and k not in ("source", "conf_num", "term")))
    return _cmp_norm("".join(parts))


def _turn_doc(tables):
    return _cmp_norm("".join((t.get("speaker_label_raw") or "") + (t.get("text_raw") or "")
                             for t in tables.get("turns") or []))


def check(n):
    import build_turns as bt
    row = {"conf_num": n}
    try:
        rx = bt.xml_extract(n, gzip.open(RAW / "viewer" / "view" / f"{n // 1000:03d}" / f"{n}.html.gz").read())
        rh = bt.hwp_extract(n, (RAW / "hwp" / f"{n // 1000:03d}" / f"{n}.hwp").read_bytes())
    except Exception as e:
        row["error"] = repr(e)[:300]
        return row
    tx, th = rx["tables"], rh["tables"]
    dx, dh = _turn_doc(tx), _turn_doc(th)
    ax, ah = _all_text(tx), _all_text(th)
    sdx, sdh, sax, sah = _shingles(dx), _shingles(dh), _shingles(ax), _shingles(ah)
    row["turns_hwp_in_all_xml"] = len(sdh & sax) / len(sdh) if sdh else None
    row["turns_xml_in_all_hwp"] = len(sdx & sah) / len(sdx) if sdx else None
    # absent runs, linear: HWP positions not covered by any 12-char shingle that also occurs in the XML
    miss = []
    if row["turns_hwp_in_all_xml"] is not None and row["turns_hwp_in_all_xml"] < 0.999:
        k = 12
        covered = bytearray(len(dh))
        for i in range(max(len(dh) - k + 1, 0)):
            if dh[i:i + k] in sax:
                covered[i:i + k] = b"\x01" * k
        i = 0
        while i < len(dh):
            if not covered[i]:
                j = i
                while j < len(dh) and not covered[j]:
                    j += 1
                if j - i >= 15:
                    miss.append(dh[i:j])
                i = j
            else:
                i += 1
    row["missing_hwp_chars"] = sum(map(len, miss))
    row["missing_hwp_runs"] = len(miss)
    row["missing_hwp_example"] = max(miss, key=len)[:200] if miss else None
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", default=str(OUT / "per_meeting_final.parquet"))
    ap.add_argument("--workers", type=int, default=10)
    a = ap.parse_args()
    d = pd.read_parquet(a.src)
    sel = d[(d.cov2_hwp_in_xml < 0.99) | (d.cov2_xml_in_hwp < 0.99) | (d.label_agree < 0.95)].conf_num.astype(int).tolist()
    t0 = time.time()
    with Pool(a.workers, maxtasksperchild=50) as pool:
        rows = list(pool.imap_unordered(check, sel, chunksize=4))
    out = pd.DataFrame(rows)
    out.to_parquet(OUT / "stage2.parquet", index=False)
    print(f"stage2 meetings={len(sel)} secs={time.time() - t0:.0f} errors={out.get('error', pd.Series(dtype=object)).notna().sum()}")


if __name__ == "__main__":
    main()
