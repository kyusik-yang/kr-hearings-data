"""Cross-check every XML-built meeting against its HWP original (the printed record).

For each meeting with both a viewer page (type=view, status ok) and an HWP file (status ok),
both raw files are parsed with the production adapters (build_turns.xml_extract and
build_turns.hwp_extract) and compared on:

  n_xml / n_hwp            merged speaker turns per source
  chars_xml / chars_hwp    normalized characters of text_raw (see _cmp_norm)
  cov_hwp_in_xml           share of HWP 12-char shingles that also occur in the XML text
  cov_xml_in_hwp           share of XML 12-char shingles that also occur in the HWP text
  spk_agree                2M / (len_a + len_b) of the aligned speaker-name sequences
  n_text_pairs             turns whose normalized text (>= 20 chars) occurs exactly once on each side
  n_attr_disagree          such text-matched pairs whose speaker names differ
  cov2_hwp_in_xml / cov2_xml_in_hwp  the same shingle coverage over the whole document written as
                           label+text per turn (robust to a label split differently by the two parsers)
  label_agree              2M / (len_a + len_b) of the aligned normalized full-label sequences

Outputs (interim/pipeline/xml_hwp_crosscheck/):
  per_meeting.parquet      one row per meeting
  attribution_pairs.parquet text-matched pairs with differing speakers
  run.log

Usage: python xml_hwp_crosscheck.py [--workers 10] [--limit N] [--only 24997,24888]
"""
from __future__ import annotations

import argparse
import difflib
import gzip
import hashlib
import sqlite3
import sys
import time
import unicodedata
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
V10 = HERE.parent.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

OUT = V10 / "interim" / "pipeline" / "xml_hwp_crosscheck"
RAW = V10 / "raw"
K = 12
SEP_TR = str.maketrans({"․": "·", "‧": "·", "ㆍ": "·", "・": "·"})
DROP = set(" \t\r\n 　.,·'\"“”‘’()[]{}「」『』｢｣<>〈〉《》-―–—…?!:;~/")


def _cmp_norm(s):
    """Comparison form: NFKC, separator unification, drop whitespace and punctuation."""
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", str(s).translate(SEP_TR))
    return "".join(ch for ch in t if ch not in DROP)


def _name_norm(t):
    n = t.get("speaker_name") or t.get("speaker_label_raw") or ""
    return _cmp_norm(n)


def _shingles(s):
    return {s[i:i + K] for i in range(max(len(s) - K + 1, 0))} if len(s) >= K else ({s} if s else set())


def _xml_turns(n):
    import build_turns as bt
    p = RAW / "viewer" / "view" / f"{n // 1000:03d}" / f"{n}.html.gz"
    r = bt.xml_extract(n, gzip.open(p).read())
    return r.get("status"), (r.get("tables") or {}).get("turns") or []


def _hwp_turns(n):
    import build_turns as bt
    p = RAW / "hwp" / f"{n // 1000:03d}" / f"{n}.hwp"
    r = bt.hwp_extract(n, p.read_bytes())
    return r.get("status"), (r.get("tables") or {}).get("turns") or []


def compare(n):
    t0 = time.time()
    row = {"conf_num": n}
    try:
        sx, tx = _xml_turns(n)
        sh, th = _hwp_turns(n)
    except Exception as e:  # recorded, never silently dropped
        row.update(error=repr(e)[:300])
        return row, []
    row.update(status_xml=str(sx), status_hwp=str(sh), n_xml=len(tx), n_hwp=len(th))
    ax = [_cmp_norm(t.get("text_raw")) for t in tx]
    ah = [_cmp_norm(t.get("text_raw")) for t in th]
    jx, jh = "␞".join(ax), "␞".join(ah)
    row["chars_xml"] = sum(map(len, ax))
    row["chars_hwp"] = sum(map(len, ah))
    shx, shh = _shingles(jx.replace("␞", "")), _shingles(jh.replace("␞", ""))
    inter = len(shx & shh)
    row["cov_hwp_in_xml"] = inter / len(shh) if shh else None
    row["cov_xml_in_hwp"] = inter / len(shx) if shx else None
    nx, nh = [_name_norm(t) for t in tx], [_name_norm(t) for t in th]
    sm = difflib.SequenceMatcher(None, nx, nh, autojunk=False)
    m = sum(b.size for b in sm.get_matching_blocks())
    row["spk_agree"] = (2 * m / (len(nx) + len(nh))) if (nx or nh) else None
    # attribution: texts (>= 20 normalized chars) unique on each side
    def index(a):
        d = {}
        for i, s in enumerate(a):
            if len(s) >= 20:
                d.setdefault(s, []).append(i)
        return {s: v[0] for s, v in d.items() if len(v) == 1}
    ix, ih = index(ax), index(ah)
    common = ix.keys() & ih.keys()
    row["n_text_pairs"] = len(common)
    pairs = []
    for s in common:
        i, j = ix[s], ih[s]
        if nx[i] != nh[j]:
            pairs.append({"conf_num": n, "xml_turn_seq": tx[i].get("turn_seq"), "hwp_turn_index": j + 1,
                          "xml_label": tx[i].get("speaker_label_raw"), "hwp_label": th[j].get("speaker_label_raw"),
                          "xml_name_norm": nx[i], "hwp_name_norm": nh[j], "n_chars": len(s), "text_head": s[:60]})
    row["n_attr_disagree"] = len(pairs)
    # label-placement-robust measures: whole document as label+text per turn, and labels without spaces
    dx = "".join(_cmp_norm((t.get("speaker_label_raw") or "") + (t.get("text_raw") or "")) for t in tx)
    dh = "".join(_cmp_norm((t.get("speaker_label_raw") or "") + (t.get("text_raw") or "")) for t in th)
    s2x, s2h = _shingles(dx), _shingles(dh)
    i2 = len(s2x & s2h)
    row["doc_chars_xml"], row["doc_chars_hwp"] = len(dx), len(dh)
    row["cov2_hwp_in_xml"] = i2 / len(s2h) if s2h else None
    row["cov2_xml_in_hwp"] = i2 / len(s2x) if s2x else None
    lx = [_cmp_norm(t.get("speaker_label_raw")) for t in tx]
    lh = [_cmp_norm(t.get("speaker_label_raw")) for t in th]
    sm2 = difflib.SequenceMatcher(None, lx, lh, autojunk=False)
    m2 = sum(b.size for b in sm2.get_matching_blocks())
    row["label_agree"] = (2 * m2 / (len(lx) + len(lh))) if (lx or lh) else None
    row["secs"] = round(time.time() - t0, 2)
    return row, pairs


def targets(limit=None, only=None):
    c = sqlite3.connect(f"file:{V10 / 'interim' / 'crawl_state.sqlite'}?mode=ro", uri=True)
    xml = {r[0] for r in c.execute("SELECT conf_num FROM fetch WHERE kind='view' AND status='ok'")}
    hwp = {r[0] for r in c.execute("SELECT conf_num FROM fetch WHERE kind='hwp' AND status='ok'")}
    ids = sorted(xml & hwp)
    if only:
        ids = [i for i in ids if i in set(only)]
    return ids[:limit] if limit else ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--only")
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    only = [int(x) for x in a.only.split(",")] if a.only else None
    ids = targets(a.limit, only)
    OUT.mkdir(parents=True, exist_ok=True)
    log = open(OUT / f"run{a.tag}.log", "a", encoding="utf-8")
    log.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} START n={len(ids)} workers={a.workers}\n"); log.flush()
    rows, pairs, t0 = [], [], time.time()
    with Pool(a.workers, maxtasksperchild=200) as pool:
        for k, (r, p) in enumerate(pool.imap_unordered(compare, ids, chunksize=8), 1):
            rows.append(r); pairs.extend(p)
            if k % 500 == 0:
                log.write(f"{time.strftime('%H:%M:%S')} {k}/{len(ids)} {time.time() - t0:.0f}s\n"); log.flush()
    pd.DataFrame(rows).to_parquet(OUT / f"per_meeting{a.tag}.parquet", index=False)
    pd.DataFrame(pairs).to_parquet(OUT / f"attribution_pairs{a.tag}.parquet", index=False)
    log.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} DONE rows={len(rows)} pairs={len(pairs)} "
              f"errors={sum('error' in r for r in rows)} secs={time.time() - t0:.0f}\n")
    log.close()


if __name__ == "__main__":
    main()
