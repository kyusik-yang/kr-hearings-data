"""Open API downloads for the legislators component (v10 pipeline).

Keyed, sequential, at most one request per 1.1 s. The key is read at runtime from the
researcher's config file and is never printed, logged or written: the request log stores
the URL without the KEY parameter.

Services (codes verified against raw/05_members/service_meta.json, 2026-09-25):
  ALLNAMEMBER          국회의원 정보 통합 API (all members, all terms), pSize 1000
  npffdutiapkzbfyvr    역대 국회의원 인적사항 (UNIT_CD=1000NN required; one row per member-term)
  nwvrqwxyaytdsfvhu    국회의원 인적사항 (sitting members)
  nfzegpkvaclgtscxt    역대 국회의원 의원이력 (PROFILE_UNIT_CD required; seat spans FRTO_DATE)
  nexgtxtmaamffofof    국회의원 의원이력 (sitting members, all their terms)
  nqbeopthavwwfbekw    역대 국회의원 위원회 경력 (PROFILE_UNIT_CD required; committee spells)
  nyzrglyvagmrypezq    국회의원 위원회 경력 (sitting members)

Usage:  python legislators_fetch.py [service ...]     (default: all)
Output: v10/interim/pipeline/legislators/api/{service}[_{unit}]_p{page}.json + request_log.jsonl
"""
import datetime as dt
import json
import re
import sys
import time
from pathlib import Path

import requests

V10 = Path(__file__).resolve().parents[2]
OUT = V10 / "interim" / "pipeline" / "legislators" / "api"
LOG = OUT / "request_log.jsonl"
BASE = "https://open.assembly.go.kr/portal/openapi/"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
MIN_GAP = 1.1
PSIZE = 1000

_session = requests.Session()
_session.headers.update({"User-Agent": UA, "Accept": "application/json,*/*"})
_last = [0.0]


def _key():
    from apikey import get_assembly_api_key      # env ASSEMBLY_API_KEY or ASSEMBLY_API_KEY_FILE
    return get_assembly_api_key()


def _log(service, params, status, nbytes, secs, err):
    OUT.mkdir(parents=True, exist_ok=True)
    safe = {k: v for k, v in params.items() if k.upper() != "KEY"}
    with LOG.open("a") as f:
        f.write(json.dumps({"ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                            "url": BASE + service, "params": safe, "status": status, "bytes": nbytes,
                            "secs": round(secs, 2), "error": err}, ensure_ascii=False) + "\n")


def call(service, retries=3, **params):
    """One keyed call. Returns (total, code, message, rows)."""
    p = {"KEY": _key(), "Type": "json", "pIndex": 1, "pSize": PSIZE}
    p.update(params)
    for attempt in range(retries):
        gap = time.time() - _last[0]
        if gap < MIN_GAP:
            time.sleep(MIN_GAP - gap)
        _last[0] = time.time()
        t0 = time.time()
        try:
            r = _session.get(BASE + service, params=p, timeout=90)
        except Exception as e:  # network error: log without the URL query (it holds the key)
            _log(service, p, None, 0, time.time() - t0, type(e).__name__)
            time.sleep(5 * (attempt + 1))
            continue
        _log(service, p, r.status_code, len(r.content), time.time() - t0, None)
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        js = r.json()
        if service in js:
            head = js[service][0]["head"]
            total = head[0].get("list_total_count")
            res = head[1]["RESULT"]
            rows = js[service][1]["row"] if len(js[service]) > 1 else []
            return total, res["CODE"], res["MESSAGE"], rows
        res = js.get("RESULT", {})
        return None, res.get("CODE"), res.get("MESSAGE"), []
    raise RuntimeError(f"failed after {retries} attempts: {service}")


def fetch_all(service, tag=None, **params):
    """Page through a service at pSize=1000 and save each page. Returns list of rows."""
    tag = tag or service
    rows, page, total = [], 1, None
    while True:
        path = OUT / f"{tag}_p{page}.json"
        if path.exists():
            js = json.loads(path.read_text())
        else:
            t, code, msg, r = call(service, pIndex=page, **params)
            js = {"service": service, "params": params, "page": page, "total": t, "code": code,
                  "message": msg, "rows": r,
                  "accessed_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
            path.write_text(json.dumps(js, ensure_ascii=False))
        total = js["total"] if js["total"] is not None else total
        rows.extend(js["rows"])
        print(f"{tag} page {page}: code={js['code']} total={js['total']} rows={len(js['rows'])} cum={len(rows)}")
        if not js["rows"] or total is None or len(rows) >= total:
            break
        page += 1
    return rows


UNITS = [f"1000{t}" for t in range(16, 23)]

JOBS = {
    "ALLNAMEMBER": lambda: fetch_all("ALLNAMEMBER"),
    "nwvrqwxyaytdsfvhu": lambda: fetch_all("nwvrqwxyaytdsfvhu"),
    "nexgtxtmaamffofof": lambda: fetch_all("nexgtxtmaamffofof"),
    "nyzrglyvagmrypezq": lambda: fetch_all("nyzrglyvagmrypezq"),
    "npffdutiapkzbfyvr": lambda: [fetch_all("npffdutiapkzbfyvr", tag=f"npffdutiapkzbfyvr_{u}", UNIT_CD=u)
                                  for u in UNITS],
    "nfzegpkvaclgtscxt": lambda: [fetch_all("nfzegpkvaclgtscxt", tag=f"nfzegpkvaclgtscxt_{u}", PROFILE_UNIT_CD=u)
                                  for u in UNITS],
    "nqbeopthavwwfbekw": lambda: [fetch_all("nqbeopthavwwfbekw", tag=f"nqbeopthavwwfbekw_{u}", PROFILE_UNIT_CD=u)
                                  for u in UNITS],
}


if __name__ == "__main__":
    todo = sys.argv[1:] or list(JOBS)
    for s in todo:
        JOBS[s]()
