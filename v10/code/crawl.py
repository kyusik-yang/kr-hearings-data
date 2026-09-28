"""Resumable, polite crawler for record.assembly.go.kr minutes (v10 raw layer).

Task kinds
  view     xml.do?id=N&type=view      -> raw/viewer/view/{N//1000}/{N}.html.gz
  summary  xml.do?id=N&type=summary   -> raw/viewer/summary/{N//1000}/{N}.html.gz
  hwp      download/hwp.do?id=N       -> raw/hwp/{N//1000}/{N}.hwp

Politeness
  - A global limiter spaces request starts at least MIN_INTERVAL seconds apart
    across all worker threads (default 1.0 s, i.e. at most 1 request/second).
  - At most WORKERS requests are in flight (default 3). Server latency (about
    3-6 s per view page) keeps the effective rate well under the cap.
  - Browser-like User-Agent. Exponential backoff on errors and a circuit breaker
    that pauses all workers after a run of consecutive failures.

State
  interim/crawl_state.sqlite, table fetch(conf_num, kind, status, http, bytes,
  sha1, attempts, error, fetched_at). Terminal statuses are
  ok, no_xml (HTTP 400 'Bad Request.'), not_found ('회의록 정보를 찾을 수 없습니다'),
  not_published (hwp.do alert: the original minutes file is not registered yet),
  no_original (hwp.do alert: no original minutes file exists for this id).
  Tasks with a terminal status are skipped on restart.

Usage
  python crawl.py --tasks interim/crawl_tasks.parquet [--workers 3] [--min-interval 1.0]
  python crawl.py --status
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

V10 = Path(__file__).resolve().parent.parent
RAW = V10 / "raw"
STATE_DB = V10 / "interim" / "crawl_state.sqlite"
LOG_FILE = V10 / "interim" / "crawl.log"

BASE = "https://record.assembly.go.kr/assembly"
URLS = {
    "view": BASE + "/viewer/minutes/xml.do?id={n}&type=view",
    "summary": BASE + "/viewer/minutes/xml.do?id={n}&type=summary",
    "hwp": BASE + "/viewer/minutes/download/hwp.do?id={n}",
}
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0 Safari/537.36")
NOT_FOUND = "회의록 정보를 찾을 수 없습니다"
OLE_MAGIC = bytes.fromhex("D0CF11E0A1B11AE1")
TERMINAL = ("ok", "no_xml", "not_found", "not_published", "no_original")
NO_ORIGINAL = "내려받을 원본 회의록이 없습니다"
NOT_PUBLISHED = "내려받을 원본 회의록 파일이 등록되지 않았습니다"
BACKOFF = [10, 30, 60, 120, 300]
BREAKER_THRESHOLD = 10
BREAKER_PAUSE = 900


def out_path(kind: str, n: int) -> Path:
    bucket = f"{n // 1000:03d}"
    if kind == "hwp":
        return RAW / "hwp" / bucket / f"{n}.hwp"
    return RAW / "viewer" / kind / bucket / f"{n}.html.gz"


def classify(kind: str, status_code: int, body: bytes) -> str:
    """Map an HTTP response to a crawl status. 'retry' means try again later."""
    if kind == "hwp":
        if status_code == 200 and body[:8] == OLE_MAGIC:
            return "ok"
        if status_code == 200 and NOT_FOUND.encode() in body:
            return "not_found"
        if status_code == 200 and NOT_PUBLISHED.encode() in body:
            return "not_published"
        if status_code == 200 and NO_ORIGINAL.encode() in body:
            return "no_original"
        if status_code == 400:
            return "no_xml"
        return "retry"
    if status_code == 400 and body.strip() == b"Bad Request.":
        return "no_xml"
    if status_code != 200:
        return "retry"
    text = body.decode("utf-8", errors="ignore")
    if NOT_FOUND in text and len(body) < 5000:
        return "not_found"
    if kind == "view" and "minutes_body" in text and "</html>" in text[-2000:].lower():
        return "ok"
    if kind == "summary" and ("summary_wrap" in text or "list_file" in text) and "</html>" in text[-2000:].lower():
        return "ok"
    return "retry"


class Limiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.next_ok = 0.0
        self.pause_until = 0.0

    def wait(self):
        while True:
            with self.lock:
                now = time.time()
                start = max(self.next_ok, self.pause_until)
                if now >= start:
                    self.next_ok = now + self.min_interval
                    return
                delay = start - now
            time.sleep(min(delay, 5.0))

    def pause(self, seconds: float):
        with self.lock:
            self.pause_until = max(self.pause_until, time.time() + seconds)


class State:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=60)
        self.lock = threading.Lock()
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS fetch (conf_num INTEGER, kind TEXT, status TEXT, http INTEGER,"
            " bytes INTEGER, sha1 TEXT, attempts INTEGER, error TEXT, elapsed REAL, fetched_at TEXT,"
            " PRIMARY KEY (conf_num, kind))")
        self.conn.commit()

    def done(self) -> set[tuple[int, str]]:
        q = f"SELECT conf_num, kind FROM fetch WHERE status IN {TERMINAL}"
        with self.lock:
            return {(int(a), b) for a, b in self.conn.execute(q)}

    def record(self, n, kind, status, http, nbytes, sha1, attempts, error, elapsed):
        with self.lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO fetch VALUES (?,?,?,?,?,?,?,?,?,?)",
                (n, kind, status, http, nbytes, sha1, attempts, error, elapsed,
                 dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")))
            self.conn.commit()

    def summary(self):
        with self.lock:
            return list(self.conn.execute(
                "SELECT kind, status, COUNT(*), SUM(bytes) FROM fetch GROUP BY kind, status ORDER BY kind, status"))


class Crawler:
    def __init__(self, workers: int, min_interval: float, timeout: float):
        self.limiter = Limiter(min_interval)
        self.state = State(STATE_DB)
        self.timeout = timeout
        self.local = threading.local()
        self.fail_lock = threading.Lock()
        self.consecutive_failures = 0
        self.workers = workers
        self.counter_lock = threading.Lock()
        self.n_done = 0
        self.t0 = time.time()
        self.stop = threading.Event()

    def session(self) -> requests.Session:
        s = getattr(self.local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"})
            self.local.s = s
        return s

    def log(self, msg: str):
        line = f"{dt.datetime.now().isoformat(timespec='seconds')} {msg}"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _note_result(self, ok: bool):
        with self.fail_lock:
            if ok:
                self.consecutive_failures = 0
                return
            self.consecutive_failures += 1
            if self.consecutive_failures >= BREAKER_THRESHOLD:
                self.log(f"BREAKER: {self.consecutive_failures} consecutive failures, pausing {BREAKER_PAUSE}s")
                self.limiter.pause(BREAKER_PAUSE)
                self.consecutive_failures = 0

    def fetch_one(self, n: int, kind: str, total: int):
        if self.stop.is_set():
            return
        url = URLS[kind].format(n=n)
        status, http, body, err, elapsed = "retry", None, b"", None, 0.0
        attempts = 0
        for attempts in range(1, len(BACKOFF) + 2):
            self.limiter.wait()
            t0 = time.time()
            try:
                r = self.session().get(url, timeout=self.timeout)
                http, body = r.status_code, r.content
                status = classify(kind, http, body)
                err = None
            except Exception as e:  # network error or timeout
                status, err = "retry", repr(e)[:300]
            elapsed = time.time() - t0
            self._note_result(status in TERMINAL)
            if status in TERMINAL:
                break
            if attempts <= len(BACKOFF):
                time.sleep(BACKOFF[attempts - 1])
        sha1 = None
        if status == "ok":
            path = out_path(kind, n)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            if kind == "hwp":
                tmp.write_bytes(body)
            else:
                with gzip.open(tmp, "wb", compresslevel=6) as f:
                    f.write(body)
            tmp.replace(path)
            sha1 = hashlib.sha1(body).hexdigest()
        final = status if status in TERMINAL else "failed"
        self.state.record(n, kind, final, http, len(body), sha1, attempts, err, round(elapsed, 3))
        with self.counter_lock:
            self.n_done += 1
            k = self.n_done
        if k % 100 == 0 or final == "failed":
            rate = k / max(time.time() - self.t0, 1)
            eta_h = (total - k) / rate / 3600 if rate > 0 else float("nan")
            self.log(f"{k}/{total} last={kind}:{n} status={final} http={http} "
                     f"rate={rate:.2f}/s eta={eta_h:.1f}h")

    def run(self, tasks: pd.DataFrame):
        done = self.state.done()
        todo = [(int(r.conf_num), r.kind) for r in tasks.itertuples()
                if (int(r.conf_num), r.kind) not in done]
        self.log(f"START tasks={len(tasks)} already_done={len(tasks) - len(todo)} todo={len(todo)} "
                 f"workers={self.workers} min_interval={self.limiter.min_interval}")
        total = len(todo)
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = [ex.submit(self.fetch_one, n, kind, total) for n, kind in todo]
            try:
                for f in futures:
                    f.result()
            except KeyboardInterrupt:
                self.stop.set()
                self.log("INTERRUPTED")
                raise
        self.log("DONE " + str(self.state.summary()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tasks", type=Path, help="parquet with columns conf_num, kind, priority (lower first)")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--min-interval", type=float, default=1.0)
    ap.add_argument("--timeout", type=float, default=180)
    ap.add_argument("--kinds", default="view,summary,hwp", help="comma list of kinds to run from the task file")
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()
    if args.status:
        for row in State(STATE_DB).summary():
            print(row)
        return
    tasks = pd.read_parquet(args.tasks)
    tasks = tasks[tasks.kind.isin(args.kinds.split(","))].sort_values(["priority", "conf_num"])
    Crawler(args.workers, args.min_interval, args.timeout).run(tasks)


if __name__ == "__main__":
    sys.exit(main())
