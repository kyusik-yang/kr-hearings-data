"""Build the prioritized crawl task list from the Open API meeting universe.

Priorities (lower runs first)
  0 view    국회본회의 outside 18대 (party-change 보고사항 tables are needed early)
  1 hwp     18대 국회본회의
  2 view    every other meeting outside 18대
  1 hwp     every other 18대 meeting (moved up so the HWP parser has data early)
  4 summary 18대 meetings (basic template: header + file links)
  5 summary id-gap scan: ids in [GAP_LO, max_id + GAP_PAD] absent from the universe
  6 summary every remaining meeting (optional tail; attendance is also in view footers)
"""
from pathlib import Path

import pandas as pd

V10 = Path(__file__).resolve().parent.parent
GAP_LO = 23790
GAP_PAD = 300

u = pd.read_parquet(V10 / "interim" / "meeting_universe_api.parquet")
u = u[["CONFER_NUM", "DAE_NUM", "CLASS_NAME_unified"]].copy()
u["conf_num"] = u.CONFER_NUM.astype(int)
is18 = u.DAE_NUM.astype(int) == 18
plen = u.CLASS_NAME_unified == "국회본회의"

rows = []
rows += [(n, "view", 0) for n in u[~is18 & plen].conf_num]
rows += [(n, "hwp", 1) for n in u[is18 & plen].conf_num]
rows += [(n, "view", 2) for n in u[~is18 & ~plen].conf_num]
rows += [(n, "hwp", 1) for n in u[is18 & ~plen].conf_num]
rows += [(n, "summary", 4) for n in u[is18].conf_num]
known = set(u.conf_num)
hi = int(u.conf_num.max()) + GAP_PAD
rows += [(n, "summary", 5) for n in range(GAP_LO, hi + 1) if n not in known]
rows += [(n, "summary", 6) for n in u[~is18].conf_num]

t = pd.DataFrame(rows, columns=["conf_num", "kind", "priority"])
assert not t.duplicated(["conf_num", "kind"]).any()
t.to_parquet(V10 / "interim" / "crawl_tasks.parquet", index=False)
print(t.groupby(["priority", "kind"]).size().to_string())
print("total", len(t), "gap range", GAP_LO, hi)
