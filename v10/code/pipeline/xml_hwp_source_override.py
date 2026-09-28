"""Write the per-meeting source override file from the XML-vs-HWP cross-check.

Inputs (interim/pipeline/xml_hwp_crosscheck/): per_meeting_full3.parquet (stage 1, all meetings with
both sources) and stage2.parquet (stage 2, flagged meetings: HWP speech text searched in the whole
XML page).

Rule (documented in docs/PIPELINE.md):
  xml_wrong_meeting  turns_hwp_in_all_xml < 0.5 and turns_xml_in_all_hwp < 0.5
  xml_incomplete     missing_hwp_chars >= 500 (runs of >= 15 normalized chars of HWP speech absent
                     from the whole XML page), the missing text is not a roll-call name list
                     (name_list_share < 0.5), turns_xml_in_all_hwp >= 0.95 (the HWP is the same
                     meeting), and xml_label_bag >= 0.9 (share of XML speaker labels, as a multiset,
                     found among the HWP labels: the HWP speakers cover the XML speakers)
Everything else keeps the XML (flags only).

Outputs: source_override.parquet (conf_num, source, reason) and source_override_report.csv
(every candidate with its metrics and decision).
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from xml_hwp_crosscheck import OUT, _cmp_norm, _hwp_turns, _xml_turns  # noqa: E402

V10 = HERE.parent.parent
MIN_MISSING = 500


def _roster_names():
    p = V10 / "interim" / "pipeline" / "legislators" / "persons.parquet"
    d = pd.read_parquet(p)
    col = next(c for c in d.columns if c.lower() in ("naas_nm", "name", "hangul", "leg_name_hangul"))
    return {"".join(str(x).split()) for x in d[col].dropna()}


def _name_share(s, names):
    s = s or ""
    i = cov = 0
    while i < len(s):
        for k in (4, 3, 2):
            if s[i:i + k] in names:
                cov += k
                i += k
                break
        else:
            i += 1
    return cov / len(s) if s else 0.0


def _label_bag(n):
    _, tx = _xml_turns(n)
    _, th = _hwp_turns(n)
    bx = Counter(_cmp_norm(t.get("speaker_label_raw")) for t in tx)
    bh = Counter(_cmp_norm(t.get("speaker_label_raw")) for t in th)
    tot = sum(bx.values())
    return (sum(min(c, bh[k]) for k, c in bx.items()) / tot) if tot else None


def main():
    s1 = pd.read_parquet(OUT / "per_meeting_full3.parquet")
    s2 = pd.read_parquet(OUT / "stage2.parquet")
    m = s1.merge(s2, on="conf_num", how="left")
    cand = m[(m.missing_hwp_chars >= MIN_MISSING)
             | ((m.turns_hwp_in_all_xml < 0.5) & (m.turns_xml_in_all_hwp < 0.5))].copy()
    names = _roster_names()
    cand["name_list_share"] = cand.missing_hwp_example.map(lambda s: _name_share(s, names))
    cand["xml_label_bag"] = cand.conf_num.map(_label_bag)
    cand["decision"] = "keep_xml"
    wrong = (cand.turns_hwp_in_all_xml < 0.5) & (cand.turns_xml_in_all_hwp < 0.5)
    cand.loc[wrong, "decision"] = "xml_wrong_meeting"
    inc = (~wrong & (cand.missing_hwp_chars >= MIN_MISSING) & (cand.name_list_share < 0.5)
           & (cand.turns_xml_in_all_hwp >= 0.95) & (cand.xml_label_bag >= 0.9))
    cand.loc[inc, "decision"] = "xml_incomplete"
    cand.to_csv(OUT / "source_override_report.csv", index=False)
    ov = cand[cand.decision != "keep_xml"][["conf_num", "decision"]].rename(columns={"decision": "reason"})
    ov.insert(1, "source", "hwp")
    ov["conf_num"] = ov.conf_num.astype("int64")
    ov.to_parquet(OUT / "source_override.parquet", index=False)
    print(cand.decision.value_counts().to_string())
    print(cand[["conf_num", "missing_hwp_chars", "turns_hwp_in_all_xml", "turns_xml_in_all_hwp",
                "name_list_share", "xml_label_bag", "decision"]].round(3).to_string())


if __name__ == "__main__":
    main()
