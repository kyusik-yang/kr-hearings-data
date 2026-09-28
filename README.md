# kr-hearings-data v10

kr-hearings-data v10 is a speaker-turn dataset of the minutes of the Korean National Assembly (국회) for the 16th to 22nd Assembly. It covers every meeting that the Assembly's Open API lists for these terms, including subcommittees, special committees, the national audit (국정감사), investigations (국정조사), the budget committee (예산결산특별위원회), confirmation hearings (인사청문특별위원회), the committee of the whole (전원위원회) and plenary sessions (본회의).

Each row of the main table is one merged speaker turn in document order. Turns carry the verbatim text, the spoken text without stage directions, the speaker label as printed, a speaker role, a link to the legislator (NAAS_CD) where the speaker is a member, the party and ruling status on the date of the speech, and government-side metadata for officials. Meeting-level tables hold agenda items, time and stage events, attendance, roll-call votes and appendix material. A dyad table pairs numerically adjacent legislator and non-legislator turns.

> **The v9 dyads are defective.** The v9 dyad file (`dyads_16_22_v9.parquet`) was built by sorting speech positions as strings, so 11.68% of its dyads pair speeches that are not adjacent and 17.91% of the truly adjacent pairs are missing. Do not use it. [docs/CHANGELOG.md](docs/CHANGELOG.md#d1-dyads-built-by-string-sort) documents this and every other verified v9 defect, and [the section below](#why-the-v9-dyads-should-not-be-used) summarizes them.

## Contents

- [Sources](#sources)
- [Coverage](#coverage)
- [Release files](#release-files)
- [How to load](#how-to-load)
- [How v10 differs from v9](#how-v10-differs-from-v9)
- [Why the v9 dyads should not be used](#why-the-v9-dyads-should-not-be-used)
- [Validation](#validation)
- [Known limitations](#known-limitations)
- [Documentation](#documentation)
- [Citation](#citation)
- [License](#license)

## Sources

All transcript text comes from the official minutes system of the National Assembly (국회회의록시스템, record.assembly.go.kr). One source is used per meeting.

| Source | What it is | Used for |
|---|---|---|
| Viewer XML | The speaker-segmented minutes page of the viewer (`viewer/minutes/xml.do?id={CONFER_NUM}&type=view`) | Every meeting of the 16th, 17th and 19th to 22nd Assembly whose page the viewer serves, unless the page fails the cross-check against the HWP record |
| HWP file | The original minutes file (`viewer/minutes/download/hwp.do?id={CONFER_NUM}`) | Every meeting of the 18th Assembly (the viewer returned HTTP 400 for every 18th-Assembly meeting tested), meetings of other terms whose viewer page returns HTTP 400, and meetings whose viewer XML fails the cross-check against the HWP record |
| Open API | open.assembly.go.kr minutes list services and `VCONFDETAIL` | The list of meetings (the meeting universe), meeting class, committee name, date and identifiers |
| Open API member services | ALLNAMEMBER, 의원이력 and 위원회경력 services | Legislator identity, seat dates, district, election type and committee membership |
| Plenary report items | The 【보고사항】 items of the plenary minutes | Dated party membership changes of members |
| President calendar | `president_calendar.csv`, each row sourced from pa.go.kr, korea.kr, KTV or saved Wikipedia text | President, presidency state and the president's party on each date |
| Minister panel | minister-data v2.0.0 (appointment spells, nominations and acting heads), and the earlier 296-row `minister_panel_comprehensive.csv` of the same project | Links of ministers, nominees, prime ministers and acting heads to their appointments (v2.0.0), and the dual-office evidence that links officials to members (296-row panel) |

The v9 speech rows (built from the Assembly's XLSX datasets, PDF files and HTML pages) are not a source of any v10 turn. They are used only to build the v9 to v10 crosswalk and as a validation reference.

The Open API meeting list was enumerated on 2026-09-25. Minutes were downloaded between 2026-09-25 17:38 and 2026-09-27 07:49 (UTC) at no more than one request per second. The raw viewer pages and HWP files are not redistributed. `meetings.raw_sha1` records the SHA-1 of the file each meeting was built from, and `v10/code/crawl.py` downloads the files again ([docs/PIPELINE.md](docs/PIPELINE.md#11-reproducibility)).

## Coverage

The release covers 26,264 meetings with 15,114,183 speaker turns and 11,279,607 dyads. The meeting universe has 26,264 meetings. It is the Open API universe plus 187 meetings that no Open API list returns and that were found by scanning gaps in the viewer id sequence. Five meetings dated 2026-09-22 to 2026-09-29 whose minutes were not yet published on 2026-09-26 are not in the universe.

| Term | Meetings | Turns | Dyads | First meeting date | Last meeting date |
|---|---|---|---|---|---|
| 16 | 3,408 | 1,159,565 | 936,410 | 2000-06-05 | 2004-05-19 |
| 17 | 4,635 | 2,221,574 | 1,642,358 | 2004-06-05 | 2008-05-23 |
| 18 | 4,310 | 2,787,681 | 2,053,571 | 2008-07-10 | 2012-05-02 |
| 19 | 4,191 | 2,906,498 | 2,177,759 | 2012-07-02 | 2016-05-19 |
| 20 | 3,761 | 2,472,098 | 1,839,635 | 2016-06-09 | 2020-05-20 |
| 21 | 3,644 | 2,074,024 | 1,550,733 | 2020-06-05 | 2024-05-28 |
| 22 | 2,315 | 1,492,743 | 1,079,141 | 2024-06-05 | 2026-09-22 |
| All | 26,264 | 15,114,183 | 11,279,607 | | |

The 22nd Assembly is still sitting (its term ends on 2028-05-29). Its last meeting date is the last meeting whose minutes were published when the minutes were downloaded.

| Hearing type | Meetings | Dyads |
|---|---|---|
| 상임위원회 (standing committee) | 16,697 | 5,259,890 |
| 국정감사 (national audit) | 4,960 | 4,322,077 |
| 특별위원회 (special committee other than confirmation) | 1,798 | 265,273 |
| 국회본회의 (plenary session) | 1,253 | 290,341 |
| 예산결산특별위원회 (budget committee) | 935 | 796,925 |
| 인사청문특별위원회 (confirmation hearing committee) | 363 | 166,298 |
| 국정조사 (investigation) | 250 | 178,541 |
| 전원위원회 (committee of the whole) | 8 | 262 |

7,269 meetings are subcommittee meetings (`is_subcommittee`). They keep the hearing type of their parent committee.

By source, 21,877 meetings (12,300,354 turns) come from viewer XML and 4,387 meetings (2,813,829 turns) from HWP files. The HWP meetings are all 4,310 meetings of the 18th Assembly (4,270 from the Open API lists and 40 from the id-gap scan), 51 meetings of other terms whose viewer page returns HTTP 400, and 26 meetings whose viewer XML failed the cross-check against the HWP record (25 incomplete pages and 1 page that carries the minutes of another meeting). `meetings.source_reason` records the reason for every meeting.

## Release files

The files of the current release are the assets of the GitHub release `v10.1`. The release `v10` keeps the files of the first v10 build, which differ only in the links of 4,193 prime-minister nominee turns ([docs/CHANGELOG.md](docs/CHANGELOG.md#v101-2026-09-28)). All tables are Apache Parquet files. Turns and dyads are split by term (`tNN` = term NN, 16 to 22).

| Asset | Rows | One row per |
|---|---|---|
| `meetings_v10.1.parquet` | 26,264 | meeting of the universe |
| `turns_t16_v10.1.parquet` to `turns_t22_v10.1.parquet` | 15,114,183 | merged speaker turn, ordered by `conf_num`, `turn_seq` |
| `dyads_t16_v10.1.parquet` to `dyads_t22_v10.1.parquet` | 11,279,607 | pair of numerically adjacent legislator and non-legislator turns |
| `agenda_v10.1.parquet` | 377,889 | agenda anchor printed in the body of the minutes |
| `agenda_header_v10.1.parquet` | 508,708 | agenda item listed in the header of the minutes |
| `events_v10.1.parquet` | 162,504 | time marker, stage line or other body line that is not a speaker turn |
| `footer_v10.1.parquet` | 5,301,479 | line or name of the appendix (attendance lists, attached documents) |
| `attendance_v10.1.parquet` | 1,395,805 | person listed in an attendance section |
| `rollcall_v10.1.parquet` | 3,402,605 | name in a recorded vote |
| `rollcall_groups_v10.1.parquet` | 51,593 | vote group (찬성, 반대, 기권, 투표) of a recorded vote |
| `crosswalk_meetings_v10.1.parquet` | 26,872 | v9 meeting, plus one row per v10 meeting that no v9 meeting carries |
| `crosswalk_turns_v10.1.parquet` | 8,603,114 | link between a v9 speech row and a v10 turn |
| `duplicate_meetings_v10.1.parquet` | 5 | pair of meetings flagged by the duplicate-content checks |
| `validation_report_v10.1.json` | | validation check with status, counts and example rows |
| `MANIFEST_v10.1.json` | | build file with rows, columns, bytes and SHA-256, plus the code version, parameters and snapshot hashes of the build |
| `SHA256SUMS` | | SHA-256 of every asset |

The turns files are the largest assets, followed by the dyads files. The 1,415 turns and 890 dyads of the 2 meetings set aside as identical copies of other meetings are not among the assets ([docs/CODEBOOK.md](docs/CODEBOOK.md#12-duplicate-meetings)).

Keys:

- `conf_num` (int64) is the viewer id (CONFER_NUM) and the key of every table.
- `turn_seq` (int32) is the 1-based position of a turn within a meeting. Turns join on (`conf_num`, `turn_seq`). Dyads carry both positions (`leg_turn_seq`, `wit_turn_seq`).
- `conf_id` (string) is the Open API CONF_ID, verbatim with its leading zero or `N` prefix. Never cast it to an integer.
- `v9_meeting_id` (string) is the v9 meeting id of the same meeting. Use `crosswalk_meetings_v10.1.parquet` for any v9 join (see [docs/MIGRATION_v9_to_v10.md](docs/MIGRATION_v9_to_v10.md)).

Every column is described in [docs/CODEBOOK.md](docs/CODEBOOK.md).

## How to load

### Python package

Version 0.2.1 of the `kr_hearings_data` package reads v10.1 by default, and `version="v10"` reads the first v10 build. Install it from PyPI:

```bash
pip install kr-hearings-data
```

```python
import kr_hearings_data as kh

meetings = kh.load_meetings()                        # one row per meeting
turns_21 = kh.load_turns(term=21, columns=["conf_num", "turn_seq", "speech_date", "role", "role_group",
                                           "naas_cd", "party", "ruling_status", "text"])
audit_21 = kh.load_turns(term=21, hearing_type="국정감사")
dyads_21 = kh.load_dyads(term=21)
crosswalk = kh.load_table("crosswalk_meetings")
```

- The loaders download an asset from the GitHub release on first use and read it from the cache afterwards. The cache is `~/.cache/kr-hearings-data`, or the folder named in the environment variable `KR_HEARINGS_CACHE`.
- `load_turns` and `load_dyads` read one file per term and download only the terms requested (`term` 16 to 22, all terms when omitted). `hearing_type` filters on the meetings table. `columns` limits the columns read.
- `load_meetings` reads the meetings table, optionally filtered by `term` and `hearing_type`.
- `load_table(name)` reads a table that is not split by term (`agenda`, `agenda_header`, `events`, `footer`, `attendance`, `rollcall`, `rollcall_groups`, `crosswalk_meetings`, `crosswalk_turns`, `duplicate_meetings`).
- `load_speeches` is `load_turns`, kept for code written for v9. The v9 files are no longer distributed, and `version="v9"` raises an error.
- `download(tables=[...], terms=[...])` fills the cache without loading anything.

The command-line tool does the same:

```bash
kr-hearings download --tables meetings,turns --terms 21
kr-hearings info
kr-hearings export --dataset turns --term 21 --format parquet -o turns_21.parquet
```

### duckdb or pandas on the downloaded files

The turns files are large. Read only the columns and terms you need. The examples assume the release assets are in a folder `kr-hearings-v10/`.

```python
import duckdb

con = duckdb.connect()
con.execute("SET memory_limit = '8GB'")

# Legislator turns of the 21st Assembly with party and ruling status on the speech date
leg = con.sql("""
    SELECT t.conf_num, t.turn_seq, t.speech_date, t.speaker_name, t.naas_cd,
           t.party, t.ruling_status, t.text, m.hearing_type, m.committee_key
    FROM read_parquet('kr-hearings-v10/turns_t21_v10.1.parquet') t
    JOIN read_parquet('kr-hearings-v10/meetings_v10.1.parquet') m USING (conf_num)
    WHERE t.role_group = 'legislator'
""").df()
```

```python
import pandas as pd

meetings = pd.read_parquet("kr-hearings-v10/meetings_v10.1.parquet")
turns_20 = pd.read_parquet(
    "kr-hearings-v10/turns_t20_v10.1.parquet",
    columns=["conf_num", "turn_seq", "role", "role_group", "speaker_name", "text"],
)
```

Dyads hold keys, flags and a core set of attributes. Join any other turn attribute from the turns table on both positions:

```python
dyads = con.sql("""
    SELECT d.*, lt.party_lineage AS leg_party_lineage, wt.affiliation_raw AS wit_affiliation_raw
    FROM read_parquet('kr-hearings-v10/dyads_t21_v10.1.parquet') d
    JOIN read_parquet('kr-hearings-v10/turns_t21_v10.1.parquet') lt
      ON lt.conf_num = d.conf_num AND lt.turn_seq = d.leg_turn_seq
    JOIN read_parquet('kr-hearings-v10/turns_t21_v10.1.parquet') wt
      ON wt.conf_num = d.conf_num AND wt.turn_seq = d.wit_turn_seq
    WHERE NOT d.leg_is_procedural
""").df()
```

## How v10 differs from v9

| Topic | v9 | v10 |
|---|---|---|
| Text source | XLSX datasets, PDF files and HTML pages, mixed within the release | Viewer XML or the HWP minutes file, one source per meeting |
| Meetings | 16,830 meetings, no standing-committee subcommittees and no other special committees | 26,264 meetings, the full Open API universe plus the id-gap scan |
| Meeting key | `meeting_id` in three namespaces | `conf_num` (CONFER_NUM), with `conf_id` and `v9_meeting_id` as columns |
| Turn order | string `speech_order` | integer `turn_seq` |
| Text | `speech_text` | `text_raw` (verbatim) and `text` (stage directions and oath signature lines removed) |
| Dyads | built by sorting `speech_order` as a string | numerically adjacent turns in one sitting, with chair and procedural flags |
| Roles | 33 roles, several known misclassifications | the same 33 roles, rebuilt with fixes, plus `role_v9_compat` |
| Legislator id | `member_id` in mixed namespaces, `member_uid` | `naas_cd` for all terms, with `id_method` and `id_confidence` |
| Party and ruling status | a constant per term and party | the value on the speech date, with `presidency_state` |
| Government metadata | panel links with date-free fallbacks | links to the appointment spells, nominations and acting heads of minister-data v2.0.0 inside a date window, administration from the speech date |
| Meeting tables | none | meetings, agenda, agenda header, events, footer, attendance, roll calls |
| Validation | no validation report was run on v9 | 44 of 47 checks pass in the release validation, and none fails |

[docs/CHANGELOG.md](docs/CHANGELOG.md) lists each verified v9 defect and what v10 does about it.

## Why the v9 dyads should not be used

The v9 dyad file was built by sorting the string column `speech_order`, so turn 10 sorted before turn 2.

- 867,825 of the 7,429,413 published v9 dyads (11.68%) pair speeches that are not adjacent in the meeting.
- 1,431,973 of the 7,993,561 truly adjacent pairs (17.91%) are missing from the v9 file.
- 13,951 of the 14,626 meetings with dyads are affected.

Other v9 defects also reach the dyads.

- A full-population classifier finds that 899 of the 2,081 meetings added in v8 (국정조사, 예결 and 본회의) carry the transcript of another meeting, which puts 280,947 dyads under the wrong meeting.
- 21.3% of the 예결 dyads have a legislator on the witness side because of role coding errors.
- `ruling_status` in the v9 speech rows (and `leg_ruling_status` in the v9 dyads) is a constant per term and party. In the speech rows it is inverted for 409,974 of the 458,352 rows of the 21st Assembly dated on or after 2022-05-10.

Any result computed from the v9 dyads, or from the v8 and v9 rows of 국정조사, 예결 and 본회의, should be recomputed on v10. The v9 files are no longer distributed. [docs/CHANGELOG.md](docs/CHANGELOG.md#verified-v9-defects) describes each defect, and [docs/MIGRATION_v9_to_v10.md](docs/MIGRATION_v9_to_v10.md) shows how to map v9 ids and rows to v10.

## Validation

`validate.py` runs 47 checks on the release tables and blocks a release when a check fails. The release validation gives 44 PASS, 0 FAIL and 3 WARN. The checks cover schemas and value domains, unique keys, contiguous turn order, text accounting against the raw sources, an independent recomputation of the dyads and of their attributes, duplicate meeting content, identifier namespaces, role groups, legislator link and party coverage, ruling status and presidency state against the president calendar, the integrity of the agenda, footer and crosswalk tables, and the absence of local paths in release files. The three warnings report 21 turns whose `text_raw` is null (`turns_text`), 2 turns printed after a meeting-end marker (`turns_after_end_marker`) and turns dated the day before the meeting date or by an appended record (`dates_meeting`). [docs/PIPELINE.md](docs/PIPELINE.md#9-validate) lists every check. `validation_report_v10.1.json` gives each check's status, counts and example rows.

## Known limitations

The main ones are below. [docs/CODEBOOK.md](docs/CODEBOOK.md#known-limitations) gives the full list with counts.

- Transcript text is kept as printed. It is not Unicode-normalized, and separators and Hanja forms differ between the XML and HWP sources.
- 2 turns in 2 meetings are printed after a meeting-end marker of their sitting. They are kept and flagged (`after_end_marker`).
- 239 legislator-side turns could not be linked to a legislator.
- Party membership has documented source gaps in the 16th, 17th and 20th Assembly. 88,209 legislator turns fall in a flagged uncertainty window (`party_uncertain`).
- Government links use minister-data v2.0.0. 186 of 1,151,689 minister turns and 1,224 of 20,984 acting-minister turns are not linked. The acting-head records of minister-data cover the prime minister's office systematically and other ministries only incidentally.
- The procedural flag of the dyads was checked for precision only. Its recall is not measured.

## Documentation

- [docs/CODEBOOK.md](docs/CODEBOOK.md) describes every table and column, the role taxonomy, the hearing type rules, the ruling status rules, the government links, the Unicode and NULL policy and the known limitations.
- [docs/PIPELINE.md](docs/PIPELINE.md) describes how the release is built and validated and how to reproduce it.
- [docs/CHANGELOG.md](docs/CHANGELOG.md) lists the verified v9 defects and the v10 changes.
- [docs/MIGRATION_v9_to_v10.md](docs/MIGRATION_v9_to_v10.md) shows how to move an analysis from v9 to v10.
- [docs/v9/CODEBOOK_v9.md](docs/v9/CODEBOOK_v9.md) keeps the v9 codebook for readers who hold the v9 files, and [docs/v9/PIPELINE_v9.md](docs/v9/PIPELINE_v9.md) the v9 pipeline description that `v10/code/legacy_rules.py` reconstructs.

## Citation

Cite the release version you use.

```text
Yang, Kyusik. 2026. kr-hearings-data: Speaker Turns from the Minutes of the Korean National Assembly,
16th to 22nd Assembly. Version 10. https://github.com/kyusik-yang/kr-hearings-data
```

```bibtex
@misc{kr_hearings_data_v10,
  author = {Yang, Kyusik},
  title  = {kr-hearings-data: Speaker Turns from the Minutes of the Korean National Assembly, 16th to 22nd Assembly},
  year   = {2026},
  note   = {Version 10},
  url    = {https://github.com/kyusik-yang/kr-hearings-data}
}
```

## License

The data are released under CC BY 4.0.
