# Pipeline, kr-hearings-data v10

This file describes how the v10 release is built, validated and reproduced.

## Contents

1. [Overview](#1-overview)
2. [Meeting universe](#2-meeting-universe)
3. [Crawl](#3-crawl)
4. [Parse](#4-parse)
5. [Meetings table](#5-meetings-table)
6. [Enrich](#6-enrich)
7. [Dyads](#7-dyads)
8. [Crosswalk](#8-crosswalk)
9. [Validate](#9-validate)
10. [Manifest and release layout](#10-manifest-and-release-layout)
11. [Reproducibility](#11-reproducibility)
12. [Configuration parameters](#12-configuration-parameters)

## 1. Overview

```
Open API minutes lists (keyed) + id-gap scan  ->  meeting universe
record.assembly.go.kr viewer pages and HWP files  ->  crawl  ->  raw/
raw/ (viewer XML and HWP file of the same meeting)  ->  XML-vs-HWP cross-check  ->  source overrides
raw/ + source overrides  ->  build_turns (XML adapter, HWP adapter)  ->  turns, meetings, agenda, agenda_header,
                                                     events, footer, attendance, rollcall, rollcall_groups
turns  ->  duplicates (identical copies set aside, overlaps flagged)  ->  duplicate_meetings
turns  ->  enrich (roles -> legislators -> party_timeline -> government)  ->  release turns
release turns + meetings  ->  dyads
v9 speech rows + release turns  ->  crosswalk_meetings, crosswalk_turns
all release tables  ->  validate  ->  validation_report.json, docs_numbers.json
->  MANIFEST.json
build/release/  ->  package_release.py  ->  release assets
```

The code is in `v10/code/`. The crawler is `crawl.py`, and `build_crawl_tasks.py` builds its task list from the meeting universe. The release pipeline is in `v10/code/pipeline/` and runs from `run_all.py`. Column names and types exchanged between components are those of [CODEBOOK.md](CODEBOOK.md). `package_release.py` turns the release folder into the release assets (section 10).

## 2. Meeting universe

The meeting universe is the list of meetings the release must contain.

1. **Open API lists.** The agenda-row list services for committees (`ncwgseseafwbuheph`), plenary sessions (`nzbyfwhwaoanttzje`) and the 전원위원회 (`ngytonzwavydlbbha`) were enumerated for terms 16 to 22 with an API key, and the rows were collapsed to one row per CONFER_NUM. The per-class lists (`VCONF…CONFLIST`, for example 국정감사, 국정조사, 인사청문회, 소위원회, 예결, 특별위원회) were merged on CONFER_NUM. Meetings that no list returns but that v9 contains were added through `VCONFDETAIL`. This gives 26,077 meetings (20,589 from the agenda-row lists, 5,061 only from the per-class lists and 427 only from `VCONFDETAIL`).
2. **Id-gap scan.** The list services miss some meetings. Viewer ids in the gaps of the CONFER_NUM sequence were probed through the summary page. 187 meetings of the 16th to 22nd Assembly were found and added with a null CONF_ID (16th 12, 17th 56, 18th 40, 19th 27, 20th 26, 21st 8, 22nd 18). Gap ids that are 1961 to 1962 records (258) or earlier than the 16th Assembly (164) are out of scope.
3. **Unpublished meetings.** 5 meetings dated 2026-09-22 to 2026-09-29 whose original minutes were not yet registered on 2026-09-26 were removed. They are listed in `interim/universe_not_published.csv`.

The result is `interim/meeting_universe_v10.parquet` with 26,264 meetings. The Open API lists were enumerated on 2026-09-25. Keyless Open API calls return at most 5 rows, so the enumeration needs a key.

## 3. Crawl

`crawl.py` downloads three kinds of pages per meeting into `raw/`. No other step sends requests to record.assembly.go.kr.

| Kind | URL (`https://record.assembly.go.kr/assembly/…`) | Saved as |
|---|---|---|
| view | `viewer/minutes/xml.do?id={CONFER_NUM}&type=view` | `raw/viewer/view/{bucket}/{id}.html.gz` |
| summary | `viewer/minutes/xml.do?id={CONFER_NUM}&type=summary` | `raw/viewer/summary/{bucket}/{id}.html.gz` |
| hwp | `viewer/minutes/download/hwp.do?id={CONFER_NUM}` | `raw/hwp/{bucket}/{id}.hwp` |

- A global limiter starts requests at least 1.0 second apart across all workers, and at most 3 requests are in flight. Errors back off exponentially, and a circuit breaker pauses all workers after a run of failures.
- The state of every download is in `interim/crawl_state.sqlite` (table `fetch`, with status, HTTP code, bytes, SHA-1, attempts and time). Terminal statuses are `ok`, `no_xml` (HTTP 400 `Bad Request.`), `not_found`, `not_published` (the hwp.do alert '내려받을 원본 회의록 파일이 등록되지 않았습니다', the original file is not registered yet) and `no_original` (the hwp.do alert '내려받을 원본 회의록이 없습니다'). A restart skips terminal tasks.
- View pages were requested for every meeting outside the 18th Assembly. HWP files were downloaded for every meeting, as the source of the 18th Assembly and of the meetings whose view page is `no_xml`, and for the XML-vs-HWP cross-check of all other meetings. 26,262 of the 26,264 meetings have an HWP file. For 31578 and 55355 the system has no original file (`no_original`), and both are built from viewer XML.

Downloads ran from 2026-09-25 17:38 to 2026-09-27 07:49 (UTC).

## 4. Parse

`build_turns.py` turns each meeting's raw file into the CONTRACT tables. One source is used per meeting.

| Source | Meetings | Parser |
|---|---|---|
| `xml` | the meetings of the 16th, 17th and 19th to 22nd Assembly with a view page, unless the cross-check below sends them to HWP (21,877 meetings) | `v10/code/parse_viewer.py` (lxml) |
| `hwp` | every 18th Assembly meeting, the meetings of other terms whose view page is `no_xml`, and the meetings the cross-check sends to HWP (4,387 meetings) | `v10/code/pipeline/hwp_parser.py` (olefile) |

Only `xml` and `hwp` are production sources, with the precedence `xml` over `hwp` unless the cross-check overrides it. The XLSX adapter remains in the code for the v9 comparison only, and no release turn comes from XLSX rows. The `sources` parameter still lists `xlsx` among the adapters `build_turns` may run (section 12.3).

**XML-vs-HWP cross-check.** Every meeting with both a view page and an HWP file (21,901 meetings) was compared source against source. A second stage searched the HWP speech text in the whole XML page (turns, interjections, stage texts, events, agenda, footer, attendance and roll-call names). A meeting uses its HWP file in two cases.

- `xml_wrong_meeting`. The XML page carries the minutes of another meeting. Less than half of the HWP turn shingles occur in the XML page, and less than half of the XML turn shingles occur in the HWP record.
- `xml_incomplete`. At least 500 normalized characters of HWP speech, in runs of at least 15 characters, are absent from the XML page. The missing text must not be a roll-call name list, at least 0.95 of the XML turn shingles must occur in the HWP record (the same meeting), and at least 0.9 of the XML speaker labels must occur among the HWP labels.

25 meetings are `xml_incomplete` and 1 is `xml_wrong_meeting`. An audit of the first run found that 7 of its 34 overrides came from documents appended after a meeting-end marker that the HWP parser read as turns. The rule was run again after the parser fix of section 4.2. `build_turns.plan` applies the overrides (`interim/pipeline/xml_hwp_crosscheck/source_override.parquet`), and `meetings.source_reason` records them.

### 4.1 XML adapter

The viewer page holds `div.speaker` blocks with `data-mem_id`, `data-name` and `data-pos`, and sentences in `span.spk_sub`. Fragments of one speaker id split by a time marker are merged into one turn. Agenda anchors (`p.tit_sm.angun`), time markers (`p.tit_sm.taR`) and every other text node of the minutes body become agenda rows, events or footer rows, so that every character is accounted for. Whole-sentence parentheticals are stage sentences, and `text` drops them and oath signature lines.

### 4.2 HWP adapter

The HWP reader reads HWP 5 files with olefile only. It inflates the BodyText sections, reads the records as a tree, decodes paragraph text (UTF-16LE with the control characters of the format) and places table cells where their table sits. Password-protected, distribution and HWP 3 files are returned as a status. The minutes grammar reads the cover (session, title, date), the agenda, speaker turns introduced by ◯ or ○, time markers with day roll-overs, stage directions, oath signature lines, later sittings and the appendix.

Text printed after a meeting-end marker is appendix (footer or events), never turns, unless an opening marker or a later sitting with an attested speaker label follows. This fix of 2026-09-28 removed 245 spurious turns and moved 351 turns to the appendix across all 26,262 HWP files, and left the agreement with the v9 XLSX rows below unchanged.

On the 2,433 18th Assembly meetings that also exist as v9 XLSX rows, the HWP parse agrees with the XLSX speaker sequence at 0.999052 (positional) and 0.999687 (aligned), and normalized text is identical on 0.999378 of aligned pairs. A stratified audit of 40 HWP files (2,187 turns, seed 8374) found no error with the final parser.

### 4.3 Text accounting

Every meeting must pass the builder's accounting before it is published (`ok_all`).

- For XML, the characters of the sentence spans equal the parsed sentences, the characters of the text blocks equal `text_raw` plus embedded nodes, the whole minutes body is accounted for, the appendix characters equal the footer rows, `turn_seq` is contiguous, and the number of speaker blocks equals the sum of `n_fragments`.
- For HWP, turn items equal labels plus `text_raw`, the whole document (markers, labels, text, agenda, events, footer and cover) is accounted for, and the appendix characters equal the footer rows.

### 4.4 Sittings and turn boundaries

`after_end_marker`, `after_final_end_marker` and `sitting_seq` follow one rule for all sources. A meeting-end marker sets `after_end_marker` for the later turns of the same sitting. An opening or continuation marker after an end marker starts a new sitting and resets the flag. `after_final_end_marker` marks the turns after the last end marker of the document. The HWP parser supplies its own sitting numbers, which also start at the cover page of a later sitting, and a disagreement with the marker rule is counted. The builder also removes a speaker label that the source prints again at the start of the speech from `text` (`text_label_prefix_stripped`), rates fused labels and labels holding text `low`, adds matching forms of the name and position, and stores empty strings as null.

### 4.5 Incremental builds

A meeting is built once and rebuilt when its best source changes, its raw file hash changes, its adapter version changes (parser file hash, adapter code, `SOURCE_VERSION`) or a rebuild is forced. New batch files are written before the manifest is committed. The previous rows of a rebuilt meeting are removed only after that commit, so a failed re-parse keeps the previous build. Batch files that an interrupted run left outside the manifest are moved to `_orphans/`, never deleted. Only one build runs at a time.

## 5. Meetings table

`build_meetings` writes one row per universe meeting from the universe row and the parsed header. The Open API committee name is authoritative for the committee and subcommittee, and the printed values are kept in `committee_printed` and `subcommittee_printed`. `hearing_type`, `is_subcommittee`, `committee_key`, `audit_team`, `doc_label` and `is_confirmation_hearing` follow the rules in [CODEBOOK.md](CODEBOOK.md#9-hearing-type-class-subcommittee-and-committee-key) section 9.

The duplicate decisions of section 5a are added to the meetings table (`duplicate_of`, `duplicate_basis`, `overlap_with`, `overlap_kinds`).

### 5a. Duplicates

The stage `duplicates` of `run_all.py` runs the three duplicate-content checks of `validate.py` (whole-meeting text, long turns, text shingles) on the built turns, without an allowlist. Each flagged pair is `identical` (equal whole-meeting normalized text), `near_identical` (same number of turns and at least 0.9 of them with equal normalized text) or `partial`. In each group of identical meetings one copy is kept, the one whose printed date, sitting and committee match its Open API row best (then the lower `conf_num`). The other copies get `duplicate_of`, and their turns and dyads are written to `duplicate_turns` and `duplicate_dyads`. Near-identical and partial pairs are kept and flagged. Every pair and its evidence is in `duplicate_meetings.parquet`.

## 6. Enrich

`run_all.py` enriches the turns term by term. Each chunk holds at most 500,000 turns and never splits a meeting. Text columns are kept out of the pandas frames and joined back in duckdb. The modules run in this order:

1. `roles.py` adds the role columns ([CODEBOOK.md](CODEBOOK.md) sections 4.7 and 8).
2. `legislators.py` adds the legislator identity columns (section 4.8), then the switchable post-step for `name_fuzzy_committee`.
3. `party_timeline.py` adds party, ruling status and presidency state (sections 4.9 and 10).
4. `government.py` adds the government columns (sections 4.10 and 11), then the switchable post-step for `nominee_in_tenure`, which matches no link of the default panel.

After each chunk `run_all.py` checks that the rows, keys and order are unchanged and that no enricher modified an input column.

Reference inputs of the enrichers:

| Input | Used by | Content |
|---|---|---|
| `interim/pipeline/legislators/{persons,person_terms,committee_spells,memid_crosswalk}.parquet` | legislators, roles | Members, member-term stints, committee membership and the record member-term id crosswalk, built from Open API downloads |
| `interim/members_term_16_22.parquet`, `interim/members_allnamember_16_22.parquet` | roles, government | Roster of each term |
| `interim/pipeline/party_timeline/*.parquet` | party_timeline | Party spells and uncertainty windows harvested from the plenary report items |
| `interim/party_lineage.csv` | party_timeline, legislators | Party renames, mergers, satellites and successors with sources |
| `interim/president_calendar.csv` | party_timeline, government, validate | President, status, acting president and party by date, with sources |
| `interim/external/minister_data_v2.0.0/` (minister-data v2.0.0) | government | Appointment spells, nominations, acting heads, ministry aliases and name variants of ministers and prime ministers, with dual-office dates |
| `minister_panel_comprehensive.csv` (296 rows, minister-data project) | legislators, and government with `panel` = `legacy_296` | Minister, nominee and prime-minister appointments with dates, dual office and notes |
| `interim/04_v9_speaker_role_table_xlsx_era.parquet` | roles | v9 speaker-role table for `role_v9_compat` |
| `raw/third_party/hanja_table_0.15.1.yml` | roles, legislators, government | Hanja readings |

**Party spells.** `party_timeline.py` builds the spells from the 【보고사항】 report items of the 1,252 plenary meetings of the Open API lists, 1,060 read from viewer XML and 192 from HWP files (the HWP files include the 12 plenary meetings whose view page is `no_xml`). The result is 4,573 party spells. The plenary meeting 56654, found by the id-gap scan, is not read.

**Minister panel.** `government.py` reads minister-data v2.0.0 from the snapshot `v10/interim/external/minister_data_v2.0.0/` (`government.minister_release`). At load it checks the SHA-256 of each of the 10 tables against the snapshot's `MANIFEST.json`, requires the version `v2.0.0` and refuses a directory outside `interim/external/`. The snapshot's `MANIFEST.json` has the SHA-256 `6c269cedb04d01b975a224ffa97da423cad72be94d261cf2f7041b92de3ee725`, and the release window is 1988-02-25 to 2026-09-24. The snapshot is minister-data release v2.0.0 (tag `v2.0.0`, commit `05227c4`). With `government.panel` = `legacy_296`, `government.py` reads the earlier 296-row `minister_panel_comprehensive.csv` with its earlier rules and windows, which reproduces the numbers of builds before 2026-09-28. `legislators.py` reads the 296-row panel in either case, for the dual-office and panel-note evidence that links a non-legislator title to a member.

## 7. Dyads

`dyads.build_dyads_file` builds the dyads of each term from the release turns and the meetings table. Pairs are found by a hash self-join on (`conf_num`, `turn_seq` + 1) in duckdb, in chunks of at most 300,000 turns, and streamed to the output file. Duplicate or null keys stop the build. Gaps in `turn_seq` are never bridged. Pairs blocked by a sitting change are counted. The procedural flag uses the frozen patterns of `dyads.PROCEDURAL_PATTERNS`.

## 8. Crosswalk

`crosswalk.py` links v9 to v10 at two levels.

- **Meetings.** Each v9 meeting's label is matched to a CONFER_NUM (`v9_to_api_crosswalk.parquet`). The v9 text is then compared with v10 text through fingerprints of normalized sentences (at least 15 characters, boilerplate found in more than 50 meetings ignored). The label meeting is `same` when it contains at least half of the v9 fingerprints. Another meeting that contains at least half makes the relation `v9_wrong_content`. When several v9 meetings carry one transcript, one is primary and the others are second copies.
- **Turns.** For each aligned meeting the v9 rows and v10 turns are aligned by matching blocks on normalized text, and the gaps between blocks by a small dynamic program over one-to-one, one-to-two and two-to-one moves with a similarity of at least 0.6. Every v9 row and every v10 turn of an aligned meeting appears once, and the order is kept.

## 9. Validate

`validate.py` runs every check as SQL in duckdb. In release mode a check whose input or column is missing fails, and any failing check makes the run exit with status 1, which blocks the release. The report `validation_report.json` gives each check's status, the number of offending rows, details and example rows. `docs_numbers.json` holds every number this documentation quotes. The release validation gives 44 PASS, 0 FAIL, 3 WARN and 0 SKIP of 47 checks.

The release validation runs these checks:

| Check | What it verifies |
|---|---|
| `schema_turns`, `schema_meetings`, `schema_dyads`, `schema_crosswalk` | Every CONTRACT column is present with its type, and the dyads have exactly the slim layout |
| `dyads_meeting_fields` | Meeting columns in the dyads equal the meetings table |
| `domains` | Categorical columns take documented values, dates and times are well formed |
| `keys_unique` | Primary keys are unique and not null in every table |
| `turns_contiguous` | `turn_seq` is exactly 1 to n in every meeting |
| `turns_meetings_consistency` | Every turn's meeting exists and is built, `n_turns` equals the turn count, one source per meeting |
| `turns_text` | `text_raw` is not null except for turns whose printed speech sits in the label (WARN), `text` is not null except for turns that are stage directions only, and `text` never has more characters than `text_raw` |
| `universe_accounted` | Every universe meeting is in meetings with a status, and missing meetings block the release |
| `coverage_builder`, `coverage_recompute`, `coverage_source_sample` | The builder's text accounting holds, an independent recount agrees, and a seeded sample of raw pages and HWP files, checked against `raw_sha1`, re-parses to the stored turns |
| `dyads_adjacent`, `dyads_endpoints`, `dyads_recompute`, `dyads_direction`, `dyads_sitting` | Dyads pair adjacent turns of the right groups in one sitting, carry those turns' texts, equal an independent recomputation and have the right direction |
| `dyads_attributes`, `dyads_flags` | Every slim dyad attribute equals the value derived from its turns and meeting, and every flag, the procedural flag included, equals its recomputation on every dyad |
| `turns_sittings`, `turns_after_end_marker` | Sitting numbers and end-marker flags are well formed, and turns after an end marker are counted (WARN when any exist) |
| `label_how_weak_share` | Every turn records how its label was found, and the share of weak labels is reported |
| `dates_term_window`, `dates_meeting` | Dates lie in the term window, the meeting date equals the Open API date, and speech dates lie in the meeting's span (WARN for turns dated the day before the meeting date or by an appended record) |
| `dup_long_turn`, `dup_shingle`, `dup_meeting_text` | No two meetings share long turns, text shingles or their whole text, unless the pair is allowlisted |
| `duplicates_resolved` | A meeting marked `duplicate_of` has the whole-meeting text of the kept copy, and its turns and dyads are only in the duplicate tables |
| `release_no_local_paths` | No release file holds an absolute local path or the user name |
| `ids_namespace` | `conf_num` and `conf_id` pairs equal the Open API, and v9 ids follow their namespace |
| `roles_staff`, `roles_wit_title_share` | No staff title on the legislator side, and few legislator titles on the witness side |
| `legislator_links`, `party_coverage` | Link and party coverage of legislator-side turns meet the per-term thresholds |
| `ruling_null`, `ruling_recompute`, `presidency_by_date`, `president_by_date`, `partyless_windows`, `admin_by_date` | Ruling status, presidency state, president, the partyless windows and the administration equal an independent recomputation from the president calendar |
| `agenda_integrity`, `footer_integrity` | Agenda and footer rows belong to built meetings and point to existing turns |
| `crosswalk_integrity`, `crosswalk_turns_integrity` | Every v9 meeting appears once, every v10 meeting is linked or `v10_only`, and the turn alignment is complete and ordered |
| `docs_numbers` | `docs_numbers.json` can be generated from the release tables |

The three warnings of the release are `turns_text` (21 turns with a null `text_raw`), `turns_after_end_marker` (2 turns after a meeting-end marker) and `dates_meeting` (turns dated the day before the meeting date, and 2 meetings whose `date_end` is one day before `date`).

## 10. Manifest and release layout

`run_all.py` writes the release into `build/release/`:

```
build/release/
  meetings.parquet  agenda.parquet  agenda_header.parquet  events.parquet  footer.parquet
  attendance.parquet  rollcall.parquet  rollcall_groups.parquet
  turns/tNN/part-KKKKK.parquet        (rows ordered by conf_num, turn_seq)
  dyads/tNN/dyads.parquet             (slim layout)
  duplicate_meetings.parquet
  duplicate_turns/tNN/part-*.parquet  duplicate_dyads/tNN/dyads.parquet   (only when a duplicate copy exists)
  crosswalk_meetings.parquet  crosswalk_turns.parquet
  validation_report.json  docs_numbers.json  MANIFEST.json
```

`package_release.py` writes the release assets into `build/assets/`. It copies each table that is not split by term to `<table>_v10.parquet`, merges the parts of each term into `turns_tNN_v10.parquet` and `dyads_tNN_v10.parquet`, checks the row counts against `MANIFEST.json`, copies `MANIFEST.json` and `validation_report.json` as `MANIFEST_v10.json` and `validation_report_v10.json`, and writes `SHA256SUMS`. `duplicate_turns`, `duplicate_dyads` and `docs_numbers.json` are not packaged.

Bookkeeping stays outside the release folder, in `build/_state/` (state, logs, run records, stage statistics, `crosswalk_stats.json`, `duplicates.json`), `build/_superseded/<run_id>/` (replaced outputs, never deleted) and `build/.staging/`.

`MANIFEST.json` records for every release file its rows, columns, bytes and SHA-256, a code version (a hash of the pipeline files' contents), the configuration values, a hash of the meeting universe snapshot and the time of the crawl snapshot. Release files carry repository-relative paths only. After writing, `run_all.py` scans every release file for a local path or the user name and fails the run when it finds one.

## 11. Reproducibility

- **Commands.** From `v10/code/`, `python3 crawl.py --tasks <task file>` downloads the raw files into `v10/raw/` and `python3 crawl.py --status` reports the state. The task file is a Parquet table with the columns `conf_num`, `kind` and `priority`, and `build_crawl_tasks.py` builds the first one from the Open API universe. From `v10/code/pipeline/`, `python3 run_all.py` builds everything into `v10/build/`, `python3 run_all.py --sample 200` runs a seeded dry run into separate folders, `python3 package_release.py` writes the release assets and `python3 -m pytest -q` runs the tests.
- **Incremental runs.** Each stage has a fingerprint made of its inputs, the code of the modules it runs, the reference files it reads and the parameters it uses. A stage is re-run only when its fingerprint changes. Outputs are written to `build/.staging/` and then moved into place. A replaced output is moved to `build/_superseded/<run_id>/`, never deleted. Only one run at a time is allowed.
- **Randomness.** Every seeded sample uses the seed 8374.
- **Resources.** Each duckdb connection is limited to 8 GB and 4 threads. A stage process whose resident memory exceeds 20 GB is stopped and nothing is published.
- **Environment.** The release build ran on Python 3.12.9.
- **Tests.** On 2026-09-28 the pipeline test suite passed with 1,250 tests.
- **Open API key.** The legislator reference tables and the party timeline need an Open API key. The key is read at run time from the environment variable `ASSEMBLY_API_KEY`, or from the file named by `ASSEMBLY_API_KEY_FILE`, and is never written to logs, outputs or this repository.
- **Third-party table.** The Hanja reading table `hanja_table_0.15.1.yml` is the file `hanja/table.yml` of the PyPI package hanja 0.15.1. `roles.py`, `legislators.py` and `government.py` read it locally for Hanja readings. Its license has not been verified, so it is not redistributed with the release or this repository. A rebuild needs a local copy at `v10/raw/third_party/hanja_table_0.15.1.yml`, taken from the wheel that `pip download hanja==0.15.1 --no-deps` fetches.
- **Raw files.** The raw viewer pages and HWP files are not redistributed. `meetings.raw_sha1` records the SHA-1 of the file each meeting was built from, and `crawl.py` downloads the files again. A file whose SHA-1 differs from `raw_sha1` is not the file the release was built from, and `coverage_source_sample` compares the two for the files it re-reads.
- **Minister panels.** The minister-data snapshot and the 296-row panel (section 6) are not part of this repository. A rebuild needs the snapshot at `v10/interim/external/minister_data_v2.0.0/` and the 296-row panel `data/minister_panel_comprehensive.csv` of a minister-data checkout next to this repository (or the file named by the environment variable `MINISTER_PANEL_CSV`). `legislators.py` also reads `data-raw/members_all_assemblies.csv` of an assemblykor checkout next to this repository when it is present (or the file named by `ASSEMBLYKOR_MEMBERS_CSV`).
- **Preparatory inputs.** The meeting universe (the Open API lists plus the id-gap scan), the member tables and the v9 crosswalk inputs under `v10/interim/` were built by preparatory scripts before the crawl. Those scripts and tables are not part of this repository, so a rebuild from scratch needs them from the maintainer. The meetings table of the release lists every meeting of the universe. The president calendar and the party lineage table are in `v10/interim/`.

## 12. Configuration parameters

All open choices are in `v10/code/pipeline/config.yaml`. Parameters under `switchable` are applied by `run_all.py` and passed to `validate.py`. Parameters under `fixed` are coded inside a component, and `run_all.py` refuses any value other than the component's current one. The values in force are recorded in `MANIFEST.json` (`config`).

### 12.1 Switchable

| Parameter | Value | Effect |
|---|---|---|
| `dyads.exclude_after_end_marker` | `false` | `true` makes turns printed after a meeting-end marker break adjacency. |
| `legislators.name_fuzzy_committee` | `keep` | `drop` sets the legislator columns of links made by the one-syllable typo rule to null. |
| `government.panel` | `v2` | `legacy_296` links government turns to the earlier 296-row panel with its earlier rules and windows. |
| `government.minister_release` | `interim/external/minister_data_v2.0.0` | Snapshot of the minister-data release read by `government.py`. |
| `government.spell_buffer_days` | `1` | Days before a spell's start and after its end that still link a minister or prime-minister turn (`spell:buffer`). |
| `government.suspended_admin` | `president` | `acting` labels impeachment-suspension windows `권한대행(NAME)` with null ideology. |
| `government.nominee_in_tenure` | `keep` | `legacy_296` only. `drop` removes the flagged `nominee_in_tenure` panel links. |
| `government.buffer_days` | `7` | `legacy_296` only. Days around a panel tenure that still link a minister turn. |
| `government.nominee_pre_days`, `government.nominee_post_days` | `60`, `60` | `legacy_296` only. Window around the panel start that links a nominee turn. |
| `crosswalk.v10_complete` | `false` | `true` makes a v9 transcript found in no v10 meeting `v9_only`. |
| `validate.mode` | `release` | `dev` skips checks whose input is missing. |
| `validate.dup_allowlist` | `[]` | Pairs of meetings exempted from the duplicate checks, with a reason. |
| `validate.link_thresholds`, `validate.party_thresholds` | 0.97 (16th to 18th), 0.99 (19th to 22nd) | Minimum share of legislator-side turns with a link and with a party. |
| `validate.wit_title_max_share`, `validate.wit_title_max_share_by_type` | 0.01, 0.02 | Maximum share of dyads with a legislator title on the witness side, overall and per hearing type. |
| `validate.max_meeting_span_days`, `validate.speech_date_end_slack_days` | 10, 1 | Maximum days of speech after the meeting date, and after the term end. |
| `validate.dup_containment` and other `dup_*` | 0.5 and others | Thresholds of the duplicate checks. |
| `validate.procedural_sample` | 0 | 0 recomputes the procedural flag on every dyad. |
| `duplicates.near_identical_min_share` | 0.9 | Share of equal turns that makes a same-length pair `near_identical`. |

### 12.2 Fixed

| Component | Parameter | Value | Alternatives |
|---|---|---|---|
| party_timeline | `partyless_president_ruling` | `last_president_party` | `all_opposition`, `null`, `de_facto_governing_party` |
| party_timeline | `committee_table_inferences` | `accept` | `drop` |
| party_timeline | `merger_rename_date` | `earlier_of_lineage_and_notice` | `notice_date`, `lineage_date` |
| party_timeline | `satellite_party_camp` | `main_party_before_merger` | `own_party` |
| party_timeline | `speaker_nonpartisan` | `recorded_status_independent` | |
| party_timeline | `individual_exceptions_22` | `use_lineage_exceptions` | `use_election_party` |
| legislators | `nonleg_title_name_uniqueness_link` | `no` | `yes` |
| legislators | `future_member_link` | `no` | `yes` |
| legislators | `label_repair_confidence_cap` | `medium` | `low`, `high` |
| government | `acting_minister_link` | `acting_heads` | `never` |
| government | `ministry_naming` | `name_in_force_at_the_time` | `latest_name` |
| government | `panel_coverage_gaps` | `leave_unlinked` | `extend_panel` |
| roles | `ijangjang_by_institution` | `by_institution_type` | `all_public_corp_head_as_v9` |
| roles | `geomsajang` | `agency_head` | `senior_bureaucrat` |
| roles | `beobwonjang` | `other_official` | `new_judiciary_role` |
| roles | `audit_team_leader_banjang` | `chair` | `legislator` |
| roles | `company_marked_titles` | `private_sector` | `ownership_list` |
| roles | `independent_commission_staff` | `organisation_based` | `rank_based` |
| roles | `gisulwonjang` | `org_head` | `research_head` |
| roles | `national_arts_directors` | `private_sector` | `cultural_institution_head` |
| roles | `witness_counsel` | `witness` | `other` |
| roles | `legislator_commission_head` | `printed_title` | `legislator` |
| roles | `contains_matching_from_v9` | `keep` | `drop` |
| build_turns | `special_committee_key` | `single_special_committee_key` | `one_key_per_committee` |
| build_turns | `agenda_adjustment_committee` | `not_subcommittee_flagged` | `subcommittee` |
| build_turns | `confirmation_hearing_rule` | `special_committee_or_own_agenda_text` | `api_flags` |
| build_turns | `fused_hanja_label_split` | `accept_flagged_rules` | `leave_name_null` |
| build_turns | `orphan_taL_paragraphs_24614` | `events` | `attach_to_previous_turn` |
| build_turns | `sitting_rule` | `end_then_open_markers` | `end_marker_starts_segment` |
| build_turns | `label_confidence_table` | `build_turns.LABEL_CONFIDENCE` | |
| hwp_parser | `table_lines_in_text` | `keep_in_text` | `text_raw_only` |
| hwp_parser | `quoted_transcript_markers` | `keep_inside_quoting_turn` | `split_as_xlsx` |
| hwp_parser | `label_missing_marker` | `keep_turn_label_missing` | |
| hwp_parser | `inner_attendance_vote_lists` | `appendix_inner_events` | |
| dyads | `procedural_regex` | `frozen_round3` | `extended_closing_formulas` |
| dyads | `chair_utterances` | `keep_with_flags` | |
| dyads | `release_layout` | `slim` | |
| duplicates | `identical_text` | `keep_copy_matching_api` | |
| duplicates | `near_identical` | `keep_flagged` | `duplicate_of` |
| duplicates | `partial_overlap` | `keep_flagged` | |
| validate | `speech_before_meeting_date` | `day_before_and_appended_records_warn` | `not_allowed` |
| crosswalk | `partial_label_match` | `same_partial` | `v9_wrong_content` |
| crosswalk | `v9_49517_to_41344` | `add_meeting` | `not_in_universe` |

`partyless_president_ruling` = `last_president_party` codes the president's most recent party and its successors as ruling while the president has no party, and `v9_49517_to_41344` = `add_meeting` adds viewer id 41344, which the id-gap scan found, for v9 meeting 49517.

### 12.3 Paths and sources

| Parameter | Value |
|---|---|
| `paths.universe` | `meeting_universe_v10.parquet` |
| `sources` | `xml`, `xlsx`, `hwp` (adapters `build_turns` may run. Only `xml` and `hwp` produce release turns.) |
| `paths.calendar` | `interim/president_calendar.csv` |
| `paths.lineage` | `interim/party_lineage.csv` |
| `paths.v9_crosswalk` | `interim/v9_to_api_crosswalk.parquet` |

