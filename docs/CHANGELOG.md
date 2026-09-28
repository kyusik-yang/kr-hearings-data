# Changelog, kr-hearings-data v10

v10 is a rebuild from the official minutes, not an update of v9. This file lists every verified defect of the published v9 release (D1 to D12), what v10 does about it and which validation check guards it, followed by the other changes.

## Verified v9 defects

| # | v9 defect | v10 |
|---|---|---|
| D1 | Dyads built by sorting the string `speech_order` | Integer `turn_seq`, independent recomputation in validation |
| D2 | v9 regressed dyads that v5 had right | Dyads rebuilt from scratch for every meeting |
| D3 | v8 meetings carry another meeting's transcript | Labels and text come from one viewer id |
| D4 | Mixed meeting-id namespaces | `conf_num` key, `conf_id` verbatim, v9 ids in a crosswalk |
| D5 | Double ingestion and wrong labels in 인사청문 | One row per viewer id, second copies marked in the crosswalk |
| D6 | Role coding errors | Roles rebuilt with fixes and checks |
| D7 | `ruling_status` constant per term and party | Party and ruling status on the speech date |
| D8 | Minister administration coding | Administration from the speech date, links need a date window |
| D9 | Order and segmentation of PDF-derived meetings | No PDF source |
| D10 | Identifiers and missing values | `naas_cd` for all terms, nulls instead of empty strings |
| D11 | Documentation numbers do not match the data | Numbers generated from the release tables and checked |
| D12 | Coverage | Full Open API universe, subcommittees included |

### D1. Dyads built by string sort

**v9.** The dyad builders sorted the string column `speech_order` (build_v6.py, build_v8.py, build_v9.py), so `10` sorted before `2`. The published dyads equal a string-sort rebuild as a multiset (7,429,413 of 7,429,413). By position, 867,825 published dyads (11.68%) pair speeches that are not adjacent, and 1,431,973 of the 7,993,561 truly adjacent pairs (17.91%) are missing. 13,951 of the 14,626 meetings with dyads are affected.

**v10.** Turns carry an integer `turn_seq` that is contiguous from 1 in every meeting. Dyads pair `turn_seq` i and i+1 in one sitting. `validate.py` checks that every dyad pairs adjacent turns (`dyads_adjacent`), that its endpoints exist with the right role groups and texts (`dyads_endpoints`), that the dyad set equals an independent recomputation from the turns (`dyads_recompute`) and that no pair crosses a sitting (`dyads_sitting`). The release has 11,279,607 dyads.

### D2. Regression from v5

**v9.** The v5 dyads equal a numeric rebuild in 12,670 of 12,670 XLSX meetings (7,225,737 dyads). v6 and v8 appended new meetings with the string sort, and v9 rebuilt every meeting with it. The v8 dyad file had no dyads for the 228 인사청문 meetings added in v7.

**v10.** No dyad is carried over from an earlier release. All dyads are built from the v10 turns by one function (`dyads.build_dyads_file`).

### D3. v8 meetings with another meeting's transcript

**v9.** The labels of the meetings added in v8 (국정조사, 예결, 본회의) come from the meeting with that CONF_ID, but the text was fetched with the same number used as a viewer id. A full-population classifier finds 899 of 2,081 meetings (43.2%, 516,836 speeches, 280,947 dyads) with another meeting's transcript, with an upper bound of 1,165. A fresh sample of 25 found 11 wrong (44.0%, 95% interval 26.7% to 62.9%). 560 of the 899 duplicate another v9 meeting.

**v10.** Labels, date and committee come from the universe row of a viewer id (`conf_num`), and the text comes from the page or file of the same id. `crosswalk_meetings` records for every v9 meeting whose transcript it actually carries. 974 v9 meetings have `relation` = `v9_wrong_content`. An independent audit of the build of 2026-09-26 matched every v9 meeting to its best v10 meeting by sentence fingerprints and agreed with the crosswalk for 16,823 of 16,830 v9 meetings.

### D4. Mixed meeting-id namespaces

**v9.** The XLSX, v7 PDF and v8 ids are Open API CONF_IDs with the leading zero dropped for 5-digit ids. The 42 v6 HTML ids are viewer CONFER_NUMs. 52162 and 052162 are different meetings, and so are 52163 and 052163.

**v10.** The key is `conf_num` (int64, the viewer id). `conf_id` keeps the Open API CONF_ID verbatim as a string, and `v9_meeting_id` keeps the v9 id. `validate.py` checks that every (`conf_num`, `conf_id`) pair equals the Open API and that v9 ids follow their namespace (`ids_namespace`). [MIGRATION_v9_to_v10.md](MIGRATION_v9_to_v10.md) explains how to join v9 ids.

### D5. Double ingestion and wrong labels in 인사청문

**v9.** 27 sittings appear twice, once from v6 HTML and once from v7 PDF. 17 of the 42 v6 HTML meetings have wrong or null labels, 9 of them plenary sessions. Meeting 43038 (null term, date and committee) is the 2018-07-24 대법관 hearing (김선수, 노정희, 이동원).

**v10.** Every meeting appears once, keyed by its viewer id. In the crosswalk, v9 meetings that carry a transcript already carried by another v9 meeting are second copies (`is_second_copy`, `duplicate_of_v9_meeting_id`). 27 v6 HTML meetings are second copies of v7 PDF meetings. v9 meeting 43038 maps to its content meeting with `relation` = `v9_wrong_content`.

### D6. Role coding errors

**v9.** 52,207 예결 rows labelled `소위원장 NAME` and 24,987 rows with Hanja 委員 or 議長 are `other_official`, so 21.3% of the 예결 dyads have a legislator on the witness side. 23,439 전문위원 rows are `legislator`. Sitting prime ministers are `other_official` (40,883 rows by name).

**v10.** `roles.py` rebuilds all roles with the v9 taxonomy and fixes these cases ([CODEBOOK.md](CODEBOOK.md#82-changes-from-v9) section 8.2). `validate.py` checks that no staff title is on the legislator side (`roles_staff`) and that the share of dyads with a legislator title on the witness side stays below 1% overall and 2% per hearing type (`roles_wit_title_share`). 0 dyads have a legislator title on the witness side.

### D7. Ruling status as a constant per term and party

**v9.** `ruling_status` did not change with the date of the speech. It is inverted for 409,974 of 458,352 21st Assembly rows on or after 2022-05-10, for 183,266 of 245,296 20th Assembly rows before 2017-05-10, for 17,500 17th Assembly rows after 2008-02-25 and for 2,034 22nd Assembly rows after 2025-06-04. Party labels are stale after renames, for example 미래통합당 on 225,041 rows after 2020-09-02.

**v10.** Party comes from dated person spells built from the plenary report items, and `ruling_status` from the party and the president calendar on the speech date, with `presidency_state` ([CODEBOOK.md](CODEBOOK.md#10-party-ruling-status-and-presidency-state) section 10). `validate.py` recomputes the ruling status of every legislator-side turn independently (`ruling_recompute`) and checks presidency state, president and party against the calendar (`presidency_by_date`, `president_by_date`, `ruling_null`).

### D8. Minister administration coding

**v9.** build_v9.py codes 김대중 as Conservative (42,509 speech rows, 78,746 dyads). The panel fallbacks ignore dates.

**v10.** `admin` and `admin_ideology` come from the speech date and the president calendar, never from the minister panel, and 김대중 is Progressive. Government turns link to minister-data v2.0.0. A minister or prime-minister turn links to an appointment spell of the same person and office lineage that covers the speech date, a nominee turn to a nomination of the same person and lineage whose hearing date lies within one day of the speech date, and an acting head to an acting-head record that covers the date. There is no date-free fallback ([CODEBOOK.md](CODEBOOK.md#11-government-metadata) section 11). `validate.py` checks administration and ideology against the calendar (`admin_by_date`).

### D9. Order and segmentation of PDF-derived meetings

**v9.** In the v7 PDF meetings 20.3% of consecutive speeches have the same speaker, against 0.44% in the XLSX meetings. The PDF text has spacing artifacts inside words.

**v10.** No turn comes from a PDF. Turns come from the viewer XML, where the viewer marks each speaker block, or from the HWP file, where each turn starts at a printed speaker marker. In v10, 0.51% of consecutive turn pairs have the same speaker label.

### D10. Identifiers and missing values

**v9.** `member_id` mixes three namespaces. `member_uid` collides across people in 61 (uid, term) cells. The v7 and v8 rows store empty strings instead of nulls, so the true `leg_party` coverage is 97.06%, not 99.9%.

**v10.** Legislators are identified by `naas_cd`, the Assembly's person code, for every term, with `id_method` and `id_confidence`. The viewer's member-term id is a separate column (`speaker_mem_id`). Missing values are nulls, and `validate.py` counts an empty or `nan` string in `naas_cd` or `party` as an error (`legislator_links`, `party_coverage`). 1,323 distinct legislators are linked.

### D11. Documentation

**v9.** 112 of the 252 numbers in the v9 README, CODEBOOK and PIPELINE do not match v9. No validation report was ever run on v9, and the existing count-based spot check would have failed it (73 of 100 meetings).

**v10.** Every count in this documentation that describes the release was computed from the release tables by a query and checked against them before publication. `validate.py` also writes the counts it computes to `docs_numbers.json` in the build folder. The validation report is published with the release (`validation_report_v10.json`).

### D12. Coverage

**v9.** The standing committees end on 2024-12-31 and the national audit on 2024-11-01, not 2025-07-21 as documented. v9 has none of the 6,152 standing-committee subcommittee meetings and none of the 1,685 other special-committee meetings.

**v10.** The release covers the whole Open API universe for the 16th to 22nd Assembly plus the meetings found by the id-gap scan, 26,264 meetings in total, with 7,269 subcommittee meetings. The last meeting date is 2026-09-22. `validate.py` blocks the release when a universe meeting is missing (`universe_accounted`).

## Other changes

### Sources and scope

- One source per meeting, viewer XML or the HWP file. The 18th Assembly, for which the viewer serves no XML, is read from HWP files for all its meetings. The v9 XLSX rows were not used for any 18th Assembly turn because they lack 89% to 97% of the time markers and all attendance, footer and event data, repeat a block of 135 turns in meeting 33042 and render Hanja names in Hangul, while the HWP parse matches the XLSX turns at 0.99969 aligned speaker agreement.
- Every meeting with both a viewer page and an HWP file was cross-checked (21,901 meetings). The viewer XML is used unless it lacks at least 500 normalized characters of speech printed in the HWP record or carries the minutes of another meeting. 26 meetings use their HWP file for this reason (25 incomplete pages and 1 page with the minutes of another meeting). One of them is 24997, whose viewer page holds 1 turn while the printed record holds 130.
- The meeting universe adds subcommittees, special committees other than confirmation committees, the 전원위원회 and 187 meetings found by the id-gap scan.
- Two hearing types are new, `특별위원회` and `전원위원회`. New committee keys cover the special committees, the 전원위원회 and the committees renamed in 2025.

### Tables

- The meeting-level tables `meetings`, `agenda`, `agenda_header`, `events`, `footer`, `attendance`, `rollcall` and `rollcall_groups` are new.
- The crosswalk tables `crosswalk_meetings` and `crosswalk_turns` are new.
- The release assets are one Parquet file per table, and one file per term for turns and dyads (`turns_t16_v10.parquet` to `turns_t22_v10.parquet`, `dyads_t16_v10.parquet` to `dyads_t22_v10.parquet`), with `MANIFEST_v10.json`, `validation_report_v10.json` and `SHA256SUMS`.
- The dyad file is slim (39 columns). Other attributes join from the turns table on both turn positions.

### Turns

- `text_raw` keeps every printed sentence, and `text` drops whole-sentence stage directions and oath signature lines. Stage kinds and interjections are recorded.
- Sittings, meeting-end markers, label provenance (`label_how`, `label_confidence`), day roll-overs and clock regressions are recorded per turn.
- Labels that fuse two speakers are coded `other` and excluded from dyads. Titles that disagree with the person's majority title in the meeting are flagged.

### Enrichment

- Legislator links record their method and confidence, and same-name members of a term are separated by seat dates, markers and committee membership.
- Party spells record their basis and uncertainty windows (`party_uncertain`).
- Ruling status in partyless-president windows follows the president's most recent party and its lineage successors, with `presidency_state` = `partyless` and `president_last_party`. The 2007 window starts on 2007-02-28.
- `ministry_normalized` keeps the name in force at the time (for example 보건복지가족부, 여성부), where v9 used later names. `ministry_family` carries the lineage.
- `affiliation_raw` holds the institution printed in the title for every role. The printed title is in `title_raw`.
- Minister, minister-nominee, prime-minister and acting-minister turns, and nominee turns whose title names a cabinet office, link to the appointment spells, nominations and acting-head records of minister-data v2.0.0. The ids are in `minister_spell_id`, `minister_nomination_id`, `minister_acting_id`, `minister_person_id` and `minister_lineage`, and `dual_office` holds the seat status of the linked minister on the speech date. The earlier 296-row panel remains selectable (`government.panel` = `legacy_296`) to reproduce the numbers of builds before 2026-09-28.
- `is_former_title` marks titles printed as a former office (`(전)`, `(前)`, `前`). Such turns keep the role of the office in the title and are never linked to an appointment (1,745 turns).

### Duplicates

- Identical duplicate meetings keep one copy (the one whose printed date and committee match the source). The other is marked `duplicate_of` and has no turns and no dyads. Meetings that only partly overlap are kept and flagged.

### Validation

- A release-blocking validation suite of 47 checks, including an independent recomputation of the dyads, of the ruling status and of the text coverage ([PIPELINE.md](PIPELINE.md#9-validate) section 9).

### Python package

- Version 0.2.0 of the `kr_hearings_data` package reads v10 by default with `load_turns`, `load_meetings`, `load_dyads` and `load_table`. For v10, `load_speeches` is `load_turns`. The v9 and older files are no longer distributed, and the package no longer loads them.
