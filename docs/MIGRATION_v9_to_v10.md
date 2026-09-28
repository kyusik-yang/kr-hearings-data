# Migrating from v9 to v10

This file shows how to move an analysis from the v9 files (`all_speeches_16_22_v9.parquet`, `dyads_16_22_v9.parquet`) to v10, and how to link v9 rows to v10 rows with the crosswalk tables. The v9 files are no longer distributed, so the v9 side of each example needs a local copy of those files. With the `kr_hearings_data` package, `load_turns()`, `load_dyads()` and `load_table()` read v10 ([README](../README.md#how-to-load)).

## 1. What changes for an analysis

- **Rows.** A v9 speech row and a v10 turn are both one speaker turn, but v10 takes the text from the official viewer or HWP file. Most v9 rows have an exact v10 counterpart. Some differ in segmentation (one v9 row split into two turns or the reverse), and the v9 rows of some meetings belong to a different meeting (section 2).
- **Order.** v10 orders turns by the integer `turn_seq`. Never sort v9 `speech_order` as a string.
- **Dyads.** Do not reuse v9 dyads ([README](../README.md#why-the-v9-dyads-should-not-be-used) explains why). Take the v10 dyads, or rebuild dyads from v10 turns with the same rule.
- **Party and ruling status.** v10 values are those of the speech date. v9 values were constants per term and party. Results that compare ruling and opposition members across an in-term change of government will change.
- **Roles.** v10 fixes several v9 role errors and changes a few definitions. `role_v9_compat` gives the v9 answer for the same title.
- **Scope.** v10 adds subcommittees, special committees and the 전원위원회. Filter on `hearing_type` and `is_subcommittee` to keep a v9-like scope.

## 2. Meetings: `crosswalk_meetings`

`crosswalk_meetings_v10.2.parquet` has one row per v9 meeting (16,830 rows) and one row per v10 meeting that no v9 meeting carries (`relation` = `v10_only`).

Each v9 row has two v10 targets:

- `conf_num` is the meeting that the v9 **label** (id, date, committee) refers to.
- `content_conf_num` is the meeting whose **transcript** the v9 rows actually carry.

| `relation` | Meaning | Rows | Which target to use |
|---|---|---|---|
| `same` | The v9 transcript is the labelled meeting. | 15,837 | either (they are equal) |
| `v9_wrong_content` | The v9 transcript belongs to another meeting (`content_conf_num`), or to an unidentified one (null). | 974 | `content_conf_num` for anything computed from v9 text |
| `duplicate` | A second v9 copy of a transcript that another v9 meeting carries as `same`. | 19 | drop, the primary is `duplicate_of_v9_meeting_id` |
| `v9_only` | The v9 transcript is in no v10 meeting. | 0 | none |
| `v10_only` | A v10 meeting no v9 meeting carries. | 10,042 | new in v10 |

When several v9 meetings carry one transcript, one of them is primary and the others have `is_second_copy` = true and point to it in `duplicate_of_v9_meeting_id` (607 second copies). `content_overlap` says how much of the v9 transcript the content meeting holds (`full`, `v10_in_v9`, `partial`, `none`).

Example with duckdb:

```python
import duckdb

con = duckdb.connect()
# v9 meetings mapped to the v10 meeting whose transcript they carry, primaries only
m = con.sql("""
    SELECT v9_meeting_id, v9_source, relation, content_conf_num AS conf_num
    FROM read_parquet('kr-hearings-v10/crosswalk_meetings_v10.2.parquet')
    WHERE v9_meeting_id IS NOT NULL
      AND relation IN ('same', 'v9_wrong_content')
      AND NOT coalesce(is_second_copy, false)
      AND content_conf_num IS NOT NULL
""").df()
```

### 2.1 Do not join v9 ids to `conf_num` directly

The v9 `meeting_id` is not one namespace.

| v9 source | v9 `meeting_id` is |
|---|---|
| XLSX, 16th to 20th Assembly | the Open API CONF_ID without its leading zero (`52162` for `052162`) |
| XLSX, 21st and 22nd Assembly | the Open API CONF_ID with its leading zero (`052829`) |
| v7 PDF, v8 | the Open API CONF_ID without its leading zero |
| v6 HTML (인사청문) | the viewer id (CONFER_NUM) |

The digits of a v9 id often equal the `conf_num` of a different meeting. In the release, 13,256 of the 16,794 non-null `meetings.v9_meeting_id` values, cast to an integer, equal the `conf_num` of another meeting. This is how the v8 transcripts went wrong ([CHANGELOG.md](CHANGELOG.md#d3-v8-meetings-with-another-meetings-transcript), D3). Always go through `crosswalk_meetings`. `meetings.v9_meeting_id` is kept for reference only.

## 3. Rows: `crosswalk_turns`

`crosswalk_turns_v10.2.parquet` links v9 speech rows to v10 turns for every v9 meeting whose v10 meeting is built and aligned (`crosswalk_meetings.turn_alignment` = `aligned`). Every v9 row and every v10 turn of an aligned meeting appears, and the order is kept.

| `match_type` | Meaning | Rows |
|---|---|---|
| `exact` | Whitespace-free texts are equal. | 8,056,412 |
| `normalized` | Texts are equal after the crosswalk's text normalization. | 421,015 |
| `similar` | Texts are similar (ratio at least 0.6). | 106,460 |
| `split` | Two v9 rows form one v10 turn. | 2,930 |
| `merge` | One v9 row forms two v10 turns. | 1,244 |
| `v9_unmatched` | A v9 row with no v10 turn (`turn_seq` null). | 9,281 |
| `v10_unmatched` | A v10 turn with no v9 row (`v9_speech_order` null). | 5,772 |

`v9_speech_order` is the v9 value as stored (a string). `v9_order_num` is the same as an integer.

Example, attaching v10 turn attributes to v9 speech rows:

```python
rows = con.sql("""
    SELECT v.meeting_id, v.speech_order, v.role AS v9_role, v.ruling_status AS v9_ruling_status,
           c.match_type, t.conf_num, t.turn_seq, t.role, t.ruling_status, t.presidency_state
    FROM read_parquet('all_speeches_16_22_v9.parquet') v
    JOIN read_parquet('kr-hearings-v10/crosswalk_turns_v10.2.parquet') c
      ON c.v9_meeting_id = v.meeting_id AND c.v9_speech_order = CAST(v.speech_order AS VARCHAR)
    LEFT JOIN read_parquet('kr-hearings-v10/turns_t*_v10.2.parquet') t
      ON t.conf_num = c.conf_num AND t.turn_seq = c.turn_seq
    WHERE c.match_type IN ('exact', 'normalized', 'similar')
""").df()
```

A v9 row can link to two turns (`merge`) and two v9 rows to one turn (`split`), so count distinct keys on the side you analyse.

## 4. v9 speech columns in v10

| v9 column | v10 column | Note |
|---|---|---|
| `meeting_id` | `crosswalk_meetings` to `conf_num` | See section 2. |
| `term` | `term` | |
| `committee` | `meetings.committee_raw` | Subcommittee names are in `meetings.subcommittee`. |
| `committee_key` | `meetings.committee_key` | New keys for special committees, the 전원위원회 and the 2025 renames. |
| `hearing_type` | `meetings.hearing_type` | Two new values, `특별위원회` and `전원위원회`. |
| `session` | `meetings.session_no`, `meetings.session_type` | `session_no` is an integer. |
| `sub_session` | `meetings.sitting` | |
| `date` | `meetings.date`, `turns.speech_date` | `speech_date` follows day roll-overs within a meeting. |
| `agenda` | `turns.agenda_text`, `agenda` table | |
| `speaker` | `speaker_label_raw` | Label as printed. |
| `member_id` | `speaker_mem_id` | Different namespace. `speaker_mem_id` is the viewer's member-term id (19th Assembly onward). Use `naas_cd` for identity. |
| `member_uid` | `naas_cd` | |
| `speech_order` | `turn_seq` | Integer. Use `crosswalk_turns` to map. |
| `role` | `role` | Section 6. `role_v9_compat` gives the v9 answer. |
| `person_name` | `speaker_name` | As printed, Hanja kept. `leg_name_hangul` for linked legislators. |
| `person_title` | `person_title` | |
| `affiliation_raw` | `affiliation_raw`, `title_raw` | v10 `affiliation_raw` is the institution for every role, and `title_raw` the printed title. |
| `speech_text` | `text_raw`, `text` | `text` drops stage directions and oath signature lines. |
| `name_clean` | `leg_name_hangul` | |
| `party` | `party` | On the speech date. `party_camp` maps satellites to their main party. |
| `ruling_status` | `ruling_status` | On the speech date, with `presidency_state`. Section 7. |
| `seniority` | `seniority` | int16 instead of float. |
| `gender` | `gender` | |
| `naas_cd` | `naas_cd` | |
| `ministry_normalized` | `ministry_normalized` | Name in force at the time. `ministry_family` for the lineage. |
| `dual_office` | `dual_office` | Seat status on the speech date, only for turns linked to an appointment spell. |
| `admin` | `admin` | From the speech date, set for every turn. |
| `admin_ideology` | `admin_ideology` | 김대중 is Progressive. |

## 5. v9 dyad columns in v10

v10 dyads hold keys, flags and a core set of attributes. Join anything else from turns on (`conf_num`, `leg_turn_seq`) or (`conf_num`, `wit_turn_seq`).

| v9 dyad column | v10 dyad column | Note |
|---|---|---|
| `meeting_id` | `conf_num` | v9 id through `crosswalk_meetings`. |
| `term`, `hearing_type`, `committee_key`, `date` | same names | |
| `committee` | join `meetings.committee_raw` | |
| `agenda` | join `turns.agenda_text` | |
| `leg_name` | `leg_name` | Linked member's Hangul name, else the printed name. |
| `leg_speaker_raw` | join `turns.speaker_label_raw` | |
| `leg_member_uid` | `leg_naas_cd` | |
| `leg_party` | `leg_party` | On the speech date. |
| `leg_ruling_status` | `leg_ruling_status` | On the speech date. |
| `leg_seniority` | `leg_seniority` | |
| `leg_gender` | `leg_gender` | |
| `witness_name` | `wit_name` | |
| `witness_speaker_raw` | join `turns.speaker_label_raw` | |
| `witness_role` | `wit_role` | |
| `witness_affiliation` | join `turns.affiliation_raw` | Institution printed in the title. |
| `witness_ministry_normalized` | `wit_ministry_normalized` | |
| `witness_dual_office` | `wit_dual_office` | |
| `witness_admin`, `witness_admin_ideology` | `admin`, `admin_ideology` | From the date of the legislator turn (the same meeting). |
| `direction` | `direction` | Same definition. |
| `leg_speech` | `leg_text` | Spoken text. `text_raw` joins from turns. |
| `witness_speech` | `wit_text` | |

The columns `leg_turn_seq`, `wit_turn_seq`, `sitting_seq`, `leg_is_chair`, `leg_is_procedural`, `wit_is_legislator_title` and the `any_` flags are new in v10. To approximate the v9 substantive dyads, drop `leg_is_procedural` pairs and decide whether to keep chair pairs (`leg_is_chair`).

The v9 dyad file is no longer distributed, and no legacy file is published with v10. `legacy_v9_dyads` in `v10/code/pipeline/dyads.py` rebuilds the v9 dyads bit for bit from the v9 speech rows.

## 6. Roles

v10 uses the same 33 roles and groups. [CODEBOOK.md](CODEBOOK.md#82-changes-from-v9) section 8.2 lists the definitional changes (for example 이사장 by institution type, 검사장 as `agency_head`, 국정감사 반장 as `chair`, 국무총리실장 as `senior_bureaucrat`) and the fixed errors (소위원장, Hanja 委員, 전문위원, sitting prime ministers).

- To reproduce a v9 analysis on v10 text with v9 role definitions, use `role_v9_compat`. On the v9 XLSX-era rows the v9 rules alone reproduce the v9 role for 8,585,360 of 8,597,178 rows.
- `role` and `role_v9_compat` differ on 415,804 turns.
- v9 put some chairs and staff on the wrong side of a dyad. Compare dyad counts by `wit_role` before and after.

## 7. Party and ruling status

- `party` and `ruling_status` are the values on `speech_date`. Party changes within a term come from the plenary report items.
- `ruling_status` is null in `acting` windows (office vacant after a removal) and for legislator turns not linked to a person. In `partyless` windows the president's most recent party and its lineage successors count as ruling ([CODEBOOK.md](CODEBOOK.md#10-party-ruling-status-and-presidency-state) section 10).
- Do not reuse v9 `ruling_status`. It is inverted after in-term changes of government ([CHANGELOG.md](CHANGELOG.md#d7-ruling-status-as-a-constant-per-term-and-party), D7).

## 8. Checklist for re-running an analysis

1. Map every v9 meeting id through `crosswalk_meetings`, and drop `duplicate` rows and second copies.
2. Re-select the sample in v10 terms (`hearing_type`, `is_subcommittee`, `committee_key`, dates).
3. Rebuild dyads from v10 (or take the v10 dyad file) instead of reusing v9 dyads.
4. Use `ruling_status` and `presidency_state` on the speech date, and decide how to treat `partyless` and `acting` windows.
5. Decide whether to keep chair and procedural pairs (`leg_is_chair`, `leg_is_procedural`), turns after a meeting-end marker (`any_after_end_marker`) and low-confidence labels (`any_low_label_confidence`).
6. Compare the v9 and v10 results side by side and trace large differences through `crosswalk_turns`.

## 9. Known crosswalk issues

- v9 meeting 052829 (21st Assembly, 2023-03-13) holds the text of two meetings, 144 turns of 47235 (CONF_ID 052829) and 1,079 turns of 51884 (CONF_ID N052829). The crosswalk maps it to 47235 (`relation` = `same`, `content_overlap` = `v10_in_v9`), and 51884 is `v10_only`. In `crosswalk_turns` the 144 rows of 47235 are linked, and the 1,079 rows of 51884 are `v9_unmatched`.
- v9 meeting 29668 carries the 130 rows of meeting 24997 (16th Assembly, 2002-02-08). The viewer page of 24997 holds only 1 turn of 126 characters, so v10 builds 24997 from its HWP file (130 turns, `source_reason` = `override:xml_incomplete`), and the crosswalk maps 29668 to it as `same`.
- v9 meetings 31168 and 46348 (v8) are labelled as a plenary session of 2003-06-30 (26362) and an investigation meeting of 2016-08-29 (40657), but carry the transcripts of viewer ids 31168 (2007-10-15) and 46348 (2021-11-18). No Open API list returns these two meetings. The id-gap scan added both, and the crosswalk maps the v9 meetings to them as `v9_wrong_content`.
- v9 meeting 49517 (20th Assembly XLSX) points to viewer id 41344, which the Open API lists do not return. The id-gap scan added 41344 (`v9_49517_to_41344` = `add_meeting`), and the crosswalk maps 49517 to it as `same`, with all 1,787 turns aligned.
