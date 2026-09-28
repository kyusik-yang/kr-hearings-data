# Codebook, kr-hearings-data v10

## Contents

1. [Conventions](#1-conventions)
2. [Tables and keys](#2-tables-and-keys)
3. [meetings](#3-meetings)
4. [turns](#4-turns)
5. [dyads](#5-dyads)
6. [Meeting-level tables](#6-meeting-level-tables)
7. [Crosswalk tables](#7-crosswalk-tables)
8. [Role taxonomy and the v9 crosswalk](#8-role-taxonomy-and-the-v9-crosswalk)
9. [Hearing type, class, subcommittee and committee key](#9-hearing-type-class-subcommittee-and-committee-key)
10. [Party, ruling status and presidency state](#10-party-ruling-status-and-presidency-state)
11. [Government metadata](#11-government-metadata)
12. [Duplicate meetings](#12-duplicate-meetings)
13. [Unicode and NULL policy](#13-unicode-and-null-policy)
14. [Known limitations](#known-limitations)

## 1. Conventions

- Types are Apache Arrow types as stored in the Parquet files (`int64`, `int32`, `int16`, `string`, `bool`, `double`, `list<string>`).
- "Null" means a Parquet null (SQL `NULL`). No string column holds an empty string (section 13).
- Dates are strings `YYYY-MM-DD`. Times are strings `HH:MM` on a 24-hour clock.
- Korean terms are given in Hangul with an English gloss where the gloss helps. Values of categorical columns are listed exactly as stored.
- "XML" means the speaker-segmented viewer page of record.assembly.go.kr. "HWP" means the original minutes file of the same system.
- A rule named in this codebook is implemented in the module named after it in `v10/code/pipeline/`. The module docstrings hold the full rule text.

## 2. Tables and keys

| Table | File | Primary key | Rows |
|---|---|---|---|
| meetings | `meetings_v10.parquet` | `conf_num` | 26,264 |
| turns | `turns_tNN_v10.parquet` | (`conf_num`, `turn_seq`) | 15,114,183 |
| dyads | `dyads_tNN_v10.parquet` | (`conf_num`, `leg_turn_seq`, `wit_turn_seq`) | 11,279,607 |
| agenda | `agenda_v10.parquet` | (`conf_num`, `ordinal`) | 377,889 |
| agenda_header | `agenda_header_v10.parquet` | (`conf_num`, `item_seq`) | 508,708 |
| events | `events_v10.parquet` | (`conf_num`, `event_seq`) | 162,504 |
| footer | `footer_v10.parquet` | (`conf_num`, `row_seq`) | 5,301,479 |
| attendance | `attendance_v10.parquet` | (`conf_num`, `row_seq`) | 1,395,805 |
| rollcall | `rollcall_v10.parquet` | (`conf_num`, `vote_seq`, `vote_group`, `name_seq`) | 3,402,605 |
| rollcall_groups | `rollcall_groups_v10.parquet` | (`conf_num`, `vote_seq`, `vote_group`) | 51,593 |
| crosswalk_meetings | `crosswalk_meetings_v10.parquet` | `v9_meeting_id` for v9 rows, `conf_num` for `v10_only` rows | 26,872 |
| crosswalk_turns | `crosswalk_turns_v10.parquet` | none (a v9 row can link to two turns and the reverse) | 8,603,114 |
| duplicate_meetings | `duplicate_meetings_v10.parquet` | (`a`, `b`) | 5 |
| duplicate_turns | not a release asset (`duplicate_turns/tNN/part-*.parquet` in the build folder) | (`conf_num`, `turn_seq`) | 1,415 |
| duplicate_dyads | not a release asset (`duplicate_dyads/tNN/dyads.parquet` in the build folder) | as dyads | 890 |

Each table is one release asset, and turns and dyads are one asset per term (`tNN` = term 16 to 22). In the build folder of the pipeline (`build/release/`) the turns of a term are split into parts (`turns/tNN/part-KKKKK.parquet`), and `package_release.py` merges them (PIPELINE.md section 10). `duplicate_turns` and `duplicate_dyads` hold the turns and dyads of the meetings set aside as identical copies of other meetings (section 12). They have the columns of turns and dyads, are written to the build folder only and are not release assets.

Every table carries `conf_num`. The meeting-level tables (agenda to rollcall_groups) also carry `source` and `term` of their meeting.

## 3. meetings

One row per meeting of the meeting universe. The universe is the Open API list of meetings for the 16th to 22nd Assembly plus the meetings found by the id-gap scan (`v10/interim/meeting_universe_v10.parquet`). Meetings that are marked as duplicates keep their row here (section 12).

### 3.1 Identifiers

| Column | Type | Null when | Definition |
|---|---|---|---|
| `conf_num` | int64 | never | Viewer id of the meeting (CONFER_NUM, the `id` of `viewer/minutes/xml.do`). Primary key of every table. |
| `conf_id` | string | the meeting was found only by the id-gap scan (187 meetings) | Open API CONF_ID, verbatim. Six digits with the leading zero (`053923`) or `N` plus six digits (`N054500`). Never cast it to an integer. The digits of a CONF_ID can equal the `conf_num` of a different meeting. |
| `v9_meeting_id` | string | no v9 meeting maps to this meeting | The v9 `meeting_id` of the same meeting. When several v9 meetings map here, the first by v9 source (XLSX, then v8, v7, v6) and then by id. The v9 namespaces differ by v9 source (see [MIGRATION_v9_to_v10.md](MIGRATION_v9_to_v10.md)). `v9_meeting_ids` lists all of them. |
| `v9_meeting_ids` | list<string> | as `v9_meeting_id` | Every v9 `meeting_id` whose crosswalk target is this meeting. |
| `v9_source` | string | as `v9_meeting_id` | v9 ingestion source of `v9_meeting_id`. One of `xlsx`, `v8_flagged`, `v8_unflagged`, `v7_pdf`, `v6_html`. |
| `v9_hearing_type` | string | as `v9_meeting_id` | v9 `hearing_type` of that meeting. |
| `v9_id_namespace` | string | as `v9_meeting_id` | Id space of `v9_meeting_id`. `conf_id` (the Open API CONF_ID with or without its leading zeros), `confer_num` (the viewer id, v6 HTML meetings) or `none` (matched on content). |
| `raw_sha1` | string | never | SHA-1 of the raw file (viewer page or HWP file) the turns were built from. |
| `source_reason` | string | when not built | Why `source` was chosen. `xml_default` (viewer XML), `hwp_no_usable_xml` (18대, or the viewer page returned HTTP 400), `override:xml_incomplete` (the viewer XML lacks at least 500 normalized characters of speech printed in the HWP record, 25 meetings) or `override:xml_wrong_meeting` (the viewer XML carries another meeting's minutes, 1 meeting). The overrides come from the XML-vs-HWP cross-check of every meeting with both sources ([PIPELINE.md](PIPELINE.md#4-parse) section 4). |

### 3.2 Classification

| Column | Type | Null when | Definition |
|---|---|---|---|
| `term` | int16 | never | Assembly term (대수, DAE_NUM), 16 to 22. |
| `class_name` | string | never | Open API class after unification (`CLASS_NAME_unified`). One of `상임위원회`, `국정감사`, `특별위원회`, `국회본회의`, `예산결산특별위원회`, `국정조사`, `전원위원회`. |
| `api_class_raw` | string | the meeting is in no Open API agenda-row list service | `CLASS_NAME` as returned by the list service. |
| `hearing_type` | string | never | v9-compatible type (section 9.1). One of `상임위원회`, `국정감사`, `특별위원회`, `인사청문특별위원회`, `국회본회의`, `예산결산특별위원회`, `국정조사`, `전원위원회`. |
| `is_subcommittee` | bool | never | True for subcommittee meetings (section 9.2). |
| `api_is_subcommittee` | bool | never | The subcommittee flag from the Open API committee name. |
| `is_agenda_adjustment` | bool | never | True when the meeting is an 안건조정위원회 (agenda adjustment committee) formed inside a committee (65 meetings). They have `is_subcommittee` false unless the Open API flags them as a subcommittee (1 meeting, section 9.2). |
| `committee_raw` | string | never | Parent committee name. The first token of the Open API committee name (COMM_NAME), else the printed header. `국회본회의` for plenary meetings. |
| `subcommittee` | string | full-committee meetings and all 국정감사 meetings | Subcommittee or 안건조정위원회 name, from the Open API committee name. |
| `committee_printed` | string | header without a committee line (2 meetings) | Committee name as printed in the minutes header. |
| `subcommittee_printed` | string | nothing printed | Subcommittee name as printed in the minutes header. |
| `api_comm_name` | string | the Open API gives no committee name | Open API COMM_NAME (or `v_CMIT_NM`) as returned. |
| `committee_key` | string | never | Harmonized committee key (section 9.3). |
| `committee_key_rule` | string | never | Rule that set `committee_key`. One of `legacy_map`, `legacy_hearing_type`, `v10_hearing_type`, `v10_new_standing`. |
| `audit_team` | string | not a 국정감사 team sitting | 국정감사 team (반) such as `제1반` or `미주반`. 145 meetings. |
| `doc_label` | string | not a 국정조사 record labelled as such | The document label that the Open API puts in the subcommittee slot of some 국정조사 meetings (`…국정조사조사록`). It is not a subcommittee. 59 meetings. |
| `is_confirmation_hearing` | bool | never | True when the meeting is a confirmation hearing (section 9.4). 778 meetings. |
| `confirmation_rule` | string | `is_confirmation_hearing` is false | Rule that set the flag. `special_committee`, `source_agenda_text`, or both joined by `;`. |
| `api_in_vconf_cfrm` | bool | never | The meeting is in the Open API 인사청문회 list (`VCONFCFRMCONFLIST`). Kept for reference only. |
| `api_agenda_confirmation_hit` | bool | never | An agenda string of the Open API names a nominee hearing. Kept for reference only. |
| `api_in_vconf_ch` | bool | never | The meeting is in the Open API 청문회 list. |
| `api_in_vconf_ph` | bool | never | The meeting is in the Open API 공청회 list. |
| `api_in_vconf_jm` | bool | never | The meeting is in the Open API 연석회의 list. |

### 3.3 Session and date

| Column | Type | Null when | Definition |
|---|---|---|---|
| `session_no` | int16 | not printed and not in the Open API | Session number (회). From the printed header when printed, else from the Open API. |
| `session_no_source` | string | no session number anywhere (1 meeting) | `xml`, `hwp` or `api`. |
| `session_type` | string | not printed or not recognised (4,962 meetings) | Session type in one Hangul spelling, `임시회`, `정기회`, `임시회(폐회중)`, `정기회(폐회중)` or `특별회`, read from the printed form (Hanja `臨時會`, `定期會` and separator variants included). |
| `session_type_raw` | string | not printed | Session type as printed. |
| `sitting` | string | not printed and not in the Open API | Sitting (차), for example `제3차`. |
| `date` | string | never | Meeting date. The Open API CONF_DATE, or for a gap-scan meeting the date recorded in the meeting universe. |
| `date_end` | string | never | Last date of the meeting. Later than `date` when the minutes roll over midnight or print a later date (158 meetings). |
| `date_printed` | string | never | Date printed in the minutes header. |
| `date_mismatch` | bool | never | True when `date_printed` differs from the Open API date. |
| `audit_year` | int16 | not a 국정감사 meeting | Audit year of a 국정감사 meeting. |
| `audited_agencies` | list<string> | not a 국정감사 meeting, or the minutes list no agencies | Audited agencies printed in the header (XML) or on the cover (HWP), split outside parentheses. |
| `audited_agencies_how` | string | as `audited_agencies` | Where they were read. `viewer_field_피감사기관`, or `hwp_cover_` plus the cover label (for example `被監査機關`). |
| `title` | string | no title can be found or built (0 meetings) | Title of the minutes. The viewer header title, else the Open API TITLE, else a title built from the meeting fields in the viewer's shape, else the printed HWP cover title. |
| `title_source` | string | as `title` | `viewer_header`, `api`, `constructed` or `printed_cover`. |

### 3.4 Build and content

| Column | Type | Null when | Definition |
|---|---|---|---|
| `source` | string | never | Source of the turns. `xml` or `hwp`. |
| `parse_status` | string | never | `ok`, or `ok_no_speeches` for a published record with no speaker turn (1 meetings). |
| `is_built` | bool | never | True for every meeting of the release. |
| `in_universe` | bool | never | True for every meeting of the release. |
| `is_provisional_minutes` | bool | HWP meetings | True when the viewer marks the minutes as provisional (XML only). |
| `n_turns` | int32 | never | Number of turns of the meeting. Equals the count in the turns table, or in `duplicate_turns` for a meeting marked `duplicate_of` (the 2 removed copies keep their turn counts). |
| `n_agenda` | int32 | never | Agenda anchors in the body. |
| `n_events` | int32 | never | Rows in events. |
| `n_footer_rows` | int32 | never | Rows in footer. |
| `n_rollcall_votes` | int32 | never | Recorded votes. |
| `duplicate_of` | int64 | the meeting is not an identical copy of another meeting | `conf_num` of the copy kept in place of this identical copy (section 12). The turns and dyads of this meeting are in `duplicate_turns` and `duplicate_dyads`, not in turns and dyads. 2 meetings. |
| `duplicate_basis` | string | as `duplicate_of` | Why this copy was not kept, with the printed-against-API evidence of both copies. |
| `overlap_with` | list<int64> | no near-identical or partial overlap | `conf_num` of the meetings whose text overlaps this meeting's text. Both meetings are kept. 6 meetings. |
| `overlap_kinds` | list<string> | as `overlap_with` | Kind of each overlap, parallel to `overlap_with`. `near_identical` or `partial`. |

## 4. turns

One row per merged speaker turn. Turns are ordered by `conf_num` and `turn_seq`. A turn is every sentence printed under one speaker marker until the next speaker marker. In XML, fragments of one speaker id that a time marker splits are merged into one turn (`n_fragments`). The turns of a meeting marked `duplicate_of` are in `duplicate_turns` instead.

The turns table has the source columns (4.1 to 4.6) and four blocks of enrichment columns (4.7 to 4.10). The enrichment modules read only the source columns and the meeting columns `term`, `class_name`, `hearing_type`, `committee_raw`, `subcommittee` and `date`.

### 4.1 Keys and source

| Column | Type | Null when | Definition |
|---|---|---|---|
| `conf_num` | int64 | never | Meeting (see meetings). |
| `turn_seq` | int32 | never | Position of the turn in the meeting, 1 to n with no gaps. All ordering uses this integer. |
| `term` | int16 | never | Assembly term of the meeting. |
| `source` | string | never | `xml` or `hwp`. |
| `text_rule` | string | never | How text and spoken text were derived. `xml_sentences` (viewer sentence spans) or `hwp_lines` (HWP paragraph lines). |

### 4.2 Speaker label

| Column | Type | Null when | Definition |
|---|---|---|---|
| `speaker_label_raw` | string | the marker has no label (`label_how` = `label_missing`) | Speaker label as printed, for example `위원장 홍길동`, `국방부장관 이종섭`, `이수진(비) 위원`, `保健福祉部長官崔善政`. |
| `speaker_pos` | string | the label has no position part | Position part of the label as printed (`위원장`, `국방부장관`, `위원`). For XML the viewer's `data-pos` attribute, split further when it fuses a title and a name. |
| `speaker_name` | string | the label has no name part | Person name as printed. Many 16th and 17th Assembly names are in Hanja. |
| `speaker_mem_id` | int64 | not XML, or the viewer gives no member id or gives 0 | Viewer `data-mem_id`, a member-term id (a new id each term). It is 0 in the viewer for all 16th and 17th Assembly speakers, so it is null there. 5,366,972 turns have it. |
| `speaker_area` | string | as `speaker_mem_id` | Constituency printed by the viewer for a member (19th Assembly onward). |
| `spk_id` | string | HWP | Viewer speaker id (`spk_N`). |
| `profile_slug` | string | no member profile link | Slug of the member profile link in the viewer. |
| `profile_term` | int16 | as `profile_slug` | Term of the member profile link. |
| `label_split` | string | never | How the label was split into position and name. XML `attr` (viewer attributes), `pos=name+role`, `pos=name_only`, `unsplit`. HWP `pos_name`, `name_pos`, `unsplit`, `fused`, `missing` (a marker without a label). Values starting `v10_` are flagged rules. They split fused Hanja labels (`v10_hanja_suffix_name`, `v10_hanja_suffix_name_spaced`, `v10_hanja_pos_hangul_name`, `v10_hangul_pos_hanja_name`, `v10_name_role_spaced`, `v10_space_pos_name`, `v10_space_pos_name_dup`) and, when the viewer gives no name, a Hangul label made of a position, a space and a Hangul name (`v10_space_pos_hangul_name`). |
| `label_fused` | bool | never | The printed label contains a second speaker marker (◯ or ○), so it may fuse two speakers. 932 turns. |
| `label_has_text` | bool | never | The printed label holds speech text (a sentence end or a comma inside the label). |
| `label_misattributed` | bool | never | The label holds speech text, then a marker and another label. The speaker fields come from the part before the marker while the text belongs to the speaker after it (for example meeting 25572, turn 190). 11 turns. |
| `speaker_name_norm` | string | `speaker_name` is null | Matching form of `speaker_name`. Separators unified to U+00B7, NFKC (compatibility ideographs folded), whitespace runs collapsed. Not for display. |
| `speaker_pos_norm` | string | `speaker_pos` is null | Matching form of `speaker_pos`, built the same way. |
| `name_from_label` | bool | never | XML only. The name was taken from the printed label instead of `data-name`. |
| `name_has_hanja` | bool | never | `speaker_name` contains a Hanja character. |
| `label_how` | string | never | How the label was found. XML `in_chk_label`, `in_chk_label_nosuffix`, `attr_fallback`. HWP `sep`, `sep_joined_pos_name`, `sep_trimmed_by_lexicon`, `lexicon_prefix`, `lexicon_prefix_punct`, `lexicon_prefix_fused`, `single_space_name_pos`, `label_only`, `label_joined_next_line`, `single_space_pos_name`, `sep_implausible`, `label_missing`. |
| `label_confidence` | string | never | `high`, `medium` or `low`, from the table `build_turns.LABEL_CONFIDENCE` keyed on `label_how` (`unrated` for a `label_how` not in the table). A label that is fused or holds text is rated `low` whatever `label_how` says. |
| `label_lex_count` | int32 | XML | HWP only. Occurrences of the label in the document's own label lexicon. |

The label confidence table:

| `label_confidence` | `label_how` values |
|---|---|
| high | `in_chk_label`, `in_chk_label_nosuffix`, `attr_fallback`, `sep`, `sep_joined_pos_name`, `sep_trimmed_by_lexicon` |
| medium | `lexicon_prefix`, `lexicon_prefix_punct`, `lexicon_prefix_fused`, `single_space_name_pos`, `label_only`, `label_joined_next_line` |
| low | `single_space_pos_name`, `sep_implausible`, `label_missing` |

### 4.3 Text

| Column | Type | Null when | Definition |
|---|---|---|---|
| `text_raw` | string | the turn has no text outside its printed label (21 turns, 7 of them with speech printed inside the label, `label_has_text`) | All sentences of the turn in document order, joined with a newline. Stage directions and oath signature lines are included. Verbatim. |
| `text` | string | the turn holds only stage directions (3,121 turns), or `text_raw` is null | Spoken text. `text_raw` without whole-sentence stage directions and oath signature lines, and without the speaker label when the source prints it again at the start of the speech. |
| `text_label_prefix_stripped` | bool | never | The printed label (or the name) repeated at the start of the first sentence was removed from `text`. `text_raw` keeps it. 25,121 turns. |
| `text_label_prefix_match` | string | `text_label_prefix_stripped` is false | What was repeated, `label` or `name`. |
| `has_stage` | bool | never | The turn contains at least one stage sentence. |
| `stage_kinds` | list<string> | never (empty list) | Kinds of the stage sentences. `mic_cut`, `chair_change`, `collective_response`, `appendix_note`, `vote_procedure`, `visual_aid`, `noise`, `movement`, `gesture`, `interjection`, `note_block`, `other`. |
| `stage_texts` | list<string> | never (empty list) | The stage sentences. |
| `n_sentences` | int32 | never | Sentences (XML) or lines (HWP) of the turn. |
| `n_stage_sentences` | int32 | never | Stage sentences. |
| `n_oath_signature` | int32 | never | Oath signature lines removed from `text`. |
| `n_embedded` | int32 | never | XML text nodes or HWP tables embedded in the turn. |
| `n_fragments` | int16 | never | XML speaker fragments merged into the turn. 1 for HWP. |
| `interjections` | string | the turn records no off-microphone remark | JSON list of remarks of other people recorded inside the turn, each `{who, where, text}` (`(◯정양석 의원 발언대 옆에서 ― …)`). |
| `inline_stage_parens` | list<string> | never (empty list) | Parentheticals inside spoken sentences that match the stage lexicon. They stay in `text`. |

### 4.4 Agenda and time

| Column | Type | Null when | Definition |
|---|---|---|---|
| `agenda_ordinal` | int32 | the turn precedes the first agenda anchor, or the meeting has none | Ordinal of the agenda anchor in force (joins `agenda.ordinal`). |
| `agenda_text` | string | as `agenda_ordinal` | Text of that anchor. |
| `agenda_item` | string | HWP, or no anchor in force | XML anchor id (`itemN`). |
| `agenda_top_text` | string | HWP, or no anchor in force | XML text of the top-level anchor in force. |
| `time_hhmm` | string | no time marker printed before or within the turn | Last time marker before or within the turn, `HH:MM`. |
| `time_hhmm_start` | string | no time marker printed before the turn | Time in force when the turn starts. |
| `time_marker` | string | as `time_hhmm_start` | That marker as printed, for example `(15시07분 개의)`. |
| `time_regress` | bool | XML | HWP only. The printed clock went backwards at this turn. 2,016 turns. |
| `speech_date` | string | never | Date of the turn. The meeting date, advanced by printed day roll-overs (`(24시 경과)`, dated markers) and, in HWP, by a later sitting's printed date. |
| `speech_date_end` | string | the date does not change inside the turn | Date after a roll-over printed inside the turn. |
| `speech_date_how` | string | XML | HWP only. `cover` (date of the cover page), `sub_cover` (date of a later sitting's cover), `inherited` (a later sitting with no printed date keeps the date in force), `rollover` (the date after a printed or implied day roll-over). |

### 4.5 Sittings and turn boundaries

| Column | Type | Null when | Definition |
|---|---|---|---|
| `after_end_marker` | bool | never | A meeting-end time marker is printed before the turn within the turn's own sitting. End markers are the exact actions `산회`, `폐회`, `감사종료`, `조사종료`, `散會`, `閉會`, and in HWP also `폐식`, `閉式`, `유회`, `流會` and the note `(계속개의되지 않았음)`. `비공개감사종료`, `투표종료` and `회의중지` are not ends. The flag resets when a new sitting starts. 2 turns. |
| `after_final_end_marker` | bool | never | The turn starts after the last meeting-end marker of the document (text printed after the close). 463 turns. |
| `sitting_seq` | int16 | never | Sitting within the meeting document, starting at 1. A new sitting starts at an opening or continuation marker (`개의`, `계속개의`, `속개`, `개회`, `감사개시`, `조사개시`, `감사계속`, `조사계속`, `회의계속`, `開議`, `續開`, `開會`, `繼續開議`) printed after an end marker. For HWP the parser's value (a later sitting's cover page also starts a sitting). Dyads never pair turns of different sittings. |
| `sitting_how` | string | never | `end_open_markers` (the marker rule, XML) or `parser` (HWP). |

A marker printed after turn k began (`after_turn_seq` = k) precedes turn k+1.

### 4.6 Source-specific columns kept from the adapters

| Column | Type | Null when | Definition |
|---|---|---|---|
| `source_member_id` | string | always in the release | v9 `member_id` of an XLSX row, kept by the XLSX adapter. No v10 turn is built from XLSX rows, so the column is null in every turn. |
| `source_speech_order` | string | always in the release | v9 `speech_order` of an XLSX row, kept by the XLSX adapter. It is null in every turn. The crosswalk carries the v9 order instead (`crosswalk_turns.v9_speech_order`). |

### 4.7 Speaker role (roles.py)

| Column | Type | Null when | Definition |
|---|---|---|---|
| `role` | string | never | Speaker role in the v9 33-role taxonomy (section 8). `unknown` only for a turn with an empty label. A turn whose label fuses two speakers' labels (`label_fused`) gets `other`. |
| `role_group` | string | never | `legislator` (roles `legislator`, `chair`), `nonlegislator` (29 roles), `excluded` (`committee_staff`, `other`, `unknown`). |
| `role_rule` | string | never | Id of the rule that set the role, for example `leg.member.memid`, `leg.chair.subcommittee.presiding`, `exec.vice_minister`. `rule_table.csv` of the roles component lists them in evaluation order. A fused label gets `label_fused_excluded[ROLE]>RULE` with the role and rule the last label would have had. A repaired label gets `meeting_majority_repair>RULE`. |
| `role_v9_compat` | string | never | The role the v9 cascade gives for the same printed title (section 8.3). |
| `role_v9_compat_src` | string | never | `lookup` (v9 speaker-role table), `chain` (v9 rules run on the title) or `empty` (no label, role `unknown`). |
| `title_raw` | string | no position printed | Printed title with whitespace removed, Hanja kept. |
| `pos_hangul` | string | `title_raw` is null (204 turns) | Title converted to Hangul (compatibility ideographs folded, Hanja read, 두음법칙 applied to the first syllable). |
| `pos_fix` | string | never | Label repairs applied, joined by `+`. `none` when no repair. `two_speakers` marks a label that fuses two speakers' labels around a marker. The role and name then come from the last label, and which speaker the text belongs to is not known. |
| `affiliation_raw` | string | the title is only an office word (위원, 증인, 국무총리) | Institution printed in the title, without acting or nominee words and one office word. `국방부` for `국방부장관`. |
| `person_title` | string | no acting or deputy form | Acting or deputy form. `대리`, `직무대행`, `직무대리`, `권한대행`, `대행`, `반장`, `반장대리`, `반장직무대리`, `반장직무대행`. |
| `label_inconsistent_in_meeting` | bool | never | The printed title of this person disagrees with the person's majority title in the meeting (for example one turn `국토해양부장관 권도엽` against many turns `국토해양부제1차관 권도엽`). The printed role is kept, and the turn is not linked to a minister-panel row. 3,030 turns. |
| `is_former_title` | bool | never | The title is printed as a former office ('(전)육군참모총장', '(前)…', '前國防部長官'). `role` still follows the office in the title. Such a turn is never linked to a minister panel spell (`link_method` `unlinked:former_title`). 1,745 turns. |
| `label_repaired` | bool | never | The title was not recognised and is an obvious typo of the person's majority title in the meeting, so the role comes from the majority title. 360 turns. |
| `label_meeting_majority` | string | the turn is neither flagged nor repaired | The person's majority printed title in the meeting. |

### 4.8 Legislator identity (legislators.py)

A turn is linked to a legislator (NAAS_CD, the Assembly's person code) when its role group is `legislator`, and for a few non-legislator titles with dual-office evidence. The first rule that yields exactly one person wins.

| Column | Type | Null when | Definition |
|---|---|---|---|
| `naas_cd` | string | not linked | NAAS_CD of the speaker. 1,323 distinct persons. |
| `id_method` | string | never | How the link was made, or why not (values below the table). |
| `id_confidence` | string | not linked | `high`, `medium` or `low` (below). |
| `id_candidates` | int16 | never | Number of candidate persons considered. |
| `id_note` | string | no note | Free-text note on the decision, for example why a non-legislator title was not linked. |
| `id_label_repair` | bool | never | The name was taken from the position or label because the name slot held a title. Links made this way are at most `medium`. |
| `id_memid_status` | string | no viewer member id | What happened to the viewer member id. `used`, `not_used_nonlegislator`, `not_used_name`, `not_in_crosswalk`. |
| `leg_name_hangul` | string | not linked | Name in Hangul from the Assembly member record. |
| `leg_name_hanja` | string | not linked, or no Hanja name recorded | Name in Hanja from the member record. |
| `gender` | string | not linked | `남` or `여`. |
| `birth_date` | string | not linked | Birth date. |
| `district` | string | not linked, or no seat in this term on record | Constituency of the member-term (`전라북도 고창,부안군`), or `비례대표` for a list seat. |
| `elect_type` | string | as `district` | `지역구` (district) or `비례` (proportional list). |
| `seniority` | int16 | not linked | Terms served up to and including this term (ALLNAMEMBER). |
| `leg_side` | bool | never | The turn is on the legislator side. |
| `leg_side_basis` | string | never | `role_group` (the roles component decided the side). |
| `leg_title_class` | string | never | Class of the printed title. `legislator`, `cabinet`, `nominee`, `other`, `none`. |
| `leg_is_term_member` | bool | not linked | The person is a member of this term. |
| `leg_seated_on_date` | bool | not linked | The member's seat covers the speech date. |
| `leg_stint` | int16 | as `district` | Stint of the member within the term (1, or 2 for a second seat in the same term). |
| `leg_record_mem_id` | string | no record member-term id (16th to 18th Assembly) | Member-term id of the record system for the linked member. |
| `leg_date_basis` | string | never | Date used for seat and committee checks. `speech_date`. |

`id_method` values that link a person are `mem_id`, `mem_id_pos_name_swapped`, `mem_id_seat_override`, `mem_id_not_seated`, `mem_id_term_mismatch`, `mem_id_name_mismatch`, `name_term`, `name_term_dueum`, `hanja_term`, `hanja_term_variant`, `hanja_term_surname_variant`, `hanja_term_hangul_wildcard`, `hanja_reading_name`, `hanja_term_partial`, `homonym_seat_dates`, `homonym_area`, `homonym_marker_elect_type`, `homonym_marker_district`, `homonym_marker_party`, `homonym_committee`, `homonym_meeting_complement`, `name_fuzzy_committee`, `nonleg_dual_office`, `nonleg_sitting_member_panel_note` and `nonleg_former_member_panel`. Other values leave `naas_cd` null. They are `unlinked:label_confidence_low` (the parser rates the label `low`, so no person is linked), `nonleg_not_member`, `nonleg_name_collision`, `nonleg_former_unverified`, `nonleg_future_member`, `nonleg_former_ambiguous`, `nonleg_former_implausible_age`, `unresolved_no_member_in_term`, `unresolved_ambiguous` and `unresolved_no_name`.

Resolution order, first match wins:

1. The viewer member id through the record member-term crosswalk (19th to 22nd Assembly), when the printed name is the record name or one character from it. If that member is not seated on the speech date and exactly one member of the same name is, that member is taken (`mem_id_seat_override`).
2. A unique Hangul name among the members of the term, then the 두음법칙 spelling of the surname (`name_term_dueum`).
3. A unique Hanja name among the members of the term, then a one-character variant, a surname variant, API Hanja names containing Hangul, the Hangul reading of the Hanja, and a dropped first character (low).
4. Members of the same name within a term are separated by seat dates on the speech date, the printed area, printed markers (`(비)`, district, party initial checked against the party lineage), committee membership on the speech date, and the complement of a marked label in the same meeting.
5. A one-syllable typo against members seated on the date and on the meeting's committee (`name_fuzzy_committee`, low).

`id_confidence` is `high` for a seated record member-term id, a unique exact name in the term, or the only same-name member seated on the date. It is `medium` for a derived cue (spelling variant, printed area or marker, committee roster, same-meeting complement, seat override, a member id whose printed name is one character off, dual-office or panel-note link, label repair). It is `low` for conflicting cues, a member not seated on the date, a dropped character or a one-syllable typo.

Non-legislator titles (ministers, nominees) keep their role. They are linked to a member only with evidence from a minister-panel row of the office named in the title that covers the speech date. This evidence comes from the 296-row `minister_panel_comprehensive.csv` of the minister-data project, not from minister-data v2.0.0, which section 11 uses (PIPELINE.md section 6). Name uniqueness alone never links. Officials who became members only later are not linked (`nonleg_future_member`). 301,257 non-legislator turns are linked.

### 4.9 Party and ruling status (party_timeline.py)

Party columns are set for legislator-side turns linked to a legislator. Section 10 gives the rules.

| Column | Type | Null when | Definition |
|---|---|---|---|
| `party` | string | not a linked legislator-side turn | Party of the member on `speech_date`, the formal label with renames and mergers applied. `무소속` for independents. |
| `party_lineage` | string | as `party` | Party family, named by its latest label (`더불어민주당`, `국민의힘`). |
| `party_camp` | string | as `party` | The main party for a satellite party before its merger, else `party`. |
| `is_satellite` | bool | as `party` | `party` is a satellite list party (`더불어시민당`, `미래한국당`, `국민의미래`, `더불어민주연합`). |
| `party_method` | string | as `party` | `person_spell` (the member's dated party spell) or `label_lineage` (the election party carried through the lineage). |
| `party_basis` | string | as `party` | The event that opened the spell, for example `start_roster`, `join`, `leave`, `switch`, `party_merge`, `lineage_relabel`, `election_party`, `inferred_from_committee_table`. |
| `party_spell_id` | string | as `party` | Id of the spell, `{term}-{naas_cd}-{n}`. |
| `party_uncertain` | bool | as `party` | The date lies in an uncertainty window of the member (the minutes bound a party change by two dates but do not date it, or a later record contradicts an undocumented stretch). 88,209 legislator turns. |
| `party_uncertain_reason` | string | `party_uncertain` is false and the weak flag is false | Reasons joined by `;`, for example `inferred_change_window`, `possible_unrecorded_membership`, `switch_from_party_unrecorded`, `undated_exit_before_switch`, `contradicted_by_term_party_record`, `contradicted_by_next_term_election_party`, `no_record_after_unreadable_report`. |
| `party_unconfirmed_after_gap` | bool | as `party` | Weak flag. No record of the member follows a plenary whose report items cannot be read. |
| `is_speaker_nonpartisan` | bool | as `party` | The member is the Speaker (의장) and holds the legally required 무소속 status. |
| `party_before_speaker` | string | not a Speaker spell | Party held before the Speaker's 무소속 spell. |
| `ruling_status` | string | see section 10.3 | `ruling`, `opposition` or `independent` on `speech_date`. |
| `ruling_null_reason` | string | `ruling_status` is set, or the turn is not legislator-side | `acting` (office vacant after a removal) or `no_naas_cd` (legislator-side turn not linked). The code also defines `no_date`, `no_party` and `outside_calendar`, which do not occur in a valid release. |
| `presidency_state` | string | the turn date lies outside the president calendar (never in a valid release) | `normal`, `partyless`, `suspended` or `acting` on the turn date (section 10.2). |
| `president` | string | no sitting president (`acting`) | President on the turn date. During a suspension the suspended president. |
| `president_party` | string | `partyless` and `acting` windows | President's formal party on the turn date. |
| `president_last_party` | string | `acting` windows | The president's party on the date, or in a partyless window the party the president held last. It is set in the 2004 suspension of the partyless president as well. |
| `acting_president` | string | `normal` and `partyless` windows | Acting president during a suspension or after a removal. |

### 4.10 Government metadata (government.py)

Section 11 gives the rules.

| Column | Type | Null when | Definition |
|---|---|---|---|
| `admin` | string | never | Administration on the turn date. The president's name, or `권한대행(NAME)` after a removal. During a suspension the suspended president. |
| `admin_ideology` | string | `admin` is an acting government | `Progressive` (김대중, 노무현, 문재인, 이재명) or `Conservative` (이명박, 박근혜, 윤석열). |
| `gov_date_source` | string | no date | Date used, `speech_date` (or `meeting_date` as fallback). |
| `ministry_normalized` | string | the title names no central-government body, or the turn is legislator-side | Central-government organisation named in `speaker_pos`, under the name in force at the time (`보건복지가족부`, `여성부`, `국세청`, `국무총리`). New grouped values are `재외공관` (overseas missions) and `교정기관` (prisons). |
| `ministry_family` | string | no lineage recorded | Rename-lineage key of the organisation (`education`, `health_welfare`, `finance_planning`). |
| `ministry_rule` | string | never | Normalization rule that fired, for example `lexicon`, `hanja+lexicon`, `deputy_pm+lexicon`, `regional_office`, `overseas_mission`, `generic_commission`, `generic_suffix`, `typo_map`, or the reason for no ministry (`legislator`, `no_org`, `non_government`, `local_government`, `military`, `judiciary`, `assembly_body`, `empty`). |
| `minister_panel_id` | string | not linked to a spell or an acting head | `spell_id` of the linked appointment spell of minister-data v2.0.0 (also for a nomination that led to a spell), or `acting_id` of the linked acting-head record. |
| `dual_office` | bool | no spell linked | The linked minister holds an Assembly seat on the speech date. Null for acting heads and for nominations without a spell. 176,959 turns are true. |
| `link_method` | string | the turn is outside the link scope (section 11) | How the link was made (`spell:exact` inside the spell, `spell:buffer` within the buffer days around it, `nomination:hearing`, `acting_head:pm`, `acting_head:lineage`), why it failed (`unlinked:name_not_in_panel`, `unlinked:lineage_unresolved`, `unlinked:vice_minister_title`, `unlinked:lineage_out_of_scope`, `unlinked:person_in_other_lineage`, `unlinked:outside_spell`, `unlinked:outside_hearing`, `unlinked:not_in_acting_heads`, `unlinked:outside_acting_period`, `unlinked:no_name`, `unlinked:no_date`) or why it was blocked (`unlinked:label_inconsistent_in_meeting`, `unlinked:label_confidence_low`, `unlinked:former_title`). |
| `gov_link_name` | string | not linked | Hangul name of the linked spell, nominee or acting head. |
| `minister_spell_id` | string | no spell linked | `spell_id` of the linked spell (`spells.csv`), for spell links and for nominations that led to a spell. |
| `minister_nomination_id` | string | not linked to a nomination | `nomination_id` of the linked nomination (`nominations.csv`). |
| `minister_acting_id` | string | not linked to an acting head | `acting_id` of the linked acting-head record (`acting_heads.csv`). |
| `minister_person_id` | string | not linked, or the linked record has no person | minister-data `person_id` of the linked person. Null for withdrawn or rejected nominations and for acting heads without a person id. |
| `minister_lineage` | string | the title resolves to no office lineage, or the link was blocked | minister-data lineage key of the office the title resolves to on the speech date (`finance`, `pm`, `defense`). It is also set for unlinked turns whose title resolves, so read it together with `link_method`. |

## 5. dyads

One row per pair of numerically adjacent turns (`turn_seq` i and i+1) in one meeting where one turn has role group `legislator` and the other `nonlegislator`, in either order.

Rules:

- Turns of role group `excluded` (committee staff, fused labels, `other`, `unknown`) stay in the sequence and break adjacency.
- The two turns must have the same `sitting_seq`. No pair crosses a sitting or a meeting.
- Turns printed after a meeting-end marker of their sitting are kept and flagged (`any_after_end_marker`). The parameter `dyads.exclude_after_end_marker` (default false) makes them break adjacency instead.
- Chair turns are kept and flagged (`leg_is_chair`). Procedural utterances are kept and flagged (`leg_is_procedural`).
- The dyads of a meeting marked `duplicate_of` are in `duplicate_dyads`, not in dyads.

The dyad file is slim. It holds 39 columns in the order below. Every other turn attribute (`text_raw`, `party_lineage`, `affiliation_raw`, …) joins from the turns table on (`conf_num`, `leg_turn_seq`) or (`conf_num`, `wit_turn_seq`). No column is copied twice.

| Column | Type | From | Definition |
|---|---|---|---|
| `conf_num` | int64 | pair | Meeting. |
| `term` | int16 | meetings | Assembly term. |
| `date` | string | meetings | Meeting date. |
| `hearing_type` | string | meetings | Hearing type. |
| `class_name` | string | meetings | Open API class. |
| `committee_key` | string | meetings | Harmonized committee key. |
| `is_subcommittee` | bool | meetings | Subcommittee meeting. |
| `sitting_seq` | int16 | legislator turn | Sitting of both turns (they never differ). |
| `leg_turn_seq` | int32 | pair | `turn_seq` of the legislator-side turn. |
| `wit_turn_seq` | int32 | pair | `turn_seq` of the non-legislator turn. |
| `direction` | string | pair | `question` when the legislator turn comes first, `answer` otherwise. 5,632,597 and 5,647,010 dyads. |
| `speech_date` | string | legislator turn | `speech_date` of the legislator turn. |
| `leg_naas_cd` | string | legislator turn | `naas_cd`. |
| `leg_name` | string | legislator turn | The linked member's Hangul name (`leg_name_hangul`), else the printed `speaker_name`. |
| `leg_role` | string | legislator turn | `role` (`legislator` or `chair`). |
| `leg_is_chair` | bool | flag | `leg_role` is `chair`. 1,364,210 dyads. |
| `leg_party` | string | legislator turn | `party`. |
| `leg_party_camp` | string | legislator turn | `party_camp`. |
| `leg_ruling_status` | string | legislator turn | `ruling_status`. |
| `presidency_state` | string | legislator turn | `presidency_state` on the legislator turn's date. |
| `leg_seniority` | int16 | legislator turn | `seniority`. |
| `leg_gender` | string | legislator turn | `gender`. |
| `wit_name` | string | non-legislator turn | Printed `speaker_name`. |
| `wit_role` | string | non-legislator turn | `role`. |
| `wit_role_group` | string | non-legislator turn | `role_group` (`nonlegislator`). |
| `wit_title_raw` | string | non-legislator turn | `title_raw`. |
| `wit_ministry_normalized` | string | non-legislator turn | `ministry_normalized`. |
| `wit_minister_panel_id` | string | non-legislator turn | `minister_panel_id`. |
| `wit_dual_office` | bool | non-legislator turn | `dual_office`. |
| `admin` | string | legislator turn | `admin` on the legislator turn's date. |
| `admin_ideology` | string | legislator turn | `admin_ideology`. |
| `leg_text` | string | legislator turn | `text` (spoken text). |
| `wit_text` | string | non-legislator turn | `text` (spoken text). |
| `leg_is_procedural` | bool | flag | The legislator turn's `text` is at most 400 characters and every sentence matches one of the procedural formulas of `dyads.PROCEDURAL_PATTERNS` (recognition of the next questioner, thanks and closing, time management, answer requests, session and vote formulas, vocatives). Fillers such as `예.` count only when the legislator side is the chair. 262,783 dyads. |
| `wit_is_legislator_title` | bool | flag | Sanity flag. The non-legislator position is a legislator title (위원, 의원, 위원장, 소위원장, 의장, 부의장, 간사, Hanja forms). 0 dyads. |
| `any_after_end_marker` | bool | either turn | Either turn is printed after a meeting-end marker. 0 dyads. |
| `any_low_label_confidence` | bool | either turn | Either turn has `label_confidence` `low`. 36 dyads. |
| `any_time_regress` | bool | either turn | Either turn has `time_regress`. 1,428 dyads. |
| `any_label_inconsistent` | bool | either turn | Either turn has `label_inconsistent_in_meeting`. 4,843 dyads. |

A turn whose flag is null counts as false in the `any_` columns.

## 6. Meeting-level tables

All meeting-level tables carry `conf_num`, `source` (`xml` or `hwp`) and `term`. `after_turn_seq` = k places a row after the start of turn k (0 = before the first turn).

### 6.1 agenda

One row per agenda anchor printed in the body.

| Column | Type | Null when | Definition |
|---|---|---|---|
| `ordinal` | int32 | never | Ordinal of the anchor in the meeting. Joins `turns.agenda_ordinal`. |
| `anchor` | string | HWP | XML anchor id (`itemN`). |
| `level` | string | HWP | XML anchor level class (`pl10`, `pl20`, `pl30`). |
| `text` | string | anchor printed without text (39 rows) | Anchor text as printed. |
| `bill_id` | string | no bill link | Bill id of the anchor's link (likms `billId`). XML only. |
| `bill_no` | string | no bill number printed | Bill number (의안번호). XML only. HWP bill numbers are not extracted. |
| `bill_url` | string | no bill link | Link printed with the anchor. |
| `is_continued` | bool | never | The anchor is marked `(계속)`. |
| `after_turn_seq` | int32 | never | Position of the anchor in the turn sequence. |
| `match_rule` | string | never | How the anchor was found. `xml_anchor`, or for HWP `hwp_cover_match`, `hwp_cover_match_indented`, `hwp_numbered`, `hwp_numbered_padded`, `hwp_numbered_run`, `hwp_after_rule`. |

### 6.2 agenda_header

One row per agenda item listed in the header of the minutes (의사일정, 상정된 안건, 부의된 안건, 심사된 안건).

| Column | Type | Null when | Definition |
|---|---|---|---|
| `item_seq` | int32 | never | Position in the header list. |
| `section` | string | never | Header section as printed, for example `상정된 안건`, `審査된案件`, `議事日程`. |
| `head_id` | string | HWP | XML id of the item. |
| `level` | string | not indented | Indentation level class. |
| `num` | string | no number printed | Item number as printed. |
| `text` | string | item printed without text (45 rows) | Item text. |
| `target` | string | HWP, or no link target | XML link target of the item. |
| `page` | int32 | XML, or no page printed | HWP page number printed with the item. |

### 6.3 events

One row per body line that is not part of a speaker turn.

| Column | Type | Null when | Definition |
|---|---|---|---|
| `event_seq` | int32 | never | Position in the meeting. |
| `kind` | string | never | `time` (time marker), `day_rollover`, `note`, `other` (text outside speaker blocks), `appendix_inner` (attendance or vote-name lists between speeches), `table`, `textbox`, `rule`, `sub_cover` (cover of a later sitting), `sitting_head`. |
| `text` | string | line with no printed text (1 row) | Line as printed. |
| `after_turn_seq` | int32 | never | Position in the turn sequence. |
| `within_turn` | bool | never | The line is printed inside a turn (the turn continues after it). |
| `hhmm` | string | not a time marker | Time of the marker, `HH:MM`. |
| `action` | string | no action printed | Action of a time marker as parsed (`개의`, `산회`, `정회`, `감사개시`, `투표종료`). The XML adapter drops a `비공개` prefix here, so use the printed `text` to apply the end-marker rule. |
| `new_date` | string | no date change | Date set by a roll-over or dated marker. |
| `tag` | string | not an XML `other` line | HTML tag of the line. |
| `cls` | string | not an XML `other` line | HTML class of the line. |

### 6.4 footer

One row per line or name of the appendix after the body (attendance lists, officials present, attached documents, report items).

| Column | Type | Null when | Definition |
|---|---|---|---|
| `row_seq` | int32 | never | Position of the row in the meeting's appendix, 1 to n. |
| `section_seq` | int32 | never | Appendix section. |
| `section_title` | string | untitled section | Section title as printed. |
| `group_seq` | int32 | no group | Group within the section. |
| `group_label` | string | no group label | Group label as printed. |
| `item_seq` | int32 | the row is a section or group line | Item within the group. |
| `item_kind` | string | never | `name`, `name_derived` (a name read from a line), `line`, `label_only`, `table_row`, `table_caption`, `org`, `title_only`. |
| `pos` | string | no position | Position printed with the name. |
| `name` | string | not a name row | Person name. |
| `org` | string | no organisation | Organisation printed with the name. |
| `line_text` | string | not a line row | Line as printed. |
| `extra` | string | always in this build | Reserved. |
| `profile_url` | string | no member profile link | Member profile link (XML). |
| `table_seq` | int32 | not a table row | Table within the section. |
| `row_idx` | int32 | not a table row | Row within the table. |

(`conf_num`, `row_seq`) is the key of the footer.

### 6.5 attendance

One row per person listed in an attendance section of the appendix.

| Column | Type | Null when | Definition |
|---|---|---|---|
| `row_seq` | int32 | never | Position of the row in the meeting's attendance lists, 1 to n. |
| `duplicate_of_row_seq` | int32 | the row is the first of its content | `row_seq` of the first row of the meeting with identical content. Such rows are kept and counted, never removed. |
| `section_seq` | int32 | never | Appendix section. |
| `section_title` | string | never | Section title as printed. |
| `category` | string | never | `present`, `excused`, `official_travel`, `seated_at_opening`, `seated_at_adjournment`, `seated_at_resumption`, `seated_during_meeting`, `present_nonmember`, `absent_listed`, `government`, `cabinet`, `agency_attendee`, `committee_staff`, `assembly_staff`, `witness`, `reference`, `statement`, `nominee`, `advisor`, `other_attendee`. |
| `n_reported` | int32 | no count printed | Count printed in the section title. |
| `group_label` | string | always in this build | Reserved. |
| `item_kind` | string | never | `name`, `name_derived`, `line`, `line_name`, `note`. |
| `org` | string | no organisation | Organisation printed with the name. |
| `pos` | string | no position | Position printed with the name. |
| `name` | string | `line` and `note` rows | Person name. |
| `line_text` | string | `name` and `name_derived` rows | Line as printed (rows `line`, `line_name`, `note`). |
| `profile_url` | string | no profile link | Member profile link (XML). |

(`conf_num`, `row_seq`) is the key. 176 rows repeat an earlier row of the same meeting (`duplicate_of_row_seq`).

### 6.6 rollcall and rollcall_groups

A recorded vote prints groups of names (찬성, 반대, 기권) under a vote title. `rollcall_groups` has one row per group with the printed count and the number of names read. `rollcall` has one row per name.

| Column | Table | Type | Definition |
|---|---|---|---|
| `vote_seq` | both | int32 | Vote within the meeting. |
| `vote_title` | both | string | Vote title as printed. |
| `vote_section_title` | rollcall | string | Section title of the vote. |
| `vote_group` | both | string | `찬성`, `반대`, `기권`, and in rollcall_groups also `투표` (the total). |
| `group_label` | both | string | Group label as printed. |
| `n_reported` | both | int32 | Count printed for the group. |
| `n_names` | rollcall_groups | int32 | Names read for the group. |
| `name_seq` | rollcall | int32 | Position of the name in the group. |
| `name` | rollcall | string | Name as printed. |
| `pos` | rollcall | string | Reserved, always null in this build. |
| `profile_url` | rollcall | string | Reserved, always null in this build. |
| `method` | both | string | `xml_footer`, `xml_footer_label_rows`, `hwp_lines`. |
| `note` | rollcall_groups | string | Correction note printed with the group, for example a member's mistaken vote. |

### 6.7 duplicate_meetings

One row per pair of meetings that the duplicate checks of `validate.py` flag on the built turns (section 12).

| Column | Type | Definition |
|---|---|---|
| `a`, `b` | int64 | The two meetings (`a` < `b`). |
| `kind` | string | `identical` (equal whole-meeting normalized text), `near_identical` (same number of turns, and at least 0.9 of them with equal normalized text) or `partial`. |
| `found_by` | list<string> | Checks that flagged the pair. `dup_meeting_text`, `dup_long_turn`, `dup_shingle`. |
| `long_turn_containment`, `shingle_containment` | double | Containment measured by the two near-duplicate checks. |
| `n_turns_a`, `n_turns_b` | int64 | Turns of each meeting. |
| `equal_turn_share` | double | Share of turns with equal normalized text, when both meetings have the same number of turns. |
| `a_date_match`, `a_sitting_match`, `a_committee_match`, `b_date_match`, `b_sitting_match`, `b_committee_match` | bool | Whether the printed date, sitting and committee of each meeting equal its Open API row. Null when not comparable. |
| `kept`, `duplicate` | int64 | For an identical pair, the copy kept and the copy marked `duplicate_of`. |
| `decision` | string | `duplicate_of`, `kept_flagged`, or `kept_flagged_other_is_duplicate`. |
| `duplicate_basis` | string | As `meetings.duplicate_basis` for the duplicate copy. |

## 7. Crosswalk tables

[MIGRATION_v9_to_v10.md](MIGRATION_v9_to_v10.md) explains how to use them.

### 7.1 crosswalk_meetings

One row per v9 meeting (16,830 rows) plus one row per v10 meeting whose transcript no v9 meeting carries (`relation` = `v10_only`, 10,042 rows).

| Column | Type | Definition |
|---|---|---|
| `v9_meeting_id` | string | v9 `meeting_id`. Null for `v10_only` rows. |
| `v9_source`, `v9_term`, `v9_hearing_type`, `v9_committee`, `v9_committee_key`, `v9_date`, `v9_n_speeches` | | v9 attributes of the meeting. |
| `v9_match_method` | string | How the v9 label was matched to a CONFER_NUM (`H1_confid_numeric_date_ok` = CONF_ID without its leading zero with an equal date, `H2_confer_num_date_ok` = viewer id, and others). |
| `conf_num`, `conf_id` | | The v10 meeting the v9 label refers to. |
| `content_conf_num`, `content_conf_id` | | The v10 meeting whose transcript the v9 rows actually carry. |
| `relation` | string | `same` (the v9 transcript is the labelled meeting, 15,837), `v9_wrong_content` (it belongs to another meeting, 974), `duplicate` (a second v9 copy of a transcript another v9 meeting carries as `same`, 19), `v9_only` (found in no v10 meeting), `v10_only`. |
| `relation_basis` | string | What decided the relation, for example `content:label_share>=0.5`, `content:other_meeting_share>=0.5`, `audit_R_D4:...`, `id_label`. |
| `content_verified` | bool | v10 text was compared. |
| `content_overlap` | string | Share of the v9 transcript found in the content meeting. `full` (at least 0.5), `v10_in_v9`, `partial` (0.1 to 0.5), `none`. |
| `content_share_fwd`, `content_share_rev` | double | Sentence-fingerprint containment, v9 in v10 and v10 in v9. |
| `is_second_copy`, `duplicate_of_v9_meeting_id`, `n_v9_carriers` | | When several v9 meetings carry one transcript, one is primary and the others point to it. 607 second copies. |
| `in_universe` | bool | The target meeting is in the meeting universe. |
| `audit_verdict`, `audit_v8_mislabel_flag`, `audit_agrees`, `label_match` | | Verdicts of the v9 audit and agreement with the content evidence. |
| `fp_n`, `fp_share_label`, `fp_best_conf_num`, `fp_share_best`, `fp_share_second`, `fp_share_id_as_confer_num` | | Fingerprint evidence. |
| `v10_status`, `v10_source`, `v10_n_turns`, `v10_class_name`, `v10_hearing_type`, `v10_committee_raw`, `v10_date` | | State and attributes of the v10 meeting. |
| `v10_duplicate_of` | | For a v10 meeting that is the removed copy of an identical-text pair, the `conf_num` of the copy that was kept. |
| `turn_alignment`, `n_v9_rows_linked`, `n_v9_rows_unmatched`, `n_v10_turns_unmatched` | | Result of the turn-level alignment. |

### 7.2 crosswalk_turns

One row per link between a v9 speech row and a v10 turn of an aligned meeting, plus one row for each unmatched row on either side. Every v9 row and every v10 turn of an aligned meeting appears, and the alignment keeps order.

| Column | Type | Definition |
|---|---|---|
| `v9_meeting_id` | string | v9 meeting. |
| `v9_speech_order` | string | v9 `speech_order` as stored. Null for `v10_unmatched`. |
| `v9_order_num` | int32 | The same as an integer. |
| `conf_num` | int64 | v10 meeting. |
| `turn_seq` | int32 | v10 turn. Null for `v9_unmatched`. |
| `match_type` | string | `exact` (whitespace-free texts equal, 8,056,412), `normalized` (normalized texts equal), `similar` (similarity at least 0.6), `split` (two v9 rows, one v10 turn), `merge` (one v9 row, two v10 turns), `v9_unmatched`, `v10_unmatched`. |
| `similarity` | double | Text similarity of the link. |
| `block_id` | int32 | Gap block of a DP-aligned link. |

## 8. Role taxonomy and the v9 crosswalk

### 8.1 Roles

v10 keeps the 33 roles of v9 and their three groups. `unknown` is used only for a turn whose label is empty.

| Group | Role | Turns | Titles and rule |
|---|---|---|---|
| legislator | `legislator` | 6,918,235 | Member titles 위원, 의원, 委員, 議員. Presiding titles outside their own body (a 소위원장 in a full committee meeting, a committee chair reporting in plenary or in another committee). A viewer member id decides for member titles and bare names. |
| legislator | `chair` | 2,118,525 | A presiding title in its own body. 위원장 and acting forms. 소위원장 and 조정위원장 in a subcommittee or 안건조정위원회 meeting. 의장 and 부의장 in plenary and the 전원위원회. 국정감사 반장. A named NA committee chair in its own committee. |
| nonlegislator | `minister` | 1,151,689 | 장관, and a dual-office minister who is a member. |
| nonlegislator | `minister_acting` | 20,984 | 장관직무대행, 장관직무대리. |
| nonlegislator | `minister_nominee` | 138,743 | 장관후보자. |
| nonlegislator | `vice_minister` | 675,615 | 차관. |
| nonlegislator | `prime_minister` | 93,982 | A sitting 국무총리, 국무총리직무대행, 대통령권한대행국무총리, 국무총리서리. |
| nonlegislator | `agency_head` | 436,546 | 청장, 검사장, 지청장, 지방청장. |
| nonlegislator | `senior_bureaucrat` | 771,626 | 실장, 국장, 본부장, 국무총리실장, 비서실장, 세무서장, 차장검사. |
| nonlegislator | `mid_bureaucrat` | 109,968 | 정책관, 감사관, 과장. |
| nonlegislator | `witness` | 257,759 | 증인, and a witness's counsel printed as 증인(X)변호인. |
| nonlegislator | `testifier` | 105,428 | 진술인. |
| nonlegislator | `expert_witness` | 69,472 | 참고인. |
| nonlegislator | `nominee` | 153,524 | 후보자 for a non-cabinet office. |
| nonlegislator | `public_corp_head` | 443,337 | 사장, 은행장, and 이사장 of a 공단, 공사, 기금, 은행, 거래소 or 금고. |
| nonlegislator | `org_head` | 269,141 | 원장, 회장, and 이사장 of a 재단, 진흥회, 연구회, 공제회 or 학교법인. Trade-union chairs. …기술원장. |
| nonlegislator | `research_head` | 40,545 | 연구원장, 과학원장. |
| nonlegislator | `financial_regulator` | 31,839 | 금융감독원, 금융위원회 and 금융감독위원회 staff, 금융통화위원. |
| nonlegislator | `broadcasting` | 8,824 | Officials of a broadcasting body. |
| nonlegislator | `cooperative_head` | 16,563 | Heads of cooperatives. |
| nonlegislator | `local_gov_head` | 139,980 | 시장, 도지사, 교육감. |
| nonlegislator | `military` | 56,109 | 사령관, 참모총장, defence attachés. |
| nonlegislator | `police` | 29,257 | Police and fire officials. |
| nonlegislator | `audit_official` | 45,756 | 감사원. |
| nonlegislator | `election_official` | 51,116 | 선거관리위원회. |
| nonlegislator | `constitutional_court` | 18,504 | 헌법재판소. |
| nonlegislator | `assembly_official` | 29,516 | 국회사무처 officials. |
| nonlegislator | `independent_official` | 290,863 | Government commissions and independent agencies, including a `{X}위원장` whose X is not an NA committee, and 국가인권위원회 staff. |
| nonlegislator | `private_sector` | 8,668 | Titles marked ㈜ or 주식회사, national arts company directors. |
| nonlegislator | `cultural_institution_head` | 7,540 | Heads of 과학관, 전당, 극장 and cultural institutions. |
| nonlegislator | `other_official` | 267,653 | Other government officials, 법원장, embassy staff below 공사. |
| excluded | `committee_staff` | 329,948 | 전문위원, 수석전문위원, 입법조사관, 입법심의관. |
| excluded | `other` | 6,927 | Titles no rule recognises. |
| excluded | `unknown` | 1 | Empty label. |

Classification runs in four steps. Label repair converts Hanja titles to Hangul and repairs fused, swapped or mistyped labels (`pos_fix`). The legislator stage applies ordered rules for member and presiding titles. The non-legislator stage applies an ordered rule table to each 겸 segment of the title after acting and designate suffixes are removed. The v9 compatibility step rebuilds the v9 answer (`role_v9_compat`).

### 8.2 Changes from v9

These changes are deliberate. The first four rows fix v9 errors verified on all v9 rows. The other rows change a v9 definition and give the number of v9 XLSX-era rows (상임위원회 and 국정감사, 8,597,178 rows) that move.

| Title | v9 role | v10 role | v9 rows |
|---|---|---|---|
| 소위원장 in the v8 예결 rows | other_official | chair when presiding, legislator when reporting | 52,207 |
| Hanja 委員, 議長 | other_official | legislator or chair | 24,987 |
| 전문위원 | legislator | committee_staff | 23,439 |
| A sitting 국무총리 | other_official | prime_minister | 40,883 |
| 소위원장 in a full committee meeting (reporting) | chair | legislator | |
| 국무총리실장, 비서실장 | prime_minister | senior_bureaucrat | 10,410 |
| 이사장 of a 재단 or similar body | public_corp_head | org_head | 59,324 |
| 국정감사 반장 | legislator | chair | 34,788 |
| 검사장 | public_corp_head | agency_head | 24,796 |
| 법원장 | org_head | other_official | 17,369 |
| Embassy staff below 공사 | senior_bureaucrat | other_official | 3,730 |
| Company-marked titles | various | private_sector | 3,035 |

Role-group moves on the v9 XLSX-era rows are 139 rows from legislator to nonlegislator, 98 from nonlegislator to legislator, 2,764 from nonlegislator to excluded and 4,384 from excluded to nonlegislator.

### 8.3 `role_v9_compat`

`role_v9_compat` is what the v9 cascade gives for the same printed title. It first looks the title up in the v9 speaker-role table (`role_v9_compat_src` = `lookup`) and otherwise runs the v9 rules (`chain`). On the v9 XLSX-era rows the chain alone reproduces the v9 role for 8,585,360 of 8,597,178 rows (0.998625). In the release, `role` differs from `role_v9_compat` on 415,804 turns. A v9 analysis can be re-run on v10 text with v9 roles by using `role_v9_compat`.

## 9. Hearing type, class, subcommittee and committee key

### 9.1 `hearing_type`

`hearing_type` equals `class_name`, except that a `특별위원회` meeting whose committee name contains `인사청문특별위원회` gets `인사청문특별위원회`. The six v9 values keep their meaning. `특별위원회` (special committees other than confirmation committees) and `전원위원회` are new. Subcommittees keep the hearing type of their parent.

| `class_name` | `hearing_type` | Meetings | Of which subcommittee meetings |
|---|---|---|---|
| 상임위원회 | 상임위원회 | 16,697 | 6,273 |
| 국정감사 | 국정감사 | 4,960 | 0 |
| 특별위원회 | 특별위원회 | 1,798 | 573 |
| 특별위원회 | 인사청문특별위원회 | 363 | 0 |
| 예산결산특별위원회 | 예산결산특별위원회 | 935 | 416 |
| 국정조사 | 국정조사 | 250 | 7 |
| 국회본회의 | 국회본회의 | 1,253 | 0 |
| 전원위원회 | 전원위원회 | 8 | 0 |

### 9.2 `is_subcommittee`

`is_subcommittee` is true when the Open API committee name marks a subcommittee, or when the meeting has a subcommittee name and is not an 안건조정위원회. 안건조정위원회 meetings are flagged `is_agenda_adjustment` (65 meetings) and keep their name in `subcommittee`. Their `is_subcommittee` is false unless the Open API marks the meeting as a subcommittee, which it does for 1 of them (46926). For 국정조사 meetings the Open API sometimes puts a document label (`…조사록`) in the subcommittee slot. It goes to `doc_label`, not `subcommittee`. For 국정감사 meetings the team (반) goes to `audit_team`. 7,269 meetings are subcommittee meetings.

### 9.3 `committee_key`

`committee_key` harmonizes committee names across renames. The v9 map is applied first (`legacy_map`, `legacy_hearing_type`). v10 adds `special_committee` for the special committees other than confirmation committees (one key for all of them, the name stays in `committee_raw`), `committee_of_whole` for the 전원위원회, and lineage keys for the committees renamed in 2025 (`v10_new_standing`). 26 keys are used.

| Committee name (2025) | `committee_key` |
|---|---|
| 기후에너지환경노동위원회 | `environment_labor` |
| 재정경제기획위원회 | `finance` |
| 성평등가족위원회 | `gender_family` |

### 9.4 Confirmation hearings

`is_confirmation_hearing` is true for every `인사청문특별위원회` meeting (`special_committee`) and for any meeting whose own agenda text is a nominee hearing, `…후보자(NAME) 인사청문회` (`source_agenda_text`). Standing committee meetings whose own agenda is a nominee hearing are flagged this way. The Open API 인사청문회 list and agenda strings are kept as columns only, because both carry other meetings' items for some rows.

## 10. Party, ruling status and presidency state

The rules follow the researcher decisions of 2026-09-25 and 2026-09-26.

### 10.1 Party on the speech date

Each member-term has dated party spells built from the 【보고사항】 report items of the plenary minutes (교섭단체 rosters, joins, expulsions, party switches, seat registrations and succession, resignations, caucus formation, dissolution and renaming, and notices). Members absent from the term's start roster start from their election party carried through the party lineage. Renames and mergers come from `party_lineage.csv` and the Assembly notices, whichever date is earlier. Satellite parties map to their main party in `party_camp` before the merger. The Speaker's 무소속 spell is kept (`is_speaker_nonpartisan`). A turn gets the party of the spell that covers its speech date.

### 10.2 `presidency_state`

`presidency_state` is set for every turn from `president_calendar.csv` on the turn date.

| Value | Meaning | Windows in the data | Turns |
|---|---|---|---|
| `normal` | The president exercises the powers and has a party. | all dates not listed below | 13,849,127 |
| `partyless` | The president exercises the powers but has left the party. | 2002-05-06 to 2003-02-24 (김대중), 2003-09-29 to 2004-03-11 and 2004-05-15 to 2004-05-19, and 2007-02-28 to 2008-02-24 (노무현) | 972,243 |
| `suspended` | Impeachment suspension. An acting president exercises the powers. | 2004-03-12 to 2004-05-14 (노무현), 2016-12-09 to 2017-03-09 (박근혜), 2024-12-14 to 2025-04-03 (윤석열) | 230,203 |
| `acting` | The office is vacant after a removal. An acting president exercises the powers. | 2017-03-10 to 2017-05-09, 2025-04-04 to 2025-06-03 | 62,610 |

- `suspended` takes precedence over `partyless`. The 2004 suspension of 노무현 falls inside his first partyless window, so it is `suspended`, and `president_last_party` is still set.
- The partyless window of 2007 starts on 2007-02-28, when 노무현 submitted his exit (탈당계) from 열린우리당. He had declared the exit on 2007-02-22.
- The calendar has no `vacant` value. Every row after a removal names an acting president.
- On 2025-06-04 the term of 이재명 began at 06:21. All turns of that date are `normal`.
- Impeachment-vote days are coded by date, so the turns of the vote day timed before the vote are `suspended` as well.

### 10.3 `ruling_status`

`ruling_status` is set for legislator-side turns linked to a legislator. For each turn, in order:

1. `acting` gives null (`ruling_null_reason` = `acting`). This includes 무소속 members.
2. A legislator-side turn not linked to a legislator gives null (`ruling_null_reason` = `no_naas_cd`).
3. `무소속` gives `independent`. This includes the Speaker's 무소속 spell.
4. Otherwise `ruling` when `party_camp` is the reference party on that date, else `opposition`. Satellite parties count through their main party (`party_camp`), and renames and mergers through the party lineage.

The reference party is the president's party in `normal` windows, the suspended president's party in `suspended` windows, and in `partyless` windows the president's most recent party (`president_last_party`) together with its lineage successors on the speech date. For example, in the 2007 window 열린우리당 counts as ruling, then 대통합민주신당 from 2007-08-20 and 통합민주당 from 2008-02-17.

| `ruling_status` of legislator-side turns | Turns |
|---|---|
| `ruling` | 3,901,054 |
| `opposition` | 4,939,041 |
| `independent` | 157,411 |
| null | 39,254 |

The parties coded `ruling` in the partyless windows are 대통합민주신당, 새천년민주당, 열린우리당, 통합민주당.

To code the partyless windows differently (for example every party `opposition`, or null as in earlier drafts), recode on `presidency_state` = `partyless` with `president_last_party`.

## 11. Government metadata

- `admin` and `admin_ideology` come from the speech date and the president calendar, never from the minister panel. A suspended president stays the administration (`suspended_admin` = `president`, config.yaml). After a removal the government is `권한대행(NAME)` with null ideology.
- `ministry_normalized` is read from `speaker_pos` alone. Hanja is converted, acting prefixes and the 부총리겸 prefix are removed, and the longest lexicon prefix, regional office, overseas mission, generic suffix or generic commission rule is applied, in that order. Names stay as they were at the time. `ministry_family` carries the lineage.
- Government turns are linked to minister-data v2.0.0 (`government.panel` = `v2`), a snapshot of its appointment spells, nominations, acting-head records, ministry aliases and name variants (PIPELINE.md section 6). The link scope is the roles `minister`, `minister_acting`, `minister_nominee` and `prime_minister`, and the role `nominee` when the printed title names a cabinet office on the date (for example `국무총리후보자`). A link needs the person, the office lineage and the date. There are no date-free fallbacks.
- Person. `speaker_name` (NFKC, whitespace removed) equals the Hangul or Hanja name of the spell, or a name variant that minister-data records for the lineage and date. Nominations and acting heads are matched by name.
- Lineage. The printed title (`speaker_pos`, then the title without 후보자, 직무대행 or 직무대리), else `ministry_normalized`, is looked up in the ministry aliases valid on the date. A vice-minister title or a lineage outside the scope of minister-data never links. `prime_minister` turns and titles that start with 국무총리 or 國務總理 are lineage `pm`.
- Three gates block a link. They are a title that disagrees with the person's majority title in the meeting (`unlinked:label_inconsistent_in_meeting`), a label rated `low` (`unlinked:label_confidence_low`) and a former-office title (`unlinked:former_title`).

| Role | Linked to | Window | `link_method` |
|---|---|---|---|
| minister, prime_minister | appointment spell of the person and lineage | spell start to spell end, or to 2026-09-24 for a spell still open | `spell:exact` |
| minister, prime_minister | appointment spell of the person and lineage | 1 day before the start or after the end (`spell_buffer_days`) | `spell:buffer` |
| minister_nominee, and nominee with a cabinet title | nomination of the person and lineage | a hearing date within 1 day of the speech date | `nomination:hearing` |
| prime_minister titled 직무대행 or 직무대리 | acting-head record acting for the prime minister | the acting period | `acting_head:pm` |
| minister_acting | acting-head record of the lineage | the acting period | `acting_head:lineage` |

An acting head is never linked to an appointment spell. The acting-head records of minister-data cover the prime minister's office systematically and other ministries only incidentally. `dual_office` is set on the speech date from the seat dates of the linked spell.

1,151,503 of 1,151,689 minister turns, all 138,743 minister_nominee turns (126,811 of them through a nomination that led to a spell), all 93,982 prime_minister turns (91,384 to a spell and 2,598 acting prime-minister turns to an acting-head record), 19,760 of 20,984 minister_acting turns and 17,724 of 17,735 cabinet-title nominee turns are linked. No link in the release falls in the buffer days (`spell:buffer`).

`government.panel` = `legacy_296` restores the earlier 296-row panel with its tenure, buffer and nominee windows (`link_method` `tenure:…`, `buffer:…`, `nominee:…`, `nominee_in_tenure:…`), which reproduces the numbers of builds before 2026-09-28. The five `minister_*` id columns are null in that mode.

## 12. Duplicate meetings

Meeting pairs flagged by the three duplicate checks (whole-meeting text, long turns, text shingles) are classified and resolved before enrichment.

| Kind | Rule | Treatment | Pairs |
|---|---|---|---|
| `identical` | The normalized text of the whole meeting is equal. | One copy is kept. The others get `duplicate_of` and `duplicate_basis`, and their turns and dyads go to `duplicate_turns` and `duplicate_dyads`. | 2 |
| `near_identical` | Same number of turns, and at least 0.9 of them with equal normalized text. | Both kept, flagged in `overlap_with` and `overlap_kinds`. | 2 |
| `partial` | Any other flagged pair. | Both kept, flagged in `overlap_with` and `overlap_kinds`. | 1 |

The copy kept from an identical group is the one with the fewest mismatches between its printed date, sitting and committee and its Open API row, then the most matches, then the lower `conf_num`. 2 meetings are marked `duplicate_of` and 6 meetings carry an overlap flag. `duplicate_meetings_v10.parquet` records every pair with its evidence.

In the release the whole-meeting text check found 2 identical pairs (35218 and 35291, 32740 and 32864). 32864 and 35218 are the removed copies. The near-duplicate checks also flagged 31578 and 31793 and 34499 and 34513, and 43313 and 43536 overlap partly. These are kept and flagged. The pair 42004 and 42009 found in the 2026-09-26 build is gone. The viewer XML of 42009 carried the minutes of 42004, so 42009 is now built from its HWP record (source_reason `override:xml_wrong_meeting`).

## 13. Unicode and NULL policy

### 13.1 Text is kept as printed

- `text_raw`, `text`, labels, names and every other text column are stored as the source prints them. They are not Unicode-normalized.
- 102,250 turns have a `text_raw` that is not in NFC form.
- CJK compatibility ideographs (U+F900 to U+FAFF) occur in names. 754,285 turns have one in `speaker_name`. For example 理 is printed as U+F9E4.
- The middle-dot separator differs by source. U+2024 (․) occurs in 623,233 turns and U+318D (ㆍ) in 18,787 turns. The v9 XLSX rows used U+318D.
- Matching inside the pipeline (names, titles, parties) folds compatibility ideographs and applies NFKC. Categorical columns (`party`, `role`, `ruling_status`, `presidency_state`, `ministry_normalized`, `admin`) are NFKC-stable and have no stray whitespace.
- To compare texts across sources, apply NFKC and unify the separators first.

### 13.2 Missing values

- Missing values are nulls. The parser tables store an empty or whitespace-only string as null in every string column (build_turns 1.8). Identifier, label, party and role columns never hold an empty string or the string `nan`.
- As a result, `text` is null when a turn holds only stage directions (3,121 turns) or has no `text_raw`, `text_raw` is null when a turn has no text outside its label (21 turns), and `agenda_text`, `agenda_top_text`, `footer.pos` and `attendance.pos` are null when nothing is printed.
- Columns added by the enrichment modules follow the same rule. `pos_hangul`, for example, is null in the 204 turns whose `title_raw` is null.
- List columns (`stage_kinds`, `stage_texts`, `inline_stage_parens`) are empty lists, not null, when there is nothing to list.
- No string column of any release table holds an empty or whitespace-only string, `footer.name` and `attendance.name` included.

## Known limitations

Counts are from the release tables unless a comment names another file.

### Text and segmentation

- **Turns after a meeting-end marker.** 2 turns in 2 meetings are printed after a meeting-end marker of their sitting, and 463 turns after the last end marker of the document. They are appended records (for example a video-call transcript after `감사종료`) or text the minutes print after the close. They are kept and flagged, and 0 dyads contain such a turn. 1,128 turns are in a later sitting of a multi-sitting document.
- **Speech before the meeting date.** In 4 meetings some turns are dated before the Open API meeting date. They are 57212 (opened at 23:56 on the day before the API date), 30775 (the API date differs from the printed date), 42688 (an HWP file that appends the record of an earlier address) and 30742 (an HWP file whose later cover page prints an earlier date, 2 turns). 2 meetings have `date_end` before `date`.
- **Label repeated in the text.** Some sources print the speaker label again at the start of the speech. It is removed from `text` and kept in `text_raw` (25,121 turns, `text_label_prefix_stripped`). An audit of the build of 2026-09-26 put an upper bound of 25,472 turns on this, 16,066 of them in 480 16th Assembly XML meetings.
- **Fused two-speaker labels.** 932 turns have a label that contains a second speaker marker (`label_fused`). The text may belong to either speaker, so these turns are coded `other` and excluded from dyads. In the build of 2026-09-26 such labels put a staff title on the legislator side 13 times (`전문위원 김종현◯소위원장 이종걸`).
- **Titles that disagree within a meeting.** 3,030 turns print a title that disagrees with the person's majority title in the meeting. They keep the printed role, are flagged, and are not linked to a minister-panel row.
- **Speech outside speaker blocks.** Some XML pages print speech paragraphs outside the speaker blocks. They are kept as events of kind `other` (9,798 rows). Meeting 24614 has 674 such rows (75,450 non-whitespace characters). In meeting 28769 the turns hold 1,600 non-whitespace characters and such rows 7,716. Both meetings are built from viewer XML, because the cross-check against the HWP record did not flag them.
- **Truncated or wrong viewer pages.** The viewer page of meeting 24997 holds 1 turn of 126 characters, while the printed record and the v9 XLSX rows of the same meeting (v9 29668) hold 130. The XML-vs-HWP cross-check sends 24997 and the other meetings whose viewer XML is incomplete or carries another meeting's minutes to their HWP files (26 meetings, `source_reason` starting `override:`). A viewer page that is incomplete by less than the cross-check's thresholds stays the source.
- **Weak HWP labels.** 1,128 turns have a `medium` and 968 a `low` label confidence.
- **HWP fields.** HWP files print no member id and no constituency, so `speaker_mem_id` and `speaker_area` are null for HWP turns. Agenda bill numbers are not extracted from HWP files. The HWP clock goes backwards in 2,016 turns (`time_regress`).

### Speakers, roles and links

- **Unlinked legislator turns.** 239 legislator-side turns have no `naas_cd`. In the evaluation of the legislators component the largest groups were fused Hanja labels of government commission heads placed on the legislator side (`中央勞動委員長林鍾律`), labels with a title and a surname only (`人事聽聞特別委員長 李`), bare titles (`議長`, `小委員長`) and single typos.
- **Links of low confidence.** 560 linked turns have `id_confidence` `low` and 314,341 `medium`.
- **Bare 위원장 in a subcommittee meeting.** A bare 위원장 in a subcommittee meeting is coded `chair` (`role_rule` `leg.chair.committee.in_subcommittee`, 643 turns). Every such meeting also has a presiding 소위원장.
- **Witness side.** 0 dyads have a legislator title on the non-legislator side.
- **Procedural flag.** The precision of `leg_is_procedural` was checked on 200 v9 XLSX-era turns labelled by an agent, not by the researcher. Strict precision was 0.975 (195 of 200). Recall is not measured. The frozen patterns miss closing formulas such as `회의를 마치겠습니다`.

### Party and ruling status

- **Source gaps in the party record.** The 2003-09-26 plenary page of the 16th Assembly (26536) has empty report items for the 통합신당 split. The 12 plenary meetings whose view page is `no_xml` (3 in the 17th and 9 in the 20th Assembly) are read from their HWP files, so every plenary meeting of the Open API lists has a readable record. The 2007-08-08 formation of 대통합민주신당 has no member roster in the minutes. Spells in these stretches are flagged. 88,209 legislator turns have `party_uncertain` and 115,700 have the weak flag `party_unconfirmed_after_gap`.
- **Undated moves.** The move of 박영순 and the exit of 설훈 have no dated source.
- **Calendar sources.** Presidents' party exits and joins and the acting-president boundaries rest partly on saved Wikipedia text. The 2007 exit of 노무현 is dated by his formal exit on 2007-02-28, from a government broadcaster's report (section 10.2).
- **Individual exceptions.** The 22nd Assembly start party of 용혜인, 정혜경, 전종덕 and 한창민 comes from Wikipedia-sourced lineage exceptions and differs from their 의원이력 election party.

### Government links

- **Minister links.** Government links use minister-data v2.0.0 (section 11). 186 of 1,151,689 minister turns are not linked. 168 of them are blocked by a gate (inconsistent title, low label confidence or former title), 13 print no name, and 5 fail on the name, lineage or spell dates.
- **Acting ministers.** The acting-head records of minister-data cover the prime minister's office systematically and other ministries only incidentally. 1,224 of 20,984 minister_acting turns are not linked, 655 because no acting-head record names the person for the lineage and 569 because the date lies outside the recorded acting period.
- **Prime-minister nominees of the 16th Assembly.** 4,193 turns in 8 confirmation hearings of prime-minister nominees (2000 to 2002) print the title 公職候補者, which names no office. They keep the role `nominee` and are not linked (`link_method` null).

### Meetings and tables

- **Small meetings.** The two near-duplicate checks cannot see meetings below their size thresholds (1,003 meetings). The whole-meeting check catches verbatim copies of any size, but not near copies of small meetings.
- **Session type spelling.** `session_type_raw` is printed Hangul in XML and often Hanja in HWP, with two separator forms. `session_type` unifies them and is null when nothing recognisable is printed.
- **Attendance.** 176 attendance rows repeat an earlier row of the same meeting. They are kept and point to the first one (`duplicate_of_row_seq`).
- **Reserved columns.** `footer.extra`, `attendance.group_label`, `rollcall.pos` and `rollcall.profile_url` are always null in this release. `turns.source_member_id` and `turns.source_speech_order` are null as well (section 4.6).
- **Agenda adjustment committees.** 2 안건조정위원회 meetings (47690, 47691) are in the Open API subcommittee list but have `is_subcommittee` false, following the rule of section 9.2.
- **v9 ids.** A `v9_meeting_id` or the digits of a `conf_id` can equal the `conf_num` of a different meeting. Join v9 data through `crosswalk_meetings` only.
