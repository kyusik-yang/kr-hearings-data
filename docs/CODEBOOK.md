# Codebook

Korean National Assembly Hearings Dataset, v8.

## Overview

| | Speeches | Dyads |
|---|---|---|
| Rows | 9,906,444 | 7,894,147 |
| Columns | 24 | 17 |
| Period | 2000-06-01 to 2025-07-21 | 2000-06-01 to 2025-07-21 |
| Assembly terms | 16th - 22nd | same |
| File | `all_speeches_16_22_v8.parquet` | `dyads_16_22_v8.parquet` |
| Compression | zstd | zstd |

### What changed in v6/v7/v8

v6 added 42 인사청문특별위원회 (confirmation hearing special committee) meetings (32,253 speeches) scraped from HTML transcripts. v7 added 228 more meetings (111,348 speeches) parsed from official PDF transcripts downloaded via the VCONFDETAIL Open API. Legislator metadata for v7 gap-fill speeches was enriched using mp_metadata (party match rate: 99.9% for legislators). Hanja names from 16th-17th Assembly PDFs were converted using the National Assembly member API (966 entries) and the assemblykor R package.

v8 added three new hearing types: 국정조사 (parliamentary investigation, 191 meetings), 예산결산특별위원회 (budget special committee, 832 meetings), and 국회본회의 (plenary session, 1,058 meetings), totaling 2,081 new meetings and 1,165,665 speeches. Source data was collected via hybrid XML viewer + PDF parsing from the National Assembly record system.

| hearing_type | Meetings | Speeches | Source |
|-------------|----------|----------|--------|
| 상임위원회 | 9,674 | 3,847,765 | v5 XLSX parsing |
| 국정감사 | 4,805 | 4,749,413 | v5 XLSX parsing |
| 인사청문특별위원회 | 270 | 143,601 | v6 HTML + v7 PDF |
| 예산결산특별위원회 | 832 | 647,589 | v8 XML viewer + PDF |
| 국회본회의 | 1,058 | 396,609 | v8 XML viewer + PDF |
| 국정조사 | 191 | 121,467 | v8 XML viewer + PDF |

## 1. Speeches dataset

### 1.1 Unit of observation

One row = one speech act by one speaker within a committee meeting. A speech act is a continuous segment of text attributed to a single speaker in the official transcript.

### 1.2 Column definitions

#### Meeting identifiers

| Column | Type | Null | Unique | Description |
|--------|------|------|--------|-------------|
| `meeting_id` | str | 0 | 14,749 | Unique meeting identifier. For v5/v6 data, from the source XLSX system. For v7 gap-fill (인사청문특별위원회), equals the VCONFDETAIL CONF_ID. Stable across versions. |
| `term` | int64 | 0 | 7 | Assembly term number. Values: 16, 17, 18, 19, 20, 21, 22. |
| `committee` | str | 0 | 94 | Original committee name as recorded in the source XLSX. Reflects historical reorganizations. |
| `committee_key` | str | 0 | 20 | Harmonized committee key. Maps 94 raw names to 20 stable categories. See Section 3. |
| `hearing_type` | str | 0 | 6 | `상임위원회` (standing committee), `국정감사` (national audit), `인사청문특별위원회` (confirmation hearing, v6/v7), `예산결산특별위원회` (budget special committee, v8), `국회본회의` (plenary session, v8), or `국정조사` (parliamentary investigation, v8). |
| `session` | str | 0 | 220 | Parliamentary session number (e.g., `제212회`). |
| `sub_session` | str | 0 | 496 | Sub-session number (e.g., `제1차`). |
| `date` | str | 0 | 3,397 | Meeting date in `YYYY-MM-DD` format. |
| `agenda` | str | 1 null | 20,059 | Agenda item under discussion. |

#### Speaker identifiers

| Column | Type | Null | Unique | Description |
|--------|------|------|--------|-------------|
| `speaker` | str | 0 | 41,370 | Raw speaker field from transcript. Contains title + name (e.g., `국방부장관 이종섭`, `김영선위원`). |
| `member_id` | str | 3,657,708 | 1,465 | Legislator ID from the National Assembly information system (의안정보시스템). Null for non-legislators. **Caution**: 4 IDs are shared between homonymous legislators; use `member_uid` for disambiguation. |
| `member_uid` | str | 3,657,708 | 1,469 | Disambiguated legislator ID. For the 4 homonymous cases, the format is `{member_id}_{A\|B}`. For all other legislators, equals `member_id`. See Section 5. |
| `speech_order` | str | 0 | 4,343 | Speech sequence number within the meeting, starting from 1. Determines dyad formation order. |
| `role` | str | 0 | 33 | Classified speaker role. See Section 2. |
| `person_name` | str | 7 empty | 19,209 | Extracted person name in Korean (e.g., `이종섭`). For legislators, canonicalized to the shortest non-ambiguous form. 7 rows have no extractable name (speaker field contains only a title). |
| `person_title` | str | 8,465,086 null | 9 | Acting/deputy prefix extracted from the speaker field. Only present for speakers who held a temporary role. See Section 4. |
| `affiliation_raw` | str | varies | 25,730 | Raw institutional affiliation or title extracted from the speaker field. |

#### Speech content

| Column | Type | Null | Description |
|--------|------|------|-------------|
| `speech_text` | str | 1 empty | Full speech text. Concatenated from up to 7 source columns. Double spaces normalized to single. |

#### Legislator metadata (null for non-legislators)

These columns are populated only for rows where `role` is `legislator` or `chair` (4,940,273 rows). Sourced from the National Assembly member database.

| Column | Type | Null | Unique | Description |
|--------|------|------|--------|-------------|
| `name_clean` | str | 3,666,067 | 1,311 | Canonical legislator name from the National Assembly DB. |
| `party` | str | 3,666,919 | 40 | Party affiliation at the time of the speech. |
| `ruling_status` | str | 3,666,919 | 3 | `ruling`, `opposition`, or `independent`. |
| `seniority` | float64 | 3,666,919 | 9 | Number of terms served (1.0 - 9.0). |
| `gender` | str | 3,666,919 | 2 | `남` (male) or `여` (female). |
| `naas_cd` | str | 3,666,919 | 1,300 | National Assembly unique code. Distinct per individual legislator (unlike `member_id`). |

## 2. Speaker roles

33 roles classified by a cascading rule-based system applied to the `speaker` field. Classification priority: `위원장` keyword > `위원` suffix > `member_id` presence > title-based pattern matching.

### 2.1 Legislator roles (form one side of dyads)

| Role | Count | Description |
|------|-------|-------------|
| `legislator` | 4,126,047 | National Assembly member, identified by `위원` suffix or `member_id`. |
| `chair` | 814,226 | Committee/subcommittee chair (`위원장`, `소위원장`, `위원장대리`). Also a legislator. |

### 2.2 Non-legislator roles (form the other side of dyads)

**Executive branch**

| Role | Count | Description |
|------|-------|-------------|
| `minister` | 861,469 | Cabinet minister (`장관`). |
| `minister_acting` | 12,924 | Acting minister (`장관직무대행`, `장관직무대리`). Typically a vice-minister filling in. |
| `vice_minister` | 96,235 | Vice-minister (`차관`). |
| `prime_minister` | 10,910 | Prime minister (`총리`). |
| `agency_head` | 346,804 | Head of a government agency (`청장`). |
| `senior_bureaucrat` | 402,193 | Senior bureaucrat: bureau/division director (`본부장`, `국장`, `실장`). |
| `mid_bureaucrat` | 23,357 | Mid-level bureaucrat (`정책관`, `감사관`, `과장`). |

**Hearing witnesses**

| Role | Count | Description |
|------|-------|-------------|
| `witness` | 182,555 | Sworn witness (`증인`). |
| `testifier` | 71,664 | Unsworn testifier (`진술인`). |
| `expert_witness` | 49,485 | Expert reference witness (`참고인`), including professors and researchers. |
| `nominee` | 75,379 | Nominee for a public office (`후보자`). |
| `minister_nominee` | 122,090 | Nominee for a cabinet minister position (`장관후보자`). |

**Organizations**

| Role | Count | Description |
|------|-------|-------------|
| `public_corp_head` | 506,367 | Head of a public corporation or state bank (`사장`, `은행장`). |
| `org_head` | 236,461 | Head of an organization (`원장`, `회장`, `이사`, `감사`). |
| `financial_regulator` | 21,702 | Financial regulatory official (`금융감독원`, `금융통화위원`). |
| `research_head` | 6,654 | Head of a government research institute. |
| `broadcasting` | 3,055 | Broadcasting official (`방송위원회`, `한국정책방송원`). |
| `cooperative_head` | 15,285 | Head of a cooperative organization. |

**Other government**

| Role | Count | Description |
|------|-------|-------------|
| `local_gov_head` | 80,675 | Head of local government (`시장`, `도지사`, `교육감`). |
| `military` | 43,335 | Military official (`사령관`, `참모총장`, `기무사`). |
| `police` | 9,841 | Police or fire department official (`경찰청`, `소방서`). |
| `audit_official` | 25,184 | Board of Audit and Inspection (`감사원`). |
| `election_official` | 24,787 | National Election Commission (`선관위`). |
| `constitutional_court` | 13,771 | Constitutional Court (`헌법재판소`). |
| `assembly_official` | 6,149 | National Assembly Secretariat (`국회사무처`). |
| `independent_official` | 267,569 | Independent agency official. |
| `other_official` | 109,351 | Other government official not matching specific patterns. |
| `private_sector` | 3,262 | Private sector representative (`㈜`, `주식회사`). |
| `cultural_institution_head` | 7,191 | Head of a cultural institution. |

### 2.3 Excluded from dyads

| Role | Count | Description |
|------|-------|-------------|
| `committee_staff` | 14,891 | Committee professional staff (`전문위원`). Reads procedural reports. |
| `other` | 6,310 | Unclassified speakers (0.07%). Titles do not match any pattern. |
| `unknown` | 683 | Speakers whose titles do not match any classification pattern. Present in v7 gap-fill data (0.5% of 인사청문특별위원회 speeches). |

## 3. Committee key mapping

94+ raw committee names are harmonized to 21 keys. Subcommittee names (e.g., `행정안전위원회-제1반`) are mapped to their parent committee.

| Key | Raw committee names | Terms active |
|-----|---------------------|--------------|
| `agriculture` | 농림해양수산위원회, 농림수산식품위원회, 농림축산식품해양수산위원회 | 16-22 |
| `assembly_operations` | 국회운영위원회 | 16-22 |
| `culture` | 문화관광위원회, 문화체육관광위원회 | 16-17, 20-22 |
| `culture_media` | 문화체육관광방송통신위원회 | 18-19 |
| `defense` | 국방위원회 | 16-22 |
| `education` | 교육위원회 | 16-17, 20-22 |
| `education_culture` | 교육문화체육관광위원회 | 19-20 |
| `education_science` | 교육과학기술위원회 | 18-19 |
| `environment_labor` | 환경노동위원회 | 16-22 |
| `finance` | 재정경제위원회, 기획재정위원회 | 16-22 |
| `foreign_affairs` | 통일외교통상위원회, 외교통상통일위원회, 외교통일위원회 | 16-22 |
| `gender_family` | 여성위원회, 여성가족위원회 | 16-22 |
| `health_welfare` | 보건복지위원회, 보건복지가족위원회 | 16-22 |
| `industry` | 산업자원위원회, 지식경제위원회, 산업통상자원위원회, 산업통상자원중소벤처기업위원회 | 16-22 |
| `intelligence` | 정보위원회 | 16-22 |
| `judiciary` | 법제사법위원회 | 16-22 |
| `land_transport` | 건설교통위원회, 국토해양위원회, 국토교통위원회 | 16-22 |
| `political_affairs` | 정무위원회 | 16-22 |
| `public_admin` | 행정자치위원회, 안전행정위원회, 행정안전위원회 | 16-22 |
| `science_ict` | 과학기술정보통신위원회, 미래창조과학방송통신위원회, 과학기술정보방송통신위원회 | 16-17, 19-22 |

| `confirmation_special` | 국무총리후보자(...)에관한인사청문특별위원회, 대법관(...)임명동의에관한인사청문특별위원회, etc. | 16-22 |
| `budget_special` | 예산결산특별위원회 | 16-22 |
| `investigation` | 국정조사특별위원회 and variants | 16-22 |
| `plenary` | 국회본회의, 국회(임시회/정기회)본회의 | 16-22 |

Committee reorganizations follow the Government Organization Act (정부조직법) amendments. Committees that were merged (e.g., `education` + `science_ict` = `education_science` in terms 18-19) appear as separate keys to preserve the structural distinction.

The `confirmation_special` key covers all 인사청문특별위원회 (confirmation hearing special committees). Each special committee has a unique raw name containing the nominee's name and position (e.g., `국무총리후보자(한덕수)에관한인사청문특별위원회`). Added in v6/v7. The `budget_special`, `investigation`, and `plenary` keys were added in v8.

## 4. Person title values

9 values indicating an acting or deputy capacity. Present for 132,092 speeches (1.5%).

| Title | Count | Meaning |
|-------|-------|---------|
| `대리` | 80,530 | Deputy/acting (general) |
| `반장` | 32,848 | Team leader (subcommittee context) |
| `직무대리` | 12,116 | Acting in official capacity |
| `직무대행` | 4,332 | Acting as replacement |
| `반장대리` | 2,187 | Deputy team leader |
| `반장직무대행` | 56 | Acting team leader replacement |
| `반장직무대리` | 21 | Acting team leader deputy |
| `위원당대리` | 1 | Party deputy (typo variant) |
| `위원장대리` | 1 | Deputy committee chair |

## 5. Homonymous legislator disambiguation

4 `member_id` values from the source system represent two different legislators with the same name serving in different Assembly terms. The `member_uid` column resolves this using `naas_cd` (National Assembly unique code).

| member_id | member_uid | Name | Gender | Party (first) | Terms |
|-----------|-----------|------|--------|---------------|-------|
| 7407 | 7407_A | 김영주 | 남 | 자유선진당 | 19 |
| 7407 | 7407_B | 김영주 | 여 | 열린우리당 | 17, 20 |
| 6182 | 6182_A | 최경환 | 남 | 한나라당 | 17, 18, 19 |
| 6182 | 6182_B | 최경환 | 남 | 국민의당 | 20 |
| 806 | 806_A | 김선동 | 남 | 민주노동당 | 18 |
| 806 | 806_B | 김선동 | 남 | 새누리당 | 20 |
| 878 | 878_A | 김성태 | 남 | 새누리당 | 18, 19 |
| 878 | 878_B | 김성태 | 남 | 새누리당 | 20 |

For all other legislators (1,461 of 1,465 member_ids), `member_uid` equals `member_id`.

## 6. Dyads dataset

### 6.1 Unit of observation

One row = one consecutive speech pair between a legislator and a non-legislator within the same meeting. Formed from adjacent speeches sorted by `speech_order`.

### 6.2 Formation algorithm

```
For each meeting (sorted by speech_order):
    For each consecutive pair (speech[i], speech[i+1]):
        If leg -> nonleg:  dyad(direction="question")
        If nonleg -> leg:  dyad(direction="answer")
        Otherwise:         skip
```

### 6.3 Column definitions

| Column | Type | Null | Description |
|--------|------|------|-------------|
| `meeting_id` | str | 0 | Meeting identifier (same as speeches). |
| `term` | int64 | 0 | Assembly term. |
| `committee` | str | 0 | Original committee name. |
| `committee_key` | str | 0 | Harmonized committee key. |
| `hearing_type` | str | 0 | Standing committee or national audit. |
| `date` | str | 0 | Meeting date (YYYY-MM-DD). |
| `agenda` | str | varies | Agenda item. |
| `leg_name` | str | 0 | Legislator person name. |
| `leg_speaker_raw` | str | 0 | Legislator raw speaker field. |
| `leg_member_uid` | str | varies | Legislator disambiguated ID. |
| `witness_name` | str | 14 empty | Non-legislator person name. |
| `witness_speaker_raw` | str | 0 | Non-legislator raw speaker field. |
| `witness_role` | str | 0 | Non-legislator classified role. 29 categories. |
| `witness_affiliation` | str | varies | Non-legislator affiliation. |
| `direction` | str | 0 | `question` (legislator spoke first) or `answer` (witness spoke first). |
| `leg_speech` | str | 0 | Legislator speech text. |
| `witness_speech` | str | 2 empty | Non-legislator speech text. |

### 6.4 Direction balance

| Direction | Count | Share |
|-----------|-------|-------|
| question | 3,612,874 | 50.0% |
| answer | 3,612,863 | 50.0% |

## 7. Term date ranges (v8)

| Term | Assembly | Start | End | Speeches | Meetings |
|------|----------|-------|-----|----------|----------|
| 16 | 16th | 2000-06-01 | 2004-05-19 | 1,003,739 | 2,816 |
| 17 | 17th | 2004-06-05 | 2008-05-23 | 1,581,747 | 3,244 |
| 18 | 18th | 2008-07-10 | 2012-05-02 | 1,884,315 | 2,839 |
| 19 | 19th | 2012-07-02 | 2016-05-19 | 1,888,749 | 2,708 |
| 20 | 20th | 2016-06-09 | 2020-05-20 | 1,612,100 | 2,334 |
| 21 | 21st | 2020-06-05 | 2024-05-28 | 1,455,862 | 2,315 |
| 22 | 22nd | 2024-06-10 | 2025-07-21 | 479,666 | 573 |

## 8. Hearing type distribution (v8)

| Term | 국정감사 | 상임위원회 | 인사청문특별위원회 | 예산결산특별위원회 | 국회본회의 | 국정조사 |
|------|---------|----------|------------------|------------------|----------|---------|
| 16 | 482,159 | 398,150 | 6,547 | 40,980 | 64,234 | 11,669 |
| 17 | 730,926 | 675,413 | 19,533 | 102,592 | 44,110 | 9,173 |
| 18 | 842,934 | 749,736 | 12,501 | 155,875 | 90,914 | 32,355 |
| 19 | 918,472 | 737,954 | 31,806 | 60,963 | 91,992 | 47,562 |
| 20 | 826,584 | 566,732 | 23,252 | 134,266 | 47,592 | 13,674 |
| 21 | 717,579 | 523,637 | 34,966 | 124,175 | 49,037 | 6,468 |
| 22 | 230,759 | 196,143 | 14,730 | 28,738 | 8,730 | 566 |

## 9. Legislator metadata

### 9.1 Coverage

4,930,259 of 4,940,273 legislator speeches (99.8%) have metadata. The 10,014 unmatched rows are legislators identified solely by title suffix (`위원`) without a corresponding National Assembly DB match.

### 9.2 Party distribution (top 10)

| Party | Speeches | Terms |
|-------|----------|-------|
| 한나라당 | 1,081,567 | 16-18 |
| 더불어민주당 | 887,202 | 20-22 |
| 새누리당 | 712,952 | 19-20 |
| 민주통합당 | 483,549 | 19 |
| 열린우리당 | 367,043 | 17 |
| 통합민주당 | 286,784 | 18 |
| 미래통합당 | 202,370 | 20 |
| 새천년민주당 | 177,030 | 16-17 |
| 무소속 | 133,824 | all |
| 국민의당 | 108,539 | 20 |

### 9.3 Ruling status

| Status | Speeches |
|--------|----------|
| ruling | 2,167,328 |
| opposition | 2,629,107 |
| independent | 133,824 |

### 9.4 Gender

| Gender | Speeches | Unique legislators |
|--------|----------|--------------------|
| 남 (male) | 4,154,759 | ~85% |
| 여 (female) | 775,500 | ~15% |

### 9.5 Seniority (terms served)

| Terms served | Speeches |
|-------------|----------|
| 1 (초선) | 2,375,922 |
| 2 (재선) | 1,251,337 |
| 3 (3선) | 915,476 |
| 4 (4선) | 266,501 |
| 5 (5선) | 94,710 |
| 6+ | 26,313 |
