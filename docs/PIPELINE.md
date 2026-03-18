# Data Pipeline Documentation

## Overview

This dataset transforms raw XLSX transcripts from the Korean National Assembly (국회) into structured speech-level and legislator-witness dyad data covering the 16th through 22nd Assembly (2000-2024).

```
Raw XLSX (의안정보시스템)
    |
    v
[1] Parse & classify speakers (33 roles)
    |
    v
[2] Harmonize committee names (94 raw -> 21 keys)
    |
    v
[3] Deduplicate
    |
    v
all_speeches_v3.parquet (8.6M rows)
    |
    v
[4] Build consecutive dyads
    |
    v
dyads_v3.parquet (7.2M rows)
```

## Stage 1: XLSX Parsing

### Source files

Two formats depending on the Assembly term:

**16th-20th Assembly** (per-committee XLSX)
- One file per committee per term
- Path pattern: `제{term}대 국회 {hearing_type} 회의록 데이터셋/{committee}.xlsx`
- Standard tabular format with headers in row 1

**21st-22nd Assembly** (multi-sheet XLSX)
- One file per term per hearing type
- Sheet 0 = index (`회의록목록`), remaining sheets = per-meeting data
- Headers detected dynamically (search for `발언자` row)

### Raw columns

| Column | Description |
|--------|-------------|
| `회의번호` | Meeting identifier |
| `대수` | Assembly term (16-22) |
| `위원회` | Committee name |
| `회의일자` | Date (mixed formats: `YYYY년M월D일(요일)` or `YYYY年M月D日(曜)`) |
| `안건` / `안건1`+`안건2` | Agenda item(s) |
| `발언자` | Speaker field (name + title, used for role classification) |
| `의원ID` | Legislator ID (strongest role signal, empty for non-legislators) |
| `발언순번` | Speech sequence number within meeting |
| `발언내용1`-`발언내용7` | Speech text parts (concatenated) |

### Text concatenation

Speech text is split across up to 7 columns (`발언내용1` through `발언내용7`). These are joined with spaces and stripped.

## Stage 2: Speaker Classification

Each speech is classified into one of 33 roles using a cascading rule-based system. The `발언자` field contains the speaker's title and name (e.g., `국방부장관 이종섭`, `김영선위원`).

### Classification priority order

1. **`위원장` in speaker** -> `chair` (committee/subcommittee chair, a legislator)
2. **Speaker ends with `위원`** -> `legislator`
3. **Non-empty `의원ID`** -> `legislator` (strongest signal, overrides all else)
4. **Executive titles**: `장관후보자` -> `minister_nominee`, `장관직무대행` -> `minister_acting`, `장관` -> `minister`, `총리` -> `prime_minister`, `차관` -> `vice_minister`
5. **Hearing roles**: `증인` -> `witness`, `진술인` -> `testifier`, `참고인` -> `expert_witness`
6. **Staff**: `전문위원` -> `committee_staff`
7. **Other nominees**: `후보자` -> `nominee`
8. **Agency heads**: `청장` -> `agency_head`
9. **Institutional**: `감사원장` -> `audit_official`, `헌법재판소` -> `constitutional_court`, `선관위` -> `election_official`, `국회사무` -> `assembly_official`
10. **Military/Police**: `사령관|참모총장` -> `military`, `경찰` -> `police`
11. **Organizational**: `사장|은행장` -> `public_corp_head`, `금융감독원` -> `financial_regulator`, `원장|회장` -> `org_head`
12. **Bureaucrats**: `본부장|처장|국장|실장` -> `senior_bureaucrat`, `정책관|감사관` -> `mid_bureaucrat`
13. **Other categories**: `independent_official`, `local_gov_head`, `research_head`, `cultural_institution_head`, `broadcasting`, `cooperative_head`, `private_sector`
14. **Fallback**: Various official titles -> `other_official`, unmatched -> `other`

### Role groups

For dyad formation, roles are grouped into two sets:

**Legislator roles** (form one side of dyads): `legislator`, `chair`

**Non-legislator roles** (form the other side): all 29 remaining substantive roles

**Excluded** (do not form dyads): `committee_staff`, `other`, `unknown`

### v2 -> v3 corrections

- **v2 over-correction**: Government agency chairs (`국사편찬위원장`, `방송통신위원장`) were correctly removed from `chair`. But `소위원장` (subcommittee chair), `위원장직무대행` (acting chair), and `위원장대리` (deputy chair) were incorrectly reclassified. These are legislators.
- **v3 fix**: Rows with `member_id` (legislator ID) and legislative chair title patterns are restored to `chair`. Remaining `member_id` holders are classified as `legislator`.

## Stage 3: Committee Harmonization

94 raw committee names (reflecting reorganizations across 7 terms) are mapped to 21 stable keys.

| Key | Example raw names |
|-----|-------------------|
| `foreign_affairs` | 통일외교통상위원회, 외교통상통일위원회, 외교통일위원회 |
| `defense` | 국방위원회 |
| `finance` | 재정경제위원회, 기획재정위원회 |
| `education` | 교육위원회 |
| `education_science` | 교육과학기술위원회 |
| `education_culture` | 교육문화체육관광위원회 |
| `science_ict` | 과학기술정보통신위원회, 미래창조과학방송통신위원회 |
| `agriculture` | 농림해양수산위원회, 농림수산식품위원회, 농림축산식품해양수산위원회 |
| `industry` | 산업자원위원회, 지식경제위원회, 산업통상자원위원회 |
| `health_welfare` | 보건복지위원회, 보건복지가족위원회 |
| `environment_labor` | 환경노동위원회 |
| `land_transport` | 건설교통위원회, 국토해양위원회, 국토교통위원회 |
| `public_admin` | 행정자치위원회, 안전행정위원회, 행정안전위원회 |
| `judiciary` | 법제사법위원회 |
| `political_affairs` | 정무위원회 |
| `assembly_operations` | 국회운영위원회 |
| `intelligence` | 정보위원회 |
| `gender_family` | 여성위원회, 여성가족위원회 |
| `culture` | 문화관광위원회, 문화체육관광위원회 |
| `culture_media` | 문화체육관광방송통신위원회 |

Subcommittee names (e.g., `행정안전위원회-예결소위`) are mapped by stripping the suffix and matching the parent committee.

## Stage 4: Dyad Formation

Dyads are formed from consecutive speech pairs within each meeting.

### Algorithm

```
For each meeting (sorted by speech_order):
    For each pair of consecutive speeches (i, i+1):
        If speech[i].role in LEGISLATOR_ROLES and speech[i+1].role in NONLEGISLATOR_ROLES:
            -> dyad(direction="question", legislator=speech[i], witness=speech[i+1])
        Elif speech[i].role in NONLEGISLATOR_ROLES and speech[i+1].role in LEGISLATOR_ROLES:
            -> dyad(direction="answer", witness=speech[i], legislator=speech[i+1])
        Else:
            -> skip (same-side consecutive speeches)
```

### Important notes

- Each speech can appear in at most 2 dyads (as the second element of one pair and the first of the next)
- `committee_staff`, `other`, and `unknown` speakers are excluded (they form neither side)
- Same-side consecutive speeches (e.g., two legislators in a row) produce no dyad
- Direction is ~50/50 (question vs answer) by construction

## Stage 5: Deduplication

v2 processing introduced 94,347 duplicate rows (primarily in 20th Assembly, from XLSX files being processed twice). Duplicates are identified by `(meeting_id, speech_order)` uniqueness and removed by keeping the first occurrence.

## Stage 6: v4 Cleanup

1. **person_title extraction**: Acting/deputy titles (대리, 직무대행, 반장, etc.) extracted from `person_name` into a separate `person_title` column.
2. **person_name canonicalization**: For legislators with `member_id`, the shortest non-empty name variant is used as canonical (removes "의원" suffix, "대리" prefix, etc.).
3. **`other` role reclassification**: ~17,800 speeches reclassified from `other` to proper roles using pattern matching on the speaker field (e.g., 사관학교장 -> military, 이사 -> org_head).
4. **Text normalization**: Double spaces collapsed to single space.
5. **Date normalization**: All dates converted to `YYYY-MM-DD` format (was mixed Korean/Japanese formats with day-of-week suffixes).

## Stage 7: v5 Data Integrity

1. **member_id null fix**: `"nan"` and `""` strings converted to proper null/NA values.
2. **person_title decontamination**: 87 rows with affiliation prefixes incorrectly extracted as titles (e.g., "국방부획득정책관") moved back to `affiliation_raw`.
3. **Empty person_name fix**: 18 of 25 empty names parsed from speaker field (remaining 7 have no personal name in the source).
4. **Homonymous member_id disambiguation**: 4 `member_id` values (7407, 6182, 806, 878) each represent two different legislators with the same name across different Assembly terms. A new `member_uid` column disambiguates using `naas_cd` (National Assembly unique code). Format: `{member_id}_{A|B}`.
5. **minister 직무대리 reclassification**: 2,357 rows where "장관직무대리" (acting minister) was classified as `minister` reclassified to `minister_acting`.
6. **Additional `other` reclassification**: 2,382 more speeches reclassified using expanded pattern rules (금융통화위원 -> financial_regulator, 소방서 -> police, etc.).
7. **Non-legislator person_name cleanup**: 63,447 rows where affiliation prefixes had leaked into `person_name` (e.g., "용인소방서이동119안전센터 서헌식" -> name="서헌식", affiliation="용인소방서이동119안전센터").
8. **Gender/party metadata consistency**: Fixed inconsistent metadata for homonymous member_id groups.
9. **Dyad rebuild**: Dyads rebuilt from corrected speeches.

## Validation

The dataset passes 52 automated checks (46 PASS, 6 WARN, 0 FAIL). See `validation/validate_dataset.py` for the full test suite.

### Key metrics (v5)

| Metric | Value |
|--------|-------|
| Total speeches | 8,597,178 |
| Total dyads | 7,225,737 |
| Dyad/speech ratio | 84.0% |
| Terms covered | 16-22 (2000-2024) |
| Hearing types | Standing committee + National audit |
| Committees | 20 keys (0% unmapped) |
| Empty text | 1 speech (0.00%) |
| Duplicates | 0 (cleaned) |
| Role classification rate | 99.9% (0.07% `other`) |
| member_id consistency | 100% |
| Dyad spot-check | 100/100 meetings pass |
| Date format | 100% YYYY-MM-DD |

### Known limitations

- **Short speeches** (17.1% under 10 chars): Procedural statements like "예", "동의합니다", "이상입니다". These are valid speech acts in parliamentary proceedings.
- **Self-pairing** (604 dyads): Same person name on both sides, confirmed as different people (homonyms). e.g., legislator 김영환 and minister 김영환 are different people.
- **Empty witness names** (14 dyads): Cases where the speaker field contains only a title without a personal name (e.g., "여성가족부 장관").
- **Homonymous member_ids** (4 IDs): Source data assigns identical `member_id` to different legislators with the same name across terms. Use `member_uid` for disambiguation.
