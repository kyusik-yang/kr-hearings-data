# kr-hearings-data

Speech-level dataset from Korean National Assembly committee proceedings (16th-22nd Assembly, 2000-2024).

## Data

- **8.6M speeches** classified into 33 speaker roles
- **7.2M legislator-witness dyads** (consecutive Q&A pairs)
- **20 committees** harmonized across 94 raw names and 7 legislative terms
- **2 hearing types**: standing committee (상임위원회) and national audit (국정감사)

## Quick start

```bash
pip install kr-hearings-data
```

```python
import kr_hearings_data as kh

# Load speeches
speeches = kh.load_speeches()

# Load dyads
dyads = kh.load_dyads()

# Filter by term and hearing type
audit_20 = kh.load_dyads(term=20, hearing_type="국정감사")
```

### CLI

```bash
# Download data
kr-hearings download

# Summary statistics
kr-hearings info

# Export filtered subset
kr-hearings export --term 20 --hearing-type 국정감사 --format csv -o output.csv
```

## Files

Data files are available under [GitHub Releases](https://github.com/kyusik-yang/kr-hearings-data/releases).

| File | Rows | Description |
|------|------|-------------|
| `speeches_v5.parquet` | 8,597,178 | All speeches with speaker classification |
| `dyads_v5.parquet` | 7,225,737 | Legislator - non-legislator speech pairs |

## Columns

### speeches

| Column | Type | Description |
|--------|------|-------------|
| `meeting_id` | str | Meeting identifier |
| `term` | int | Assembly term (16-22) |
| `committee` | str | Original committee name |
| `committee_key` | str | Harmonized committee key (20 categories) |
| `hearing_type` | str | `상임위원회` or `국정감사` |
| `session` | str | Session number (e.g., `제212회`) |
| `sub_session` | str | Sub-session number (e.g., `제1차`) |
| `date` | str | Meeting date (YYYY-MM-DD) |
| `agenda` | str | Agenda item |
| `speaker` | str | Raw speaker field (title + name) |
| `member_id` | str | Legislator ID from source data (null for non-legislators) |
| `member_uid` | str | Disambiguated legislator ID (resolves 4 homonymous member_ids) |
| `speech_order` | str | Speech sequence number within meeting |
| `role` | str | Classified speaker role (33 categories) |
| `person_name` | str | Extracted person name |
| `person_title` | str | Acting/deputy title if applicable (e.g., 대리, 직무대행) |
| `affiliation_raw` | str | Raw affiliation or institutional title |
| `speech_text` | str | Full speech text |
| `name_clean` | str | Legislator name (from National Assembly DB, legislators only) |
| `party` | str | Party affiliation (legislators only) |
| `ruling_status` | str | Ruling/opposition status (legislators only) |
| `seniority` | float | Number of terms served (legislators only) |
| `gender` | str | Gender (legislators only) |
| `naas_cd` | str | National Assembly unique code (legislators only) |

### dyads

| Column | Type | Description |
|--------|------|-------------|
| `meeting_id` | str | Meeting identifier |
| `term` | int | Assembly term |
| `committee` | str | Original committee name |
| `committee_key` | str | Harmonized committee key |
| `hearing_type` | str | `상임위원회` or `국정감사` |
| `date` | str | Meeting date (YYYY-MM-DD) |
| `agenda` | str | Agenda item |
| `leg_name` | str | Legislator name |
| `leg_speaker_raw` | str | Legislator raw speaker field |
| `leg_member_uid` | str | Legislator disambiguated ID |
| `witness_name` | str | Non-legislator name |
| `witness_speaker_raw` | str | Non-legislator raw speaker field |
| `witness_role` | str | Non-legislator classified role |
| `witness_affiliation` | str | Non-legislator affiliation |
| `direction` | str | `question` (legislator first) or `answer` (witness first) |
| `leg_speech` | str | Legislator speech text |
| `witness_speech` | str | Non-legislator speech text |

## Speaker roles

33 categories organized in 3 tiers:

**Legislator** (form one side of dyads): `legislator`, `chair`

**Non-legislator** (form the other side):
- Executive: `minister`, `vice_minister`, `prime_minister`, `agency_head`, `senior_bureaucrat`, `mid_bureaucrat`, `minister_acting`
- Hearing witnesses: `witness`, `testifier`, `expert_witness`, `nominee`, `minister_nominee`
- Organizational: `public_corp_head`, `org_head`, `financial_regulator`, `research_head`, `broadcasting`, `cooperative_head`
- Other: `local_gov_head`, `military`, `police`, `audit_official`, `election_official`, `constitutional_court`, `assembly_official`, `independent_official`, `private_sector`, `cultural_institution_head`, `other_official`

**Excluded from dyads**: `committee_staff`, `other`, `unknown`

## Documentation

- [docs/CODEBOOK.md](docs/CODEBOOK.md) - Full codebook with column definitions, role taxonomy, committee mapping, and value distributions
- [docs/PIPELINE.md](docs/PIPELINE.md) - Data pipeline documentation (XLSX parsing through v5 integrity fixes)

## Validation

52 automated checks, 0 failures. See [validation/](validation/) for the test suite.

### Known limitations

- **Short speeches** (17.1% under 10 chars): Procedural utterances like "예", "동의합니다". Valid speech acts in parliamentary proceedings.
- **Self-pairing dyads** (604): Same person name on both sides, confirmed as different people (homonyms). e.g., legislator 김영환 and minister 김영환.
- **Empty witness names** (14 dyads): Cases where the speaker field contains only a title without a personal name (e.g., "여성가족부 장관", "산업통상자원부 제1차관").
- **Remaining `other` role** (6,310 speeches, 0.07%): Speakers whose titles do not match any classification pattern. These are excluded from dyad formation.
- **Homonymous member_ids**: 4 member_ids (7407, 6182, 806, 878) each represent two different legislators with the same name across different Assembly terms. Use `member_uid` for disambiguation.

## Version history

| Version | Speeches | Dyads | Changes |
|---------|----------|-------|---------|
| v5 | 8,597,178 | 7,225,737 | member_id null fix, person_title cleanup, member_uid disambiguation, minister 직무대리 reclassification, additional 'other' reclassification, non-legislator person_name cleanup |
| v4 | 8,597,178 | 7,221,024 | person_title extraction, person_name cleanup, 'other' reclassification, text normalization, date normalization |
| v3 | 8,597,178 | 7,185,949 | Speaker classification fix (소위원장), deduplication, dyad rebuild |

## Source

Raw data: National Assembly proceeding XLSX datasets (의안정보시스템).

## Author

Kyusik Yang, New York University

## License

CC BY 4.0
