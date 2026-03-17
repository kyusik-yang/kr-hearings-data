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
| `speeches_v3.parquet` | 8,597,178 | All speeches with speaker classification |
| `dyads_v3.parquet` | 7,185,949 | Legislator - non-legislator speech pairs |

## Columns

### speeches

| Column | Description |
|--------|-------------|
| `meeting_id` | Meeting identifier |
| `term` | Assembly term (16-22) |
| `committee` | Original committee name |
| `committee_key` | Harmonized committee key (20 categories) |
| `hearing_type` | `상임위원회` or `국정감사` |
| `date` | Meeting date |
| `agenda` | Agenda item |
| `speaker` | Raw speaker field |
| `member_id` | Legislator ID (empty for non-legislators) |
| `speech_order` | Speech sequence number |
| `role` | Classified speaker role (33 categories) |
| `person_name` | Extracted person name |
| `affiliation_raw` | Raw affiliation / title |
| `speech_text` | Full speech text |

### dyads

| Column | Description |
|--------|-------------|
| `meeting_id` | Meeting identifier |
| `term` | Assembly term |
| `committee_key` | Harmonized committee key |
| `hearing_type` | `상임위원회` or `국정감사` |
| `date` / `agenda` | Meeting metadata |
| `leg_name` / `leg_speaker_raw` | Legislator side |
| `witness_name` / `witness_speaker_raw` | Non-legislator side |
| `witness_role` | Classified role (29 non-legislator categories) |
| `direction` | `question` (legislator first) or `answer` (witness first) |
| `leg_speech` / `witness_speech` | Speech texts |

## Speaker roles

33 categories organized in 3 tiers:

**Legislator** (form one side of dyads): `legislator`, `chair`

**Non-legislator** (form the other side):
- Executive: `minister`, `vice_minister`, `prime_minister`, `agency_head`, `senior_bureaucrat`, `mid_bureaucrat`
- Hearing witnesses: `witness`, `testifier`, `expert_witness`, `nominee`, `minister_nominee`
- Organizational: `public_corp_head`, `org_head`, `financial_regulator`, `research_head`, `broadcasting`, `cooperative_head`
- Other: `local_gov_head`, `military`, `police`, `audit_official`, `election_official`, `constitutional_court`, `assembly_official`, `independent_official`, `private_sector`, `cultural_institution_head`, `other_official`, `minister_acting`

**Excluded from dyads**: `committee_staff`, `other`, `unknown`

## Validation

50 automated checks, 0 failures. See [docs/PIPELINE.md](docs/PIPELINE.md) for the full data pipeline documentation and [validation/](validation/) for the test suite.

## Source

Raw data: National Assembly proceeding XLSX datasets (의안정보시스템).

## Author

Kyusik Yang, New York University

## License

CC BY 4.0
