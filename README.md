# kr-hearings-data

[![PyPI](https://img.shields.io/pypi/v/kr-hearings-data)](https://pypi.org/project/kr-hearings-data/)
[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

Speech-level dataset from Korean National Assembly committee proceedings (16th-22nd Assembly, 2000-2025).

## Data

- **9.9M speeches** classified into 33 speaker roles
- **7.4M legislator-witness dyads** (consecutive Q&A pairs)
- **20+ committees** harmonized across 94+ raw names and 7 legislative terms
- **6 hearing types**: standing committee (상임위원회), national audit (국정감사), confirmation hearing (인사청문특별위원회), parliamentary investigation (국정조사), budget committee (예산결산특별위원회), plenary session (국회본회의)
- **16,830 meetings** covering all major National Assembly proceedings (16th-22nd Assembly)

## Installation

Requires Python 3.9+.

```bash
pip install kr-hearings-data
```

Dependencies: `pandas`, `pyarrow`, `requests`, `tqdm`.

For development:

```bash
git clone https://github.com/kyusik-yang/kr-hearings-data.git
cd kr-hearings-data
pip install -e .
```

## Quick start

```python
import kr_hearings_data as kh

# Load full speeches dataset
# First call downloads ~1.1 GB from GitHub Releases and caches locally.
# Subsequent calls load from cache.
speeches = kh.load_speeches()

# Load full dyads dataset (~1.0 GB on first call)
dyads = kh.load_dyads()
```

## Python API

All loader functions are keyword-only (except `version` in `download` and `info`).

### `load_speeches(*, version, term, hearing_type, columns) -> pd.DataFrame`

Load the speeches dataset. Downloads from GitHub Releases on first call; cached afterwards.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `version` | `str` | `"v9"` | Data version tag matching a GitHub Release (e.g., `"v9"`, `"v8"`) |
| `term` | `int \| None` | `None` | Filter by Assembly term (16-22). Applied at the Parquet row-group level for speed. |
| `hearing_type` | `str \| None` | `None` | Filter by hearing type. One of `상임위원회`, `국정감사`, `인사청문특별위원회`, `예산결산특별위원회`, `국회본회의`, `국정조사`. |
| `columns` | `list[str] \| None` | `None` | Read only these columns. Reduces memory and speeds up loading. |

Returns a `pandas.DataFrame`.

```python
# All 20th-term national audit speeches, metadata columns only
audit_20 = kh.load_speeches(
    term=20,
    hearing_type="국정감사",
    columns=["meeting_id", "date", "person_name", "role", "party"],
)
```

### `load_dyads(*, version, term, hearing_type, columns) -> pd.DataFrame`

Load the dyads dataset. Same parameters as `load_speeches`.

```python
# 21st-term standing committee dyads
dyads_21 = kh.load_dyads(term=21, hearing_type="상임위원회")
```

### `download(version="v9") -> dict[str, Path]`

Download both datasets to the local cache without loading them into memory. Returns a dict mapping `"speeches"` and `"dyads"` to their cached file paths.

```python
paths = kh.download()
# {'speeches': PosixPath('~/.cache/kr-hearings-data/v9/all_speeches_16_22_v9.parquet'),
#  'dyads':    PosixPath('~/.cache/kr-hearings-data/v9/dyads_16_22_v9.parquet')}
```

### `info(version="v9") -> None`

Print summary statistics (row counts by term and hearing type) for cached datasets. If data is not yet downloaded, prompts to run `download()` first.

```python
kh.info()
```

## CLI

The `kr-hearings` command is installed alongside the package.

### Global option

| Flag | Default | Description |
|------|---------|-------------|
| `--version` | `v9` | Data version tag (e.g., `v9`, `v8`) |

### `kr-hearings download`

Download both datasets to local cache.

```bash
kr-hearings download
kr-hearings download --version v8   # download an older version
```

### `kr-hearings info`

Print summary statistics for cached datasets.

```bash
kr-hearings info
```

### `kr-hearings export`

Export a filtered subset to CSV or Parquet.

| Flag | Values | Default | Description |
|------|--------|---------|-------------|
| `--dataset` | `speeches`, `dyads` | `speeches` | Which dataset to export |
| `--term` | integer | all | Filter by Assembly term |
| `--hearing-type` | string | all | Filter by hearing type |
| `--format` | `csv`, `parquet` | `csv` | Output format |
| `-o`, `--output` | path | (required) | Output file path |

```bash
# Export 20th-term national audit speeches to CSV
kr-hearings export --term 20 --hearing-type 국정감사 -o audit_20.csv

# Export 21st-term dyads to Parquet
kr-hearings export --dataset dyads --term 21 --format parquet -o dyads_21.parquet
```

## Cache

Data is downloaded from [GitHub Releases](https://github.com/kyusik-yang/kr-hearings-data/releases) and stored in a local cache directory with the following structure:

```
~/.cache/kr-hearings-data/
  v9/
    all_speeches_16_22_v9.parquet   (1.1 GB)
    dyads_16_22_v9.parquet          (1.0 GB)
```

Multiple versions can coexist in the cache (each under its own subdirectory).

| Environment variable | Default | Description |
|---------------------|---------|-------------|
| `KR_HEARINGS_CACHE` | `~/.cache/kr-hearings-data` | Override the cache directory |

To clear the cache, delete the directory:

```bash
rm -rf ~/.cache/kr-hearings-data
```

## Usage examples

### Memory-efficient loading

The full speeches dataset is ~1.1 GB on disk and expands in memory. Use `columns` to load only what you need:

```python
# ~28 columns -> 3 columns: much faster and lighter
meta = kh.load_speeches(columns=["term", "role", "hearing_type"])
```

Combining `columns` with `term` or `hearing_type` filters further reduces memory by skipping irrelevant Parquet row groups at read time:

```python
# Only 20th-term committee speeches, 3 columns
subset = kh.load_speeches(
    term=20,
    hearing_type="상임위원회",
    columns=["person_name", "party", "speech_text"],
)
```

### Speeches by speaker role

```python
speeches = kh.load_speeches(columns=["role"])
print(speeches["role"].value_counts())
```

### Party-level analysis in national audit dyads

```python
dyads = kh.load_dyads(
    hearing_type="국정감사",
    columns=["term", "leg_party", "leg_ruling_status", "witness_role"],
)

# Ruling vs. opposition interactions with ministers
ministers = dyads[dyads["witness_role"] == "minister"]
print(ministers.groupby(["term", "leg_ruling_status"]).size().unstack(fill_value=0))
```

### Minister dual-office analysis (v9)

```python
speeches = kh.load_speeches(
    columns=["role", "dual_office", "admin", "admin_ideology"],
)
ministers = speeches[speeches["role"] == "minister"]

# Ministers who simultaneously held an NA seat
dual = ministers[ministers["dual_office"] == True]
print(f"Dual-office minister speeches: {len(dual):,}")
print(dual["admin"].value_counts())
```

### Loading an older version

```python
speeches_v8 = kh.load_speeches(version="v8")
```

The v8 file is downloaded and cached independently of v9.

## Files

Data files are available under [GitHub Releases](https://github.com/kyusik-yang/kr-hearings-data/releases).

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| `all_speeches_16_22_v9.parquet` | 9,906,444 | 28 | All speeches + minister panel metadata |
| `dyads_16_22_v9.parquet` | 7,429,413 | 25 | Dyads with legislator + minister metadata |

## Columns

### speeches

| Column | Type | Description |
|--------|------|-------------|
| `meeting_id` | str | Meeting identifier |
| `term` | int | Assembly term (16-22) |
| `committee` | str | Original committee name |
| `committee_key` | str | Harmonized committee key (20 categories) |
| `hearing_type` | str | 6 types: `상임위원회`, `국정감사`, `인사청문특별위원회`, `예산결산특별위원회`, `국회본회의`, `국정조사` |
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
| `ministry_normalized` | str | Standardized ministry/agency name (v9, govt officials only) |
| `dual_office` | bool | Minister simultaneously held NA seat (v9, ministers only) |
| `admin` | str | Presidential administration name (v9, ministers only) |
| `admin_ideology` | str | Progressive or Conservative (v9, ministers only) |

### dyads

| Column | Type | Description |
|--------|------|-------------|
| `meeting_id` | str | Meeting identifier |
| `term` | int | Assembly term |
| `committee` | str | Original committee name |
| `committee_key` | str | Harmonized committee key |
| `hearing_type` | str | 6 types: `상임위원회`, `국정감사`, `인사청문특별위원회`, `예산결산특별위원회`, `국회본회의`, `국정조사` |
| `date` | str | Meeting date (YYYY-MM-DD) |
| `agenda` | str | Agenda item |
| `leg_name` | str | Legislator name |
| `leg_speaker_raw` | str | Legislator raw speaker field |
| `leg_member_uid` | str | Legislator disambiguated ID |
| `leg_party` | str | Legislator party (v9, 99.9% coverage) |
| `leg_ruling_status` | str | Ruling/opposition/independent (v9, 97.1%) |
| `leg_seniority` | float | Terms served (v9) |
| `leg_gender` | str | Gender (v9) |
| `witness_name` | str | Non-legislator name |
| `witness_speaker_raw` | str | Non-legislator raw speaker field |
| `witness_role` | str | Non-legislator classified role |
| `witness_affiliation` | str | Non-legislator raw affiliation |
| `witness_ministry_normalized` | str | Standardized ministry name (v9) |
| `witness_dual_office` | bool | Minister held NA seat simultaneously (v9) |
| `witness_admin` | str | Presidential administration (v9) |
| `witness_admin_ideology` | str | Progressive or Conservative (v9) |
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

52 automated checks across speeches, dyads, speaker classification, committee harmonization, and cross-dataset consistency. See [validation/](validation/) for the test suite and `report_v8.json` for the latest results.

### Known limitations

- **Short speeches** (15.9% under 10 chars): Procedural utterances like "예", "동의합니다". Valid speech acts in parliamentary proceedings.
- **Self-pairing dyads** (637): Same person name on both sides, confirmed as different people (homonyms). e.g., legislator 김영환 and minister 김영환.
- **Empty witness names** (919 dyads): Cases where the speaker field contains only a title without a personal name (e.g., "여성가족부 장관", "산업통상자원부 제1차관").
- **Remaining `other` role** (6,472 speeches, 0.07%): Speakers whose titles do not match any classification pattern. These are excluded from dyad formation.
- **member_id on non-legislators** (29,182 speeches): Former legislators appearing as ministers or other officials retain their member_id from legislative service.
- **Homonymous member_ids**: 4 member_ids (7407, 6182, 806, 878) each represent two different legislators with the same name across different Assembly terms. Use `member_uid` for disambiguation.

## Version history

| Version | Speeches | Dyads | Changes |
|---------|----------|-------|---------|
| v9 | 9,906,444 | 7,429,413 | Minister panel enrichment (dual_office, admin, admin_ideology). Legislator metadata in dyads (party, ruling_status). ruling_status cleanup. Full dyad rebuild across 6 hearing types |
| v8 | 9,906,444 | 7,894,147 | +국정조사 191건, 예산결산특별위원회 832건, 국회본회의 1,058건 (1.17M speeches). Hybrid XML viewer + PDF parsing. Dyads rebuilt for all 6 hearing types |
| v7 | 8,740,779 | - | +228 인사청문특별위원회 meetings from PDF parsing (111K speeches). Hanja name conversion, mp_metadata enrichment (99.9% legislator party coverage) |
| v6 | 8,629,431 | 7,225,737 | +42 인사청문특별위원회 meetings from HTML scraping (32K speeches). New hearing_type value: `인사청문특별위원회` |
| v5 | 8,597,178 | 7,225,737 | member_id null fix, person_title cleanup, member_uid disambiguation, minister 직무대리 reclassification, additional 'other' reclassification, non-legislator person_name cleanup |
| v4 | 8,597,178 | 7,221,024 | person_title extraction, person_name cleanup, 'other' reclassification, text normalization, date normalization |
| v3 | 8,597,178 | 7,185,949 | Speaker classification fix (소위원장), deduplication, dyad rebuild |

## Source

Raw data: National Assembly proceeding XLSX datasets (의안정보시스템), PDF transcripts, and structured HTML from 국회회의록시스템 (record.assembly.go.kr, likms.assembly.go.kr).

## Author

Kyusik Yang, New York University

## License

CC BY 4.0
