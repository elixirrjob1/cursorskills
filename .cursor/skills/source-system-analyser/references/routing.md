# Source Routing Guide

Use this file to pick the correct source module quickly.

## Decision Order

1. If input is a SQL connection string, route to a database module.
2. If input is an HTTP endpoint plus auth, route to API generic module.
3. If input is a local/remote CSV/Excel or object URI, route to flat generic module.
4. If multiple source types are present, process each independently and merge outputs into one `schema.json` contract.

## Source-Type Detection Rules (Agent)

- `http://` or `https://` -> `api`
- known DB URL schemes (`postgresql`, `mssql`, `oracle`, etc.) -> `database`
- `.csv`, `.xlsx`, `.xls`, `.tsv`, `.json`, `.ndjson` local/object files -> `flat`
- if uncertain after rule checks, ask the user to confirm source type before execution

## Database Routes

- PostgreSQL: `references/databases/postgresql/README.md`
- MSSQL/Azure SQL: `references/databases/mssql/README.md`
- Oracle: `references/databases/oracle/README.md`

## API Route

- Current test API workflow: `references/apis/test-api/README.md`
- Generic API fallback: `references/apis/generic/README.md`
- Reader script: `scripts/apis/api_reader.py`
- Analyzer script: `scripts/apis/api_analyzer.py`
- Test provider wrapper: `scripts/apis/test_api/test_api_reader.py`

### API Module Standard (all current and future API subfolders)

For every API module folder under `references/apis/`:
- include `README.md`
- include `auth.md`
- include `discovered-references.md` for confirmed reference names (names only, no values)
- link `discovered-references.md` from the module `README.md` and `auth.md`

Template for new modules:
- `references/apis/_shared/discovered-references-template.md`

## Flat Route

- Generic flat-file workflow: `references/flat/generic/README.md`
- Script: `scripts/flat/tabular_schema_json.py`

## Volume Projection Route

- Volume forecasting workflow: `references/volume-projection/README.md`
- Collector: `scripts/volume_projection/collector.py`
- Predictor: `scripts/volume_projection/predictor.py`
