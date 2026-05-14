# Oracle Module

Use this module for Oracle sources (`oracle+cx_oracle://...` or compatible).

## Load Next

- Connection details: `references/databases/oracle/connection.md`
- Analysis workflow: `references/databases/oracle/analysis-flow.md`
- Quality rules: `references/databases/oracle/quality-rules.md`

## Command

```bash
.venv/bin/python scripts/source_system_analyzer.py "$ORACLE_URL" schema.json MYSCHEMA --dialect oracle
```

Use uppercase schema names where required by Oracle environments.

Before running the command, check for `db-analysis-config.json`. If it is missing, ask the user whether to exclude schemas, exclude tables, or set `max_row_limit`; create the JSON only when at least one of those values is requested.
