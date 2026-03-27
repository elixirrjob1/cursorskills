# MSSQL Module

Use this module for Microsoft SQL Server and Azure SQL sources (`mssql+pyodbc://...`).

## Load Next

- Connection details: `references/databases/mssql/connection.md`
- Analysis workflow: `references/databases/mssql/analysis-flow.md`
- Quality rules: `references/databases/mssql/quality-rules.md`

## Command

```bash
.venv/bin/python scripts/source_system_analyzer.py "$MSSQL_URL" schema.json dbo --dialect mssql
```

`schema` can be omitted to use `DATABASE_SCHEMA`/`SCHEMA` or default `dbo`.

Before running the command, check for `db-analysis-config.json`. If it is missing, ask the user whether to exclude schemas, exclude tables, or set `max_row_limit`; create the JSON only when at least one of those values is requested.
