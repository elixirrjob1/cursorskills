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
