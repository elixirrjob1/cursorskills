# PostgreSQL Module

Use this module when the source uses PostgreSQL or compatible URLs beginning with `postgresql://`.

## Load Next

- Connection details: `references/databases/postgresql/connection.md`
- Analysis workflow: `references/databases/postgresql/analysis-flow.md`
- Quality rules: `references/databases/postgresql/quality-rules.md`

## Command

```bash
.venv/bin/python scripts/source_system_analyzer.py "$DATABASE_URL" schema.json public --dialect postgresql
```

`schema` can be omitted to use `DATABASE_SCHEMA`/`SCHEMA` or the default `public`.

Before running the command, check for `db-analysis-config.json`. If it is missing, ask the user whether to exclude schemas, exclude tables, or set `max_row_limit`; create the JSON only when at least one of those values is requested.
