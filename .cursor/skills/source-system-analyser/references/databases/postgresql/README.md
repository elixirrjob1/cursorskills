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
