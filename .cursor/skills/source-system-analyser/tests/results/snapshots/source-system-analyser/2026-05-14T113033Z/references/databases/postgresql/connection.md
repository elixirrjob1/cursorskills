# PostgreSQL Connection

## URL Format

`postgresql://user:password@host:port/database`

## Discovery Order

1. User-provided value
2. `DATABASE_URL`, `DB_URL`, `POSTGRES_URL`, `DB_CONNECTION_STRING`
3. Local config files (`.env`, app settings, compose files)

## Schema Selection

- Prefer explicit CLI schema argument.
- Fallback to `DATABASE_SCHEMA` or `SCHEMA`.
- Final fallback: `public`.
