# Shared Prerequisites

## Runtime

- Python 3.8+
- Recommended virtual environment at `.venv`

## Required Packages

- `sqlalchemy`
- `psycopg2-binary` for PostgreSQL
- `pyodbc` for MSSQL/Azure SQL
- `cx_Oracle` or `oracledb` for Oracle
- `requests` for API source reads
- `openpyxl` for Excel (`.xlsx`) tabular inputs

Install baseline:

```bash
python3 -m venv .venv
.venv/bin/pip install sqlalchemy psycopg2-binary
.venv/bin/pip install requests openpyxl
```

Add optional drivers based on source:

```bash
.venv/bin/pip install pyodbc
.venv/bin/pip install cx_Oracle
```

## Secret Handling

- Prefer `DATABASE_URL`, `DB_URL`, `POSTGRES_URL`, `DB_CONNECTION_STRING`.
- For schema defaults use `DATABASE_SCHEMA` or `SCHEMA`.
- Never commit credentials to files tracked in git.
