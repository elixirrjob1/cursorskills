# MSSQL Connection

## URL Format

`mssql+pyodbc://user:password@host:port/database?driver=ODBC+Driver+17+for+SQL+Server`

## Requirements

- `pyodbc`
- Installed ODBC driver compatible with SQL Server

## Schema Selection

- Prefer explicit schema argument.
- Fallback to env schema values.
- Final fallback: `dbo`.
