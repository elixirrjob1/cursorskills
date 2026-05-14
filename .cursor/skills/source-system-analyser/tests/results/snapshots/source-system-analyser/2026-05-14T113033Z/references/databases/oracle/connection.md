# Oracle Connection

## URL Format

`oracle+cx_oracle://user:password@host:port/?service_name=XE`

## Requirements

- `cx_Oracle` or `oracledb`
- Oracle client/runtime as needed by selected driver

## Schema Selection

- Prefer explicit schema argument.
- If omitted, analyzer may fall back to current user schema.
