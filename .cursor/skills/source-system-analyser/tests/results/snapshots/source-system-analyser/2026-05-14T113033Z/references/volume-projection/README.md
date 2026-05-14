# Volume Projection Module

Use this module for storage growth and capacity forecasting. This module is merged into `source-system-analyser` and supports PostgreSQL, MSSQL, and Oracle.

## Scripts

- Setup prediction tables:

```bash
.venv/bin/python scripts/volume_projection/collector.py <database_url> --setup
```

- Collect snapshots and growth metrics:

```bash
.venv/bin/python scripts/volume_projection/collector.py <database_url> --collect --schema <schema_name>
```

- Generate capacity report:

```bash
.venv/bin/python scripts/volume_projection/predictor.py <database_url> capacity_report.json
```

## Dialect Notes

- PostgreSQL: full table size + churn metrics.
- MSSQL: table size and row-count metrics via `sys.*` catalog views; churn counters are limited.
- Oracle: segment-based size metrics and growth history when timestamp columns exist.

## Output

Produces `capacity_report.json` with:
- summary projections for 6/12/24 months
- per-table growth + size projections
- database-level capacity snapshot
