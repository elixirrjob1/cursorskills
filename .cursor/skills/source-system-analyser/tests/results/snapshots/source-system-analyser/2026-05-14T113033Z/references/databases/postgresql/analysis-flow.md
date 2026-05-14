# PostgreSQL Analysis Flow

1. Resolve connection string and schema.
2. Check for `db-analysis-config.json` in the working directory.
3. If the file is missing, ask whether to exclude schemas, exclude tables, or set `max_row_limit`; write the JSON only if the user requests at least one setting.
4. Ensure `.venv` and dependencies are installed.
5. Run `scripts/source_system_analyzer.py`.
6. Validate `schema.json` contains `metadata`, `connection`, `data_quality_summary`, and `tables`.
7. Report major risks from `data_quality_summary` and per-table findings.
