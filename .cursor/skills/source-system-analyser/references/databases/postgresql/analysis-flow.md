# PostgreSQL Analysis Flow

1. Resolve connection string and schema.
2. Ensure `.venv` and dependencies are installed.
3. Run `scripts/source_system_analyzer.py`.
4. Validate `schema.json` contains `metadata`, `connection`, `data_quality_summary`, and `tables`.
5. Report major risks from `data_quality_summary` and per-table findings.
