# MSSQL Analysis Flow

1. Resolve connection and confirm ODBC driver availability.
2. Check for `db-analysis-config.json` in the working directory.
3. If the file is missing, ask whether to exclude schemas, exclude tables, or set `max_row_limit`; write the JSON only if the user requests at least one setting.
4. Run analyzer with `--dialect mssql`.
5. Validate table and constraint discovery succeeded for the selected schema.
6. Review findings for FK gaps, delete-management patterns, and timezone consistency.
