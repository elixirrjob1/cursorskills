# MSSQL Analysis Flow

1. Resolve connection and confirm ODBC driver availability.
2. Run analyzer with `--dialect mssql`.
3. Validate table and constraint discovery succeeded for the selected schema.
4. Review findings for FK gaps, delete-management patterns, and timezone consistency.
