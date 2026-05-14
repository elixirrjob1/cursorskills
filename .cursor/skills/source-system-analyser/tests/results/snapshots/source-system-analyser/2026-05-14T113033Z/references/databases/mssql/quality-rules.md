# MSSQL Quality Rules

Run the same logical checks as PostgreSQL where metadata and sampling are available.

Focus areas:
- FK integrity and orphan detection
- soft-delete indicators (`is_deleted`, `deleted_at`, status flags)
- ingestion lag between business and insertion timestamps
- timezone consistency across datetime columns

If engine metadata limits a check, keep section shape in output and record reduced confidence.
