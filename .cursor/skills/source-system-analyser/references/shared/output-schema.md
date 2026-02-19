# Shared Output Schema Contract

All source modules should emit a compatible `schema.json` object with these top-level sections:

- `metadata`: generation timestamp, source descriptor, table/entity totals, finding totals
- `connection`: source endpoint summary (redacted), driver/provider, timezone if known
- `data_quality_summary`: severity counts and per-check totals
- `tables`: collection of normalized entities with schema and quality details

## Table/Entity Minimum Shape

Each item in `tables` should include:

- `table`
- `schema`
- `columns`
- `primary_keys`
- `foreign_keys`
- `row_count`
- `data_quality`

## Data Quality Structure

`data_quality` should include typed sections when available:

- `controlled_value_candidates`
- `nullable_but_never_null`
- `missing_primary_key`
- `missing_foreign_keys`
- `format_inconsistency`
- `range_violations`
- `delete_management`
- `late_arriving_data`
- `timezone`
- `findings` (flat list for programmatic consumption)

If a section is not applicable for a source, emit an empty collection/object and keep overall shape consistent.
