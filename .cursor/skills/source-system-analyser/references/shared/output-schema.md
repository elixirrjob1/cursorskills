# Shared Output Schema Contract

All source modules should emit a compatible `schema.json` object with these top-level sections:

- `metadata`: generation timestamp, source descriptor, table/entity totals, finding totals
- `connection`: source endpoint summary (redacted), driver/provider, timezone if known
- `source_system_context`: source-level operational context (`contacts`, `delete_management_instruction`, `restrictions`)
- `data_quality_summary`: severity counts and per-check totals
- `concept_registry`: schema-level summary of discovered canonical concepts and supporting columns
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

Optional enrichments:

- `unit_summary` (`columns_with_units`, `columns_without_units`, `mixed_unit_groups`, `unknown_unit_columns`)
- `table_description` (always present; use source comment/description when available, otherwise generate during analysis)
- `classification_summary` (`concept_counts`, `low_confidence_columns`)

Column-level optional enrichments:

- `semantic_class`
- `unit_context` (`detected_unit`, `canonical_unit`, `unit_system`, `conversion`, `detection_confidence`, `detection_source`, `notes`)
- `column_description` (always present; use source column comment/description when available, otherwise generate during analysis)
- `concept_id` (canonical domain-dot concept label such as `contact.email`)
- `concept_confidence` (0.0-1.0 score after evidence scoring and cross-table reconciliation)
- `concept_evidence` (ordered evidence strings)
- `concept_sources` (`name`, `type`, `values`, `profile`, `table_context`, `cross_table_consensus`)
- `concept_alias_group` (normalized alias used for reconciliation)

Backward compatibility:

- Existing legacy fields such as `field_classifications`, `sensitive_fields`, `data_category`, `semantic_class`, and `unit_context` remain valid.
- `concept_*` fields are additive and should be treated as optional enrichments.

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
- `unit_unknown`
- `unit_noncanonical_but_convertible`
- `unit_mismatch_within_semantic_group`
- `findings` (flat list for programmatic consumption)

If a section is not applicable for a source, emit an empty collection/object and keep overall shape consistent.
