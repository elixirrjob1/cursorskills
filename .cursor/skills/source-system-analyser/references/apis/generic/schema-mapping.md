# API Schema Mapping

Map API resources to `tables` entries in shared output schema:

- Resource name -> `table`
- Parent namespace/provider -> `schema`
- Object fields -> `columns`
- Declared IDs -> `primary_keys`
- Link fields (e.g. `*_id`) -> candidate `foreign_keys`
- Record counts from pagination totals or sampled counts -> `row_count`

If constraints are unknown, emit empty arrays and create informational findings.
