# Flat Schema Mapping

Map file structures to shared output schema:

- filename/logical dataset -> `table`
- source container/folder -> `schema`
- parsed columns -> `columns`
- detected unique columns -> `primary_keys` candidates
- detected relation columns (`*_id`) -> `foreign_keys` candidates
- row count from file scan -> `row_count`

If key detection is uncertain, keep arrays empty and add recommendations.
