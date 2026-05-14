# Test API Schema Mapping

Map test API resources to the shared schema contract:

- API resource name -> `table`
- Provider namespace -> `schema` (use `api_test` if no namespace exists)
- Fields from payload -> `columns`
- ID fields (`id`, `*_id`) -> `primary_keys` / candidate `foreign_keys`
- Endpoint counts or sampled counts -> `row_count`

When relational constraints are not explicit in payload metadata, leave constraints empty and emit informational findings.
