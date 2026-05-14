# API Endpoint Scoping Procedure

Use this document after the API Preflight in `SKILL.md` has produced `api-scope-config.json`.
Load the config file at the start and read all values from it without asking the user again.

---

## Phase 1 — Discovery

### 1a. OpenAPI/Swagger spec (preferred)

If `openapi_spec_url` is set in config:

1. Download/fetch the spec file (YAML or JSON).
2. Parse all `paths` entries.
3. For each path + method combination extract:
   - `method` (uppercase)
   - `path`
   - `summary` and `description`
   - `parameters`: for each — `name`, `in` (path/query/header), `required`, `type`
   - `responses.200` (or `201`) schema: resolve all `$ref` references; capture full field list with types (use `allOf`/`oneOf` flattening where needed)
   - `operationId` if present; derive a slug from path + method if absent
4. Filter out methods not in `methods_to_scope` (default `["GET"]`).
5. Assign `proposed_table` as `stg_{slug}` where slug is the operation ID or a sanitized path+method string.

### 1b. Live probe (fallback when no spec)

If `openapi_spec_url` is null or unreachable:

1. Probe these common spec paths (in order, stop at first 200):
   - `{base_url}/openapi.json`
   - `{base_url}/openapi.yaml`
   - `{base_url}/swagger.json`
   - `{base_url}/swagger.yaml`
   - `{base_url}/api-docs`
   - `{base_url}/v1/openapi.json`
   - `{base_url}/v2/openapi.json`
2. If a spec is found, proceed as 1a.
3. If no spec is found, probe the API root (`GET {base_url}/`) and collect any endpoint references from the response body (common in REST HAL/HATEOAS APIs).
4. Record all discovered paths; flag source as `live_probe_no_spec` in output metadata.

---

## Phase 2 — Sampling

For each endpoint discovered in Phase 1:

1. Skip endpoints with unresolvable path parameters (parameters marked `in: path` with no known value). Record as `probe_skipped: unresolved_path_params`.
2. For endpoints whose path parameters can be resolved (e.g. account-level IDs from config or a prior list-endpoint response), make one `GET` request:
   - Use auth from `api-scope-config.json` (see Auth Resolution below).
   - Timeout: 30 seconds.
3. Record for each request:
   - `status_code`
   - `response_time_ms`
   - `response_shape`: `object` or `array`
   - `top_level_keys`: list of top-level JSON keys in the response
   - `data_item_keys`: if response contains a `data` array, the keys of `data[0]`; otherwise the keys of the response object itself
   - `sample_error`: the error message if status >= 400 or request failed
4. Endpoints that return 4xx or 5xx: record error, set `probe_status: failed`, do **not** skip — include in output so the user can see gaps.
5. Endpoints with status 200/201: set `probe_status: ok`.

---

## Phase 3 — Entity Mapping

Goal: collapse endpoint-level data into business-entity target tables.

Rules:

1. **List + detail pair → one table**: if a `GET /resources/` and a `GET /resources/{id}/` both exist and return the same entity shape, merge them into one proposed table (`stg_{entity_name}`).
2. **Nested resource → child table**: if an endpoint returns a different entity nested inside the parent (e.g. `run_steps` inside a run), propose a separate table for that nested entity.
3. **Artifact/blob endpoints → raw table**: endpoints returning binary artifacts or free-form text (logs, SQL) propose a table with a single `payload` or `content` column.

For each proposed table produce:
- `table`: proposed destination table name
- `source_endpoints`: list of endpoint IDs that contribute to this table
- `columns`: union of all fields from spec schema + live-sampled keys; for each column:
  - `name`
  - `type` (from spec where available, else `string`)
  - `required`: true if declared required in spec
  - `description`: from spec description field; generate a short description if blank
- `primary_keys`: any field named `id` or ending in `_id` that appears consistently
- `foreign_keys`: candidate link fields (fields ending in `_id` that reference another entity)
- `table_description`: derived from endpoint summary/description; generate if blank

---

## Phase 4 — Output

### 4a. Write `endpoint_catalog.json`

Write to `.cursor/flat/{source_slug}_endpoint_catalog.json`.

Use this top-level structure (matches `.cursor/flat/dbt_logs_endpoint_catalog.json`):

```json
{
  "metadata": {
    "source": "<api name or base_url>",
    "version": "<spec version or 'live_probe'>",
    "generated_at": "<ISO timestamp>",
    "endpoint_count": 0,
    "table_blueprint_count": 0,
    "base_url": "<base_url from config>",
    "auth_type": "<from config>",
    "auth_env_var": "<variable name only, never value>"
  },
  "how_to_use": {
    "objective": "Map each endpoint response shape to staging tables for custom connectors.",
    "mapping_rule": "One entity to one staging table; flatten nested fields into destination columns."
  },
  "endpoints": [
    {
      "endpoint_id": "<slug>",
      "operation_id": "<operationId or derived>",
      "method": "GET",
      "path": "<path template>",
      "full_path_template": "<base_url + path>",
      "summary": "<from spec>",
      "description": "<from spec>",
      "tags": [],
      "parameters": [],
      "responses": {
        "200": {
          "description": "",
          "schema": {
            "type": "object",
            "fields": [],
            "item_fields": [],
            "notes": []
          }
        }
      },
      "probe_status": "ok | failed | skipped",
      "probe_status_code": 200,
      "probe_sample_keys": [],
      "probe_error": null,
      "proposed_table": "stg_<slug>"
    }
  ],
  "table_blueprints": [
    {
      "table": "stg_<slug>",
      "source_endpoints": ["<endpoint_id>"],
      "columns": [
        {
          "name": "<field>",
          "type": "<type>",
          "required": false,
          "description": "<description>"
        }
      ],
      "primary_keys": ["id"],
      "foreign_keys": [],
      "table_description": "<description>"
    }
  ]
}
```

### 4b. Write `schema.json`

Write to `.cursor/flat/{source_slug}_schema.json`.

The schema must satisfy `references/shared/output-schema.md` so it can be consumed by `ingestion-from-analyzer`. Use the exact key names below — they must match the DB analyzer output contract.

**Column shape** (use `column_description`, not `description`):

```json
{
  "name": "<field>",
  "type": "<type>",
  "required": false,
  "column_description": "",
  "nullable": true,
  "is_incremental": false,
  "data_category": "nominal | continuous | temporal | identifier | binary",
  "semantic_class": null,
  "unit_context": null,
  "cardinality": null,
  "null_count": null,
  "data_range": null,
  "concept_id": null,
  "concept_confidence": null,
  "concept_evidence": [],
  "concept_alias_group": null,
  "concept_sources": null,
  "classification_tags": [],
  "glossary_terms": []
}
```

**Table shape**:

```json
{
  "table": "<table_blueprint.table>",
  "schema": "api",
  "table_description": "",
  "columns": [],
  "primary_keys": [],
  "foreign_keys": [],
  "row_count": null,
  "has_primary_key": false,
  "has_foreign_keys": false,
  "has_sensitive_fields": false,
  "sensitive_fields": {},
  "cdc_enabled": false,
  "incremental_columns": [],
  "partition_columns": [],
  "join_candidates": [],
  "unit_summary": { "columns_with_units": 0, "columns_without_units": 0, "mixed_unit_groups": [], "unknown_unit_columns": [] },
  "classification_summary": { "concept_counts": {}, "low_confidence_columns": [] },
  "classification_tags": [],
  "glossary_terms": [],
  "field_classifications": {},
  "row_count_projection_1y": null,
  "row_count_projection_2y": null,
  "row_count_projection_5y": null,
  "data_quality": {
    "findings": [],
    "delete_management": { "strategy": "not_applicable_for_api" },
    "late_arriving_data": { "risk": "unknown" },
    "timezone": { "value": "UTC" }
  }
}
```

**Top-level shape**:

```json
{
  "metadata": {
    "generated_at": "<ISO timestamp>",
    "source_type": "api",
    "source_name": "<api name>",
    "provider": "<api provider>",
    "base_url": "<base_url>",
    "total_tables": 0,
    "total_rows": null,
    "total_findings": 0,
    "openmetadata_classifications": []
  },
  "connection": {
    "endpoint_summary": { "base_path": "<base_url>", "auth_type": "<from config>", "content_type": "application/json" },
    "driver_provider": "HTTP REST API",
    "timezone": "UTC"
  },
  "source_system_context": {
    "contacts": [],
    "delete_management_instruction": "API source; track deletions via status fields or tombstone records where available.",
    "restrictions": [],
    "system_description": "<one-sentence description of the API and its domain>",
    "openmetadata_enrichment": { "enabled": false, "configured": false },
    "late_arriving_data_manual": null,
    "volume_size_projection_manual": null
  },
  "data_quality_summary": {
    "critical": 0,
    "warning": 0,
    "info": 0,
    "by_check": {
      "endpoint_reachable": 0,
      "endpoint_failed": 0,
      "endpoint_skipped": 0,
      "sample_schema_captured": 0,
      "delete_management": 0,
      "late_arriving_data": 0
    },
    "constraints_found": { "check_constraints": 0, "enum_columns": 0, "unique_constraints": 0 }
  },
  "concept_registry": {
    "concepts": [],
    "note": "Concept classification not yet run. Execute concept pipeline against this schema to populate."
  },
  "tables": []
}
```

### 4b-post. Derive computed fields

After writing the initial `schema.json`, compute and fill these fields — they are derivable from the column list without any live API calls:

For each **table**:
- `has_primary_key` — `bool(primary_keys)`
- `has_foreign_keys` — `bool(foreign_keys)`
- `incremental_columns` — columns whose name matches `*_at`, `*_date`, `updated*`, `created*`, `started*`, `finished*` or whose type is `timestamp` / `datetime` / `date`
- `partition_columns` — subset of `incremental_columns` where name contains `_at` or `_date`
- `join_candidates` — list of FK column names already in `foreign_keys`
- `has_sensitive_fields` / `sensitive_fields` — scan column names for PII patterns: `email`, `phone`, `address`, `first_name`, `last_name`, `full_name`, `username`, `password`, `token`, `secret`, `ssn`, `dob`, `birth`
- `unit_summary.columns_without_units` — `len(columns)`
- `metadata.total_tables` — `len(tables)`
- `metadata.total_findings` — sum of `len(data_quality.findings)` across all tables
- `data_quality_summary.constraints_found.unique_constraints` — count of tables with `has_primary_key = true`

For each **column**:
- `nullable` — `not required`
- `is_incremental` — same pattern as `incremental_columns` above applied per column
- `data_category` — rule-based:
  - `identifier` if name ends in `_id` or equals `id`
  - `temporal` if type contains `timestamp`, `datetime`, or `date`
  - `continuous` if type contains `int`, `numeric`, `float`, `decimal`, `money`
  - `binary` if type is `boolean`
  - `nominal` otherwise

### 4c. Description Enrichment

After writing and computing `schema.json`, generate descriptions using Azure OpenAI — the same `AzureDescriptionGenerator` used by the database analyzer. This step is **not optional**.

Requires: `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT` in `.env`.

For each table, in order:
1. Generate `column_description` for every column — pass column name, type, nullable, PK/FK status, and a one-sentence API system description to the model.
2. Generate `table_description` from the completed column descriptions.

After generation, verify with the checklist builder:

```bash
.venv/bin/python scripts/build_description_enrichment_checklist.py <source_slug>_schema.json
```

If it reports zero pending items, descriptions are complete. If any remain, generate and apply:

```bash
.venv/bin/python scripts/apply_description_enrichment.py <source_slug>_schema.json <source_slug>_schema_description_checklist.json
```

If Azure OpenAI is unavailable, fall back to rule-based generation using `_generate_column_description` and `_generate_table_description` from `source_system_analyzer.py` — but flag the schema as `descriptions_generated_by: rule_based` in metadata.

### 4d. Gate A — User Confirmation

After writing both files and completing description enrichment, present this summary table to the user:

| Endpoint | Method | Proposed Table | Probe Status | Notes |
|----------|--------|----------------|--------------|-------|
| ... | GET | stg_... | ok / failed / skipped | ... |

Ask:
- Are there any endpoints you want to exclude from ingestion?
- Are there any proposed table names you want to rename?
- Do you want to proceed to connector generation (ingestion-from-analyzer)?

Record any exclusions and renames, update `table_blueprints` in the catalog, then confirm output is ready.

---

## Auth Resolution

Follow `references/apis/generic/auth.md` and the `api-reader-auth.mdc` rule:

1. Read `auth_type` and `auth_env_var` from `api-scope-config.json`.
2. Resolve the token value at runtime:
   - Check `.env` for the variable name.
   - If not found and `KEYVAULT_NAME` is set, fetch from Key Vault using the variable name as the secret name.
3. Never print, log, or output the token value in any file or response.
4. Always report which variable name (`auth_env_var`) was used to resolve the token.
5. If the variable cannot be resolved, stop and ask the user to confirm the correct variable name before making any authenticated calls.
6. Persist the confirmed variable name (name only) to `references/apis/generic/discovered-references.md`.

---

## Common Mistakes

- **Re-running preflight on rerun**: always check for `api-scope-config.json` first; reuse silently.
- **Skipping failed endpoints**: always include 4xx/5xx endpoints in output with `probe_status: failed`; do not silently drop them.
- **Using endpoint name as table name directly**: group list+detail pairs into one table; do not create two tables for the same entity.
- **Using `description` instead of `column_description`**: the contract field is `column_description`. Using `description` causes the checklist builder to see all columns as blank and breaks `apply_description_enrichment.py`.
- **Wrong `data_quality_summary` key names**: use `critical`/`warning`/`info`/`by_check`/`constraints_found` — not `severity_counts`/`per_check_totals`. Mismatched keys break downstream consumers and the Excel exporter.
- **Skipping Phase 4b-post derived fields**: `has_primary_key`, `is_incremental`, `data_category`, `sensitive_fields`, `join_candidates`, etc. are all derivable without any live calls — always compute them before Gate A.
- **Skipping description enrichment**: Phase 4c is mandatory. Use `AzureDescriptionGenerator` (Azure OpenAI) — not manual AI fill and not just the checklist alone. Fall back to rule-based only when Azure OpenAI is unavailable.
- **Leaving blank descriptions**: generate descriptions for tables and columns where spec provides none.
- **Exposing auth values**: only variable names are written to files; never token or secret values.
