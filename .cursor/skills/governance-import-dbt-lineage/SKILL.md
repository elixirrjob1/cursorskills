---
name: governance-import-dbt-lineage
description: Import dbt model dependency lineage into OpenMetadata using dbt manifest artifacts and OpenMetadata REST APIs. Use when the user asks to add, sync, or refresh dbt lineage in OpenMetadata.
---

# Governance | Import dbt lineage

Imports lineage from dbt artifacts into OpenMetadata so upstream/downstream
dependencies in dbt are visible in the catalog lineage graph.

Supports:

- table-level lineage edges (default)
- optional inclusion of view-model nodes (`--include-views`)
- optional same-name column lineage mappings (`--include-column-lineage`)

## When to use

- User asks to add dbt lineage to OpenMetadata
- dbt models already exist in OpenMetadata tables
- You need an idempotent lineage refresh after dbt model changes

## Preconditions

- OpenMetadata table entities are already ingested and discoverable.
- dbt `manifest.json` is available (generate with `dbt parse` if missing).
- OpenMetadata connection and credentials come from the environment only (no hardcoded hosts or secrets in the script):
  - `OPENMETADATA_BASE_URL` (include scheme, e.g. `https://host:port`; for `http://`, set `OPENMETADATA_ALLOW_INSECURE_HTTP=true` when you intentionally accept cleartext).
  - `OPENMETADATA_EMAIL`
  - `OPENMETADATA_PASSWORD`

Optional:

- `--env-file PATH` or env `OPENMETADATA_ENV_FILE`: explicit dotenv path.
- Defaults load only `cwd/.env` unless `OPENMETADATA_DOTENV_WALK_PARENTS=true` restores parent-directory search.
- `OPENMETADATA_DEBUG_ERRORS=true` to include API response snippets in failure output (avoid in shared logs).

## Workflow

Copy this checklist and track progress:

```text
- [ ] Step 1: Confirm dbt artifact path and OpenMetadata service name
- [ ] Step 2: Run importer in dry-run mode and review unresolved entities
- [ ] Step 3: Fix service/database/schema mapping if needed
- [ ] Step 4: Run importer in write mode
- [ ] Step 5: Spot-check lineage graph in OpenMetadata UI/API
```

### Step 1 — Confirm inputs

Required:

- `manifest.json` path
- OpenMetadata database service name (for table FQN composition)

Optional:

- `--default-database` override when dbt manifest database values do not match
  OpenMetadata naming.
- `--schema-map` when manifest schema differs from catalog schema, for example
  `DBT_DEV:DBT_PROD,DBT_DEV_ENRICHED:DBT_PROD_ENRICHED`.

### Step 2 — Dry run

```bash
python .cursor/skills/governance-import-dbt-lineage/scripts/import_dbt_lineage_to_openmetadata.py \
  --manifest dbt_project/drip_transformations/target/manifest.json \
  --service snowflake_fivetran \
  --default-database DRIP_DATA_INTELLIGENCE \
  --schema-map DBT_DEV:DBT_PROD,DBT_DEV_ENRICHED:DBT_PROD_ENRICHED \
  --include-views \
  --include-column-lineage \
  --dry-run
```

Review output:

- `[DRY]` rows are valid lineage edges that can be written
- `[SKIP] unresolved entity` rows must be fixed before write mode
- When `--include-column-lineage` is enabled, each dry line includes
  `column mappings: <n>`

### Step 3 — Write lineage

```bash
python .cursor/skills/governance-import-dbt-lineage/scripts/import_dbt_lineage_to_openmetadata.py \
  --manifest dbt_project/drip_transformations/target/manifest.json \
  --service snowflake_fivetran \
  --default-database DRIP_DATA_INTELLIGENCE \
  --schema-map DBT_DEV:DBT_PROD,DBT_DEV_ENRICHED:DBT_PROD_ENRICHED \
  --include-views \
  --include-column-lineage
```

The script writes lineage with `PUT /api/v1/lineage` and prints `[OK]`/`[ERR]`
per edge plus a final summary.

Column lineage behavior:

- Generated only when `--include-column-lineage` is enabled.
- Uses case-insensitive same-name matching between upstream and downstream
  OpenMetadata table columns.
- If a table pair has no overlapping names, table-level lineage is still written.

## Guardrails

- Do not assume service name; confirm it first.
- Do not ignore unresolved entities; fix ingestion/mapping first.
- Run dry-run before write mode unless the user explicitly asks to skip it.
- Treat the run as incomplete if write errors remain.
- If you use `--include-views`, ensure view entities are also ingested in
  OpenMetadata.
- Prefer HTTPS for `OPENMETADATA_BASE_URL`; use `OPENMETADATA_ALLOW_INSECURE_HTTP=true` only when you knowingly accept HTTP.

- Detailed behavior and troubleshooting: [reference.md](reference.md)

