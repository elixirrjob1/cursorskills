# governance-import-dbt-lineage reference

## What the importer does

1. Loads dbt `manifest.json`.
2. Reads each `model` node dependency from `depends_on.nodes`.
3. Resolves dependency and target objects to table references:
   - database
   - schema
   - alias/identifier/name
4. Builds OpenMetadata table FQNs as:
   - `<service>.<database>.<schema>.<table>`
5. Fetches table entities by name from OpenMetadata.
6. Writes lineage edges with:
   - `PUT /api/v1/lineage`

Optional:

- Include dbt view nodes in lineage (`--include-views`).
- Add same-name column mappings under `lineageDetails.columnsLineage`
  (`--include-column-lineage`).

## Supported dbt resource types for lineage sources

- `model`
- `source`
- `seed`
- `snapshot`

Other dependency types are skipped and counted as unsupported.

## View handling

Default behavior skips view-model targets and flattens through view dependencies.

When `--include-views` is provided:

- view models are kept as lineage nodes
- edges like `source -> vw_* -> enriched_table` are written when resolvable

This requires matching view entities to exist in OpenMetadata.

## Column-level lineage support

OpenMetadata supports column lineage in `PUT /api/v1/lineage` under
`edge.lineageDetails.columnsLineage`.

When `--include-column-lineage` is provided, this importer adds case-insensitive,
same-name mappings using table column metadata returned by OpenMetadata:

- `fromColumns`: `<from-table-fqn>.<column>`
- `toColumn`: `<to-table-fqn>.<column>`

If there are no overlapping column names, the importer keeps table-level lineage
and reports the skip in summary counters.

## Idempotency

The importer uses OpenMetadata's lineage PUT API. Re-running the same manifest
refreshes existing edges rather than creating duplicate records.

## Configuration and security

- **No hardcoded endpoints or credentials** — values come only from env vars and CLI arguments (`OPENMETADATA_*`, `--manifest`, `--service`, `--schema-map`, `--env-file`).
- **HTTPS-first** — `http://` in `OPENMETADATA_BASE_URL` is rejected unless `OPENMETADATA_ALLOW_INSECURE_HTTP` is truthy (`true`, `1`, `yes`, `on`).
- **Scheme required** — base URL must include a scheme (`https://` or `http://` when explicitly allowed).
- **Dotenv** — default behavior loads only `./.env` in the current working directory. Use `OPENMETADATA_ENV_FILE`, `--env-file`, or `OPENMETADATA_DOTENV_WALK_PARENTS=true` when your layout differs.
- **Errors** — response bodies are not attached to errors unless `OPENMETADATA_DEBUG_ERRORS` is truthy.

### HTTP rejected / TLS policy

Cause:
- `OPENMETADATA_BASE_URL` uses `http://` and `OPENMETADATA_ALLOW_INSECURE_HTTP` is not set.

Fix:
- Prefer `https://` for production.
- Or set `OPENMETADATA_ALLOW_INSECURE_HTTP=true` only when you knowingly accept cleartext to that host.

## Common troubleshooting

### "unresolved entity" skips

Cause:
- OpenMetadata table not ingested yet, or naming mismatch.

Fix:
- Re-run metadata ingestion for missing schemas.
- Verify `--service` and `--default-database` values.
- Check table casing and naming conventions.

### Write errors on `/v1/lineage`

Cause:
- Missing permissions, invalid IDs, or API version mismatch.

Fix:
- Validate OpenMetadata user permissions for lineage write.
- Check OpenMetadata version supports `PUT /api/v1/lineage`.
- Re-run with `--dry-run` and resolve entity mapping first.

### Missing manifest

Generate manifest in dbt project:

```bash
cd dbt_project/drip_transformations
dbt parse
```

Then use `target/manifest.json` with the importer.

