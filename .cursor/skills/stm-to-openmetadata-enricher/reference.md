# Reference — STM → OpenMetadata Enricher

## Known MCP quirks

### `update_metadata_ingestion_pipeline` returns opaque 400

The OpenMetadata MCP's `update_metadata_ingestion_pipeline` (and `create_metadata_ingestion_pipeline`) tool returns `400 Bad Request` with no body for requests that succeed via raw REST. Observed with schema filter updates on an existing pipeline.

**Workaround**: Use `scripts/patch_pipeline_filter.py` which calls the OM REST API's JSON-patch endpoint directly. Confirmed working against OpenMetadata 1.5.x.

### `run_ingestion_pipeline` requires FQN, not short name

- ✅ `pipeline_name=snowflake_fivetran.snowflake_fivetran_metadata`
- ❌ `pipeline_name=snowflake_fivetran_metadata` → 404

### First `run_ingestion_pipeline` call only deploys the DAG

On some OM+Airflow setups, the first MCP call after a config update returns `"Workflow [...] has been created"` but does not actually queue a run. The second call triggers the run. `scripts/wait_for_ingestion.py` handles this: it polls for a new `pipelineStatuses.startDate` greater than the last known one and retriggers once if nothing appears after ~30 seconds.

### OM login requires base64-encoded password

The `/api/v1/users/login` endpoint expects the `password` field in the JSON body to be base64-encoded, not plaintext. Python one-liner:

```python
import base64
payload = {"email": email, "password": base64.b64encode(password.encode()).decode()}
```

## Snowflake identifier casing

Snowflake stores unquoted identifiers as UPPERCASE. OpenMetadata preserves that casing verbatim. STMs use PascalCase (`CustomerHashPK`, `DimCustomer`). The mapping is therefore:

| Artifact | Casing |
|---|---|
| STM Section 4 table name | `DimCustomer` |
| STM Section 7 column names | `CustomerHashPK` |
| Snowflake schema | `DBT_PROD_ENRICHED` |
| Snowflake table | `DIMCUSTOMER` |
| Snowflake column | `CUSTOMERHASHPK` |
| OpenMetadata table FQN | `<service>.<db>.DBT_PROD_ENRICHED.DIMCUSTOMER` |
| OpenMetadata column_name arg | `CUSTOMERHASHPK` |

The subagent should always run `get_table` first and use the exact strings OM returns, rather than reconstruct from STM names.

## Schemas to include

The dbt Cloud prod env writes to `DBT_PROD` (views) and `DBT_PROD_ENRICHED` (materialized tables). The local dev profile writes to `DBT_DEV` / `DBT_DEV_ENRICHED`.

| dbt source | Include in Snowflake **metadata** pipeline filter |
|---|---|
| dbt Cloud prod | `DBT_PROD_ENRICHED` only (not `DBT_PROD` — views are not catalogued) |
| Local `dbt run` | `DBT_DEV_ENRICHED` only (not `DBT_DEV`) |
| Bronze sources | e.g. `BRONZE_ERP__DBO` as needed |

Tagging and STMs always target the enriched schema; the view layer is omitted from OpenMetadata so the catalog does not duplicate dim/fact assets.

## Subagent concurrency limits

Empirically, 10 simultaneous subagents each making ~60 MCP calls against a single OpenMetadata instance will either:
- crash the MCP server's internal HTTP connection pool; or
- cause 2–3 subagents to stall on a tool call that never returns.

Safe limit: **4 parallel subagents**, each instructed to make calls sequentially. This keeps concurrent in-flight HTTP requests against OM in the single digits.

## Verification contract

A fully enriched table satisfies all of:

1. `table.description` is a non-empty string.
2. `len(table.tags) >= 1` (at minimum Architecture + Certification).
3. For every column `c` whose name appears in the STM Section 7 Final with a non-empty Description cell: `c.description` is a non-empty string.

`scripts/verify_enrichment.py` checks 1 and 2 automatically. Checking 3 requires parsing the STM; use the script's `--stm <path>` flag per table for a full check.

## Troubleshooting

### "Column X not found in OM"

The subagent will log columns that the STM references but that OM doesn't know about. Common causes:
- dbt regen didn't run; OM has the old schema.
- Column was removed from the dim/fact model without regenerating STMs.

Re-run the ingestion pipeline (Step 3) then retry the subagent for that STM only.

### Subagent hangs on first tool call

Observed when the MCP server is overloaded. Mitigations:
1. Reduce parallelism to ≤ 4 (see above).
2. Test MCP liveness with `test_connection` before launching the next batch.
3. If a subagent has been idle for > 5 minutes and its transcript file hasn't grown, consider it dead and relaunch its STM in a fresh subagent (enrichment is idempotent).

### 401 on PATCH after a while

OpenMetadata JWT tokens expire after a few hours. The helper scripts re-login automatically per invocation. If you're running things manually, re-run the login block before each PATCH.
