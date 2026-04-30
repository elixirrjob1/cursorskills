---
name: dbt-cloud-log-extractor
description: Query the dbt Cloud MCP server to extract model run history, pass/fail status, execution times, and failing test details for a given time window. Returns a structured report in chat. Use when asked to check dbt Cloud logs, monitor model runs, review test failures, extract dbt run history, or report on dbt Cloud job health.
---

# dbt Cloud Log Extractor

Extract full model and test execution logs from dbt Cloud.

**Full log (default)** → run the Python script `scripts/fetch_dbt_logs.py` — one shot, no MCP spam.  
**Scoped / ad-hoc query** → use the `project-0-cursorskills-dbt` MCP tools directly.

## Prerequisites

- `.env` contains `DBT_ACCOUNT_ID`, `DBT_HOST`
- `dbt_project/drip_transformations/mcp.yml` has a valid refresh token  
  If the script prints "refresh token expired", re-authenticate:
  ```
  cd dbt_project/drip_transformations && uvx dbt-mcp auth
  ```

## Project constants

| Variable | Value |
|---|---|
| `account_id` | `70471823552613` |
| `prod_env_id` | `70471823537955` |

---

## Default workflow — full log via Python script

### Step 1 — Run the fetcher script

```bash
python3 scripts/fetch_dbt_logs.py --runs 10
```

Options:
- `--runs N` — number of recent runs to include (default 10)
- `--job <id>` — limit to a specific job ID

The script outputs JSON to stdout:

```json
{
  "model_runs": [
    {
      "executed_at": "2026-04-28 13:17:19 UTC",
      "job": "Dbt run full refresh",
      "model": "FactSales",
      "status": "success",
      "exec_time": "4.55s",
      "rows_affected": "508",
      "rows_inserted": "508",
      "rows_updated": "—",
      "rows_deleted": "—"
    }
  ],
  "test_runs": [
    {
      "executed_at": "2026-04-28 13:17:25 UTC",
      "job": "Dbt run full refresh",
      "model": "FactSales",
      "test": "not_null_SalesHashPK",
      "status": "pass",
      "exec_time": "1.23s"
    }
  ],
  "meta": { "runs_fetched": 10, "generated_at": "...", "account_id": "..." }
}
```

Row count fields:
- `rows_affected` — total rows touched (always populated when available)
- `rows_inserted` / `rows_updated` / `rows_deleted` — only populated for incremental `MERGE` runs; `"—"` otherwise

### Step 2 — Render in canvas

Read and follow `~/.cursor/skills-cursor/canvas/SKILL.md`, then build a canvas with:

1. **Stat grid** — total model runs, successes, errors, total tests, passes, fails
2. **dbt model runs** table
3. **dbt tests** table

## Output format

```
dbt model runs:
| DateTimestamp | Job | Model | Status | Execution Time | Rows Affected | Rows Inserted | Rows Updated | Rows Deleted |

dbt tests:
| DateTimestamp | Job | Model | Test | Status | Execution Time |
```

- **DateTimestamp**: `YYYY-MM-DD HH:MM:SS UTC`
- **Model**: short name only (`FactSales`, not `model.drip_transformations.FactSales`)
- **Status**: `success` / `error` / `skipped` — apply `rowTone` (`success` / `danger` / `warning`)
- **Execution Time**: seconds to 2 dp, e.g. `4.55s`
- **Rows Affected / Inserted / Updated / Deleted**: integer string or `—` when unavailable

---

## Scoped workflow — MCP tools (single model or run)

When the user limits scope (e.g. "show me FactSales" or "last 3 runs"):

```
get_model_performance(
  unique_id  = "model.drip_transformations.<Name>",
  num_runs   = <N>,
  include_tests = true
)
```

Skip the Python script entirely. Extract fields directly from MCP response and render canvas.

---

## Known models

All belong to `drip_transformations`. `unique_id` format: `model.drip_transformations.<Name>`

**Facts:** `FactSales`, `FactPurchaseOrder`, `FactInventorySnapshot`  
**Dims:** `DimCustomer`, `DimProduct`, `DimEmployee`, `DimStore`, `DimSupplier`, `DimWarehouse`, `DimDate`  
**Views:** prefix `vw_` on each of the above

## Known recurring issues (as of Apr 2026)

- `FactPurchaseOrder` — BINARY→NUMBER type mismatch on `PURCHASEORDERHASHPK`; fix with `dbt run --full-refresh`. **Any column data type change requires a full refresh** — incremental runs do not alter existing column types and will fail.
- `vw_DimEmployee` — `Department` and `IsActive` nulls from bronze source (data quality)
- Unit tests on `vw_DimCustomer` / `vw_DimEmployee` — require schema `DBT_UNIT_TEST_RUN` in Snowflake
