---
name: dbt-cloud-log-extractor
description: Query the dbt Cloud MCP server to extract model run history, pass/fail status, execution times, and failing test details for a given time window. Returns a structured report in chat. Use when asked to check dbt Cloud logs, monitor model runs, review test failures, extract dbt run history, or report on dbt Cloud job health.
---

# dbt Cloud Log Extractor

Extract full model and test execution logs from dbt Cloud via the `project-0-cursorskills-dbt` MCP server.

## Prerequisites

- `project-0-cursorskills-dbt` MCP server must be active in Cursor
- Env vars in `.env`: `DBT_ACCOUNT_ID`, `DBT_PROD_ENV_ID`
- Log retention in dbt Cloud: **365 days** — any time window within the last year is queryable

## Project constants

| Variable | Value |
|---|---|
| `account_id` | `70471823552613` |
| `prod_env_id` | `70471823537955` |

## Default workflow — full log for all models

Run all steps below by default, every time, unless the user scopes the request to a single model or job.

### Step 1 — Discover all models

```
get_all_models()
```

Collect every model's `unique_id` (format: `model.drip_transformations.<Name>`).

### Step 2 — Fetch performance + test logs per model (parallel)

For **every** model discovered in Step 1:

```
get_model_performance(
  unique_id  = "<model unique_id>",
  num_runs   = 20,          # retrieve last 20 executions
  include_tests = true      # always include test execution history
)
```

From each run entry extract:
- `executedAt` (ISO timestamp) — the run start time
- `status` — `success`, `error`, `skipped`
- `executionTime` — duration in seconds
- `tests[]` — array of `{ name, status, executionTime }` for that run

### Step 3 — Build two flat log lists

Flatten all model runs and all test runs across every model into two separate lists:

**Model runs list** — one row per (model × run):
- `executedAt`, `model_name` (short name, no package prefix), `status`, `executionTime`

**Test runs list** — one row per (test × run):
- `executedAt`, `model_name` (the model the test belongs to), `status`, `executionTime`

Sort each list by `executedAt` descending (newest first).

### Step 4 — Render in canvas

Always render the output as a canvas. Read and follow the canvas skill at `~/.cursor/skills-cursor/canvas/SKILL.md`.

## Output format

The canvas must contain exactly two sections:

```
dbt model runs:
| DateTimestamp | Job | Model | Status | Execution Time |

dbt tests:
| DateTimestamp | Job | Model | Test | Status | Execution Time |
```

- **DateTimestamp**: formatted as `YYYY-MM-DD HH:MM:SS UTC`
- **Model**: short model name only (e.g. `FactSales`, not `model.drip_transformations.FactSales`)
- **Status**: render as text — `success`, `error`, `skipped`; apply `rowTone` (`success` / `danger` / `warning`) accordingly
- **Execution Time**: seconds rounded to 2 decimal places, e.g. `1.23s`

Use a `Stat` grid above the tables to summarise: total model runs, success count, error count, total tests run, test pass count, test fail count.

## Scoped requests

When the user limits the scope (e.g. "show me only FactSales" or "last 5 runs"):
- Pass `unique_id` directly to `get_model_performance` and skip Step 1
- Adjust `num_runs` to match the requested window

## Known models

All belong to `drip_transformations`. Use `unique_id` format `model.drip_transformations.<Name>`:

**Facts:** `FactSales`, `FactPurchaseOrder`, `FactInventorySnapshot`  
**Dims:** `DimCustomer`, `DimProduct`, `DimEmployee`, `DimStore`, `DimSupplier`, `DimWarehouse`, `DimDate`  
**Views:** prefix `vw_` on each of the above

## Known recurring issues (as of Apr 2026)

- `FactPurchaseOrder` — type mismatch `VARCHAR(128)` → `BINARY(64)` on `PURCHASEORDERHASHPK` (causes cascade of skipped tests)
- `vw_DimEmployee` — `Department` and `IsActive` columns have null values from bronze source (data quality failures)
- Unit tests on `vw_DimCustomer` / `vw_DimEmployee` — require schema `DBT_UNIT_TEST_RUN` to exist in Snowflake
