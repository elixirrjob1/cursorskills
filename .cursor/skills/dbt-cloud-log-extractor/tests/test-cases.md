# Eval Suite: dbt-cloud-log-extractor

_Re-run this suite after every skill update to catch regressions._

## Should-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Check dbt Cloud logs for the last 10 runs" | Default workflow: agent runs `python3 scripts/fetch_dbt_logs.py --runs 10`, then builds a canvas with stat grid + dbt model runs table + dbt tests table |
| 2 | "Show me the last 5 FactSales runs including tests" | Scoped workflow: agent calls `get_model_performance(unique_id="model.drip_transformations.FactSales", num_runs=5, include_tests=true)` directly via MCP — skips Python script |
| 3 | "Extract dbt run history for job 123" | Full log with job filter: agent runs `python3 scripts/fetch_dbt_logs.py --job 123`, renders canvas |
| 4 | "Report on dbt Cloud job health — any failures in the last 20 runs?" | Runs fetcher with `--runs 20`, identifies failed models/tests in output, includes in canvas |
| 5 | "Review test failures from the last dbt run" | Triggers log extraction, filters `test_runs` array for `status != pass`, renders failing tests in canvas tests table |

## Should-not-trigger

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Run the dbt models now" | dbt execution task — skill covers log extraction, not triggering runs; agent should not invoke this skill |
| 2 | "Show me Snowflake query history" | Wrong platform — this skill is dbt Cloud only; agent answers from general knowledge |
| 3 | "What's the status of our CI/CD pipeline?" | General DevOps — not a dbt Cloud log question; agent asks for clarification or answers generally |

## Edge cases

| # | Input | Expected behavior |
|---|-------|-------------------|
| 1 | "Get dbt logs" but the refresh token is expired | Agent detects "refresh token expired" message from script, instructs user to run `cd dbt_project/drip_transformations && uvx dbt-mcp auth` to re-authenticate |
| 2 | "Show me only the last 3 DimCustomer runs" | Scoped MCP path: `get_model_performance(unique_id="model.drip_transformations.DimCustomer", num_runs=3)` — skips Python script entirely |
| 3 | "Show me the dbt logs for FactPurchaseOrder and warn me about known issues" | Retrieves logs for FactPurchaseOrder, and surfaces the known recurring issue about BINARY→NUMBER type mismatch on PURCHASEORDERHASHPK with the full-refresh fix recommendation |
