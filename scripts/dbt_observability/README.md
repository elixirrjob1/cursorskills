# dbt Cloud observability in Snowflake

This folder holds DDL and analyzer config for landing **dbt Cloud** run metadata, step logs (including debug logs), and `run_results.json` node rows into Snowflake, then running the **source system analyser** on that schema.

## 1. Sync (extract + load)

From the repo root (with `.env` containing dbt + Snowflake variables):

```bash
pip install -r requirements.txt
python3 scripts/sync_dbt_observability_to_snowflake.py --runs 15
```

Dry-run (dbt API only, no Snowflake writes):

```bash
python3 scripts/sync_dbt_observability_to_snowflake.py --runs 3 --dry-run
```

### Environment variables

**dbt Cloud** (see [`scripts/dbt_cloud_client.py`](../dbt_cloud_client.py)):

- `DBT_ACCOUNT_ID`, `DBT_HOST`
- Prefer OAuth: **`mcp.yml`** written by **`dbt-mcp`** / Cursor’s dbt MCP — the client uses the **cached `access_token`** in that file first (same credential the MCP server uses), then refresh-token exchange, then **`DBT_PAT`**.

**Snowflake**:

- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` (or `SNOWFLAKE_DRIP_DATABASE` / `SNOWFLAKE_FIVETRAN_DATABASE`)
- User: `SNOWFLAKE_USERNAME` **or** `SNOWFLAKE_USER` **or** `SNOWFLAKE_FIVETRAN_USER`
- Password: `SNOWFLAKE_PASSWORD` **or** `SNOWFLAKE_FIVETRAN_PASSWORD`
- Warehouse: `SNOWFLAKE_WAREHOUSE` **or** `SNOWFLAKE_FIVETRAN_WAREHOUSE` **or** `SNOWFLAKE_SQL_API_EXECUTION_WAREHOUSE`
- Optional role: `SNOWFLAKE_ROLE` **or** `SNOWFLAKE_FIVETRAN_ROLE`
- Optional: `SNOWFLAKE_DBT_OBSERVABILITY_SCHEMA` (default `DBT_OBSERVABILITY`)

Tables created: `DBT_RUNS`, `DBT_RUN_STEPS`, `DBT_NODE_RESULTS` (see [`ddl.sql`](ddl.sql)).

## 2. Schedule

Pick one: **cron** or **Airflow** task after dbt jobs, **GitHub Actions** on a timer, or a **dbt Cloud** post-hook / external orchestrator that runs the Python script. Fivetran’s built-in **dbt orchestration** only triggers transforms; it does not replicate these logs. If EL must live inside Fivetran, use the **Connector SDK** with the same API logic as this script (see stakeholder note below).

## 3. Source system analyser (Snowflake)

The analyser supports `--dialect snowflake` and a `snowflake://` SQLAlchemy URL.

1. Copy this folder’s `db-analysis-config.json` to the **current working directory** (the analyser loads it from `cwd`):

   ```bash
   cp scripts/dbt_observability/db-analysis-config.json .
   ```

2. Set `DATABASE_URL` to a Snowflake SQLAlchemy URL, for example:

   `snowflake://USER:PASSWORD@xy12345.us-east-1.aws/MY_DATABASE/MY_SCHEMA?warehouse=COMPUTE_WH&role=MY_ROLE`

   (URL-encode special characters in the password.)

3. Run (schema must match the observability schema, typically uppercase):

   ```bash
   python3 .cursor/skills/source-system-analyser/scripts/source_system_analyzer.py \
     "$DATABASE_URL" \
     .cursor/flat/dbt_observability_schema.json \
     DBT_OBSERVABILITY \
     --dialect snowflake
   ```

   Output file name is derived automatically (schema + dialect suffix).

4. Optional description enrichment (per source-system-analyser skill):

   ```bash
   python3 scripts/build_description_enrichment_checklist.py <written_schema_json>
   # fill proposed_description values, then:
   python3 scripts/apply_description_enrichment.py <written_schema_json> schema_description_checklist.json
   ```

Convenience wrapper (same steps 1 + 3 from repo root):

```bash
bash scripts/dbt_observability/run_analyzer.sh
```

(requires `DATABASE_URL` and a successful `cp` of `db-analysis-config.json` into cwd — the script copies from this directory).

## 4. Stakeholder note: Fivetran vs this script

**Fivetran’s native dbt integration** is **orchestration** (run dbt after loads), not a connector that copies **debug logs** into Snowflake. This repository implements **Phase A**: a small Python pipeline that calls the **dbt Cloud Administrative API** and writes tabular data to Snowflake. If the hard requirement is “EL must be a Fivetran connector,” port the same extraction logic to the **Fivetran Connector SDK** and keep these tables as the contract the analyser reads.
