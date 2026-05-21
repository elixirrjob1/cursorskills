---
name: setup-verifier
description: Verify a new team member's local setup by testing each MCP connection (Snowflake, OpenMetadata, dbt Cloud, Fivetran) and checking that required .env variables are present. Reports pass/fail per system with a specific fix for each failure. Use when the user explicitly asks to check setup, verify connections, validate environment, test MCP access, or run a setup check — not on bare statements that setup is done.
---

# Setup Verifier

Runs a live connection check against each MCP server and validates the `.env`. Produces a pass/fail report with specific remediation for each failure.

## Checks to run

Run all four in sequence. For each: attempt the MCP call, mark ✅ or ❌, record the error message if it fails.

### 1. Snowflake

Call the Snowflake MCP with:
```
project-0-cursorskills-snowflake:run_snowflake_query(statement: "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
```
Expected: a single row with non-null values.

**If it fails:**
- `connection refused` / `could not connect` → MCP server not running; restart Cursor or run `Cursor → Reload MCP`
- `Incorrect username or password` → wrong credentials in `mcp.json` or `.env`; re-check `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD`
- `Role … does not exist` → request the correct Snowflake role from a team admin

### 2. OpenMetadata

Call the OpenMetadata MCP to list database services:
```
user-openmetadata:list_database_services()
```
Expected: at least one service returned (HTTP 200).

**If it fails:**
- `401 Unauthorized` → `OM_TOKEN` in `.env` is missing or expired; re-generate a token from the OpenMetadata UI under Settings → Access Tokens
- `Connection refused` / no response → wrong `OM_BASE_URL`; confirm the URL with a team admin
- `403 Forbidden` → user lacks Viewer role; ask a team admin to grant it

### 3. dbt Cloud

Call the dbt MCP:
```
project-0-cursorskills-dbt:list_jobs()
```
Expected: list of at least one job.

**If it fails:**
- `401` / `invalid token` → dbt refresh token is expired; re-authenticate:
  ```bash
  cd dbtproject && uvx dbt-mcp auth
  ```
- `Account not found` → `DBT_ACCOUNT_ID` in `.env` is wrong; get the correct value from the dbt Cloud URL (Settings → Account)
- MCP not responding → dbt MCP server not started; open `dbtproject` in Cursor and wait for auto-start, or run `bash start-dbt-mcp.sh`

### 4. Fivetran

Call the Fivetran MCP:
```
user-fivetran-example:list_connectors()
```
Expected: at least one connector returned.

**If it fails:**
- `401` / `403` → `FIVETRAN_API_KEY` or `FIVETRAN_API_SECRET` in `.env` is wrong; get the values from Fivetran → Account → API Config
- Empty list (no connectors) → credentials are valid but the API key belongs to a group with no connectors; confirm the correct API key with a team admin

---

## .env variable check

After the MCP checks, verify that these keys are present (non-empty) in the active `.env`:

**dbtproject/.env — required:**
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_FIVETRAN_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DBT_USER`, `SNOWFLAKE_DBT_ROLE`, `SNOWFLAKE_DBT_WAREHOUSE`, `SNOWFLAKE_DBT_PASSWORD`, `DBT_HOST`, `DBT_ACCOUNT_ID`

**Skills/scripts `.env` — required:**
`OM_BASE_URL`, `OM_TOKEN`, `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`

Report any missing keys as ❌ with the note: *"Obtain from the team Key Vault — ask a team admin for the vault name"*.

---

## Output format

Present results as a table followed by a summary:

```
| System         | Status | Detail                          |
|----------------|--------|---------------------------------|
| Snowflake      | ✅     | Connected as USER / ROLE        |
| OpenMetadata   | ✅     | 2 services found                |
| dbt Cloud      | ❌     | 401 — token expired             |
| Fivetran       | ✅     | 4 connectors found              |
| .env variables | ⚠️     | DBT_PROD_ENV_ID missing (opt.)  |
```

Then:
- If all ✅: *"Setup complete. Follow §8 of ONBOARDING.md to run the first verification tasks."*
- If any ❌: list each failure with its specific fix from the sections above.
- Distinguish required failures (❌) from missing optional variables (⚠️).

---

## Gotchas

**Ask mode blocks live checks.** This skill requires Agent mode — live MCP calls will not execute in Ask/read-only mode. If asked in Ask mode:
1. Explain the mode constraint and ask the user to switch.
2. Still show the full planned output table with all four systems and the `.env` row listed (use `—` for status since no live check was run).
3. Surface at least one concrete example fix from the remediation sections above so the user can prepare in advance.

**Ambiguous statements.** Only trigger on an explicit request to check or verify setup. A bare statement like "my setup is done" is not a trigger — acknowledge it and ask if they want to run a verification check.

---

## Registry

| Field | Value |
|-------|-------|
| Owner | Platform / AI Engineering team |
| Reviewer | Peer or tech-lead review required before promoting to `plugins` |
| Version | v0.1 — git 0b3ce01 |
| Lifecycle stage | Create / Review — not yet promoted to Deploy |
| Last evaluated | 2026-05-19 (run_slug: 2026-05-19T093403Z) |
| Dependencies | project-0-cursorskills-snowflake MCP, user-openmetadata MCP, project-0-cursorskills-dbt MCP, user-fivetran-example MCP |
| Model compatibility | Validated on Claude 3.5 Sonnet / Cursor Agent mode |
| Repo | [responsum-team/skill-setup-verifier](https://github.com/responsum-team/skill-setup-verifier) |
