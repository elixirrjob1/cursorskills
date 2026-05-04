---
name: natural-language-data-query
description: Answer business users' natural language data questions by querying OpenMetadata for metadata context, generating SQL, and executing against Snowflake. Use when a business user asks any data question (sales, customers, orders, inventory, finance, HR, supply chain, etc.), wants results without writing SQL, or needs metrics, trends, summaries, or reports from Snowflake. Triggers: "what were the sales", "how many customers", "show me data about", "query the data", "can you look up", OpenMetadata, Snowflake, business question, data question, report, metric.
---

# Natural Language Data Query

Translates any business question into a Snowflake SQL result using OpenMetadata metadata discovery via the native OpenMetadata MCP server.

## Setup (once per environment)

```bash
pip install -r scripts/requirements.txt
```

OpenMetadata is connected via the MCP server configured in `.cursor/mcp.json` — no extra steps needed for metadata discovery. The `openmetadata` MCP server must appear in Cursor's MCP panel as **connected** before using this skill. If it shows as disconnected, check that `OPENMETADATA_API_URL` / credentials in `.cursor/mcp.json` are reachable.

---

## Credential Management

### OpenMetadata — via MCP (automatic)

OpenMetadata credentials are stored in the workspace `.env` file (gitignored) and loaded
automatically by the MCP bridge (`om_mcp_bridge.py`). The bridge uses this priority:

| Priority | Source | Notes |
|---|---|---|
| 1 | `SECRETS_PROVIDER` env var | Production — Azure KV / AWS / HashiCorp Vault |
| 2 | `OPENMETADATA_*` env vars | From `.env` (auto-loaded) or `mcp.json` env block |

The agent **never** needs to pass OpenMetadata credentials to any script — the MCP connection handles authentication.

### Snowflake — for execute and resolve commands

Snowflake credentials are needed to run `query_engine.py execute` and `query_engine.py resolve`. They are auto-loaded from `.env` — no CLI args required in development. Resolution order:

| Priority | Source | When to use |
|---|---|---|
| 1 | **Cloud secrets manager** | Production — `SECRETS_PROVIDER` + `SECRETS_URL` env vars set |
| 2 | **`.env` file** (auto-loaded) | Dev — gitignored, single source of truth |
| 3 | **Ask the user** | Last resort only |

**Env vars for Snowflake (set in `.env`):**
```
SNOWFLAKE_BEARER_TOKEN     # OAuth Bearer / PAT (preferred — works behind PrivateLink)
SNOWFLAKE_SQL_API_HOST     # Full SQL API URL
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_ROLE
SNOWFLAKE_ACCOUNT          # Fallback: account locator (connector mode)
SNOWFLAKE_USERNAME         # Fallback: username (connector mode)
SNOWFLAKE_PASSWORD         # Fallback: password (connector mode)
```

---

## Workflow

### Step 1 — Collect Snowflake credentials + business question

Before asking the user anything, resolve Snowflake credentials automatically (env vars → reference.md → ask). Also collect the user's **natural language business question** if not already provided.

OpenMetadata credentials are already handled by the MCP server — skip them here.

### Step 2 — Discover metadata via OpenMetadata MCP tools

Use the `openmetadata` MCP server tools directly. **Do not** run `query_engine.py` for discovery.

**2a. Keyword search — always run first:**

Call `search_metadata` with the most relevant keywords extracted from the question:
- Strip time expressions ("last quarter", "last year") and common stop words
- Keep business entity terms ("orders", "sales", "revenue", "customers", "products")
- Pass `entityType: "table"` and `size: 8`

Example for "How many orders were placed last year?":
```
search_metadata(query="orders", entityType="table", size=8)
```

**2b. Semantic search — run for vague or exploratory questions:**

If the keyword search returns few or unrelated results, also call `semantic_search` with the full natural language question:
```
semantic_search(query="How many orders were placed last year?", size=5)
```

**What to look for in results:**
- `fullyQualifiedName` — the FQN to pass to `get_entity_details`
- `tags` — includes `Certification.Gold/Silver/Bronze` and `Architecture.Raw/Enriched/Curated` for layer classification
- `description` — confirms whether the table is relevant

### Step 2.5 — Get full entity details + resolve Snowflake casing

**Part A — Full metadata for each candidate table (MCP):**

For every table that looks relevant from Step 2, call `get_entity_details`:
```
get_entity_details(entityType="table", fqn="<fullyQualifiedName from Step 2>")
```

This returns:
- All columns with data types and descriptions
- Table constraints (PK / FK) and join history
- View definition (if applicable)
- Full tag set (for layer classification)

**Part B — Exact Snowflake identifier casing (script):**

**Always run this before generating SQL.** OpenMetadata normalises all identifiers to UPPERCASE on ingestion — Snowflake objects may be mixed-case and will fail with `invalid identifier` without correct quoting.

For each table you plan to use:
```bash
python <skill-folder>/scripts/query_engine.py resolve \
  --tables "<DATABASE>.<SCHEMA>.<TABLE1>,<DATABASE>.<SCHEMA>.<TABLE2>" \
  --sf-account "<snowflake-account>" \
  --sf-user "<snowflake-username>" \
  --sf-password "<snowflake-password>" \
  --sf-warehouse "<snowflake-warehouse>" \
  --sf-database "<snowflake-database>"
```

With secrets manager:
```bash
python <skill-folder>/scripts/query_engine.py resolve \
  --tables "<DATABASE>.<SCHEMA>.<TABLE>" \
  --secrets-provider azure-keyvault \
  --secrets-url "https://<vault-name>.vault.azure.net"
```

The command returns a `resolved_tables` map with:
- `exact_table` — real Snowflake table name with correct casing
- `quoted_ref` — full `DATABASE.SCHEMA."ExactTable"` reference ready for SQL
- `columns` — mapping of `UPPERCASENAME → ExactSnowflakeName` for every column

Use **only** the `exact_table` and `columns` values from this output when writing SQL. Never use the uppercase names from Step 2 directly.

### Step 3 — Synthesize the SQL query

Using the entity details from Step 2.5 and the resolve output, construct an accurate Snowflake SQL query:

#### Layer preference — driven by metadata, not naming conventions

Use two signals from `get_entity_details` output to determine data layer. Do NOT infer layer from table or schema names — those vary per client.

**1. `tags`** — look for `Certification.Gold`, `Certification.Silver`, `Certification.Bronze`, `Architecture.Curated`, `Architecture.Enriched`, `Architecture.Raw`. These are set by the data team and are the most authoritative signal.

**2. Schema context** — if multiple tables from different schemas appear in results, prefer the one whose schema tag indicates a more curated layer.

**Decision logic (apply in order):**

1. Group all candidate tables by layer.
2. If any **Gold/Curated** tables can answer the question → use them. Do not check lower layers.
3. If no Gold table covers the question → check whether a more curated schema might have relevant tables not returned by the search. If so, call `search_metadata` again with broader or different keywords.
4. Only fall back to Silver/Bronze when Gold genuinely lacks the required columns or grain.
5. Never mix layers in the same query without a clear reason.

**Grain awareness**: curated fact/event tables are often at line-item grain. Read the table description. If finer-grained than the question requires, use `COUNT(DISTINCT <id_column>)` not `COUNT(*)`.

**Date dimension**: if a calendar/date table exists in the same schema (visible in search results), prefer joining to it for date filtering using its pre-computed attributes rather than raw date arithmetic.

#### Identifier casing — always double-quote everything

**OpenMetadata normalises all identifiers to UPPERCASE. Snowflake objects may be mixed-case.** Unquoted uppercase references will fail with "invalid identifier" errors.

Rules:
- **Always** wrap every table name and every column name in double quotes: `"FactSales"."TransactionNumber"` not `FACTSALES.TRANSACTIONNUMBER`
- Use the `quoted_ref` value from the `resolve` output for the full table reference
- Use the `columns` map from `resolve` for exact column names; map `UPPERCASENAME → ExactSnowflakeName`
- If `resolve` was skipped (Snowflake not yet available), still double-quote using the OpenMetadata names and note to the user that casing may need adjustment

#### General SQL rules
- **Table mapping**: match business concepts to tables/columns via `description` and `tags`
- **Joins**: use `tableConstraints` (FK/PK) and `joins.columnJoins` (join frequency) to identify correct join paths
- **Qualified names**: always reference tables as `DATABASE.SCHEMA."table"` using the FQN from metadata
- **Views first**: if a view definition covers the question, prefer it over raw base tables
- **Date handling**: use Snowflake date functions (`DATEADD`, `DATE_TRUNC`, `LAST_DAY`) when no date dimension is available
- **Time anchoring**: never anchor relative time expressions ("last quarter", "last year") to `CURRENT_DATE()` directly. First derive the most recent date in the fact data (`SELECT MAX(date_col) FROM fact JOIN dim`) and anchor relative periods to that. This ensures queries work for both live and historical/demo datasets
- **Aggregations**: apply `SUM`, `COUNT`, `AVG`, `GROUP BY` as required
- **Safety**: add `LIMIT 1000` unless the user explicitly asks for a full dataset or a COUNT
- Show the SQL to the user, state which layer was used and why, then execute
- **Never include `USE ROLE`, `USE DATABASE`, or `USE WAREHOUSE` statements in generated SQL** — these cause "statement count mismatch" errors when submitted via connectors or REST API. If a role switch is needed, tell the user to run it separately first

### Step 4 — Execute the SQL on Snowflake

**Option A — secrets manager (production, SECRETS_PROVIDER env var is set):**
```bash
python <skill-folder>/scripts/query_engine.py execute \
  --sql "<generated SQL>" \
  --secrets-provider azure-keyvault \
  --secrets-url "https://<vault-name>.vault.azure.net"
```

**Option B — direct credentials (dev/CI, from env vars or reference.md):**
```bash
python <skill-folder>/scripts/query_engine.py execute \
  --sql "<generated SQL>" \
  --sf-account "<snowflake-account>" \
  --sf-user "<snowflake-username>" \
  --sf-password "<snowflake-password>" \
  --sf-warehouse "<snowflake-warehouse>" \
  --sf-database "<snowflake-database>"
```

### Step 5 — Present results to the business user

- **Lead with the direct answer** (e.g., "Total sales for Product X last month were $1.23M")
- Present tabular data as a readable markdown table (max ~20 rows inline; offer to export more)
- Call out key metrics, trends, or comparisons
- State any assumptions made (date range, NULL handling, filters applied)
- Offer to refine, drill down, or add breakdowns

---

## Error Handling

| Error | Action |
|---|---|
| `openmetadata` MCP server disconnected | Check `.cursor/mcp.json` credentials; verify the OM instance is reachable at the configured URL; restart Cursor to reconnect |
| `search_metadata` returns 0 results | Broaden keywords; try `semantic_search` with the full question |
| `search_metadata` returns unrelated tables | Use more specific terms; filter by `entityType: "table"` |
| Query returns NULL on aggregation | The WHERE clause likely filters out all rows — check that the time period actually exists in the data by running `SELECT MIN(date_col), MAX(date_col) FROM fact JOIN dim`. Metadata is trusted in production — do not assume column documentation is wrong |
| SQL syntax / execution error | Read the error, fix the SQL, re-execute once before asking for help |
| `invalid identifier` on column | Re-run Step 2.5 Part B (`resolve` command) to get exact Snowflake column casing, then regenerate SQL |
| `object does not exist` on table | First check role (`SELECT CURRENT_ROLE()`); if correct, re-run `resolve` command to get exact table name, then regenerate SQL |
| Auth failure (Snowflake) | Verify account identifier, user, password, and role permissions |
| Snowflake account format error | Ensure account does not include `.snowflakecomputing.com` (script strips it automatically) |
| Secrets provider access denied | Azure: run `az login`; AWS: check IAM; HC Vault: verify VAULT_TOKEN |

---

## Additional Resources

- For Snowflake date patterns, secrets key names, and provider setup guides, see [reference.md](reference.md)
- Credentials live in `.env` (gitignored) — never in reference.md or committed files
- OpenMetadata MCP tools: `search_metadata`, `semantic_search`, `get_entity_details`, `get_entity_lineage`
- OpenMetadata MCP documentation: `{OM_URL}/how-to-guides/mcp`
