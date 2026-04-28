---
name: ingestion-from-analyzer

---

# Fivetran setup from analyzer JSON

## When to use

Apply when the user provides (or points to) a **`schema.json`** produced by the **source-system-analyser** skill (normalized contract in `references/shared/output-schema.md`). Translate that artifact into **actionable Fivetran** recommendations—not to run Fivetran APIs unless the user asks.

## Inputs

1. **Path or pasted JSON** of the analyzer output (`metadata`, `connection`, `source_system_context`, `data_quality_summary`, `tables`, optional `concept_registry`).
2. Optionally: **destination** type (e.g. Postgres, Snowflake, BigQuery) and **constraints** (no CDC permissions, Azure SQL elastic pools, etc.).

If the file path is omitted, check `.cursor/flat/` for recent `schema*.json` and ask which file to use if ambiguous.

## Workflow

### 1. Validate shape

Confirm top-level keys align with the shared contract: at least `metadata`, `connection`, `tables`. Note gaps (e.g. missing `source_system_context`) in the report.

If `source_system_context.db_analysis_config` is present, read:

- `exclude_schemas`
- `exclude_tables`
- `max_row_limit`

Treat `exclude_schemas` and `exclude_tables` as authoritative source-side exclusions. Carry them into Fivetran recommendations so those schemas/tables are not proposed for sync, onboarding, or rollout.
Preserve the exclusion order exactly as it appears in the JSON. Do not sort or regroup it unless the user explicitly asks.

### 2. Map source → Fivetran connector type

Use `connection.driver` / implied dialect:

| Analyzer driver / pattern | Fivetran connector (typical) |
|---------------------------|------------------------------|
| `mssql`, SQL Server       | `sql_server` |
| `postgresql`              | `postgres` |
| `mysql`                   | `mysql` |
| `oracle`                  | `oracle` |
| API / SaaS                | Provider-specific connector (not covered by DB table mapping alone) |

Document **assumptions** if the source is hybrid or unclear.

### 3. Connection-level parameters (recommendations)

From **`connection`** + **`metadata`** + **`data_quality_summary`** + **`source_system_context`**:

- **Host / database / port**: echo for config checklist (redact secrets; never paste credentials).
- **Timezone**: use `connection.timezone` and table/column timezone / `data_quality.timezone` findings to recommend consistent **data processing timezone** or flag review items.
- **Deletes**: aggregate `delete_management` counts and per-table `data_quality.delete_management`. Recommend capture mechanism (CDC vs teleport vs other) per connector capabilities. See [reference.md](reference.md).
- **Late-arriving data**: use `late_arriving_data` findings to recommend **sync frequency**, **lookback**, or **history** where relevant.
- **Keys**: flag tables with `missing_primary_key` or weak keys; recommend fixing source or Fivetran column config before relying on merges.

### 3b. Update method options (required for database connectors)

Do **not** collapse this to a single sentence. For **`sql_server`** (and analogous DB connectors), include a dedicated subsection that **names each supported update/sync path** from current Fivetran docs (names and availability change). At minimum, contrast:

- **Change Tracking (CT)** — lightweight, common on Azure SQL / RDS when enabled.
- **Change Data Capture (CDC)** — aligns when analyzer shows **`cdc_enabled`** and delete capture matters.
- **Fivetran Teleport** — Fivetran’s Teleport sync path; call out tradeoffs vs CT/CDC (permissions, PKs, resync behavior—confirm in docs).
- **Other** methods if documented for the edition (e.g. binary log / HVR-style options where applicable).

For each option: **when to prefer**, **when to avoid**, **signal from analyzer JSON** (e.g. `cdc_enabled`, `delete_management`, `missing_primary_key`), and **user** constraints (permissions, Azure tier). See [reference.md](reference.md#update-method-options-sql-server-and-peers).

### 4. Table-level: sync mode

For each table intended for sync:

- First remove any table excluded by `source_system_context.db_analysis_config.exclude_schemas` or `exclude_tables`.
- Do not include excluded tables in the table plan, sync-mode recommendations, rollout sizing, or open-action lists except to note that they are intentionally excluded.
- Add a dedicated exclusion subsection that explicitly lists excluded schemas first, then excluded tables, in the same order they appear in the analyzer JSON.

- Recommend **`SOFT_DELETE`** vs **`HISTORY`** (and **`LIVE`** only if connector docs say it is supported—often it is not on databases).
- **Do not** recommend **`HARD_DELETE`** as a REST `sync_mode` unless the connector/API explicitly supports it; many database connectors only expose **`SOFT_DELETE`** and **`HISTORY`**.

Justify using analyzer signals: SCD needs, audit requirements, delete behavior, `delete_management` severity.

### 5. Column-level: hashing and keys

For each table’s `columns`:

- **Hashing**: recommend `hashed: true` for columns under **`sensitive_fields`**, plus columns whose **`concept_id`** indicates PII/contact/financial identifiers (e.g. `contact.email`, `contact.phone`, government ids), unless the user opts out. Note Fivetran hashing implications (search/filter limitations).
- **Primary keys**: map **`primary_keys`** to columns that should be **`is_primary_key`** in Fivetran when the connector allows column config.
- **Exclude / disable**: optional recommendation for columns that are noise or problematic blobs if profiling flags issues.

### 6. Volumes and scheduling

Use **`row_count`**, **`metadata.total_rows`**, and **`data_quality_summary`** to suggest **initial sync** expectations, **sync frequency**, and phased rollout for large tables.

If `max_row_limit` is present in `source_system_context.db_analysis_config`, mention that analyzer sampling was capped at that value and mark any sampling-derived conclusions as bounded by that cap. Do not treat `max_row_limit` as a Fivetran row filter unless the user explicitly asks for that behavior.

### 7. Full parameter checklist (required)

Walk **[parameter-checklist.md](parameter-checklist.md)** sections **1–11** and produce a **filled matrix** for this engagement. For every row group (destination, connector lifecycle, source `config`/`auth`, schema/table/column, operations, webhooks):

- Give a **recommended value or action** (or **n/a** with reason).
- Tag **Source** as **json** / **heuristic** / **user** / **n/a** per the legend in that file.

Connector-specific **`config`** keys are **not** listed exhaustively: call out **n/a** until `connector_type` is fixed, then instruct **verify via Fivetran connector metadata / docs** for remaining keys.

### 8. Output format

Produce a **single markdown report** with this structure:

```markdown
# Fivetran recommendations (from analyzer)

## Source summary
- Connector type: ...
- Connection checklist (non-secret): host, database, port, SSL, ...

## Risks from analyzer
- Deletes / late data / timezone / PK gaps (bullet list with table references)

## Recommended connection parameters
- Schema handling: ...
- Other connector-specific settings: ...

## Explicit exclusions from analyzer
- Excluded schemas (ordered): ...
- Excluded tables (ordered): ...

## Update method options (SQL Server / database)
- List **Change Tracking**, **CDC**, **Teleport**, and any other documented methods for this connector.
- Per option: prefer/avoid, analyzer signals, user constraints (permissions, edition).
- State a **recommended** default and **alternatives**.

## Full parameter checklist
(Tables covering sections 1–11 from parameter-checklist.md — destination, connector, schema/table/column, operations, webhooks. Each row: parameter, recommended value, source tag, notes.)

## Table plan
| Schema | Table | Recommended sync_mode | Notes |
|--------|-------|----------------------|-------|

## Column plan
| Schema | Table | Column | Recommendation | Rationale |
|--------|-------|--------|----------------|-----------|

## Open decisions / user actions
- Secrets: reference env/Key Vault **names** only, never values
- Connector-specific config keys still to confirm in Fivetran docs

## Next steps
- Create destination → create connection → schema config → validate → sync
```

## Guardrails

- **Secrets**: never output secret values. Use placeholders and env/Key Vault **reference names** only.
- **Accuracy**: final settings depend on **Fivetran connector version**, **destination**, and **account limits**—verify in Fivetran docs or UI.
- **Analyzer limits**: if `concept_id` is null or confidence is low, mark hashing/PII recommendations as **provisional** and suggest classification review per `references/shared/classification-review-workflow.md` in source-system-analyser.
- **Analyzer exclusions**: if `source_system_context.db_analysis_config` lists excluded schemas or tables, preserve those exclusions in every Fivetran recommendation. Do not reintroduce excluded objects into connector scope, table plans, or rollout suggestions.

## Further reading

- [reference.md](reference.md) — analyzer→Fivetran mapping and API-oriented notes.
- [parameter-checklist.md](parameter-checklist.md) — **full** Fivetran parameter family matrix (required for complete reports).
