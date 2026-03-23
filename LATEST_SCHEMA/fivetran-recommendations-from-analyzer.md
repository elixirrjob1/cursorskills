# Fivetran recommendations (from analyzer)

**Source file:** `LATEST_SCHEMA/schema_dbo_mssql_dbo_mssql.json`  
**Analyzer contract:** `metadata`, `connection`, `source_system_context`, `concept_registry`, `data_quality_summary`, `tables` — all present.

---

## Source summary

- **Connector type (recommended):** `sql_server` (maps from `connection.driver`: `mssql`).
- **Connection checklist (non-secret):**
  - **Host:** `pioneertest.database.windows.net` (`connection.host`)
  - **Port:** `1433` (`connection.port`)
  - **Database:** `free-sql-db-3300567` (`connection.database`)
  - **Driver / dialect:** MSSQL → Fivetran SQL Server connector
  - **SSL / trust:** Azure SQL Database — use TLS as required by Fivetran + Microsoft docs; set `trust_certificates` / `trust_fingerprints` per org policy (not in JSON).
  - **Timezone:** `(UTC) Coordinated Universal Time` (`connection.timezone`) — align destination `time_zone_offset` and document staging semantics for `datetime2` columns.

---

## Risks from analyzer

| Theme | Evidence (json paths) | Notes |
|--------|----------------------|--------|
| **Deletes not visible to incremental sync** | `data_quality_summary.by_check.delete_management`: 10; every table has `data_quality.findings` with `check: "delete_management"`, `delete_strategy: "hard_delete"`, `cdc_enabled: false` | Prefer **Change Tracking**, **CDC**, or **Teleport** (see below); or accept periodic re-sync / full load. |
| **CDC disabled at source** | `tables[].cdc_enabled`: **false** for all 10 tables | Incremental delete capture requires enabling CT/CDC on Azure SQL or using Teleport per Fivetran docs. |
| **Late-arriving / ordering** | `late_arriving_data`: 3 (tables: `employees`, `purchase_orders`, `sales_orders` per findings) | Consider tighter sync frequency or lookback; validate business rules for order dates vs `created_at`/`updated_at`. |
| **Timestamps / timezone** | `timezone` findings: 10 tables | Review `datetime2` as UTC vs local; align warehouse modeling. |
| **Primary keys** | `missing_primary_key`: 0; `has_primary_key`: true; unique constraints: 10 | Good for merge keys; map `primary_keys` to Fivetran `is_primary_key` where supported. |
| **PII / hashing** | `has_sensitive_fields: true` on `customers`, `employees`, `stores`, `suppliers` | Hash or exclude per policy (see column plan). |
| **Scope** | `metadata.total_rows`: 1364, 10 tables | Small footprint; initial sync and schema iteration are low risk. |

---

## Recommended connection parameters

- **Schema handling:** Enable **`dbo`** in connection schema config (`tables[].schema` is uniformly `dbo`).
- **Schema change handling:** Start with **ALLOW_COLUMNS** or org-default; tighten to **BLOCK_ALL** in prod if governance requires (heuristic — confirm Fivetran group policy).
- **Paused on create:** **true** until schema/table/column review completes (heuristic).
- **Sync frequency:** **1440** minutes (daily) or lower if ops need fresher data; small row counts allow more frequent syncs if desired (heuristic + `metadata.total_rows`).
- **Incremental columns:** Analyzer lists `incremental_columns`: **`updated_at`** on all tables — align cursor/replication keys with Fivetran SQL Server connector requirements (confirm exact `config` keys in connector metadata).

---

## Update method options (SQL Server / Azure SQL)

Enumerate options; final `config` keys are **connector-version-specific** — verify in Fivetran SQL Server setup guide and **Get connector metadata**.

| Method | When to prefer | When to avoid | Analyzer signals |
|--------|----------------|---------------|------------------|
| **SQL Server Change Tracking (CT)** | Lightweight incremental changes; acceptable admin overhead to enable CT on DB | When you need full audit of all column changes with minimal setup — CDC may be richer | `cdc_enabled: false`, `delete_management` warnings — CT still needs enabling; helps once configured |
| **SQL Server CDC** | Strong change + delete capture; fits compliance | Higher DBA overhead; edition/feature constraints | Same delete warnings; if deletes must be replicated, CDC is often the right direction after enabling |
| **Fivetran Teleport** | CT/CDC not allowed or not feasible; Azure/elastic constraints | Different ops model (resync behavior, permissions) — compare latency and PK requirements in docs | `cdc_enabled: false`, PKs present (good) — still validate Teleport requirements |
| **Other** | Rare for pure Azure SQL in this profile | — | **user** / platform |

**Recommended default (heuristic):** Pursue **Change Tracking** or **CDC** on Azure SQL first if DBAs can enable them, because analyzer shows **hard deletes** with **no CDC**. If permissions block both, evaluate **Teleport** against Fivetran docs and run a pilot. **Do not** rely on plain incremental-by-`updated_at` alone to detect **row deletes** without one of the above capture methods.

---

## Full parameter checklist

### 1. Account & placement

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| Fivetran **group** | Create/select `group_id` for this workload | user | Naming / env separation |
| **Region** (processing vs warehouse) | Match latency/compliance needs | user | Not in analyzer |

### 2. Destination

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| **service** | e.g. Snowflake / BigQuery / Postgres warehouse — user choice | user | |
| **region** | Per warehouse policy | user | |
| **time_zone_offset** | Align with UTC if warehouse is UTC (`connection.timezone`) | heuristic | From `connection.timezone` |
| **config** | Host, credentials, database — use env/KV **names** only at runtime | user | Never in analyzer |
| **trust_certificates** / **trust_fingerprints** | Often true in dev; security approves prod | user | |
| **networking_method** / **private_link_id** | If PrivateLink required | user | |
| **run_setup_tests** | **true** after destination config changes | heuristic | |

### 3. Connector — lifecycle & schedule

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| **connector_type** | `sql_server` | json + heuristic | `connection.driver` → `mssql` |
| **group_id** | Same as destination’s group | user | |
| **paused** | **true** until schema reviewed | heuristic | |
| **sync_frequency** | **1440** (daily) baseline; increase if needed | heuristic | Small `metadata.total_rows` |
| **trust_certificates** / **run_setup_tests** | Per org; **true** for tests after setup | heuristic | |

### 4. Connector — config & auth (source)

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| Host, port, database, SSL | As `connection.*` checklist above | json | |
| **Update method** | CT vs CDC vs Teleport — decision tree above | heuristic + user | Required subsection completed |
| SSH/tunnel | **n/a** unless network requires bastion | n/a / user | |
| Other **config** keys | Fetch from Fivetran connector metadata for `sql_server` | n/a → user | |
| **auth** | Per connector pattern (named secrets only) | user | |

### 5. Connection-level schema behavior

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| **schema_change_handling** | ALLOW_COLUMNS or ALLOW_ALL in dev; stricter in prod | heuristic | |
| **enable_new_by_default** | **false** until validated | user / heuristic | |

### 6. Per-source-schema

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| **schema enabled** | Enable **`dbo`** | json | All `tables[].schema` = `dbo` |

### 7. Per-table

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| **table enabled** | Enable all 10 analyzer tables unless scope excludes | json | |
| **sync_mode** | **HISTORY** if full slowly-changing audit needed; else **SOFT_DELETE** if connector captures deletes via CT/CDC/Teleport | heuristic | Avoid assuming HARD_DELETE as API `sync_mode` |
| **name_in_destination** | Default unless naming prefix policy | user | |

### 8. Per-column

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| **column enabled** | Enable all unless blob/junk flagged | json + heuristic | No blob exclusions flagged |
| **hashed** | See **Column plan** | json + heuristic | `sensitive_fields` + PII concepts |
| **is_primary_key** | Map from `tables[].primary_keys` | json | |

### 9. Operations & validation

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| Setup tests | Run after connection create/update | heuristic | |
| Reload schema | After DDL changes on source | heuristic | |
| Initial sync / resync | User cutover window | user | Small data volume |
| Pause/resume | Maintenance windows | user | |

### 10. Webhooks & observability

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| Group webhook | Configure URL/events/secret if ops needs alerts | user | |

### 11. Transformations & HVR

| Parameter | Recommended | Source | Notes |
|-----------|-------------|--------|-------|
| dbt / transforms | Out of scope unless requested | n/a | |
| HVR | **n/a** unless requested | n/a | |

---

## Table plan

| Schema | Table | Recommended sync_mode | Notes |
|--------|-------|----------------------|-------|
| dbo | customers | SOFT_DELETE or HISTORY | `delete_management` + `updated_at` incremental |
| dbo | employees | SOFT_DELETE or HISTORY | `late_arriving_data` finding; FK to stores |
| dbo | inventory | SOFT_DELETE or HISTORY | No sensitive_fields; same delete story |
| dbo | products | SOFT_DELETE or HISTORY | |
| dbo | purchase_order_items | SOFT_DELETE or HISTORY | |
| dbo | purchase_orders | SOFT_DELETE or HISTORY | `late_arriving_data` finding |
| dbo | sales_order_items | SOFT_DELETE or HISTORY | |
| dbo | sales_orders | SOFT_DELETE or HISTORY | `late_arriving_data` finding |
| dbo | stores | SOFT_DELETE or HISTORY | Address/phone PII — hash columns |
| dbo | suppliers | SOFT_DELETE or HISTORY | Email/phone PII |

*Final `sync_mode` must match **Fivetran `sql_server` connector** supported values at your account version.*

---

## Column plan

| Schema | Table | Column | Recommendation | Rationale |
|--------|-------|--------|------------------|-----------|
| dbo | customers | email | **hashed: true** (if policy) | `sensitive_fields.email`, `concept_id` contact.email |
| dbo | customers | phone | **hashed: true** (if policy) | `sensitive_fields.phone` |
| dbo | employees | email | **hashed: true** (if policy) | `sensitive_fields.email` |
| dbo | stores | address | **hashed: true** (if policy) | `sensitive_fields.address` |
| dbo | stores | postal_code | **hashed: true** (if policy) | `sensitive_fields.postal_code` |
| dbo | stores | phone | **hashed: true** (if policy) | `sensitive_fields.phone` |
| dbo | suppliers | email | **hashed: true** (if policy) | `sensitive_fields.email` |
| dbo | suppliers | phone | **hashed: true** (if policy) | `sensitive_fields.phone` |
| dbo | * | *primary_keys* | **is_primary_key: true** on PK columns | `tables[].primary_keys` |

**Provisional:** Other columns with `concept_id` `contact.email` / `contact.phone` / `contact.person_name` (e.g. cross-table) may warrant hashing if classification review upgrades risk — see low-confidence columns on `stores` (`classification_summary.low_confidence_columns`).

---

## Open decisions / user actions

- **Secrets:** Provide only **names** of env vars or Key Vault secrets for DB user/password (never values in docs).
- **Azure SQL:** Confirm whether **Change Tracking** or **SQL Server CDC** can be enabled; if not, validate **Teleport** feasibility and cost.
- **Destination:** Choose warehouse **service** and **region**; run Fivetran **connector metadata** for exact `sql_server` **config** keys and enums.
- **Compliance:** Confirm hashing vs exclusion for PII columns; hashed columns limit equality joins on raw values.

---

## Next steps

1. Create **destination** in chosen Fivetran group.  
2. Create **`sql_server`** connection (start **paused**).  
3. Configure **update method** (CT / CDC / Teleport) per decision above.  
4. **Schema config:** enable `dbo`, enable tables, set **sync_mode**, set column **hash** / PK flags.  
5. **Test** connection, **reload schema** if source changes, then **initial sync**.  
6. Add **webhooks** if operational alerting is required.
