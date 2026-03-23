# Fivetran from analyzer — reference

## Analyzer → Fivetran: field mapping

| Analyzer location | Use for Fivetran |
|-------------------|------------------|
| `connection.host`, `port`, `database`, `driver` | Connector config (non-secret checklist) |
| `connection.timezone` + per-column timezone findings | Timezone alignment, staging review |
| `metadata.total_tables`, `total_rows` | Scope / initial load sizing |
| `data_quality_summary.by_check.*` | Prioritize risks (deletes, late data, PKs) |
| `tables[].primary_keys` | PK column config in connector schema where supported |
| `tables[].foreign_keys` | Document lineage; optional join design in warehouse |
| `tables[].row_count` | Table prioritization, sync window planning |
| `tables[].data_quality.delete_management` | SOFT_DELETE vs HISTORY discussion |
| `tables[].data_quality.late_arriving_data` | Sync frequency, history mode |
| `tables[].sensitive_fields` / `has_sensitive_fields` | Column **hashing** candidates |
| `columns[].concept_id`, `concept_confidence` | Refine hashing when `sensitive_fields` incomplete |
| `tables[].columns[].name`, `type`, `nullable` | Column enable/disable, type expectations in destination |

## Update / capture method (SQL Server–oriented hints)

**Terms:** “**Replicate** / incremental” here usually means **Change Tracking** or **CDC** (log-based change capture)—distinct from **Fivetran Teleport**. Exact **`config`** keys and enum values (e.g. `update_method` on `sql_server`) are **connector- and version-specific**—check Fivetran’s SQL Server setup guide and API metadata.

These are **heuristic**; always confirm against current Fivetran docs for `sql_server`:

- **CDC / change tracking available** and deletes matter → prefer change-based replication when permitted.
- **CDC / CT not available** or not allowed → **Teleport** or other documented methods may be appropriate; flag resync / permission tradeoffs.
- **Primary key gaps** in analyzer → fix source or accept merge limitations before trusting incremental behavior.

Analyzer **`delete_management`** findings should drive the delete-handling discussion in the written report.

## Update method options (SQL Server and peers)

Reports must **enumerate options**, not only one recommendation. Use this pattern (adapt names to current Fivetran docs):

| Method (conceptual) | Role | Analyzer signals | Typical tradeoffs |
|---------------------|------|------------------|-------------------|
| **SQL Server Change Tracking (CT)** | Incremental via CT | `delete_management`, need for low admin overhead | Often lighter than CDC; requires CT enabled on DB |
| **SQL Server CDC** | Incremental via CDC | `cdc_enabled: true`, deletes | Strong delete capture; more ops overhead; column/type limits per docs |
| **Fivetran Teleport** | Fivetran Teleport sync path | PK gaps, CT/CDC blocked, or doc says Teleport fits | Different operational model than CT/CDC—compare resync, permissions, latency |
| **Other** (e.g. binary log / HVR-style) | If listed for your SQL Server edition | `user` / platform | Windows/edition constraints often apply |

**Never** only mention Teleport or only CDC without naming the other primary options for SQL Server unless the user has already locked the method.

## Sync modes (REST API perspective)

Documented table `sync_mode` values for many database connectors include **`SOFT_DELETE`**, **`HISTORY`**; **`LIVE`** may be unsupported for some connectors (API returns error).

Do not assume **`HARD_DELETE`** exists as a `sync_mode` unless confirmed for that connector.

## Column hashing

Recommend hashing when:

1. Analyzer lists the column in **`sensitive_fields`**, or
2. **`concept_id`** indicates direct identifiers (email, phone, national id, payment account, etc.), with confidence noted.

Call out tradeoffs: hashed columns are not useful for equality joins to raw identifiers in the warehouse without separate mapping.

## Secrets and auth

The skill produces **names** of env vars or Key Vault secret references (e.g. connection URL secret names). Never emit secret values—align with project rules for Key Vault and API auth.

## Full parameter coverage

For exhaustive **section-by-section** coverage (destination, connector, schema/table/column, operations, webhooks), use **[parameter-checklist.md](parameter-checklist.md)**. Complete reports must include the filled checklist tables with **json / heuristic / user / n/a** source tags.
