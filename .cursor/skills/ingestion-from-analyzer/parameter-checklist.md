# Fivetran parameter coverage matrix

Use this document when producing recommendations so **every major Fivetran parameter family** is addressed. For each row, fill **Recommended value or action** and **Source**:

| Tag | Meaning |
|-----|---------|
| **json** | Directly grounded in analyzer `schema.json` (cite path). |
| **heuristic** | Inferred from analyzer signals + Fivetran patterns; confirm in docs/UI. |
| **user** | Not in analyzer; user, platform, or security must decide. |
| **n/a** | Not applicable to this engagement, or unknown until connector metadata is fetched. |

Connector-specific **config** keys (inside `config` / `auth`) are **not** enumerable here—use Fivetran **Get connector metadata** / docs for the chosen `connector_type`. Mark those rows **user** or **n/a** until type is fixed.

---

## 1. Account & placement

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| Fivetran **group** for destination + connections | `group_id` | user | Naming, env separation. |
| **Region** (Fivetran processing) vs destination region | destination create | user | Latency/compliance. |

---

## 2. Destination

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **service** (e.g. `postgres_warehouse`, Snowflake, BigQuery) | `create_destination.service` | user | Warehouse choice. |
| **region** | `region` | user | |
| **time_zone_offset** | `time_zone_offset` | heuristic | Align with `connection.timezone` from analyzer if sensible; else user. |
| **config** (host, database, user, password, role, etc.) | `config` | user | Values never in analyzer; **names** of env/KV refs only in output. |
| **trust_certificates** / **trust_fingerprints** | create/update destination | user | Often true for dev; security team for prod. |
| **networking_method** | destination | user | Public vs PrivateLink etc. |
| **private_link_id** | destination | user | If using PrivateLink. |
| **run_setup_tests** | destination create / update | heuristic | Recommend true after config changes. |

---

## 3. Connector (connection) — lifecycle & schedule

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **connector_type** (`service` in API) | `create_connector` | json + heuristic | From `connection.driver` → map to Fivetran type. |
| **group_id** | `create_connector` | user | Must match destination’s group. |
| **paused** (start paused) | `create_connector` / `update_connector` | heuristic | Recommend paused until schema reviewed. |
| **sync_frequency** (minutes) | `create_connector` / `update_connector` | heuristic | From `row_count`, `late_arriving_data`, ops needs; default e.g. 1440. |
| **trust_certificates** / **trust_fingerprints** / **run_setup_tests** | often on create | heuristic | Match org policy. |

---

## 4. Connector — config & auth (source)

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **Host, port, database, schema**, SSL | `config` | json | From `connection.*`; secrets via named refs. |
| **Update method** (e.g. SQL Server: **Change Tracking** vs **CDC** vs **Teleport** vs other documented methods) | `config` | heuristic + user | **Required:** report must **list** each viable option with short prefer/avoid + analyzer signals (`cdc_enabled`, `delete_management`, PKs) and **user** permissions. Do not give only one method name without alternatives. |
| **SSH / tunnel / proxy** fields | `config` | user | If bastion required. |
| **Additional connector-specific keys** | `config` | n/a → user | Fetch from Fivetran connector metadata for `connector_type`. |
| **auth** object (if separate from config) | `auth` | user | Pattern depends on connector. |

---

## 5. Connection-level schema behavior

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **schema_change_handling** (ALLOW_ALL / ALLOW_COLUMNS / BLOCK_ALL) | connection schema config response; may be set at connection level in UI | heuristic | Balance agility vs control; not always in analyzer. |
| **enable_new_by_default** | schema config | user | Default inclusion of new objects. |

---

## 6. Per-source-schema

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **schema enabled** | `PATCH .../schemas/{schema}` | json | Enable each `tables[].schema` present in analyzer output. |
| **schema_change_handling** (per schema, if supported) | schema PATCH | heuristic | |

---

## 7. Per-table

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **table enabled** | `update_table_config.enabled` | json | One row per analyzer table; disable if out of scope. |
| **sync_mode** (SOFT_DELETE / HISTORY / LIVE if supported) | `update_table_config.sync_mode` | heuristic | From deletes, history needs, connector support. |
| **name_in_destination** (if customizable) | schema config | user | Often default; prefix rules. |

---

## 8. Per-column

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **column enabled** | `update_column_config.enabled` or table patch `columns` | json + heuristic | Disable blobs/junk if flagged. |
| **hashed** | `update_column_config.hashed` | json + heuristic | `sensitive_fields`, `concept_id` PII. |
| **is_primary_key** | `update_column_config.is_primary_key` | json | From `primary_keys`. |

---

## 9. Operations & validation

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **Setup / connection tests** | test endpoints | heuristic | After config changes. |
| **Reload schema** (refresh metadata) | connector API | heuristic | After source DDL changes. |
| **Initial / manual sync**, **resync** scope | sync trigger APIs | user | Cutover timing. |
| **Pause / resume** | `update_connector.paused` | user | Maintenance windows. |

---

## 10. Webhooks & observability

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **Group webhook** URL, events, secret | `create_group_webhook` | user | Ops integration. |

---

## 11. Transformations & HVR (if used)

| Parameter / question | Typical API / UI locus | Source | Notes |
|----------------------|------------------------|--------|--------|
| **dbt / transformations** | Fivetran product | user | Not in analyzer JSON. |
| **HVR / extra pipelines** | product-specific | n/a | Out of scope unless user asks. |

---

## Report template (copy into SKILL output)

For each **section 1–11**, include a compact table in the main deliverable:

```markdown
## Full parameter checklist

### Destination
| Parameter | Recommended | Source (json/heuristic/user/n/a) | Notes |
|----------|-------------|----------------------------------|-------|
...

### Connector (source)
| Parameter | Recommended | Source | Notes |
|----------|-------------|--------|-------|
...
```

Rows with **n/a** should still appear with a one-line justification.
