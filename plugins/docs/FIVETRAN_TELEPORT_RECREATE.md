# Fivetran TELEPORT recreate (destination + SQL Server)

After running [`scripts/fivetran_recreate_teleport.py`](../scripts/fivetran_recreate_teleport.py), the Fivetran **connection id** and possibly **destination id** change. Re-apply pipeline settings that are not in the snapshot file.

## Prerequisites

- `KEYVAULT_NAME` in `.env` (or `DATABASE_URL` / `AZURE_MSSQL_URL` in `.env` for local fallback)
- `FIVETRAN_API_KEY` and `FIVETRAN_API_SECRET` in `.env` for the Fivetran REST API
- Key Vault secrets: **`DATABASE-URL`**, **`AZURE-MSSQL-URL`** (see [`references/fivetran/reconnect_teleport_snapshot.json`](../references/fivetran/reconnect_teleport_snapshot.json))

## Run

```bash
# Preview
python scripts/fivetran_recreate_teleport.py --dry-run

# Execute (destructive: deletes connections and destination)
python scripts/fivetran_recreate_teleport.py
```

Update [`references/fivetran/reconnect_teleport_snapshot.json`](../references/fivetran/reconnect_teleport_snapshot.json) if your **group id**, **legacy destination id**, or **non-secret** host/user/database values change.

## After recreate (manual follow-ups)

1. **Note new ids** from the script output; update any docs or scripts that hardcoded `insatiable_cyst` / `horns_nozzle`.
2. **MCP**: `reload_connection_schema_config` for the new connector id.
3. **Re-apply** per-table **HISTORY** sync mode and **column hashing** (Fivetran resets these on a new connection).
4. **Trigger** a sync or wait for the schedule; expect a **initial/historical** load.
5. **Optional**: Delete tools in MCP — `delete_connection`, `delete_destination` — if you need to delete individual resources without the script.

## MCP tools added

- `delete_connection` — `DELETE /v1/connections/{id}`
- `delete_destination` — `DELETE /v1/destinations/{id}`

See [`tools/fivetran_mcp/README.md`](../tools/fivetran_mcp/README.md).

## Risk

Deleting the destination removes warehouse-side objects tied to that destination until the new connection re-syncs. Plan for downtime in replicated schemas.
