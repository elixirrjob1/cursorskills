# Fivetran Troubleshooting Guide

## Common Issues and Solutions

### Issue: "Unsupported service 'X'" when creating destination

**Symptoms:**
- API returns: `{"code":"InvalidInput","message":"Unsupported service 'postgres'"}`
- UI shows the service is available, but API rejects it

**Root Cause:**
- Fivetran API service names differ from UI display names
- PostgreSQL destination requires `postgres_warehouse`, not `postgres`

**Solutions:**

1. **Use the discovery script:**
   ```bash
   export FIVETRAN_API_KEY="your_key"
   export FIVETRAN_API_SECRET="your_secret"
   ./scripts/discover_fivetran_services.sh postgres_warehouse
   ```

2. **Use the MCP validation tool:**
   ```python
   # Via MCP
   validate_destination_service(service="postgres_warehouse", group_id="your_group_id")
   ```

3. **Check existing destinations:**
   ```bash
   curl -u "$API_KEY:$API_SECRET" \
     -H "Accept: application/json;version=2" \
     "https://api.fivetran.com/v1/destinations" | jq '.data.items[].service'
   ```

4. **Reference the service names doc:**
   - See `docs/FIVETRAN_SERVICE_NAMES.md` for known correct names

### Issue: Connector setup stuck in "incomplete" state

**Symptoms:**
- `setup_state: "incomplete"`
- `sync_state: "scheduled"` but never starts
- Setup tests show failures

**Common Causes:**

1. **Change Tracking/CDC not enabled** (for SQL Server)
   - Error: "Neither Change Tracking nor Change Data Capture are enabled"
   - Solution: Enable Change Tracking on database and tables
   - See: `scripts/enable_change_tracking.sql`

2. **Database not accessible**
   - Error: "Database 'X' is not currently available"
   - Solution: 
     - Resume paused Azure SQL Database
     - Check firewall rules
     - Verify credentials

3. **Setup tests failing**
   - Check `setup_tests` array in connector status
   - Fix any FAILED tests
   - Retry by updating connector config

**Solution Steps:**

1. Check connector status for errors:
   ```bash
   curl -u "$API_KEY:$API_SECRET" \
     -H "Accept: application/json;version=2" \
     "https://api.fivetran.com/v1/connectors/{connector_id}" | jq '.data.setup_tests'
   ```

2. Fix underlying issue (enable Change Tracking, resume DB, etc.)

3. Trigger setup test rerun:
   ```bash
   # Update connector config to trigger tests
   curl -X PATCH -u "$API_KEY:$API_SECRET" \
     -H "Accept: application/json;version=2" \
     -H "Content-Type: application/json" \
     -d '{"config": {...existing config...}}' \
     "https://api.fivetran.com/v1/connections/{connector_id}"
   ```

### Issue: Sync won't start

**Symptoms:**
- Connector is active (`paused: false`)
- `sync_state: "scheduled"` but never changes to `syncing`
- `setup_state: "incomplete"` or `connected`

**Solutions:**

1. **If setup is incomplete:**
   - Fix setup issues first (see above)
   - Wait for `setup_state: "connected"`

2. **Force sync:**
   ```bash
   curl -X POST -u "$API_KEY:$API_SECRET" \
     -H "Accept: application/json;version=2" \
     "https://api.fivetran.com/v1/connectors/{connector_id}/force"
   ```

3. **Check for warnings:**
   ```bash
   curl -u "$API_KEY:$API_SECRET" \
     -H "Accept: application/json;version=2" \
     "https://api.fivetran.com/v1/connectors/{connector_id}" | jq '.data.status.warnings'
   ```

## Prevention Checklist

Before creating a new Fivetran resource:

- [ ] Verify service name using discovery script or validation tool
- [ ] Check `docs/FIVETRAN_SERVICE_NAMES.md` for known service names
- [ ] Ensure database is accessible and not paused
- [ ] Enable Change Tracking/CDC if required for your connector type
- [ ] Verify firewall rules allow Fivetran IPs
- [ ] Test connection credentials before creating connector
- [ ] Check existing resources to see correct naming patterns

## Quick Reference

### Service Name Discovery
```bash
# Test a service name
./scripts/discover_fivetran_services.sh postgres_warehouse

# List existing destinations to see service names
curl -u "$API_KEY:$API_SECRET" \
  -H "Accept: application/json;version=2" \
  "https://api.fivetran.com/v1/destinations" | jq '.data.items[].service'
```

### Common Service Names
- PostgreSQL Destination: `postgres_warehouse`
- SQL Server Connector: `sql_server`
- PostgreSQL Connector: `postgres`

See `docs/FIVETRAN_SERVICE_NAMES.md` for complete list.
