# Fivetran API Service Names Reference

This document tracks the correct service names for Fivetran API endpoints, especially when they differ from UI display names.

## Destinations

### PostgreSQL
- **UI Name:** PostgreSQL
- **API Service Name:** `postgres_warehouse` ✅
- **Common Mistakes:** `postgres`, `postgresql`, `postgres_destination` ❌

### Other Common Destinations
- **Snowflake:** `snowflake`
- **Google BigQuery:** `big_query`
- **Amazon Redshift:** `redshift`
- **Databricks:** `databricks`
- **Azure Synapse:** `azure_synapse`
- **S3:** `s3`

## Connectors (Sources)

### SQL Server
- **UI Name:** SQL Server
- **API Service Name:** `sql_server` ✅

### PostgreSQL (as source)
- **UI Name:** PostgreSQL
- **API Service Name:** `postgres` ✅

## Discovery Methods

### 1. Test Service Name
```bash
# Use the discovery script
export FIVETRAN_API_KEY="your_key"
export FIVETRAN_API_SECRET="your_secret"
./scripts/discover_fivetran_services.sh postgres_warehouse
```

### 2. Check Fivetran Documentation
- API Reference: https://fivetran.com/docs/rest-api
- Look for service names in endpoint examples

### 3. Check Existing Resources
```bash
# List existing destinations to see service names
curl -u "$API_KEY:$API_SECRET" \
  -H "Accept: application/json;version=2" \
  "https://api.fivetran.com/v1/destinations" | jq '.data.items[].service'
```

## Common Issues

### Issue: "Unsupported service 'X'"
**Solution:** The service name is incorrect. Try variations:
- Add `_warehouse` suffix for destinations
- Check existing resources for correct naming
- Use discovery script to test variations

### Issue: Service name works in UI but not API
**Solution:** UI names often differ from API service names. Always check API documentation or test with discovery script.

## Notes

- Service names are case-sensitive
- Destination service names often differ from connector service names
- When in doubt, test with the discovery script before creating resources
