# Fivetran UTC Timezone Handling Verification

## Current Configuration

### Source Database (SQL Server)
- **Server Timezone:** `(UTC) Coordinated Universal Time`
- **Timestamp Type:** `datetime2` (timezone-naive)
- **All Timestamps:** TZ-naive, implicitly UTC
- **Columns:** `created_at`, `updated_at` (and others)

### Fivetran Connector
- **Connector ID:** `insatiable_cyst`
- **Service:** `sql_server`
- **Timezone Config:** ❌ **No explicit timezone parameter** (SQL Server connectors don't have one)
- **Update Method:** `NATIVE_UPDATE`
- **Always Encrypted:** `true` (TLS enabled)

### Destination (PostgreSQL)
- **Service:** `postgres_warehouse`
- **Time Zone Offset:** `-8` (PST) ⚠️ **This is for sync schedule only, NOT data handling**
- **Region:** `AWS_US_EAST_1`

## How Fivetran Handles UTC Timestamps

### Default Behavior (Expected)
According to Fivetran documentation:

1. **SQL Server `datetime2` columns** → Treated as **TIMESTAMP WITHOUT TIME ZONE**
2. **Timezone-naive timestamps** → **Assumed to be UTC** by default
3. **PostgreSQL destination** → Stored as `TIMESTAMP WITHOUT TIME ZONE`
4. **No timezone conversion** → Values are preserved as-is (UTC)

### Important Notes

**The `time_zone_offset: -8` in destination config:**
- ✅ **Only affects sync schedule** (when syncs run in PST)
- ❌ **Does NOT affect timestamp data handling**
- Timestamps are still treated as UTC

**SQL Server Connector:**
- ✅ **No timezone configuration needed**
- ✅ **Automatically assumes UTC** for timezone-naive timestamps
- ✅ **This is the correct behavior** for your setup

## Verification Steps

### 1. Check Source Data
```sql
-- In SQL Server
SELECT TOP 1 
    customer_id,
    created_at,
    updated_at
FROM dbo.customers;
```

### 2. Check Destination Data
```sql
-- In PostgreSQL
SELECT 
    customer_id,
    created_at,
    updated_at
FROM mssql__dbo.customers
LIMIT 1;
```

### 3. Compare Values
- ✅ **Values should match exactly** (no timezone conversion)
- ✅ **Both should show UTC times**
- ⚠️ **If values differ, there may be a timezone issue**

## Expected Behavior

✅ **Correct (Current Setup):**
- Source: `2026-02-04 14:48:25.951501` (UTC, TZ-naive)
- Destination: `2026-02-04 14:48:25.951501` (UTC, TZ-naive)
- **No conversion, values preserved**

❌ **Incorrect (Would indicate problem):**
- Source: `2026-02-04 14:48:25.951501` (UTC)
- Destination: `2026-02-04 06:48:25.951501` (PST conversion - WRONG)
- **Would indicate timezone conversion bug**

## Current Status

### ✅ Configuration is Correct
- Source timestamps are UTC (TZ-naive)
- Fivetran assumes UTC for TZ-naive timestamps
- No explicit timezone config needed (SQL Server connectors don't have it)
- Destination timezone offset only affects sync schedule

### ⚠️ Verification Needed
To confirm UTC handling is working correctly:

1. **Check actual data** in PostgreSQL destination
2. **Compare sample timestamps** between source and destination
3. **Verify no timezone conversion** occurred

## Recommendation

**Current setup should handle UTC correctly by default.** 

However, to be 100% certain:
1. ✅ Run a sync
2. ✅ Query a sample row from PostgreSQL
3. ✅ Compare `created_at`/`updated_at` values with source
4. ✅ If values match exactly → UTC handling is correct ✅
5. ⚠️ If values differ → Contact Fivetran support

## Summary

- **Source:** UTC timestamps (TZ-naive) ✅
- **Fivetran:** Assumes UTC for TZ-naive timestamps ✅
- **Destination:** Stores as TIMESTAMP WITHOUT TIME ZONE ✅
- **Expected:** No timezone conversion, values preserved ✅

**Conclusion:** Fivetran should be handling UTC correctly by default. The `time_zone_offset: -8` only affects sync scheduling, not data handling.
