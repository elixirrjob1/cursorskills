# Fivetran Tuning Recommendations Based on Schema Analysis

Generated from: `LATEST_SCHEMA/schema_dbo_mssql.json`

## Summary Statistics
- **Total Tables**: 10
- **Total Rows**: 1,364
- **Total Findings**: 64

## Critical Tuning Recommendations

### 1. 🔒 Hash Sensitive PII Fields

**Tables with PII that should be hashed:**

#### `customers` table
- **email** (`pii_contact`) - Should be hashed
- **phone** (`pii_contact`) - Should be hashed

#### `employees` table
- **email** (`pii_contact`) - Should be hashed

#### `stores` table
- **address** (`pii_address`) - Should be hashed
- **postal_code** (`pii_address`) - Should be hashed
- **phone** (`pii_contact`) - Should be hashed

#### `suppliers` table
- **email** (`pii_contact`) - Should be hashed
- **phone** (`pii_contact`) - Should be hashed

**Action Required:**
```python
# Hash email columns
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="customers", column_name="email", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="employees", column_name="email", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="suppliers", column_name="email", hashed=True)

# Hash phone columns
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="customers", column_name="phone", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="stores", column_name="phone", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="suppliers", column_name="phone", hashed=True)

# Hash address fields
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="stores", column_name="address", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo", 
                     table_name="stores", column_name="postal_code", hashed=True)
```

### 2. ✅ Delete Strategy Configuration

**Current Status:**
- Most tables: `hard_delete_with_cdc` (Change Tracking enabled)
- `products` table: `soft_delete` (has `active` column)

**Current Sync Mode:** `SOFT_DELETE` ✅ **CORRECT**

**Recommendation:** Keep `SOFT_DELETE` sync mode for all tables. This works perfectly with Change Tracking and ensures deleted rows are marked rather than removed.

### 3. 📊 Incremental Sync Configuration

**All tables have:**
- ✅ Primary keys (required for Fivetran)
- ✅ `updated_at` columns (perfect for `NATIVE_UPDATE`)
- ✅ Change Tracking enabled (we enabled this)

**Current Configuration:** `NATIVE_UPDATE` ✅ **OPTIMAL**

**Recommendation:** Current setup is optimal. Fivetran will use `updated_at` columns for efficient incremental syncs.

### 4. 📈 Table Size Considerations

**Large Tables (may benefit from optimization):**
- `sales_order_items`: 508 rows
- `purchase_order_items`: 246 rows
- `sales_orders`: 200 rows

**Small Tables (low priority):**
- `stores`: 5 rows
- `suppliers`: 10 rows
- `employees`: 20 rows

**Recommendation:** Current dataset is small. No special partitioning or filtering needed. As data grows, consider:
- Row filtering for large tables
- Partitioning by `created_at` or `updated_at` if tables exceed 1M rows

### 5. 🔄 Sync Mode Recommendations

**Current:** `SOFT_DELETE` for all tables

**Alternative Options:**
- `HISTORY` mode: Track all changes over time (useful for audit trails)
- `LIVE` mode: Real-time sync (lower latency, higher cost)

**Recommendation:** Keep `SOFT_DELETE` unless you need full change history, then use `HISTORY` mode.

### 6. ⚠️ Special Case: `products` Table

**Unique Characteristics:**
- Has soft-delete column: `active`
- Delete strategy: `soft_delete` (not `hard_delete_with_cdc`)

**Recommendation:** 
- Current `SOFT_DELETE` sync mode is appropriate
- Consider using `active` column for filtering if you only want active products

## Priority Actions

### High Priority
1. ✅ **Hash all PII fields** (email, phone, address) - **SECURITY CRITICAL**
2. ✅ Current sync mode (`SOFT_DELETE`) is correct
3. ✅ Current incremental method (`NATIVE_UPDATE`) is optimal

### Medium Priority
4. Consider `HISTORY` sync mode for audit-critical tables if needed
5. Monitor sync performance as data volume grows

### Low Priority
6. Consider row filtering for large tables if they exceed 100K rows
7. Review column exclusions for any unused columns

## Current Configuration Summary

✅ **Correctly Configured:**
- Sync mode: `SOFT_DELETE` (perfect for Change Tracking)
- Update method: `NATIVE_UPDATE` (uses `updated_at` columns)
- Change Tracking: Enabled (allows delete capture)
- Primary keys: All tables have them

⚠️ **Needs Attention:**
- PII fields not hashed (security risk)
- No column-level exclusions configured

## Implementation Script

See `scripts/apply_fivetran_tuning.sh` for automated application of these recommendations.
