# Advanced Fivetran Tuning Insights from Schema Analysis

Generated from: `LATEST_SCHEMA/schema_dbo_mssql.json`

## Additional Optimization Opportunities

### 1. 🔒 Hash Controlled Value Columns (Low Cardinality)

**Columns with limited distinct values that could be hashed for privacy:**

#### `employees.role` (4 values)
- Values: Cashier, Manager, Sales, Stock
- **Recommendation:** Hash if role information is sensitive

#### `products.category` (5 values)
- Values: Books, Clothing, Electronics, Home, Sports
- **Recommendation:** Usually safe to keep unhashed, but hash if needed

#### `purchase_orders.status` (3 values)
- Values: Ordered, Pending, Received
- **Recommendation:** Consider hashing if order status is sensitive

#### `sales_orders.status` (3 values)
- Values: Completed, Pending, Shipped
- **Recommendation:** Consider hashing if order status is sensitive

#### `stores.city` (3 values)
- Values: Cork, Galway, Limerick
- **Recommendation:** Already hashed `address` and `postal_code`, consider hashing `city` too

#### `stores.state` (3 values)
- Values: Connacht, Leinster, Munster
- **Recommendation:** Consider hashing if location is sensitive

#### `stores.postal_code` (5 values)
- Values: 10000, 10001, 10002, 10003, 10004
- **Recommendation:** ✅ Already hashed

**Action:**
```python
# Hash status columns if sensitive
update_column_config(connector_id="insatiable_cyst", schema_name="dbo",
                     table_name="purchase_orders", column_name="status", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo",
                     table_name="sales_orders", column_name="status", hashed=True)

# Hash location columns
update_column_config(connector_id="insatiable_cyst", schema_name="dbo",
                     table_name="stores", column_name="city", hashed=True)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo",
                     table_name="stores", column_name="state", hashed=True)
```

### 2. ⏰ Timezone Configuration

**Current Status:**
- All timestamps are TZ-naive (timezone-unaware)
- Server timezone: UTC
- All tables: `datetime2` columns implicitly UTC

**Fivetran Configuration:**
- ✅ Current config: `always_encrypted: true` (TLS enabled)
- ✅ Timezone handling: Fivetran should handle UTC correctly
- ⚠️ **Recommendation:** Verify Fivetran is configured to treat timestamps as UTC

**No action needed** - Current setup is correct, but verify in Fivetran UI that timestamps appear correctly.

### 3. 📊 Foreign Key Relationships & Sync Order

**Dependency Chain:**
```
stores (base)
  ↓
employees → stores.store_id
suppliers (base)
  ↓
products → suppliers.supplier_id
customers (base)
  ↓
sales_orders → customers.customer_id, employees.employee_id, stores.store_id
  ↓
sales_order_items → sales_orders.sales_order_id, products.product_id

purchase_orders → suppliers.supplier_id, employees.employee_id, stores.store_id
  ↓
purchase_order_items → purchase_orders.po_id, products.product_id

inventory → products.product_id, stores.store_id
```

**Fivetran Impact:**
- Fivetran handles FK dependencies automatically
- No manual sync order configuration needed
- ✅ Current setup handles this correctly

### 4. 📈 Volume Projections & Future Optimization

**High Growth Tables (Plan for scaling):**

#### `stores` - 240% growth in 1 year
- Current: 5 rows
- 1 year: 17 rows
- 5 year: 65 rows (1,200% growth!)
- **Action:** Monitor closely, may need row filtering if growth continues

#### `suppliers` - 120% growth in 1 year
- Current: 10 rows
- 1 year: 22 rows
- 5 year: 74 rows (640% growth)
- **Action:** Monitor, but still small scale

#### `employees` - 75% growth in 1 year
- Current: 20 rows
- 1 year: 35 rows
- 5 year: 96 rows (380% growth)
- **Action:** Monitor for scaling needs

**Stable Tables:**
- `purchase_order_items`: 0% growth projected
- `sales_order_items`: 0% growth projected
- These may be historical/archived data

**Recommendation:** Set up monitoring alerts for high-growth tables. Consider row filtering or partitioning when tables exceed 100K rows.

### 5. 🗂️ Partition Column Candidates

**All tables have partition candidates:**
- `created_at` - Good for time-based partitioning
- `updated_at` - Good for incremental sync optimization
- `order_date` (sales_orders, purchase_orders) - Good for date-based queries

**Future Optimization:**
- When tables grow large (>1M rows), consider partitioning by date columns
- Fivetran can benefit from partitioned tables for faster incremental syncs
- **No action needed now** - dataset is too small

### 6. ⚠️ Late Arriving Data

**Tables with arrival delays:**

#### `employees`
- Max lag: 15.7 hours between `hire_date` and `created_at`
- **Impact:** Employee records may arrive after hire date
- **Recommendation:** Monitor for data quality issues

#### `purchase_orders`
- Max lag: 15.7 hours between `order_date` and `created_at`
- **Impact:** Orders may be backdated
- **Recommendation:** Monitor for late-arriving data patterns

#### `sales_orders`
- ✅ No lag detected - data arrives promptly

**Fivetran Configuration:**
- Current sync frequency: 1440 minutes (24 hours)
- **Recommendation:** Consider more frequent syncs (e.g., every 6-12 hours) if real-time data is critical
- Or use `LIVE` sync mode for critical tables

### 7. 🔍 Nullable Columns That Are Never Null

**13 columns identified as nullable but never contain NULLs:**

**Tables affected:**
- `customers`: email, phone
- `inventory`: last_restocked_at
- `products`: primary_supplier_id
- `purchase_orders`: expected_date
- `sales_orders`: customer_id
- `stores`: address, city, state, postal_code, phone
- `suppliers`: email, phone

**Fivetran Impact:**
- No direct impact on sync
- **Recommendation:** Consider excluding these columns if they're not needed, or add NOT NULL constraints in source
- Can help reduce sync volume slightly

### 8. ⏱️ Timestamp Ordering

**All 10 tables have potential timestamp ordering issues:**
- `created_at` and `updated_at` may have inconsistent ordering
- No CHECK constraint ensuring `created_at <= updated_at`

**Fivetran Impact:**
- Could cause issues with incremental sync if `updated_at < created_at`
- **Recommendation:** Monitor for data quality issues
- Consider excluding `created_at` if only `updated_at` is needed for sync

### 9. 🔄 Sync Mode Optimization

**Current:** `SOFT_DELETE` for all tables

**Consider `HISTORY` mode for:**
- `sales_orders` - Track order status changes over time
- `purchase_orders` - Track order status changes
- `products` - Track price/category changes

**Consider `LIVE` mode for:**
- `sales_orders` - Real-time order processing
- `inventory` - Real-time stock levels

**Action:**
```python
# Enable HISTORY mode for audit-critical tables
update_table_config(connector_id="insatiable_cyst", schema_name="dbo",
                    table_name="sales_orders", sync_mode="HISTORY")
update_table_config(connector_id="insatiable_cyst", schema_name="dbo",
                    table_name="purchase_orders", sync_mode="HISTORY")
```

### 10. 📉 Column Exclusions

**Consider excluding if not needed:**
- `created_at` columns (if only `updated_at` is used for sync)
- Low-value columns (if they don't add business value)
- Calculated/derived columns (if they can be computed in destination)

**Example:**
```python
# Exclude created_at if not needed (only if updated_at is sufficient)
update_column_config(connector_id="insatiable_cyst", schema_name="dbo",
                     table_name="customers", column_name="created_at", enabled=False)
```

## Priority Recommendations Summary

### High Priority (Security)
1. ✅ **Hash PII fields** - DONE
2. ⚠️ **Consider hashing status/location columns** - Optional

### Medium Priority (Performance)
3. ⚠️ **Monitor high-growth tables** (stores, suppliers, employees)
4. ⚠️ **Consider more frequent syncs** for critical tables (currently 24h)
5. ⚠️ **Consider HISTORY mode** for audit-critical tables

### Low Priority (Optimization)
6. ⚠️ **Exclude unnecessary columns** to reduce sync volume
7. ⚠️ **Monitor timestamp ordering** for data quality
8. ⚠️ **Plan for partitioning** when tables exceed 100K rows

## Current Configuration Status

✅ **Optimal:**
- Sync mode: `SOFT_DELETE` (perfect for Change Tracking)
- Update method: `NATIVE_UPDATE` (uses `updated_at` columns)
- PII hashing: Applied
- Change Tracking: Enabled
- Timezone: UTC (correctly configured)

⚠️ **Consider:**
- More frequent syncs for critical tables
- HISTORY mode for audit trails
- Additional column hashing for status/location fields
