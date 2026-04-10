# Retail Operations Target Model

**Generated**: 2026-04-09
**Framework**: Source-Aligned Target Model
**Naming Convention**: Preserve analyzer-compatible table and column names in target tables

## Summary

This target model defines source-aligned target tables that preserve the current analyzer-compatible table and column names. The structure is target-oriented for STM generation, while remaining directly compatible with the analyzer JSON for glossary and classification matching.

### Key Design Decisions

- Target table names intentionally match the analyzer JSON table names exactly.
- Target column names intentionally match the analyzer JSON column names exactly.
- Table and column descriptions are phrased as target-table specifications while preserving analyzer compatibility.
- This model is intended for source-aligned target structures, not a dimensional/star-schema redesign.

## Version History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 1.0 | 2026-04-09 | Cursor | Initial source-aligned target model generated from analyzer JSON |

---

## Table Catalog

### FivetranConnectors

**Type**: Target Table (Source-Aligned)
**Description**: Target table for Fivetran connector operations, preserving source-aligned status and message fields for operational monitoring and downstream ingestion control.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| code | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | Stores the status or result code for Fivetran connector operations, represented as a string. |
| message | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | Stores optional status or informational messages related to Fivetran connector operations. |

**Grain**: One row per source-system record

---

### customers

**Type**: Target Table (Source-Aligned)
**Description**: Target customer master table preserving customer identifiers, names, contact details, and audit timestamps in an analyzer-compatible structure.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| customer_id | bigint | NO | Unique identifier for each customer in the retail operations system. |
| first_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | Stores the given name of a customer as a non-null, case-insensitive Unicode string. |
| last_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | The "last_name" column in the "customers" table stores the non-nullable family name of a customer as a case-insensitive Unicode string. |
| email | nvarchar(450) | YES | Stores the email address of the customer, allowing null values, for contact purposes. |
| phone | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | Stores the customer's phone number as an optional text field for contact purposes. |
| created_at | datetime2 | NO | The `created_at` column records the non-nullable timestamp indicating when a customer record was initially created in the system. |
| updated_at | datetime2 | NO | Tracks the timestamp of the most recent update to a customer's record, ensuring accurate change history. |

**Primary Key**: customer_id
**Grain**: One row per customer_id
**Business Rules**:
- Incremental candidates: updated_at
- Contains sensitive or personal data fields according to the analyzer.

---

### employees

**Type**: Target Table (Source-Aligned)
**Description**: Target employee master table preserving employee identifiers, store relationships, personal attributes, role data, and audit timestamps in a source-aligned target structure.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| employee_id | bigint | NO | Unique identifier for each employee in the retail operations system. |
| store_id | bigint | NO | Associates each employee with a specific store, referencing the `store_id` column in the `stores` table. |
| first_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | Stores the given name of an employee as a non-null, case-insensitive Unicode string. |
| last_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | The "last_name" column in the "employees" table stores the non-nullable family name of an employee as a case-insensitive Unicode string. |
| email | nvarchar(450) | NO | Stores the unique email address of each employee, used as a mandatory contact identifier. |
| role | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | Indicates the job position or function of an employee within the organization, stored as a non-nullable text value. |
| hire_date | date | NO | The `hire_date` column in the `employees` table records the non-nullable date an employee was hired, used for tracking employment start dates. |
| created_at | datetime2 | NO | The `created_at` column records the non-nullable timestamp indicating when an employee record was initially created in the system. |
| updated_at | datetime2 | NO | Tracks the timestamp of the most recent update to an employee record, ensuring accurate change history. |

**Primary Key**: employee_id
**Foreign Keys**: store_id
**Grain**: One row per employee_id
**Business Rules**:
- Incremental candidates: updated_at
- Contains sensitive or personal data fields according to the analyzer.

---

### inventory

**Type**: Target Table (Source-Aligned)
**Description**: Target inventory table preserving current stock levels, store-product relationships, stock value measures, and unit-normalized inventory attributes.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| inventory_id | bigint | NO | Unique identifier for each inventory record, serving as the primary key for the inventory table. |
| store_id | bigint | NO | Identifier for the store associated with the inventory record, referencing the `store_id` column in the `stores` table. |
| product_id | bigint | NO | References the unique identifier of a product in the products table to associate inventory records with specific products. |
| quantity_on_hand | integer | NO | The `quantity_on_hand` column in the `inventory` table stores the current stock level of a product as a non-null integer. |
| reorder_level | integer | NO | Indicates the minimum quantity of a product in inventory that triggers a reorder, stored as a non-null integer. |
| last_restocked_at | datetime2 | YES | The `last_restocked_at` column records the timestamp of the most recent restocking event for an inventory item, allowing null values if the item has not been restocked. |
| created_at | datetime2 | NO | The `created_at` column records the non-nullable timestamp indicating when the inventory record was initially created. |
| updated_at | datetime2 | NO | The `updated_at` column in the `inventory` table stores the non-nullable timestamp indicating the last update to the inventory record. |
| stock_value | numeric(10,2) | YES | Represents the monetary value of inventory stock, stored as a numeric value with up to 10 digits and 2 decimal places, and may be null. |
| stock_unit | nvarchar(16) | YES | Current stock unit label. |

**Primary Key**: inventory_id
**Foreign Keys**:
- product_id
- store_id
**Grain**: One row per inventory_id
**Business Rules**: Incremental candidates: updated_at

---

### products

**Type**: Target Table (Source-Aligned)
**Description**: Target product master table preserving supplier relationships, product attributes, pricing, and physical measurement fields in a source-aligned target layout.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| product_id | bigint | NO | Unique identifier for each product in the retail system, serving as the primary key for the products table. |
| supplier_id | bigint | NO | Represents the unique identifier of the supplier associated with a product, referencing the `supplier_id` column in the `suppliers` table. |
| sku | nvarchar(450) | NO | Unique alphanumeric identifier for a product used for inventory and sales tracking, required and limited to 450 characters. |
| name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | The "name" column in the "products" table stores the non-nullable name of each product as a case-insensitive Unicode string. |
| category | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | Indicates the product's category classification, such as "Books," "Clothing," or "Electronics," stored as a non-nullable nvarchar string. |
| unit_price | numeric(10,2) | NO | The `unit_price` column stores the non-null selling price of a product as a numeric value with up to 10 digits and 2 decimal places, representing a currency amount. |
| cost_price | numeric(10,2) | NO | The `cost_price` column stores the non-null numeric cost amount (up to 10 digits with 2 decimal places) representing the purchase price of a product in the retail system. |
| active | bit | NO | Indicates whether a product is active and available for transactions, stored as a non-nullable boolean value. |
| created_at | datetime2 | NO | Indicates the timestamp when the product record was initially created, stored as a non-nullable datetime value. |
| updated_at | datetime2 | NO | The `updated_at` column stores the non-nullable timestamp of the most recent update to a product record in the `products` table. |
| weight_unit | nvarchar(16) | YES | Source weight unit (kg/lb) used for unit inference testing. |
| weight_value | numeric(10,2) | YES | The `weight_value` column stores the weight of a product as a numeric value with up to 10 digits and 2 decimal places, nullable if the weight is not specified. |
| length_value | numeric(10,2) | YES | Stores the length measurement of a product as a numeric value with up to two decimal places, nullable if not applicable. |
| length_unit | nvarchar(16) | YES | Source length unit (cm/in) used for unit inference testing. |
| product_description | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | Stores optional textual details about a product, such as features or specifications. |
| primary_supplier_id | bigint | YES | Primary supplier relationship used for join candidate detection. |

**Primary Key**: product_id
**Foreign Keys**:
- primary_supplier_id
- supplier_id
**Grain**: One row per product_id
**Business Rules**: Incremental candidates: updated_at

---

### purchase_order_items

**Type**: Target Table (Source-Aligned)
**Description**: Target purchase order line table preserving ordered quantities, unit costs, and explicit quantity-unit fields for procurement analytics and traceability.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| po_item_id | bigint | NO | Unique identifier for each purchase order item in the system, serving as the primary key for the `purchase_order_items` table. |
| po_id | bigint | NO | Represents the unique identifier of the associated purchase order, linking purchase order items to their parent purchase order via a foreign key relationship to `purchase_orders.po_id`. |
| product_id | bigint | NO | Identifier linking each purchase order item to a specific product in the products table. |
| quantity | integer | NO | The `quantity` column in the `purchase_order_items` table stores the non-null integer value representing the number of units of a product included in a specific purchase order item. |
| unit_cost | numeric(10,2) | NO | Represents the per-unit cost of a product in a purchase order, stored as a non-null numeric value with up to 10 digits and 2 decimal places. |
| created_at | datetime2 | NO | The `created_at` column records the non-nullable timestamp indicating when each purchase order item record was created. |
| updated_at | datetime2 | NO | Records the timestamp of the last update made to a purchase order item, ensuring accurate tracking of modifications. |
| ordered_qty_value | numeric(10,2) | YES | The `ordered_qty_value` column stores the numeric quantity value (up to two decimal places) of items ordered in a purchase order, which can be null. |
| ordered_qty_unit | nvarchar(16) | YES | Ordered quantity unit (ea/box). |

**Primary Key**: po_item_id
**Foreign Keys**:
- po_id
- product_id
**Grain**: One row per po_item_id
**Business Rules**: Incremental candidates: updated_at

---

### purchase_orders

**Type**: Target Table (Source-Aligned)
**Description**: Target purchase order header table preserving supplier, store, order-status, and approver relationships in an analyzer-compatible target structure.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| po_id | bigint | NO | Unique identifier for purchase orders in the retail operations system. |
| supplier_id | bigint | NO | The `supplier_id` column in the `purchase_orders` table is a non-nullable foreign key referencing the `supplier_id` column in the `suppliers` table, identifying the supplier associated with each purchase order. |
| store_id | bigint | NO | Identifies the store associated with the purchase order, referencing the `store_id` column in the `stores` table. |
| status | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | Indicates the current state of a purchase order, such as 'Ordered' or 'Received', using predefined category values. |
| order_date | date | NO | The `order_date` column in the `purchase_orders` table records the date a purchase order was placed, is mandatory, and uses the `date` data type. |
| expected_date | date | YES | The "expected_date" column in the "purchase_orders" table records the anticipated delivery date of a purchase order, allowing null values. |
| created_at | datetime2 | NO | The `created_at` column records the timestamp when a purchase order record was initially created, stored as a non-nullable `datetime2` value. |
| updated_at | datetime2 | NO | The `updated_at` column stores the non-nullable timestamp indicating the last modification date and time of a purchase order record. |
| approver_employee_id | bigint | YES | Approver employee foreign key for procurement workflow. |

**Primary Key**: po_id
**Foreign Keys**:
- approver_employee_id
- store_id
- supplier_id
**Grain**: One row per po_id
**Business Rules**: Incremental candidates: updated_at

---

### sales_order_items

**Type**: Target Table (Source-Aligned)
**Description**: Target sales order line table preserving sold quantities, unit prices, and explicit quantity-unit fields for downstream commercial reporting.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| sales_order_item_id | bigint | NO | Unique identifier for each sales order item in the sales_order_items table. |
| sales_order_id | bigint | NO | References the unique identifier of the associated sales order in the `sales_orders` table, establishing a relationship between sales order items and their parent sales orders. |
| product_id | bigint | NO | Identifies the product associated with a sales order item, referencing the `products.product_id` column. |
| quantity | integer | NO | The `quantity` column in the `sales_order_items` table stores the non-null integer value representing the number of units of a product included in a specific sales order item. |
| unit_price | numeric(15,2) | NO | The `unit_price` column in the `sales_order_items` table stores the per-unit selling price of a product in the sales order, represented as a non-nullable numeric value with two decimal places. |
| created_at | datetime2 | NO | Records the timestamp when a sales_order item entry is created, ensuring accurate tracking of creation times; this field is mandatory and uses the datetime2 data type. |
| updated_at | datetime2 | NO | The `updated_at` column in the `sales_order_items` table stores the non-nullable timestamp of the most recent update to a sales order item record. |
| sold_qty_value | numeric(10,2) | YES | The `sold_qty_value` column stores the numeric value representing the quantity of a product sold in a sales order item, allowing up to two decimal places, and can be null. |
| sold_qty_unit | nvarchar(16) | YES | Sold quantity unit (ea/box). |

**Primary Key**: sales_order_item_id
**Foreign Keys**:
- product_id
- sales_order_id
**Grain**: One row per sales_order_item_id
**Business Rules**: Incremental candidates: updated_at

---

### sales_orders

**Type**: Target Table (Source-Aligned)
**Description**: Target sales order header table preserving customer, store, employee, and sales-representative relationships together with order totals and statuses.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| sales_order_id | bigint | NO | Unique identifier for each sales order in the system. |
| store_id | bigint | NO | Identifies the store associated with a sales order, referencing the `store_id` column in the `stores` table. |
| customer_id | bigint | YES | Identifies the customer associated with a sales order, referencing the `customer_id` column in the `customers` table; nullable to accommodate orders without a registered customer. |
| employee_id | bigint | NO | Identifies the employee responsible for processing the sales order, referencing the `employees.employee_id` column. |
| order_date | datetime2 | NO | The `order_date` column records the date and time when a sales order was placed, stored as a non-nullable `datetime2` value. |
| status | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | Indicates the current state of a sales order, such as 'Pending' or 'Completed', stored as a non-nullable text category. |
| total_amount | numeric(12,2) | NO | The `total_amount` column in the `sales_orders` table stores the non-nullable total monetary value of a sales order as a numeric value with up to 12 digits and 2 decimal places. |
| created_at | datetime2 | NO | The `created_at` column records the timestamp when a sales order record was initially created, stored as a non-nullable `datetime2` value. |
| updated_at | datetime2 | NO | The `updated_at` column stores the non-nullable timestamp of the most recent update to a sales order record. |
| sales_rep_employee_id | bigint | YES | Sales representative foreign key for order ownership. |

**Primary Key**: sales_order_id
**Foreign Keys**:
- customer_id
- employee_id
- sales_rep_employee_id
- store_id
**Grain**: One row per sales_order_id
**Business Rules**: Incremental candidates: updated_at

---

### stores

**Type**: Target Table (Source-Aligned)
**Description**: Target store master table preserving store identifiers, reference codes, address details, contact fields, and audit timestamps.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| store_id | bigint | NO | Unique identifier for each store in the retail operations system. |
| name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | The "name" column in the "stores" table is a non-nullable nvarchar field intended to store the name of a retail store. |
| code | nvarchar(450) | NO | Unique alphanumeric identifier for each store, used for internal reference and operations. |
| address | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | The "address" column in the "stores" table stores the street address of a retail store location as a nullable Unicode string. |
| city | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | Stores.city: The nullable nvarchar column stores the name of the city where each store is located. |
| state | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | The "state" column in the "stores" table stores the name of the state or province where a store is located, allowing null values. |
| postal_code | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | The "postal_code" column in the "stores" table stores the postal or ZIP code of a store location as a nullable nvarchar value. |
| phone | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | The "phone" column in the "stores" table stores the contact phone number of a store as a nullable nvarchar value. |
| created_at | datetime2 | NO | The `created_at` column stores the non-nullable timestamp indicating when a store record was initially created in the system. |
| updated_at | datetime2 | NO | The `updated_at` column records the timestamp of the most recent update to a store's record and cannot be null. |

**Primary Key**: store_id
**Grain**: One row per store_id
**Business Rules**:
- Incremental candidates: updated_at
- Contains sensitive or personal data fields according to the analyzer.

---

### suppliers

**Type**: Target Table (Source-Aligned)
**Description**: Target supplier master table preserving supplier identifiers, names, contact details, and audit timestamps in a source-aligned target design.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| supplier_id | bigint | NO | Unique identifier for each supplier, serving as the primary key in the suppliers table. |
| name | nvarchar collate "sql_latin1_general_cp1_ci_as" | NO | The "name" column in the "suppliers" table is a non-nullable nvarchar field intended to store the name of the supplier, though sample data indicates it is currently unused. |
| contact_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | The "contact_name" column in the "suppliers" table stores the name of the primary contact person for a supplier, allowing null values. |
| email | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | Stores the email address of the supplier, allowing null values. |
| phone | nvarchar collate "sql_latin1_general_cp1_ci_as" | YES | The "phone" column in the "suppliers" table stores the contact phone number of a supplier as a nullable string. |
| created_at | datetime2 | NO | Records the timestamp when a supplier entry is initially created in the system, stored as a non-nullable datetime value. |
| updated_at | datetime2 | NO | The `updated_at` column stores the non-nullable timestamp of the most recent update to a supplier record in the `suppliers` table. |

**Primary Key**: supplier_id
**Grain**: One row per supplier_id
**Business Rules**:
- Incremental candidates: updated_at
- Contains sensitive or personal data fields according to the analyzer.

---
