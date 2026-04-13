## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Retail Operations Target Model |
| **System / Module** | Retail Operations Target Model |
| **STM Version** | 1.0 |
| **Author** | Cursor |
| **Date Created** | 2026-04-09 |
| **Last Updated** |  |
| **Approved By** |  |

---

## 2. Business Context
**Purpose / Use Case:**  
> Target inventory table preserving current stock levels, store-product relationships, stock value measures, and unit-normalized inventory attributes.

**Stakeholders:**  
- **Business Owner(s):**  
- **Technical Owner(s):**  
- **Data Consumer(s):**  

**Dependencies / Related Documentation:**  
- Requirements Document:  
- ERD / Data Model:  retail-operations-source-model-2026-04-09.md  
- Analyzer Schema JSON:  schema_azure_mssql_dbo.json  
- Job Orchestration Diagram:  

---

## 3. Source System Inventory
| Source System | Database / Schema | Table / File | Frequency | Owner | Notes |
|---------------|-------------------|--------------|-----------|-------|-------|
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | See field-level mapping |  |  | Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | inventory |  | One row per inventory_id / inventory_id |  | Target Table (Source-Aligned) | Target inventory table preserving current stock levels, store-product relationships, stock value measures, and unit-normalized inventory attributes. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Silver | Certification |
| Table |  | Criticality.StockReplenishment | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.TransientOperational | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | inventory_id | Architecture.Raw | Architecture |
| Column | inventory_id | Criticality.TransactionalCore | Criticality |
| Column | inventory_id | PII.None | PII |
| Column | inventory_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | store_id | Criticality.Operational | Criticality |
| Column | store_id | PII.None | PII |
| Column | product_id | Criticality.TransactionalCore | Criticality |
| Column | product_id | PII.None | PII |
| Column | quantity_on_hand | Architecture.Raw | Architecture |
| Column | quantity_on_hand | Criticality.StockReplenishment | Criticality |
| Column | quantity_on_hand | PII.None | PII |
| Column | quantity_on_hand | QualityTrust.SystemOfRecord | QualityTrust |
| Column | reorder_level | Criticality.StockReplenishment | Criticality |
| Column | reorder_level | PII.None | PII |
| Column | last_restocked_at | Criticality.StockReplenishment | Criticality |
| Column | last_restocked_at | PII.None | PII |
| Column | created_at | Architecture.Raw | Architecture |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | Lifecycle.Active | Lifecycle |
| Column | created_at | PII.None | PII |
| Column | created_at | QualityTrust.SystemOfRecord | QualityTrust |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | updated_at | QualityTrust.SystemOfRecord | QualityTrust |
| Column | updated_at | Retention.TransientOperational | Retention |
| Column | stock_value | Criticality.StockReplenishment | Criticality |
| Column | stock_value | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | stock_value | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Inventory | Inventory | The quantity and value of products held at locations or in transit, maintained to meet customer demand while controlling carrying cost. **Type:** business_measure \| **Usage:** Stock reporting, replenishment planning, and balance-sheet valuation.

Review status: draft. |
| Table |  | RetailDomainGlossary.InventoryTracking | InventoryTracking | The ongoing recording and reconciliation of stock quantities and movements across locations and products. **Type:** business_process \| **Usage:** Perpetual inventory, cycle counts, and stock accuracy programmes.

Review status: draft. |
| Table |  | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation | A distinct site used to scope inventory, sales, and operational activity within the retail network. **Type:** business_entity \| **Usage:** Inventory allocation, replenishment triggers, and cross-location performance comparison.

Review status: draft. |
| Column | product_id | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. **Type:** business_entity \| **Usage:** Merchandising, assortment planning, pricing, and inventory management.

Review status: draft. |
| Column | quantity_on_hand | RetailDomainGlossary.Inventory | Inventory | The quantity and value of products held at locations or in transit, maintained to meet customer demand while controlling carrying cost. **Type:** business_measure \| **Usage:** Stock reporting, replenishment planning, and balance-sheet valuation.

Review status: draft. |
| Column | reorder_level | RetailDomainGlossary.ReorderPoint | ReorderPoint | The inventory level at which a replenishment order is triggered, calculated from demand rate, lead time, and safety stock. **Type:** business_measure \| **Usage:** Automatic replenishment systems and stock-out prevention.

Inferred; complements safety stock in inventory management.

Review status: draft. |
| Column | stock_value | RetailDomainGlossary.Inventory | Inventory | The quantity and value of products held at locations or in transit, maintained to meet customer demand while controlling carrying cost. **Type:** business_measure \| **Usage:** Stock reporting, replenishment planning, and balance-sheet valuation.

Review status: draft. |
| Column | stock_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| inventory | inventory_id | bigint | Attribute | Snowflake | INVENTORY | INVENTORY_ID | Source type: number(38,0) | NO |  | Unique identifier for each inventory record, serving as the primary key for the inventory table. |
| inventory | store_id | bigint | Attribute | Snowflake | INVENTORY | STORE_ID | Source type: number(38,0) | NO |  | Identifier for the store associated with the inventory record, referencing the `store_id` column in the `stores` table. |
| inventory | product_id | bigint | Attribute | Snowflake | INVENTORY | PRODUCT_ID | Source type: number(38,0) | NO |  | References the unique identifier of a product in the products table to associate inventory records with specific products. |
| inventory | quantity_on_hand | integer | Attribute | Snowflake | INVENTORY | QUANTITY_ON_HAND | Source type: number(38,0) | NO |  | The `quantity_on_hand` column in the `inventory` table stores the current stock level of a product as a non-null integer. |
| inventory | reorder_level | integer | Attribute | Snowflake | INVENTORY | REORDER_LEVEL | Source type: number(38,0) | NO |  | Indicates the minimum quantity of a product in inventory that triggers a reorder, stored as a non-null integer. |
| inventory | last_restocked_at | datetime2 | Attribute | Snowflake | INVENTORY | LAST_RESTOCKED_AT | Source type: timestamp_ntz | YES |  | The `last_restocked_at` column records the timestamp of the most recent restocking event for an inventory item, allowing null values if the item has not been restocked. |
| inventory | created_at | datetime2 | Attribute | Snowflake | INVENTORY | CREATED_AT | Source type: timestamp_ntz | NO |  | The `created_at` column records the non-nullable timestamp indicating when the inventory record was initially created. |
| inventory | updated_at | datetime2 | Attribute | Snowflake | INVENTORY | UPDATED_AT | Source type: timestamp_ntz | NO |  | The `updated_at` column in the `inventory` table stores the non-nullable timestamp indicating the last update to the inventory record. |
| inventory | stock_value | numeric(10,2) | Attribute | Snowflake | INVENTORY | STOCK_VALUE | Source type: number(10,2) | YES |  | Represents the monetary value of inventory stock, stored as a numeric value with up to 10 digits and 2 decimal places, and may be null. |
| inventory | stock_unit | nvarchar(16) | Attribute | Snowflake | INVENTORY | STOCK_UNIT | Source type: text(32) | YES |  | Current stock unit label. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | INVENTORY_ID must not be NULL (primary key) | NOT NULL | INVENTORY_ID IS NOT NULL | Reject record |  |
| DQ2 | INVENTORY_ID must be unique | Uniqueness | COUNT(DISTINCT INVENTORY_ID) = COUNT(*) | Reject record |  |
| DQ3 | PRODUCT_ID referential integrity check | Referential Integrity | All PRODUCT_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ4 | STORE_ID referential integrity check | Referential Integrity | All STORE_ID values exist in referenced parent table | Flag / quarantine |  |
|  |  |  |  |  |  |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | Delta load using UPDATED_AT |  |  |  |  |
|  |  |  |  |  |  |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-13 | Cursor | Initial generation from target data model and analyzer schema JSON |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
