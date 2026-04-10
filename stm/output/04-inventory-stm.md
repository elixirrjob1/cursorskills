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
|  |  |  |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
|  |  | inventory |  | One row per inventory_id / inventory_id |  | Target Table (Source-Aligned) | Target inventory table preserving current stock levels, store-product relationships, stock value measures, and unit-normalized inventory attributes. |

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
| Table |  | RetailDomainGlossary.Inventory | Inventory |  |
| Table |  | RetailDomainGlossary.InventoryTracking | InventoryTracking |  |
| Table |  | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation |  |
| Column | product_id | RetailDomainGlossary.Product | Product |  |
| Column | quantity_on_hand | RetailDomainGlossary.Inventory | Inventory |  |
| Column | reorder_level | RetailDomainGlossary.ReorderPoint | ReorderPoint |  |
| Column | stock_value | RetailDomainGlossary.Inventory | Inventory |  |
| Column | stock_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| inventory | inventory_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each inventory record, serving as the primary key for the inventory table. |
| inventory | store_id | bigint | Attribute |  |  |  |  | NO |  | Identifier for the store associated with the inventory record, referencing the `store_id` column in the `stores` table. |
| inventory | product_id | bigint | Attribute |  |  |  |  | NO |  | References the unique identifier of a product in the products table to associate inventory records with specific products. |
| inventory | quantity_on_hand | integer | Attribute |  |  |  |  | NO |  | The `quantity_on_hand` column in the `inventory` table stores the current stock level of a product as a non-null integer. |
| inventory | reorder_level | integer | Attribute |  |  |  |  | NO |  | Indicates the minimum quantity of a product in inventory that triggers a reorder, stored as a non-null integer. |
| inventory | last_restocked_at | datetime2 | Attribute |  |  |  |  | YES |  | The `last_restocked_at` column records the timestamp of the most recent restocking event for an inventory item, allowing null values if the item has not been restocked. |
| inventory | created_at | datetime2 | Attribute |  |  |  |  | NO |  | The `created_at` column records the non-nullable timestamp indicating when the inventory record was initially created. |
| inventory | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | The `updated_at` column in the `inventory` table stores the non-nullable timestamp indicating the last update to the inventory record. |
| inventory | stock_value | numeric(10,2) | Attribute |  |  |  |  | YES |  | Represents the monetary value of inventory stock, stored as a numeric value with up to 10 digits and 2 decimal places, and may be null. |
| inventory | stock_unit | nvarchar(16) | Attribute |  |  |  |  | YES |  | Current stock unit label. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
|  |  |  |  |  |  |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
|  |  |  |  |  |  |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-10 | Cursor | Initial generation from target data model and analyzer schema JSON |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
