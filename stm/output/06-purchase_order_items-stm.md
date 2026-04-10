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
> Target purchase order line table preserving ordered quantities, unit costs, and explicit quantity-unit fields for procurement analytics and traceability.

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
|  |  | purchase_order_items |  | One row per po_item_id / po_item_id |  | Target Table (Source-Aligned) | Target purchase order line table preserving ordered quantities, unit costs, and explicit quantity-unit fields for procurement analytics and traceability. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | po_item_id | Architecture.Raw | Architecture |
| Column | po_item_id | Criticality.TransactionalCore | Criticality |
| Column | po_item_id | PII.None | PII |
| Column | po_item_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | po_id | Criticality.TransactionalCore | Criticality |
| Column | po_id | PII.None | PII |
| Column | product_id | Architecture.Raw | Architecture |
| Column | product_id | Criticality.TransactionalCore | Criticality |
| Column | product_id | PII.None | PII |
| Column | quantity | Architecture.Raw | Architecture |
| Column | quantity | Criticality.TransactionalCore | Criticality |
| Column | quantity | PII.None | PII |
| Column | unit_cost | Criticality.TransactionalCore | Criticality |
| Column | unit_cost | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | unit_cost | PII.None | PII |
| Column | unit_cost | Privacy.AnonymousAggregate | Privacy |
| Column | unit_cost | QualityTrust.SystemOfRecord | QualityTrust |
| Column | unit_cost | Retention.FinancialStatutory | Retention |
| Column | created_at | Criticality.Operational | Criticality |
| Column | created_at | PII.None | PII |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | ordered_qty_value | Architecture.Raw | Architecture |
| Column | ordered_qty_value | Criticality.TransactionalCore | Criticality |
| Column | ordered_qty_value | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.OrderLine | OrderLine |  |
| Table |  | RetailDomainGlossary.Quantity | Quantity |  |
| Table |  | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Table |  | RetailDomainGlossary.PurchaseOrder | PurchaseOrder |  |
| Column | po_id | RetailDomainGlossary.PurchaseOrder | PurchaseOrder |  |
| Column | product_id | RetailDomainGlossary.Product | Product |  |
| Column | quantity | RetailDomainGlossary.Quantity | Quantity |  |
| Column | unit_cost | RetailDomainGlossary.CostPrice | CostPrice |  |
| Column | ordered_qty_value | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | ordered_qty_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| purchase_order_items | po_item_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each purchase order item in the system, serving as the primary key for the `purchase_order_items` table. |
| purchase_order_items | po_id | bigint | Attribute |  |  |  |  | NO |  | Represents the unique identifier of the associated purchase order, linking purchase order items to their parent purchase order via a foreign key relationship to `purchase_orders.po_id`. |
| purchase_order_items | product_id | bigint | Attribute |  |  |  |  | NO |  | Identifier linking each purchase order item to a specific product in the products table. |
| purchase_order_items | quantity | integer | Attribute |  |  |  |  | NO |  | The `quantity` column in the `purchase_order_items` table stores the non-null integer value representing the number of units of a product included in a specific purchase order item. |
| purchase_order_items | unit_cost | numeric(10,2) | Attribute |  |  |  |  | NO |  | Represents the per-unit cost of a product in a purchase order, stored as a non-null numeric value with up to 10 digits and 2 decimal places. |
| purchase_order_items | created_at | datetime2 | Attribute |  |  |  |  | NO |  | The `created_at` column records the non-nullable timestamp indicating when each purchase order item record was created. |
| purchase_order_items | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | Records the timestamp of the last update made to a purchase order item, ensuring accurate tracking of modifications. |
| purchase_order_items | ordered_qty_value | numeric(10,2) | Attribute |  |  |  |  | YES |  | The `ordered_qty_value` column stores the numeric quantity value (up to two decimal places) of items ordered in a purchase order, which can be null. |
| purchase_order_items | ordered_qty_unit | nvarchar(16) | Attribute |  |  |  |  | YES |  | Ordered quantity unit (ea/box). |

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
