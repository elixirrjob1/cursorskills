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
> Target purchase order header table preserving supplier, store, order-status, and approver relationships in an analyzer-compatible target structure.

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
|  |  | purchase_orders |  | One row per po_id / po_id |  | Target Table (Source-Aligned) | Target purchase order header table preserving supplier, store, order-status, and approver relationships in an analyzer-compatible target structure. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Silver | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.PseudonymisedTransactional | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | po_id | Criticality.TransactionalCore | Criticality |
| Column | po_id | PII.None | PII |
| Column | po_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | supplier_id | Criticality.TransactionalCore | Criticality |
| Column | supplier_id | PII.None | PII |
| Column | supplier_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | store_id | Criticality.TransactionalCore | Criticality |
| Column | store_id | PII.None | PII |
| Column | status | Criticality.TransactionalCore | Criticality |
| Column | status | PII.None | PII |
| Column | order_date | Criticality.TransactionalCore | Criticality |
| Column | order_date | PII.None | PII |
| Column | expected_date | Criticality.Operational | Criticality |
| Column | expected_date | PII.None | PII |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | PII.None | PII |
| Column | created_at | QualityTrust.SystemOfRecord | QualityTrust |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | approver_employee_id | Criticality.TransactionalCore | Criticality |
| Column | approver_employee_id | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.PurchaseOrder | PurchaseOrder |  |
| Table |  | RetailDomainGlossary.Supplier | Supplier |  |
| Table |  | RetailDomainGlossary.StoreLocation | StoreLocation |  |
| Table |  | RetailDomainGlossary.OrderStatus | OrderStatus |  |
| Column | po_id | RetailDomainGlossary.PurchaseOrder | PurchaseOrder |  |
| Column | supplier_id | RetailDomainGlossary.Supplier | Supplier |  |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation |  |
| Column | status | RetailDomainGlossary.OrderStatus | OrderStatus |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| purchase_orders | po_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for purchase orders in the retail operations system. |
| purchase_orders | supplier_id | bigint | Attribute |  |  |  |  | NO |  | The `supplier_id` column in the `purchase_orders` table is a non-nullable foreign key referencing the `supplier_id` column in the `suppliers` table, identifying the supplier associated with each purchase order. |
| purchase_orders | store_id | bigint | Attribute |  |  |  |  | NO |  | Identifies the store associated with the purchase order, referencing the `store_id` column in the `stores` table. |
| purchase_orders | status | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | Indicates the current state of a purchase order, such as 'Ordered' or 'Received', using predefined category values. |
| purchase_orders | order_date | date | Attribute |  |  |  |  | NO |  | The `order_date` column in the `purchase_orders` table records the date a purchase order was placed, is mandatory, and uses the `date` data type. |
| purchase_orders | expected_date | date | Attribute |  |  |  |  | YES |  | The "expected_date" column in the "purchase_orders" table records the anticipated delivery date of a purchase order, allowing null values. |
| purchase_orders | created_at | datetime2 | Attribute |  |  |  |  | NO |  | The `created_at` column records the timestamp when a purchase order record was initially created, stored as a non-nullable `datetime2` value. |
| purchase_orders | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | The `updated_at` column stores the non-nullable timestamp indicating the last modification date and time of a purchase order record. |
| purchase_orders | approver_employee_id | bigint | Attribute |  |  |  |  | YES |  | Approver employee foreign key for procurement workflow. |

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
