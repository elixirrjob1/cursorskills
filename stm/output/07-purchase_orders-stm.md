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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | See field-level mapping |  |  | Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | purchase_orders |  | One row per po_id / po_id |  | Target Table (Source-Aligned) | Target purchase order header table preserving supplier, store, order-status, and approver relationships in an analyzer-compatible target structure. |

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
| Table |  | RetailDomainGlossary.PurchaseOrder | PurchaseOrder | A commitment document from the retailer to a supplier specifying products, quantities, prices, and delivery expectations. **Type:** identifier \| **Usage:** Order tracking, receiving, and three-way match with invoices.

Inferred; standard procurement artifact.

Review status: draft. |
| Table |  | RetailDomainGlossary.Supplier | Supplier | An external party that provides goods to the retailer, typically under negotiated commercial terms. **Type:** business_entity \| **Usage:** Procurement, vendor scorecards, cost negotiation, and accounts payable.

Review status: draft. |
| Table |  | RetailDomainGlossary.StoreLocation | StoreLocation | A distinct site used to scope inventory, sales, and operational activity within the retail network. **Type:** business_entity \| **Usage:** Inventory allocation, replenishment triggers, and cross-location performance comparison.

Review status: draft. |
| Table |  | RetailDomainGlossary.OrderStatus | OrderStatus | The current state of an order in its lifecycle—placed, confirmed, picking, shipped, delivered, returned, or cancelled. **Type:** status \| **Usage:** Customer service, fulfilment dashboards, and SLA tracking.

Inferred from managing deliveries, returns, and financial monitoring of orders.

Review status: draft. |
| Column | po_id | RetailDomainGlossary.PurchaseOrder | PurchaseOrder | A commitment document from the retailer to a supplier specifying products, quantities, prices, and delivery expectations. **Type:** identifier \| **Usage:** Order tracking, receiving, and three-way match with invoices.

Inferred; standard procurement artifact.

Review status: draft. |
| Column | supplier_id | RetailDomainGlossary.Supplier | Supplier | An external party that provides goods to the retailer, typically under negotiated commercial terms. **Type:** business_entity \| **Usage:** Procurement, vendor scorecards, cost negotiation, and accounts payable.

Review status: draft. |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation | A distinct site used to scope inventory, sales, and operational activity within the retail network. **Type:** business_entity \| **Usage:** Inventory allocation, replenishment triggers, and cross-location performance comparison.

Review status: draft. |
| Column | status | RetailDomainGlossary.OrderStatus | OrderStatus | The current state of an order in its lifecycle—placed, confirmed, picking, shipped, delivered, returned, or cancelled. **Type:** status \| **Usage:** Customer service, fulfilment dashboards, and SLA tracking.

Inferred from managing deliveries, returns, and financial monitoring of orders.

Review status: draft. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| purchase_orders | po_id | bigint | Attribute | Snowflake | PURCHASE_ORDERS | PO_ID | Source type: number(38,0) | NO |  | Unique identifier for purchase orders in the retail operations system. |
| purchase_orders | supplier_id | bigint | Attribute | Snowflake | PURCHASE_ORDERS | SUPPLIER_ID | Source type: number(38,0) | NO |  | The `supplier_id` column in the `purchase_orders` table is a non-nullable foreign key referencing the `supplier_id` column in the `suppliers` table, identifying the supplier associated with each purchase order. |
| purchase_orders | store_id | bigint | Attribute | Snowflake | PURCHASE_ORDERS | STORE_ID | Source type: number(38,0) | NO |  | Identifies the store associated with the purchase order, referencing the `store_id` column in the `stores` table. |
| purchase_orders | status | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | PURCHASE_ORDERS | STATUS | Source type: text(256) | NO |  | Indicates the current state of a purchase order, such as 'Ordered' or 'Received', using predefined category values. |
| purchase_orders | order_date | date | Attribute | Snowflake | PURCHASE_ORDERS | ORDER_DATE |  | NO |  | The `order_date` column in the `purchase_orders` table records the date a purchase order was placed, is mandatory, and uses the `date` data type. |
| purchase_orders | expected_date | date | Attribute | Snowflake | PURCHASE_ORDERS | EXPECTED_DATE |  | YES |  | The "expected_date" column in the "purchase_orders" table records the anticipated delivery date of a purchase order, allowing null values. |
| purchase_orders | created_at | datetime2 | Attribute | Snowflake | PURCHASE_ORDERS | CREATED_AT | Source type: timestamp_ntz | NO |  | The `created_at` column records the timestamp when a purchase order record was initially created, stored as a non-nullable `datetime2` value. |
| purchase_orders | updated_at | datetime2 | Attribute | Snowflake | PURCHASE_ORDERS | UPDATED_AT | Source type: timestamp_ntz | NO |  | The `updated_at` column stores the non-nullable timestamp indicating the last modification date and time of a purchase order record. |
| purchase_orders | approver_employee_id | bigint | Attribute | Snowflake | PURCHASE_ORDERS | APPROVER_EMPLOYEE_ID | Source type: number(38,0) | YES |  | Approver employee foreign key for procurement workflow. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | PO_ID must not be NULL (primary key) | NOT NULL | PO_ID IS NOT NULL | Reject record |  |
| DQ2 | PO_ID must be unique | Uniqueness | COUNT(DISTINCT PO_ID) = COUNT(*) | Reject record |  |
| DQ3 | APPROVER_EMPLOYEE_ID referential integrity check | Referential Integrity | All APPROVER_EMPLOYEE_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ4 | STORE_ID referential integrity check | Referential Integrity | All STORE_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ5 | SUPPLIER_ID referential integrity check | Referential Integrity | All SUPPLIER_ID values exist in referenced parent table | Flag / quarantine |  |
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
