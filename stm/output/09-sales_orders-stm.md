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
> Target sales order header table preserving customer, store, employee, and sales-representative relationships together with order totals and statuses.

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
| DRIP_DATA_INTELLIGENCE | GOLD | sales_orders |  | One row per sales_order_id / sales_order_id |  | Target Table (Source-Aligned) | Target sales order header table preserving customer, store, employee, and sales-representative relationships together with order totals and statuses. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Silver | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | PII.Sensitive | PII |
| Table |  | Privacy.PseudonymisedTransactional | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | sales_order_id | Criticality.TransactionalCore | Criticality |
| Column | sales_order_id | PII.None | PII |
| Column | sales_order_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | store_id | Criticality.TransactionalCore | Criticality |
| Column | store_id | PII.None | PII |
| Column | customer_id | Criticality.TransactionalCore | Criticality |
| Column | customer_id | PersonalData.Personal | PersonalData |
| Column | customer_id | PII.NonSensitive | PII |
| Column | employee_id | Criticality.TransactionalCore | Criticality |
| Column | employee_id | PII.None | PII |
| Column | order_date | Architecture.Raw | Architecture |
| Column | order_date | Criticality.TransactionalCore | Criticality |
| Column | order_date | Lifecycle.Active | Lifecycle |
| Column | order_date | PII.None | PII |
| Column | order_date | QualityTrust.SystemOfRecord | QualityTrust |
| Column | order_date | Retention.FinancialStatutory | Retention |
| Column | order_date | Tier.Tier1 | Tier |
| Column | status | Criticality.TransactionalCore | Criticality |
| Column | status | PII.None | PII |
| Column | total_amount | Criticality.TransactionalCore | Criticality |
| Column | total_amount | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | total_amount | PII.None | PII |
| Column | total_amount | Privacy.AnonymousAggregate | Privacy |
| Column | total_amount | QualityTrust.SystemOfRecord | QualityTrust |
| Column | total_amount | Retention.FinancialStatutory | Retention |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | PII.None | PII |
| Column | created_at | Architecture.Raw | Architecture |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | updated_at | QualityTrust.SystemOfRecord | QualityTrust |
| Column | sales_rep_employee_id | Criticality.TransactionalCore | Criticality |
| Column | sales_rep_employee_id | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.SalesOrder | SalesOrder | A customer-facing order recording one or more items requested for purchase, along with delivery and payment expectations. **Type:** business_entity \| **Usage:** Fulfilment, delivery scheduling, and revenue recognition.

Inferred as the customer-side specialisation of Order.

Review status: draft. |
| Column | sales_order_id | RetailDomainGlossary.SalesOrder | SalesOrder | A customer-facing order recording one or more items requested for purchase, along with delivery and payment expectations. **Type:** business_entity \| **Usage:** Fulfilment, delivery scheduling, and revenue recognition.

Inferred as the customer-side specialisation of Order.

Review status: draft. |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation | A distinct site used to scope inventory, sales, and operational activity within the retail network. **Type:** business_entity \| **Usage:** Inventory allocation, replenishment triggers, and cross-location performance comparison.

Review status: draft. |
| Column | customer_id | RetailDomainGlossary.CustomerIdentifier | CustomerIdentifier | A unique key assigned to a known customer, often linked to a loyalty card or account registration. **Type:** identifier \| **Usage:** Transaction linking, customer service, and CRM.

Inferred from selling to customers.

Review status: draft. |
| Column | status | RetailDomainGlossary.OrderStatus | OrderStatus | The current state of an order in its lifecycle—placed, confirmed, picking, shipped, delivered, returned, or cancelled. **Type:** status \| **Usage:** Customer service, fulfilment dashboards, and SLA tracking.

Inferred from managing deliveries, returns, and financial monitoring of orders.

Review status: draft. |
| Column | total_amount | RetailDomainGlossary.NetSales | NetSales | Total sales revenue after deducting returns, refunds, and allowances. **Type:** business_measure \| **Usage:** Performance reporting, budgeting, and like-for-like comparison.

Inferred; foundational revenue measure for any retailer.

Review status: draft. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| sales_orders | sales_order_id | bigint | Attribute | Snowflake | SALES_ORDERS | SALES_ORDER_ID | Source type: number(38,0) | NO |  | Unique identifier for each sales order in the system. |
| sales_orders | store_id | bigint | Attribute | Snowflake | SALES_ORDERS | STORE_ID | Source type: number(38,0) | NO |  | Identifies the store associated with a sales order, referencing the `store_id` column in the `stores` table. |
| sales_orders | customer_id | bigint | Attribute | Snowflake | SALES_ORDERS | CUSTOMER_ID | Source type: number(38,0) | YES |  | Identifies the customer associated with a sales order, referencing the `customer_id` column in the `customers` table; nullable to accommodate orders without a registered customer. |
| sales_orders | employee_id | bigint | Attribute | Snowflake | SALES_ORDERS | EMPLOYEE_ID | Source type: number(38,0) | NO |  | Identifies the employee responsible for processing the sales order, referencing the `employees.employee_id` column. |
| sales_orders | order_date | datetime2 | Attribute | Snowflake | SALES_ORDERS | ORDER_DATE | Source type: timestamp_ntz | NO |  | The `order_date` column records the date and time when a sales order was placed, stored as a non-nullable `datetime2` value. |
| sales_orders | status | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | SALES_ORDERS | STATUS | Source type: text(256) | NO |  | Indicates the current state of a sales order, such as 'Pending' or 'Completed', stored as a non-nullable text category. |
| sales_orders | total_amount | numeric(12,2) | Attribute | Snowflake | SALES_ORDERS | TOTAL_AMOUNT | Source type: number(12,2) | NO |  | The `total_amount` column in the `sales_orders` table stores the non-nullable total monetary value of a sales order as a numeric value with up to 12 digits and 2 decimal places. |
| sales_orders | created_at | datetime2 | Attribute | Snowflake | SALES_ORDERS | CREATED_AT | Source type: timestamp_ntz | NO |  | The `created_at` column records the timestamp when a sales order record was initially created, stored as a non-nullable `datetime2` value. |
| sales_orders | updated_at | datetime2 | Attribute | Snowflake | SALES_ORDERS | UPDATED_AT | Source type: timestamp_ntz | NO |  | The `updated_at` column stores the non-nullable timestamp of the most recent update to a sales order record. |
| sales_orders | sales_rep_employee_id | bigint | Attribute | Snowflake | SALES_ORDERS | SALES_REP_EMPLOYEE_ID | Source type: number(38,0) | YES |  | Sales representative foreign key for order ownership. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | SALES_ORDER_ID must not be NULL (primary key) | NOT NULL | SALES_ORDER_ID IS NOT NULL | Reject record |  |
| DQ2 | SALES_ORDER_ID must be unique | Uniqueness | COUNT(DISTINCT SALES_ORDER_ID) = COUNT(*) | Reject record |  |
| DQ3 | CUSTOMER_ID referential integrity check | Referential Integrity | All CUSTOMER_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ4 | EMPLOYEE_ID referential integrity check | Referential Integrity | All EMPLOYEE_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ5 | SALES_REP_EMPLOYEE_ID referential integrity check | Referential Integrity | All SALES_REP_EMPLOYEE_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ6 | STORE_ID referential integrity check | Referential Integrity | All STORE_ID values exist in referenced parent table | Flag / quarantine |  |
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
