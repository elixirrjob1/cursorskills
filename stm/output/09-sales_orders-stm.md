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
|  |  |  |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
|  |  | sales_orders |  | One row per sales_order_id / sales_order_id |  | Target Table (Source-Aligned) | Target sales order header table preserving customer, store, employee, and sales-representative relationships together with order totals and statuses. |

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
| Table |  | RetailDomainGlossary.SalesOrder | SalesOrder |  |
| Column | sales_order_id | RetailDomainGlossary.SalesOrder | SalesOrder |  |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation |  |
| Column | customer_id | RetailDomainGlossary.CustomerIdentifier | CustomerIdentifier |  |
| Column | status | RetailDomainGlossary.OrderStatus | OrderStatus |  |
| Column | total_amount | RetailDomainGlossary.NetSales | NetSales |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| sales_orders | sales_order_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each sales order in the system. |
| sales_orders | store_id | bigint | Attribute |  |  |  |  | NO |  | Identifies the store associated with a sales order, referencing the `store_id` column in the `stores` table. |
| sales_orders | customer_id | bigint | Attribute |  |  |  |  | YES |  | Identifies the customer associated with a sales order, referencing the `customer_id` column in the `customers` table; nullable to accommodate orders without a registered customer. |
| sales_orders | employee_id | bigint | Attribute |  |  |  |  | NO |  | Identifies the employee responsible for processing the sales order, referencing the `employees.employee_id` column. |
| sales_orders | order_date | datetime2 | Attribute |  |  |  |  | NO |  | The `order_date` column records the date and time when a sales order was placed, stored as a non-nullable `datetime2` value. |
| sales_orders | status | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | Indicates the current state of a sales order, such as 'Pending' or 'Completed', stored as a non-nullable text category. |
| sales_orders | total_amount | numeric(12,2) | Attribute |  |  |  |  | NO |  | The `total_amount` column in the `sales_orders` table stores the non-nullable total monetary value of a sales order as a numeric value with up to 12 digits and 2 decimal places. |
| sales_orders | created_at | datetime2 | Attribute |  |  |  |  | NO |  | The `created_at` column records the timestamp when a sales order record was initially created, stored as a non-nullable `datetime2` value. |
| sales_orders | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | The `updated_at` column stores the non-nullable timestamp of the most recent update to a sales order record. |
| sales_orders | sales_rep_employee_id | bigint | Attribute |  |  |  |  | YES |  | Sales representative foreign key for order ownership. |

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
