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
> Target product master table preserving supplier relationships, product attributes, pricing, and physical measurement fields in a source-aligned target layout.

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
|  |  | products |  | One row per product_id / product_id |  | Target Table (Source-Aligned) | Target product master table preserving supplier relationships, product attributes, pricing, and physical measurement fields in a source-aligned target layout. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier1 | Tier |
| Column | product_id | Architecture.Raw | Architecture |
| Column | product_id | Criticality.TransactionalCore | Criticality |
| Column | product_id | PII.None | PII |
| Column | product_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | supplier_id | Criticality.TransactionalCore | Criticality |
| Column | supplier_id | PII.None | PII |
| Column | supplier_id | QualityTrust.SupplierProvided | QualityTrust |
| Column | sku | Criticality.TransactionalCore | Criticality |
| Column | sku | PII.None | PII |
| Column | sku | QualityTrust.SystemOfRecord | QualityTrust |
| Column | category | Criticality.Analytical | Criticality |
| Column | category | PII.None | PII |
| Column | unit_price | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | unit_price | Criticality.TransactionalCore | Criticality |
| Column | unit_price | PII.None | PII |
| Column | unit_price | Privacy.AnonymousAggregate | Privacy |
| Column | unit_price | QualityTrust.SystemOfRecord | QualityTrust |
| Column | unit_price | Retention.FinancialStatutory | Retention |
| Column | unit_price | Tier.Tier2 | Tier |
| Column | cost_price | Criticality.TransactionalCore | Criticality |
| Column | cost_price | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | cost_price | PII.None | PII |
| Column | cost_price | Privacy.AnonymousAggregate | Privacy |
| Column | cost_price | QualityTrust.SystemOfRecord | QualityTrust |
| Column | cost_price | Retention.FinancialStatutory | Retention |
| Column | active | Criticality.TransactionalCore | Criticality |
| Column | active | Lifecycle.Active | Lifecycle |
| Column | active | PII.None | PII |
| Column | created_at | Architecture.Raw | Architecture |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | PII.None | PII |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | weight_value | Criticality.Operational | Criticality |
| Column | weight_value | PII.None | PII |
| Column | length_value | Criticality.Analytical | Criticality |
| Column | length_value | PII.None | PII |
| Column | primary_supplier_id | Criticality.Operational | Criticality |
| Column | primary_supplier_id | QualityTrust.SupplierProvided | QualityTrust |
| Column | primary_supplier_id | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Product | Product |  |
| Table |  | RetailDomainGlossary.ProductCategory | ProductCategory |  |
| Table |  | RetailDomainGlossary.CostPrice | CostPrice |  |
| Table |  | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | product_id | RetailDomainGlossary.Product | Product |  |
| Column | supplier_id | RetailDomainGlossary.Supplier | Supplier |  |
| Column | sku | RetailDomainGlossary.Product | Product |  |
| Column | name | RetailDomainGlossary.Product | Product |  |
| Column | category | RetailDomainGlossary.ProductCategory | ProductCategory |  |
| Column | unit_price | RetailDomainGlossary.SellingPrice | SellingPrice |  |
| Column | cost_price | RetailDomainGlossary.CostPrice | CostPrice |  |
| Column | weight_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | weight_value | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | length_value | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | length_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure |  |
| Column | product_description | RetailDomainGlossary.Product | Product |  |
| Column | primary_supplier_id | RetailDomainGlossary.Supplier | Supplier |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| products | product_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each product in the retail system, serving as the primary key for the products table. |
| products | supplier_id | bigint | Attribute |  |  |  |  | NO |  | Represents the unique identifier of the supplier associated with a product, referencing the `supplier_id` column in the `suppliers` table. |
| products | sku | nvarchar(450) | Attribute |  |  |  |  | NO |  | Unique alphanumeric identifier for a product used for inventory and sales tracking, required and limited to 450 characters. |
| products | name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | The "name" column in the "products" table stores the non-nullable name of each product as a case-insensitive Unicode string. |
| products | category | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | Indicates the product's category classification, such as "Books," "Clothing," or "Electronics," stored as a non-nullable nvarchar string. |
| products | unit_price | numeric(10,2) | Attribute |  |  |  |  | NO |  | The `unit_price` column stores the non-null selling price of a product as a numeric value with up to 10 digits and 2 decimal places, representing a currency amount. |
| products | cost_price | numeric(10,2) | Attribute |  |  |  |  | NO |  | The `cost_price` column stores the non-null numeric cost amount (up to 10 digits with 2 decimal places) representing the purchase price of a product in the retail system. |
| products | active | bit | Attribute |  |  |  |  | NO |  | Indicates whether a product is active and available for transactions, stored as a non-nullable boolean value. |
| products | created_at | datetime2 | Attribute |  |  |  |  | NO |  | Indicates the timestamp when the product record was initially created, stored as a non-nullable datetime value. |
| products | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | The `updated_at` column stores the non-nullable timestamp of the most recent update to a product record in the `products` table. |
| products | weight_unit | nvarchar(16) | Attribute |  |  |  |  | YES |  | Source weight unit (kg/lb) used for unit inference testing. |
| products | weight_value | numeric(10,2) | Attribute |  |  |  |  | YES |  | The `weight_value` column stores the weight of a product as a numeric value with up to 10 digits and 2 decimal places, nullable if the weight is not specified. |
| products | length_value | numeric(10,2) | Attribute |  |  |  |  | YES |  | Stores the length measurement of a product as a numeric value with up to two decimal places, nullable if not applicable. |
| products | length_unit | nvarchar(16) | Attribute |  |  |  |  | YES |  | Source length unit (cm/in) used for unit inference testing. |
| products | product_description | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | YES |  | Stores optional textual details about a product, such as features or specifications. |
| products | primary_supplier_id | bigint | Attribute |  |  |  |  | YES |  | Primary supplier relationship used for join candidate detection. |

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
