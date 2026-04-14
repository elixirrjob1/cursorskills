## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Retail Dimensional |
| **System / Module** | Retail Dimensional |
| **STM Version** | 1.0 |
| **Author** | fillip |
| **Date Created** | 2026-04-13 |
| **Last Updated** |  |
| **Approved By** |  |

---

## 2. Business Context
**Purpose / Use Case:**  
> Product master with full hierarchy (Category > Subcategory > Brand > Product). Supports SCD Type 2 for historical tracking.

**Stakeholders:**  
- **Business Owner(s):**  
- **Technical Owner(s):**  
- **Data Consumer(s):**  

**Dependencies / Related Documentation:**  
- Requirements Document:  
- ERD / Data Model:  retail-data-model-2026-04-13.md  
- Analyzer Schema JSON:  schema_azure_mssql_dbo.json  
- Job Orchestration Diagram:  

---

## 3. Source System Inventory
| Source System | Database / Schema | Table / File | Frequency | Owner | Notes |
|---------------|-------------------|--------------|-----------|-------|-------|
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | PRODUCTS |  |  | Bronze replica via Fivetran. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimProduct | Type 2 | ProductHashPK |  | Conformed Dimension | Product master with full hierarchy (Category > Subcategory > Brand > Product). Supports SCD Type 2 for historical tracking. |

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
| Column | ProductHashPK | Architecture.Raw | Architecture |
| Column | ProductHashPK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashPK | PII.None | PII |
| Column | ProductHashPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsActive | Criticality.TransactionalCore | Criticality |
| Column | IsActive | Lifecycle.Active | Lifecycle |
| Column | IsActive | PII.None | PII |
| Column | EffectiveDate | Architecture.Raw | Architecture |
| Column | EffectiveDate | Criticality.TransactionalCore | Criticality |
| Column | EffectiveDate | PII.None | PII |
| Column | UnitListPrice | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | UnitListPrice | Criticality.TransactionalCore | Criticality |
| Column | UnitListPrice | PII.None | PII |
| Column | UnitListPrice | Privacy.AnonymousAggregate | Privacy |
| Column | UnitListPrice | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitListPrice | Retention.FinancialStatutory | Retention |
| Column | UnitListPrice | Tier.Tier2 | Tier |
| Column | CategoryName | Criticality.Analytical | Criticality |
| Column | CategoryName | PII.None | PII |
| Column | ProductHashBK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashBK | PII.None | PII |
| Column | ProductHashBK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitCost | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | UnitCost | Criticality.TransactionalCore | Criticality |
| Column | UnitCost | PII.None | PII |
| Column | UnitCost | Privacy.AnonymousAggregate | Privacy |
| Column | UnitCost | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitCost | Retention.FinancialStatutory | Retention |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | UnitListPrice | RetailDomainGlossary.SellingPrice | Selling price | The amount charged to the customer for a product at the point of sale, before or after promotional adjustments. |
| Column | CategoryName | RetailDomainGlossary.ProductCategory | Product category | A grouping of related products used to organise the range for merchandising, reporting, and buying. |
| Column | ProductHashBK | RetailDomainGlossary.Barcode | Barcode | A machine-readable code (e.g. EAN, UPC) printed on a product or shelf label that uniquely identifies a SKU at point of sale and in logistics. |
| Column | UnitCost | RetailDomainGlossary.CostPrice | Cost price | The amount the retailer pays the supplier per unit, before any rebates, allowances, or landed-cost adjustments. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimProduct | ProductHashPK | INT | Primary Key | Snowflake | PRODUCTS | PRODUCT_ID | Generate surrogate key from PRODUCT_ID (source number(38,0) → INT hash/surrogate per ETL) | NO |  | Surrogate primary key for product dimension |
| DimProduct | ProductHashBK | VARCHAR(20) | Business Key | Snowflake | PRODUCTS | SKU | Source text(900); cast/truncate to VARCHAR(20) per target constraint | NO |  | Natural business key from source system (SKU) |
| DimProduct | ProductName | VARCHAR(100) | Attribute | Snowflake | PRODUCTS | NAME | Source text(256); truncate or validate length vs target VARCHAR(100) | NO |  | Full product display name |
| DimProduct | ProductDescription | VARCHAR(500) | Attribute | Snowflake | PRODUCTS | PRODUCT_DESCRIPTION | Source text(256); no widening needed for target VARCHAR(500) | YES |  | Detailed product description |
| DimProduct | CategoryCode | VARCHAR(10) | Attribute | Snowflake |  |  | PRODUCTS has CATEGORY (text) only; no separate category code column in catalogue | NO |  | Product category code |
| DimProduct | CategoryName | VARCHAR(50) | Attribute | Snowflake | PRODUCTS | CATEGORY | Source text(256); truncate vs target VARCHAR(50) if needed | NO |  | Product category name (top level of hierarchy) |
| DimProduct | SubcategoryCode | VARCHAR(10) | Attribute | Snowflake |  |  | No subcategory column on PRODUCTS in catalogue | NO |  | Product subcategory code |
| DimProduct | SubcategoryName | VARCHAR(50) | Attribute | Snowflake |  |  | No subcategory column on PRODUCTS in catalogue | NO |  | Product subcategory name (second level of hierarchy) |
| DimProduct | BrandCode | VARCHAR(10) | Attribute | Snowflake |  |  | No brand column on PRODUCTS in catalogue | NO |  | Brand identifier code |
| DimProduct | BrandName | VARCHAR(50) | Attribute | Snowflake |  |  | No brand column on PRODUCTS in catalogue | NO |  | Brand display name (third level of hierarchy) |
| DimProduct | UnitOfMeasure | VARCHAR(20) | Attribute | Snowflake |  |  | No retail UOM column on PRODUCTS (LENGTH_UNIT, WEIGHT_UNIT are dimension units only) | NO |  | Standard unit of measure (Each, Case, Pound, etc.) |
| DimProduct | PackSize | VARCHAR(20) | Attribute | Snowflake |  |  | No pack-size description column on PRODUCTS in catalogue | YES |  | Package size description |
| DimProduct | UnitCost | DECIMAL(19,4) | Attribute | Snowflake | PRODUCTS | COST_PRICE | Source number(10,2) → DECIMAL(19,4) | NO |  | Standard unit cost at time of record |
| DimProduct | UnitListPrice | DECIMAL(19,4) | Attribute | Snowflake | PRODUCTS | UNIT_PRICE | Source number(10,2) → DECIMAL(19,4) | NO |  | Standard list price at time of record |
| DimProduct | IsActive | BOOLEAN | Attribute | Snowflake | PRODUCTS | ACTIVE | Direct map (source boolean) | NO |  | True if product is currently active for sale |
| DimProduct | IsDiscontinued | BOOLEAN | Attribute | Snowflake |  |  | No discontinued flag on PRODUCTS in catalogue | NO |  | True if product has been discontinued |
| DimProduct | EffectiveDate | DATE | Attribute | Snowflake | PRODUCTS | CREATED_AT | Cast source timestamp_ntz to DATE; confirm vs SCD2 effective-dating design | NO |  | Start date when this version became effective |
| DimProduct | ExpirationDate | DATE | Attribute | Snowflake |  |  | ETL-generated SCD Type 2 end date | YES |  | End date when this version expired (NULL if current) |
| DimProduct | IsCurrent | BOOLEAN | Attribute | Snowflake |  |  | ETL-generated SCD Type 2 current-row flag | NO |  | True if this is the current active version |
| DimProduct | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimProduct | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | High-water mark / change capture on PRODUCTS.UPDATED_AT (timestamp_ntz) | Per schedule (e.g. daily) | Upstream Fivetran sync of BRONZE_ERP__DBO.PRODUCTS | Replay from last successful watermark; quarantine rows with NULL UPDATED_AT if encountered |  |

---

## 9. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-14 | fillip | Initial generation from target data model and analyzer schema JSON |  |

---

## 10. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
