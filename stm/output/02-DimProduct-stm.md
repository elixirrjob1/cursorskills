## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Retail Dimensional |
| **System / Module** | Retail Dimensional |
| **STM Version** | 2.0 |
| **Author** | fillip |
| **Date Created** | 2026-04-13 |
| **Last Updated** | 2026-04-21 |
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
| ERP | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | PRODUCTS | Near real-time via Fivetran | Data Engineering | Fivetran **history mode** — source already exposes per-version rows with `_FIVETRAN_START` / `_FIVETRAN_END` / `_FIVETRAN_ACTIVE`. Hard deletes surface as rows whose `_FIVETRAN_END` is not the sentinel '9999-12-31 23:59:59.999'. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimProduct | Type 2 | ProductHashPK | n/a (Snowflake) | Conformed Dimension | Product master with full hierarchy (Category > Subcategory > Brand > Product). Supports SCD Type 2 for historical tracking. |

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
| Column | ProductHashPK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashPK | PII.None | PII |
| Column | ProductHashPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsDiscontinued | Criticality.TransactionalCore | Criticality |
| Column | IsDiscontinued | Lifecycle.Active | Lifecycle |
| Column | IsDiscontinued | PII.None | PII |
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
| Column | ProductHashPK | Architecture.Enriched | Architecture |
| Column | ProductHashPK | Certification.Gold | Certification |
| Column | ProductHashPK | Lifecycle.Active | Lifecycle |
| Column | ProductHashPK | Privacy.AnonymousAggregate | Privacy |
| Column | ProductHashPK | Tier.Tier1 | Tier |
| Column | ProductHashBK | Architecture.Enriched | Architecture |
| Column | ProductHashBK | Certification.Gold | Certification |
| Column | ProductHashBK | Lifecycle.Active | Lifecycle |
| Column | ProductHashBK | Privacy.AnonymousAggregate | Privacy |
| Column | ProductHashBK | Tier.Tier1 | Tier |
| Column | BrandCode | Architecture.Enriched | Architecture |
| Column | BrandCode | Certification.Gold | Certification |
| Column | BrandCode | Criticality.Analytical | Criticality |
| Column | BrandCode | PII.None | PII |
| Column | BrandCode | Privacy.AnonymousAggregate | Privacy |
| Column | BrandCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | BrandName | Architecture.Enriched | Architecture |
| Column | BrandName | Certification.Gold | Certification |
| Column | BrandName | Criticality.Analytical | Criticality |
| Column | BrandName | PII.None | PII |
| Column | BrandName | Privacy.AnonymousAggregate | Privacy |
| Column | BrandName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | CategoryCode | Architecture.Enriched | Architecture |
| Column | CategoryCode | Certification.Gold | Certification |
| Column | CategoryCode | Criticality.Analytical | Criticality |
| Column | CategoryCode | PII.None | PII |
| Column | CategoryCode | Privacy.AnonymousAggregate | Privacy |
| Column | CategoryCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | CategoryName | Architecture.Enriched | Architecture |
| Column | CategoryName | Certification.Gold | Certification |
| Column | CategoryName | Privacy.AnonymousAggregate | Privacy |
| Column | CategoryName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsActive | Architecture.Enriched | Architecture |
| Column | IsActive | Certification.Gold | Certification |
| Column | IsActive | Criticality.TransactionalCore | Criticality |
| Column | IsActive | Lifecycle.Active | Lifecycle |
| Column | IsActive | PII.None | PII |
| Column | IsActive | Privacy.AnonymousAggregate | Privacy |
| Column | IsActive | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsDiscontinued | Architecture.Enriched | Architecture |
| Column | IsDiscontinued | Certification.Gold | Certification |
| Column | IsDiscontinued | Privacy.AnonymousAggregate | Privacy |
| Column | IsDiscontinued | QualityTrust.SystemOfRecord | QualityTrust |
| Column | PackSize | Architecture.Enriched | Architecture |
| Column | PackSize | Certification.Gold | Certification |
| Column | PackSize | Criticality.Analytical | Criticality |
| Column | PackSize | PII.None | PII |
| Column | PackSize | Privacy.AnonymousAggregate | Privacy |
| Column | PackSize | QualityTrust.SystemOfRecord | QualityTrust |
| Column | ProductDescription | Architecture.Enriched | Architecture |
| Column | ProductDescription | Certification.Gold | Certification |
| Column | ProductDescription | Criticality.Analytical | Criticality |
| Column | ProductDescription | PII.None | PII |
| Column | ProductDescription | Privacy.AnonymousAggregate | Privacy |
| Column | ProductDescription | QualityTrust.SystemOfRecord | QualityTrust |
| Column | ProductName | Architecture.Enriched | Architecture |
| Column | ProductName | Certification.Gold | Certification |
| Column | ProductName | Criticality.TransactionalCore | Criticality |
| Column | ProductName | PII.None | PII |
| Column | ProductName | Privacy.AnonymousAggregate | Privacy |
| Column | ProductName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | SubcategoryCode | Architecture.Enriched | Architecture |
| Column | SubcategoryCode | Certification.Gold | Certification |
| Column | SubcategoryCode | Criticality.Analytical | Criticality |
| Column | SubcategoryCode | PII.None | PII |
| Column | SubcategoryCode | Privacy.AnonymousAggregate | Privacy |
| Column | SubcategoryCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | SubcategoryName | Architecture.Enriched | Architecture |
| Column | SubcategoryName | Certification.Gold | Certification |
| Column | SubcategoryName | Criticality.Analytical | Criticality |
| Column | SubcategoryName | PII.None | PII |
| Column | SubcategoryName | Privacy.AnonymousAggregate | Privacy |
| Column | SubcategoryName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitCost | Architecture.Enriched | Architecture |
| Column | UnitCost | Certification.Gold | Certification |
| Column | UnitCost | Tier.Tier2 | Tier |
| Column | UnitListPrice | Architecture.Enriched | Architecture |
| Column | UnitListPrice | Certification.Gold | Certification |
| Column | UnitOfMeasure | Architecture.Enriched | Architecture |
| Column | UnitOfMeasure | Certification.Gold | Certification |
| Column | UnitOfMeasure | Criticality.Analytical | Criticality |
| Column | UnitOfMeasure | PII.None | PII |
| Column | UnitOfMeasure | Privacy.AnonymousAggregate | Privacy |
| Column | UnitOfMeasure | QualityTrust.SystemOfRecord | QualityTrust |
| Column | EffectiveStartDateTime | Architecture.Enriched | Architecture |
| Column | EffectiveStartDateTime | Certification.Gold | Certification |
| Column | EffectiveStartDateTime | Criticality.TransactionalCore | Criticality |
| Column | EffectiveStartDateTime | Lifecycle.Active | Lifecycle |
| Column | EffectiveStartDateTime | PII.None | PII |
| Column | EffectiveStartDateTime | Privacy.AnonymousAggregate | Privacy |
| Column | EffectiveStartDateTime | QualityTrust.SystemOfRecord | QualityTrust |
| Column | EffectiveEndDateTime | Architecture.Enriched | Architecture |
| Column | EffectiveEndDateTime | Certification.Gold | Certification |
| Column | EffectiveEndDateTime | Criticality.TransactionalCore | Criticality |
| Column | EffectiveEndDateTime | Lifecycle.Active | Lifecycle |
| Column | EffectiveEndDateTime | PII.None | PII |
| Column | EffectiveEndDateTime | Privacy.AnonymousAggregate | Privacy |
| Column | EffectiveEndDateTime | QualityTrust.SystemOfRecord | QualityTrust |
| Column | CurrentFlagYN | Architecture.Enriched | Architecture |
| Column | CurrentFlagYN | Certification.Gold | Certification |
| Column | CurrentFlagYN | Criticality.TransactionalCore | Criticality |
| Column | CurrentFlagYN | Lifecycle.Active | Lifecycle |
| Column | CurrentFlagYN | PII.None | PII |
| Column | CurrentFlagYN | Privacy.AnonymousAggregate | Privacy |
| Column | CurrentFlagYN | QualityTrust.SystemOfRecord | QualityTrust |
| Column | CreatedDateTime | Architecture.Enriched | Architecture |
| Column | CreatedDateTime | Certification.Gold | Certification |
| Column | CreatedDateTime | Criticality.Operational | Criticality |
| Column | CreatedDateTime | PII.None | PII |
| Column | CreatedDateTime | Privacy.AnonymousAggregate | Privacy |
| Column | CreatedDateTime | QualityTrust.SystemOfRecord | QualityTrust |
| Column | ModifiedDateTime | Architecture.Enriched | Architecture |
| Column | ModifiedDateTime | Certification.Gold | Certification |
| Column | ModifiedDateTime | Criticality.Operational | Criticality |
| Column | ModifiedDateTime | PII.None | PII |
| Column | ModifiedDateTime | Privacy.AnonymousAggregate | Privacy |
| Column | ModifiedDateTime | QualityTrust.SystemOfRecord | QualityTrust |
| Column | SourceSystemCode | Architecture.Enriched | Architecture |
| Column | SourceSystemCode | Certification.Gold | Certification |
| Column | SourceSystemCode | Criticality.Reference | Criticality |
| Column | SourceSystemCode | PII.None | PII |
| Column | SourceSystemCode | Privacy.AnonymousAggregate | Privacy |
| Column | SourceSystemCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | SourceProductPK | Architecture.Enriched | Architecture |
| Column | SourceProductPK | Certification.Gold | Certification |
| Column | SourceProductPK | Criticality.TransactionalCore | Criticality |
| Column | SourceProductPK | PII.None | PII |
| Column | SourceProductPK | Privacy.AnonymousAggregate | Privacy |
| Column | SourceProductPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | SourceProductBK | Architecture.Enriched | Architecture |
| Column | SourceProductBK | Certification.Gold | Certification |
| Column | SourceProductBK | Criticality.TransactionalCore | Criticality |
| Column | SourceProductBK | PII.None | PII |
| Column | SourceProductBK | Privacy.AnonymousAggregate | Privacy |
| Column | SourceProductBK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | FileName | Architecture.Enriched | Architecture |
| Column | FileName | Certification.Gold | Certification |
| Column | FileName | Criticality.Operational | Criticality |
| Column | FileName | PII.None | PII |
| Column | FileName | Privacy.AnonymousAggregate | Privacy |
| Column | FileName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | StageInsertedDateTimeUTC | Architecture.Enriched | Architecture |
| Column | StageInsertedDateTimeUTC | Certification.Gold | Certification |
| Column | StageInsertedDateTimeUTC | Criticality.Operational | Criticality |
| Column | StageInsertedDateTimeUTC | PII.None | PII |
| Column | StageInsertedDateTimeUTC | Privacy.AnonymousAggregate | Privacy |
| Column | StageInsertedDateTimeUTC | QualityTrust.SystemOfRecord | QualityTrust |
| Column | Hashbytes | Architecture.Enriched | Architecture |
| Column | Hashbytes | Certification.Gold | Certification |
| Column | Hashbytes | Criticality.Operational | Criticality |
| Column | Hashbytes | PII.None | PII |
| Column | Hashbytes | Privacy.AnonymousAggregate | Privacy |
| Column | Hashbytes | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DataCondition | Architecture.Enriched | Architecture |
| Column | DataCondition | Certification.Gold | Certification |
| Column | DataCondition | Criticality.Operational | Criticality |
| Column | DataCondition | PII.None | PII |
| Column | DataCondition | Privacy.AnonymousAggregate | Privacy |
| Column | DataCondition | QualityTrust.SystemOfRecord | QualityTrust |

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
| Column | ProductHashPK | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | BrandCode | RetailDomainGlossary.Brand | Brand | The manufacturer or label identity associated with a product, influencing customer perception and vendor negotiations. |
| Column | BrandName | RetailDomainGlossary.Brand | Brand | The manufacturer or label identity associated with a product, influencing customer perception and vendor negotiations. |
| Column | CategoryCode | RetailDomainGlossary.ProductCategory | Product category | A grouping of related products used to organise the range for merchandising, reporting, and buying. |
| Column | ProductDescription | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | ProductName | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | SubcategoryCode | RetailDomainGlossary.ProductHierarchy | Product hierarchy | The multi-level classification tree (e.g. division > department > category > sub-category > SKU) that structures the product range. |
| Column | SubcategoryName | RetailDomainGlossary.ProductHierarchy | Product hierarchy | The multi-level classification tree (e.g. division > department > category > sub-category > SKU) that structures the product range. |
| Column | UnitOfMeasure | RetailDomainGlossary.UnitOfMeasure | Unit of measure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. |
| Column | SourceProductPK | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | SourceProductBK | RetailDomainGlossary.Barcode | Barcode | A machine-readable code (e.g. EAN, UPC) printed on a product or shelf label that uniquely identifies a SKU at point of sale and in logistics. |

---

## 7. Field-Level Mapping Matrix

### Data Condition 1
> Per-version rows coming from Fivetran history-mode replication of `BRONZE_ERP__DBO.PRODUCTS`. Each version of a product row arrives as a distinct record with its own `_FIVETRAN_START` / `_FIVETRAN_END` window.

| Source System | Source Table | Source Column(s) | Column Alias | Transformation / Business Rule | Partition Field Rank | Order By Field Rank |
| -- | -- | -- | -- | -- | -- | -- |
| ERP | BRONZE_ERP__DBO.PRODUCTS | PRODUCT_ID | PRODUCT_ID |  | 1 | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | SKU | SKU |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | NAME | NAME |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | PRODUCT_DESCRIPTION | PRODUCT_DESCRIPTION |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | CATEGORY | CATEGORY |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | COST_PRICE | COST_PRICE |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | UNIT_PRICE | UNIT_PRICE |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | ACTIVE | ACTIVE |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | CREATED_AT | CREATED_AT |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | UPDATED_AT | UPDATED_AT |  | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | _FIVETRAN_START | EffectiveStartDateTimeUTC | Use `_FIVETRAN_START` directly (already `TIMESTAMP_TZ`, UTC). Partition-level start of each SCD2 version. | | 1 |
| ERP | BRONZE_ERP__DBO.PRODUCTS | _FIVETRAN_END | EffectiveEndDateTimeRaw | Use `_FIVETRAN_END` directly. Sentinel `'9999-12-31 23:59:59.999'` means current/open version. | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | _FIVETRAN_ACTIVE | IsFivetranActive | Passthrough — `TRUE` means current/open version for this PRODUCT_ID. | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | _FIVETRAN_SYNCED | InsertedDateTimeUTC | Pipeline sync timestamp (audit). | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | 'Data Condition 1' | DataCondition | Hard code as 'Data Condition 1'. | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | 'ERP' | SourceSystemCode | Hard code as 'ERP'. | | |
| ERP | BRONZE_ERP__DBO.PRODUCTS | '' | FileName | Hard code as empty string — Fivetran-sourced, no file origin. | | |

### Final
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| GOLD.DimProduct | ProductHashPK | NUMBER(19,0) | Primary Key | Derived from Data Condition 1 | Derived from Data Condition 1 | PRODUCT_ID | `HASH(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#'), 'ERP')` (TX1) | NO |  | Surrogate primary key for product dimension. |
| GOLD.DimProduct | ProductHashBK | NUMBER(19,0) | Business Key | Derived from Data Condition 1 | Derived from Data Condition 1 | SKU | `HASH(COALESCE(CAST(SKU AS VARCHAR), '#@#@#@#@#'), 'ERP')` (TX1) | NO |  | Natural business key from source system (SKU). |
| GOLD.DimProduct | BrandCode | VARCHAR(10) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Brand identifier code. |
| GOLD.DimProduct | BrandName | VARCHAR(50) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Brand display name (third level of hierarchy). |
| GOLD.DimProduct | CategoryCode | VARCHAR(10) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Product category code. |
| GOLD.DimProduct | CategoryName | VARCHAR(50) | Attribute | ERP | PRODUCTS | CATEGORY | `TRIM(CATEGORY)` | YES |  | Product category name (top level of hierarchy). |
| GOLD.DimProduct | IsActive | BOOLEAN | Attribute | ERP | PRODUCTS | ACTIVE | Direct mapping. | NO | FALSE | True if product is currently active for sale. |
| GOLD.DimProduct | IsDiscontinued | BOOLEAN | Attribute | ERP | PRODUCTS | ACTIVE | `IFF(ACTIVE, FALSE, TRUE)` | NO | TRUE | True if product has been discontinued. |
| GOLD.DimProduct | PackSize | VARCHAR(20) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Package size description. |
| GOLD.DimProduct | ProductDescription | VARCHAR(500) | Attribute | ERP | PRODUCTS | PRODUCT_DESCRIPTION | `TRIM(PRODUCT_DESCRIPTION)` | YES |  | Detailed product description. |
| GOLD.DimProduct | ProductName | VARCHAR(100) | Attribute | ERP | PRODUCTS | NAME | `TRIM(NAME)` | NO |  | Full product display name. |
| GOLD.DimProduct | SubcategoryCode | VARCHAR(10) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Product subcategory code. |
| GOLD.DimProduct | SubcategoryName | VARCHAR(50) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Product subcategory name (second level of hierarchy). |
| GOLD.DimProduct | UnitCost | NUMBER(19,4) | Attribute | ERP | PRODUCTS | COST_PRICE | `CAST(COST_PRICE AS NUMBER(19,4))` | YES |  | Standard unit cost at time of record. |
| GOLD.DimProduct | UnitListPrice | NUMBER(19,4) | Attribute | ERP | PRODUCTS | UNIT_PRICE | `CAST(UNIT_PRICE AS NUMBER(19,4))` | YES |  | Standard list price at time of record. |
| GOLD.DimProduct | UnitOfMeasure | VARCHAR(20) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Standard unit of measure (Each, Case, Pound, etc.). |
| GOLD.DimProduct | EffectiveStartDateTime | TIMESTAMP_TZ | Type 2 Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_START | `_FIVETRAN_START` passthrough. | NO |  | Start of Type 2 version validity window. |
| GOLD.DimProduct | EffectiveEndDateTime | TIMESTAMP_TZ | Type 2 Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_END | `_FIVETRAN_END` passthrough — `'9999-12-31 23:59:59.999'` for current versions. | NO | '9999-12-31 23:59:59.999 UTC' | End of Type 2 version validity window. |
| GOLD.DimProduct | CurrentFlagYN | VARCHAR(1) | Type 2 Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_ACTIVE | `IFF(_FIVETRAN_ACTIVE, 'Y', 'N')` | NO | 'N' | Flag indicating if this is the current active version (Y) or historical (N). |
| GOLD.DimProduct | CreatedDateTime | TIMESTAMP_TZ | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_START | `_FIVETRAN_START` — the moment this version entered the warehouse (BR1). | NO |  | Timestamp when the record version was created in the target. |
| GOLD.DimProduct | ModifiedDateTime | TIMESTAMP_TZ | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_SYNCED | `_FIVETRAN_SYNCED` (BR2). | NO |  | Timestamp when the record version was last modified. |
| GOLD.DimProduct | SourceSystemCode | VARCHAR(5) | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | 'ERP' | Hard code. | NO | 'ERP' | Source system identifier. |
| GOLD.DimProduct | SourceProductPK | VARCHAR(40) | Source | Derived from Data Condition 1 | Derived from Data Condition 1 | PRODUCT_ID | `CAST(PRODUCT_ID AS VARCHAR)` (TX2). | NO |  | Original PRODUCT_ID from source system (pre-hash). |
| GOLD.DimProduct | SourceProductBK | VARCHAR(100) | Source | Derived from Data Condition 1 | Derived from Data Condition 1 | SKU | `CAST(SKU AS VARCHAR)` (TX4). | NO |  | Original SKU from source system (pre-hash). |
| GOLD.DimProduct | FileName | VARCHAR(255) | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | '' | Hard code as empty string — Fivetran-sourced (BR3). | NO | '' | Source file name for audit (Fivetran: none). |
| GOLD.DimProduct | StageInsertedDateTimeUTC | TIMESTAMP_TZ | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_SYNCED | `_FIVETRAN_SYNCED` (BR4). | NO |  | UTC timestamp when record was synced into the bronze staging table by Fivetran. |
| GOLD.DimProduct | Hashbytes | BINARY | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | All attributes | SHA2_BINARY over alphabetically-sorted business attributes, pipe-separated, NULL replaced with '#@#@#@#@#' (BR5, TX6). | YES |  | Hash over business attributes used for change-detection in Type 2 SCD. |
| GOLD.DimProduct | DataCondition | VARCHAR(50) | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | 'Data Condition 1' | Hard code. | NO | 'Data Condition 1' | Indicates which Data Condition the record originated from. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Standard Attribute Fields | `CreatedDateTime` should be included on all objects as a `TIMESTAMP_TZ` datatype. |  |  |
| BR2 | Standard Attribute Fields | `ModifiedDateTime` should be included on all objects as a `TIMESTAMP_TZ` datatype. |  |  |
| BR3 | Standard Audit/Metadata Fields | `FileName` should be included on all objects as a `VARCHAR(255)` field. For Fivetran-sourced tables this is hard-coded to an empty string (no file concept). |  |  |
| BR4 | Standard Audit/Metadata Fields | `StageInsertedDateTimeUTC` should be included on all objects as a `TIMESTAMP_TZ`. Sourced from `_FIVETRAN_SYNCED`. The greatest value should be returned when multiple source tables contribute. |  |  |
| BR5 | Standard Audit/Metadata Fields | `Hashbytes` should be included on all objects as a `BINARY` column, sourced from a SHA2_256 hash of all business-attribute fields (excluding PK/FK/BK and all metadata columns). |  |  |
| BR6 | Standard Audit/Metadata Fields | Not applicable — Fivetran history mode replaces the need for an explicit `InsertedDateTimeUTC` per-record attribute. |  |  |
| BR7 | Standard Audit/Metadata Fields | Not applicable — Fivetran-sourced tables have no `InsertedByUser` semantic. |  |  |
| BR8 | Primary Key Cardinality | All objects should include only 1 `*HashPK` field. |  |  |
| BR9 | Foreign Key Cardinality | All objects should include 0 or more `*HashFK` fields. DimProduct has 0. |  |  |
| BR10 | Business Key Cardinality | All objects should include 0 or more `*HashBK` fields. DimProduct has 1 (`ProductHashBK`). |  |  |
| BR11 | Type 2 Data Conditions | SCD Type = Type 2 objects require at least 1 Data Condition and exactly 1 Final section. |  | Fivetran history mode supplies the full version stream in a single Data Condition. |
| BR12 | Type 2 Metadata | SCD Type = Type 2 objects require `EffectiveStartDateTime`, `EffectiveEndDateTime`, and `CurrentFlagYN` fields. |  |  |
| TX1 | Hash[PFB]K Transformation | All `*HashPK`, `*HashFK`, `*HashBK` values must be computed as `HASH(cast_fields, 'ERP')` with `SourceSystemCode` as the last argument. Returns a Snowflake 64-bit signed `NUMBER`. | `HASH(COALESCE(CAST(PRODUCT_ID AS VARCHAR),'#@#@#@#@#'),'ERP')` |  |
| TX2 | Source*PK Transformation | `Source[xxx]PK` stores the pre-hash source key(s), concatenated with `;` separator. | `CAST(PRODUCT_ID AS VARCHAR)` |  |
| TX3 | Source*FK Transformation | `Source[xxx]FK` stores pre-hash foreign-key value(s), `;` separator. | n/a for DimProduct |  |
| TX4 | Source*BK Transformation | `Source[xxx]BK` stores pre-hash business-key value(s), `;` separator. | `CAST(SKU AS VARCHAR)` |  |
| TX5 | stg Deduplication logic | Fivetran history mode already delivers one row per version per PK. No stg dedup needed. Optional hash-based `LAG` filter included as safety net. |  |  |
| TX6 | Hashbytes Transformation | Individual fields are concatenated together with `'\|'` separator and NULL-replaced with `'#@#@#@#@#'`, sorted alphabetically by source field name, then SHA2_256-hashed into a `BINARY`. |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | Multiple records from the source mapping view with the same (ProductHashPK, EffectiveStartDateTime) | SourceDupes | > 0 | Alert Owner | Data Engineering |
| DQ2 | Multiple records from the target with the same (ProductHashPK, EffectiveStartDateTime) | TargetDupes | > 0 | Alert Owner | Data Engineering |
| DQ3 | ProductHashPK appears in source mapping view but not in target table | SourceNotInTarget | > 0 | Alert Owner | Data Engineering |
| DQ4 | ProductHashPK appears in target table but not in source mapping view | TargetNotInSource | > 0 | Alert Owner | Data Engineering |
| DQ5 | Hashbytes differ in source mapping view compared to target for the same (ProductHashPK, EffectiveStartDateTime) | HashDiff | > 0 | Alert Owner | Data Engineering |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Full refresh | `dbt run -m vw_DimProduct DimProduct` — view rebuilt from Fivetran history every run; enriched table `table` materialization (full rebuild) to preserve Type 2 correctness. | Scheduled (per dbt Cloud job) | `BRONZE_ERP__DBO.PRODUCTS` loaded | Standard dbt error handling; failed tests skip downstream | dbt Cloud |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-16 | fillip | Initial generation from target data model and analyzer schema JSON. |  |
| 2.0 | 2026-04-21 | fillip | Restructured to follow Ajay Kalyan's Type 2 SCD pattern: Data Conditions, Final mapping, BR1–BR12, TX1–TX6. Switched to Fivetran history-mode-aware `_FIVETRAN_START`/`_FIVETRAN_END`/`_FIVETRAN_ACTIVE` for Type 2 metadata. Renamed EffectiveDate→EffectiveStartDateTime, ExpirationDate→EffectiveEndDateTime, IsCurrent→CurrentFlagYN. Dropped EtlBatchId; added SourceSystemCode, SourceProductPK/BK, FileName, StageInsertedDateTimeUTC, Hashbytes, DataCondition. |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
