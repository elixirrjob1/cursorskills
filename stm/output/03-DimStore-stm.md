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
> Retail store locations with geographic hierarchy (Region > District > Store).

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | STORES |  |  | Bronze replica via Fivetran. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimStore | Type 1 | StoreHashPK |  | Dimension | Retail store locations with geographic hierarchy (Region > District > Store). |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | Criticality.Operational | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.Sensitive | PII |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | StoreHashPK | Criticality.TransactionalCore | Criticality |
| Column | StoreHashPK | PII.None | PII |
| Column | StoreHashPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | StoreHashBK | Criticality.Operational | Criticality |
| Column | StoreHashBK | PII.None | PII |
| Column | StreetAddress | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | StreetAddress | PII.Sensitive | PII |
| Column | StreetAddress | PersonalData.Personal | PersonalData |
| Column | City | PII.None | PII |
| Column | StoreName | Criticality.Operational | Criticality |
| Column | StoreName | PII.NonSensitive | PII |
| Column | StoreName | PersonalData.Personal | PersonalData |
| Column | PostalCode | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | PostalCode | PII.NonSensitive | PII |
| Column | StoreType | Architecture.Enriched | Architecture |
| Column | StoreType | Certification.Gold | Certification |
| Column | StoreType | Criticality.Operational | Criticality |
| Column | StoreType | PII.None | PII |
| Column | Country | Architecture.Enriched | Architecture |
| Column | Country | Certification.Gold | Certification |
| Column | Country | Criticality.Operational | Criticality |
| Column | Country | PII.None | PII |
| Column | Latitude | Architecture.Enriched | Architecture |
| Column | Latitude | Certification.Gold | Certification |
| Column | Latitude | Criticality.Operational | Criticality |
| Column | Latitude | PII.None | PII |
| Column | Longitude | Architecture.Enriched | Architecture |
| Column | Longitude | Certification.Gold | Certification |
| Column | Longitude | Criticality.Operational | Criticality |
| Column | Longitude | PII.None | PII |
| Column | DistrictCode | Architecture.Enriched | Architecture |
| Column | DistrictCode | Certification.Gold | Certification |
| Column | DistrictCode | Criticality.Operational | Criticality |
| Column | DistrictCode | PII.None | PII |
| Column | DistrictName | Architecture.Enriched | Architecture |
| Column | DistrictName | Certification.Gold | Certification |
| Column | DistrictName | Criticality.Operational | Criticality |
| Column | DistrictName | PII.None | PII |
| Column | RegionCode | Architecture.Enriched | Architecture |
| Column | RegionCode | Certification.Gold | Certification |
| Column | RegionCode | Criticality.Operational | Criticality |
| Column | RegionCode | PII.None | PII |
| Column | RegionName | Architecture.Enriched | Architecture |
| Column | RegionName | Certification.Gold | Certification |
| Column | RegionName | Criticality.Operational | Criticality |
| Column | RegionName | PII.None | PII |
| Column | StoreManager | Architecture.Enriched | Architecture |
| Column | StoreManager | Certification.Gold | Certification |
| Column | StoreManager | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | StoreManager | Criticality.Operational | Criticality |
| Column | StoreManager | PII.NonSensitive | PII |
| Column | StoreManager | PersonalData.Personal | PersonalData |
| Column | OpenDate | Architecture.Enriched | Architecture |
| Column | OpenDate | Certification.Gold | Certification |
| Column | OpenDate | Criticality.Operational | Criticality |
| Column | OpenDate | PII.None | PII |
| Column | CloseDate | Architecture.Enriched | Architecture |
| Column | CloseDate | Certification.Gold | Certification |
| Column | CloseDate | Criticality.Operational | Criticality |
| Column | CloseDate | PII.None | PII |
| Column | SquareFootage | Architecture.Enriched | Architecture |
| Column | SquareFootage | Certification.Gold | Certification |
| Column | SquareFootage | Criticality.Operational | Criticality |
| Column | SquareFootage | PII.None | PII |
| Column | IsActive | Architecture.Enriched | Architecture |
| Column | IsActive | Certification.Gold | Certification |
| Column | IsActive | Criticality.Operational | Criticality |
| Column | IsActive | PII.None | PII |
| Column | EtlBatchId | Architecture.Enriched | Architecture |
| Column | EtlBatchId | Certification.Gold | Certification |
| Column | EtlBatchId | Criticality.Operational | Criticality |
| Column | EtlBatchId | PII.None | PII |
| Column | LoadTimestamp | Architecture.Enriched | Architecture |
| Column | LoadTimestamp | Certification.Gold | Certification |
| Column | LoadTimestamp | Criticality.Operational | Criticality |
| Column | LoadTimestamp | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.PhysicalStore | Physical store | A brick-and-mortar location where customers browse and purchase goods and where local inventory and staff are managed. |
| Column | StoreHashPK | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | StoreHashBK | RetailDomainGlossary.StoreCode | Store code | A unique short identifier assigned to each store location for use in systems, reporting, and logistics addressing. |
| Column | StoreType | RetailDomainGlossary.StoreFormat | Store format | The classification of a store by size, layout, or trade area focus, such as superstore, convenience, or neighbourhood format. |
| Column | Country | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | Latitude | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | Longitude | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | DistrictCode | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | DistrictName | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | RegionCode | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | RegionName | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimStore | StoreHashPK | NUMBER(19,0) | Primary Key | Snowflake | STORES | STORE_ID | HASH(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Surrogate primary key for store dimension |
| DimStore | StoreHashBK | NUMBER(19,0) | Business Key | Snowflake | STORES | CODE | HASH(COALESCE(CAST(CODE AS VARCHAR), '#@#@#@#@#')) | NO |  | Natural business key (store number) |
| DimStore | StoreName | VARCHAR(100) | Attribute | Snowflake | STORES | NAME |  | NO |  | Store display name |
| DimStore | StoreType | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Type of store (Flagship, Standard, Outlet, Express) |
| DimStore | StreetAddress | VARCHAR(200) | Attribute | Snowflake | STORES | ADDRESS |  | NO |  | Store street address |
| DimStore | City | VARCHAR(50) | Attribute | Snowflake | STORES | CITY |  | NO |  | City where store is located |
| DimStore | StateProvince | VARCHAR(50) | Attribute | Snowflake | STORES | STATE |  | NO |  | State or province |
| DimStore | PostalCode | VARCHAR(20) | Attribute | Snowflake | STORES | POSTAL_CODE |  | NO |  | Postal or ZIP code |
| DimStore | Country | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Country name |
| DimStore | Latitude | DECIMAL(9,6) | Attribute | Snowflake |  |  |  | YES |  | Geographic latitude for mapping |
| DimStore | Longitude | DECIMAL(9,6) | Attribute | Snowflake |  |  |  | YES |  | Geographic longitude for mapping |
| DimStore | DistrictCode | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | District identifier code |
| DimStore | DistrictName | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | District name (second level of hierarchy) |
| DimStore | RegionCode | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Region identifier code |
| DimStore | RegionName | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Region name (top level of hierarchy) |
| DimStore | StoreManager | VARCHAR(100) | Attribute | Snowflake |  |  |  | YES |  | Current store manager name |
| DimStore | OpenDate | DATE | Attribute | Snowflake |  |  |  | NO |  | Date store opened for business |
| DimStore | CloseDate | DATE | Attribute | Snowflake |  |  |  | YES |  | Date store closed (NULL if still open) |
| DimStore | SquareFootage | INT | Attribute | Snowflake |  |  |  | YES |  | Total store square footage |
| DimStore | IsActive | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if store is currently operating |
| DimStore | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimStore | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | Merge / Upsert on STORE_ID | Daily | STORES.UPDATED_AT |  |  |

---

## 9. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-16 | fillip | Initial generation from target data model and analyzer schema JSON |  |

---

## 10. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
