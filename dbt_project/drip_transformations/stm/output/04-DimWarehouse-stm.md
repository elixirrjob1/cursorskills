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
> Warehouse and distribution center locations with geographic hierarchy (Region > District > Warehouse).

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | See field-level mapping |  |  | Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimWarehouse | Type 1 | WarehouseHashPK |  | Conformed Dimension | Warehouse and distribution center locations with geographic hierarchy (Region > District > Warehouse). |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Certification.Gold | Certification |
| Table |  | Architecture.Enriched | Architecture |
| Column | WarehouseHashPK | Architecture.Enriched | Architecture |
| Column | WarehouseHashPK | Certification.Gold | Certification |
| Column | WarehouseHashPK | PII.None | PII |
| Column | WarehouseHashPK | Privacy.Non-Personal | Privacy |
| Column | WarehouseHashPK | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | WarehouseHashPK | Lifecycle.Active | Lifecycle |
| Column | WarehouseHashPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WarehouseHashPK | Criticality.StockReplenishment | Criticality |
| Column | WarehouseHashPK | Retention.Business Defined | Retention |
| Column | WarehouseHashBK | Architecture.Enriched | Architecture |
| Column | WarehouseHashBK | Certification.Gold | Certification |
| Column | WarehouseHashBK | PII.None | PII |
| Column | WarehouseHashBK | Privacy.Non-Personal | Privacy |
| Column | WarehouseHashBK | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | WarehouseHashBK | Lifecycle.Active | Lifecycle |
| Column | WarehouseHashBK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WarehouseHashBK | Criticality.StockReplenishment | Criticality |
| Column | WarehouseHashBK | Retention.Business Defined | Retention |
| Column | WarehouseName | Architecture.Enriched | Architecture |
| Column | WarehouseName | Certification.Gold | Certification |
| Column | WarehouseName | PII.None | PII |
| Column | WarehouseName | Privacy.Non-Personal | Privacy |
| Column | WarehouseName | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | WarehouseName | Lifecycle.Active | Lifecycle |
| Column | WarehouseName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WarehouseName | Criticality.StockReplenishment | Criticality |
| Column | WarehouseName | Retention.Business Defined | Retention |
| Column | WarehouseType | Architecture.Enriched | Architecture |
| Column | WarehouseType | Certification.Gold | Certification |
| Column | WarehouseType | PII.None | PII |
| Column | WarehouseType | Privacy.Non-Personal | Privacy |
| Column | WarehouseType | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | WarehouseType | Lifecycle.Active | Lifecycle |
| Column | WarehouseType | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WarehouseType | Criticality.StockReplenishment | Criticality |
| Column | WarehouseType | Retention.Business Defined | Retention |
| Column | StreetAddress | Architecture.Enriched | Architecture |
| Column | StreetAddress | Certification.Gold | Certification |
| Column | StreetAddress | PII.None | PII |
| Column | StreetAddress | Privacy.Non-Personal | Privacy |
| Column | StreetAddress | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | StreetAddress | Lifecycle.Active | Lifecycle |
| Column | StreetAddress | QualityTrust.SystemOfRecord | QualityTrust |
| Column | StreetAddress | Criticality.StockReplenishment | Criticality |
| Column | StreetAddress | Retention.Business Defined | Retention |
| Column | City | Architecture.Enriched | Architecture |
| Column | City | Certification.Gold | Certification |
| Column | City | PII.None | PII |
| Column | City | Privacy.Non-Personal | Privacy |
| Column | City | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | City | Lifecycle.Active | Lifecycle |
| Column | City | QualityTrust.SystemOfRecord | QualityTrust |
| Column | City | Criticality.StockReplenishment | Criticality |
| Column | City | Retention.Business Defined | Retention |
| Column | StateProvince | Architecture.Enriched | Architecture |
| Column | StateProvince | Certification.Gold | Certification |
| Column | StateProvince | PII.None | PII |
| Column | StateProvince | Privacy.Non-Personal | Privacy |
| Column | StateProvince | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | StateProvince | Lifecycle.Active | Lifecycle |
| Column | StateProvince | QualityTrust.SystemOfRecord | QualityTrust |
| Column | StateProvince | Criticality.StockReplenishment | Criticality |
| Column | StateProvince | Retention.Business Defined | Retention |
| Column | PostalCode | Architecture.Enriched | Architecture |
| Column | PostalCode | Certification.Gold | Certification |
| Column | PostalCode | PII.None | PII |
| Column | PostalCode | Privacy.Non-Personal | Privacy |
| Column | PostalCode | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | PostalCode | Lifecycle.Active | Lifecycle |
| Column | PostalCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | PostalCode | Criticality.StockReplenishment | Criticality |
| Column | PostalCode | Retention.Business Defined | Retention |
| Column | Country | Architecture.Enriched | Architecture |
| Column | Country | Certification.Gold | Certification |
| Column | Country | PII.None | PII |
| Column | Country | Privacy.Non-Personal | Privacy |
| Column | Country | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | Country | Lifecycle.Active | Lifecycle |
| Column | Country | QualityTrust.SystemOfRecord | QualityTrust |
| Column | Country | Criticality.StockReplenishment | Criticality |
| Column | Country | Retention.Business Defined | Retention |
| Column | DistrictCode | Architecture.Enriched | Architecture |
| Column | DistrictCode | Certification.Gold | Certification |
| Column | DistrictCode | PII.None | PII |
| Column | DistrictCode | Privacy.Non-Personal | Privacy |
| Column | DistrictCode | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | DistrictCode | Lifecycle.Active | Lifecycle |
| Column | DistrictCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DistrictCode | Criticality.StockReplenishment | Criticality |
| Column | DistrictCode | Retention.Business Defined | Retention |
| Column | DistrictName | Architecture.Enriched | Architecture |
| Column | DistrictName | Certification.Gold | Certification |
| Column | DistrictName | PII.None | PII |
| Column | DistrictName | Privacy.Non-Personal | Privacy |
| Column | DistrictName | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | DistrictName | Lifecycle.Active | Lifecycle |
| Column | DistrictName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DistrictName | Criticality.StockReplenishment | Criticality |
| Column | DistrictName | Retention.Business Defined | Retention |
| Column | RegionCode | Architecture.Enriched | Architecture |
| Column | RegionCode | Certification.Gold | Certification |
| Column | RegionCode | PII.None | PII |
| Column | RegionCode | Privacy.Non-Personal | Privacy |
| Column | RegionCode | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | RegionCode | Lifecycle.Active | Lifecycle |
| Column | RegionCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | RegionCode | Criticality.StockReplenishment | Criticality |
| Column | RegionCode | Retention.Business Defined | Retention |
| Column | RegionName | Architecture.Enriched | Architecture |
| Column | RegionName | Certification.Gold | Certification |
| Column | RegionName | PII.None | PII |
| Column | RegionName | Privacy.Non-Personal | Privacy |
| Column | RegionName | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | RegionName | Lifecycle.Active | Lifecycle |
| Column | RegionName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | RegionName | Criticality.StockReplenishment | Criticality |
| Column | RegionName | Retention.Business Defined | Retention |
| Column | TotalCapacityUnits | Architecture.Enriched | Architecture |
| Column | TotalCapacityUnits | Certification.Gold | Certification |
| Column | TotalCapacityUnits | PII.None | PII |
| Column | TotalCapacityUnits | Privacy.Non-Personal | Privacy |
| Column | TotalCapacityUnits | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | TotalCapacityUnits | Lifecycle.Active | Lifecycle |
| Column | TotalCapacityUnits | QualityTrust.SystemOfRecord | QualityTrust |
| Column | TotalCapacityUnits | Criticality.StockReplenishment | Criticality |
| Column | TotalCapacityUnits | Retention.Business Defined | Retention |
| Column | IsActive | Architecture.Enriched | Architecture |
| Column | IsActive | Certification.Gold | Certification |
| Column | IsActive | PII.None | PII |
| Column | IsActive | Privacy.Non-Personal | Privacy |
| Column | IsActive | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | IsActive | Lifecycle.Active | Lifecycle |
| Column | IsActive | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsActive | Criticality.StockReplenishment | Criticality |
| Column | IsActive | Retention.Business Defined | Retention |
| Column | EtlBatchId | Architecture.Enriched | Architecture |
| Column | EtlBatchId | Certification.Gold | Certification |
| Column | EtlBatchId | PII.None | PII |
| Column | EtlBatchId | Privacy.Non-Personal | Privacy |
| Column | EtlBatchId | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | EtlBatchId | Lifecycle.Active | Lifecycle |
| Column | EtlBatchId | QualityTrust.SystemOfRecord | QualityTrust |
| Column | EtlBatchId | Criticality.Operational | Criticality |
| Column | EtlBatchId | Retention.OperationalTransient | Retention |
| Column | LoadTimestamp | Architecture.Enriched | Architecture |
| Column | LoadTimestamp | Certification.Gold | Certification |
| Column | LoadTimestamp | PII.None | PII |
| Column | LoadTimestamp | Privacy.Non-Personal | Privacy |
| Column | LoadTimestamp | ComplianceLegal.No Obligation | ComplianceLegal |
| Column | LoadTimestamp | Lifecycle.Active | Lifecycle |
| Column | LoadTimestamp | QualityTrust.SystemOfRecord | QualityTrust |
| Column | LoadTimestamp | Criticality.Operational | Criticality |
| Column | LoadTimestamp | Retention.OperationalTransient | Retention |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
|  |  |  |  |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimWarehouse | WarehouseHashPK | NUMBER(19,0) | Primary Key | Snowflake |  |  | HASH(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#')) | NO |  | Surrogate primary key for warehouse dimension |
| DimWarehouse | WarehouseHashBK | NUMBER(19,0) | Business Key | Snowflake |  |  | HASH(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#')) | NO |  | Natural business key (warehouse code) |
| DimWarehouse | WarehouseName | VARCHAR(100) | Attribute | Snowflake |  |  |  | NO |  | Warehouse display name |
| DimWarehouse | WarehouseType | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Type of warehouse (Distribution Center, Regional, Local) |
| DimWarehouse | StreetAddress | VARCHAR(200) | Attribute | Snowflake |  |  |  | NO |  | Warehouse street address |
| DimWarehouse | City | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | City where warehouse is located |
| DimWarehouse | StateProvince | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | State or province |
| DimWarehouse | PostalCode | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Postal or ZIP code |
| DimWarehouse | Country | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Country name |
| DimWarehouse | DistrictCode | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | District identifier code |
| DimWarehouse | DistrictName | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | District name (second level of hierarchy) |
| DimWarehouse | RegionCode | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Region identifier code |
| DimWarehouse | RegionName | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Region name (top level of hierarchy) |
| DimWarehouse | TotalCapacityUnits | INT | Attribute | Snowflake |  |  |  | YES |  | Total storage capacity in standard units |
| DimWarehouse | IsActive | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if warehouse is currently operating |
| DimWarehouse | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimWarehouse | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
|  |  |  |  |  |  |

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
