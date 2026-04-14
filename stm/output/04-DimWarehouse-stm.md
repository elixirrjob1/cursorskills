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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | No matching entity in bronze |  |  | OpenMetadata `list_tables` on `snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO` lists CUSTOMERS, EMPLOYEES, INVENTORY, PRODUCTS, PURCHASE_ORDERS, PURCHASE_ORDER_ITEMS, SALES_ORDERS, SALES_ORDER_ITEMS, STORES, SUPPLIERS only — no warehouse / DC table. DimWarehouse attributes are reference-sourced (see §8). |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimWarehouse | Type 1 | WarehouseHashPK |  | Conformed Dimension | Warehouse and distribution center locations with geographic hierarchy (Region > District > Warehouse). |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
|  |  |  |  |

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
| DimWarehouse | WarehouseHashPK | INT | Primary Key | Snowflake |  |  |  | NO |  | Surrogate primary key for warehouse dimension |
| DimWarehouse | WarehouseHashBK | VARCHAR(10) | Business Key | Snowflake |  |  |  | NO |  | Natural business key (warehouse code) |
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
| Full | Manual reference data load (file, MDM, or operational master feed) | On change or scheduled maintenance window | Authoritative warehouse hierarchy outside `BRONZE_ERP__DBO` | Idempotent full refresh; reject duplicates on `WarehouseHashBK`; validate required attributes |  |

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
