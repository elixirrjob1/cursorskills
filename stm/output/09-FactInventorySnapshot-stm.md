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
> Daily snapshot of inventory positions at each warehouse location.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | INVENTORY |  |  | Bronze replica via Fivetran. |
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | PRODUCTS |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | FactInventorySnapshot |  | One row per product per warehouse per day / InventorySnapshotHashPK |  | Periodic Snapshot Fact | Daily snapshot of inventory positions at each warehouse location. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Silver | Certification |
| Table |  | Criticality.StockReplenishment | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.TransientOperational | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | InventorySnapshotHashPK | Architecture.Raw | Architecture |
| Column | InventorySnapshotHashPK | Criticality.TransactionalCore | Criticality |
| Column | InventorySnapshotHashPK | PII.None | PII |
| Column | InventorySnapshotHashPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WarehouseHashFK | Criticality.Operational | Criticality |
| Column | WarehouseHashFK | PII.None | PII |
| Column | ReorderPoint | Criticality.StockReplenishment | Criticality |
| Column | ReorderPoint | PII.None | PII |
| Column | ProductHashFK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashFK | PII.None | PII |
| Column | QuantityAvailable | Architecture.Raw | Architecture |
| Column | QuantityAvailable | Criticality.StockReplenishment | Criticality |
| Column | QuantityAvailable | PII.None | PII |
| Column | QuantityAvailable | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier1 | Tier |
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
| Table |  | RetailDomainGlossary.Inventory | Inventory | The quantity and value of products held at locations or in transit, maintained to meet customer demand while controlling carrying cost. |
| Column | WarehouseHashFK | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | ReorderPoint | RetailDomainGlossary.ReorderPoint | Reorder point | The inventory level at which a replenishment order is triggered, calculated from demand rate, lead time, and safety stock. |
| Column | ProductHashFK | RetailDomainGlossary.ProductInventoryRelationship | Product-inventory relationship | The association between a product and its stock positions across locations, enabling availability and replenishment logic. |
| Column | QuantityAvailable | RetailDomainGlossary.LocationLevelInventory | Location-level inventory | Stock quantities and values attributed to a specific site, enabling store-specific availability and replenishment decisions. |
| Table |  | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | UnitCost | RetailDomainGlossary.CostPrice | Cost price | The amount the retailer pays the supplier per unit, before any rebates, allowances, or landed-cost adjustments. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| FactInventorySnapshot | InventorySnapshotHashPK | INT | Primary Key | Snowflake | INVENTORY | INVENTORY_ID | Generate surrogate key from INVENTORY_ID and the snapshot as-of calendar date from the ETL job; snapshot date is not a column on INVENTORY in OpenMetadata. | NO |  | Surrogate primary key for inventory snapshot |
| FactInventorySnapshot | DateHashFK | INT | Foreign Key | Snowflake |  |  | Use snapshot as-of calendar date from job context; lookup DimDate.HashPK. INVENTORY has no snapshot-date column in OpenMetadata. | NO |  | Foreign key to date dimension (snapshot date) |
| FactInventorySnapshot | ProductHashFK | INT | Foreign Key | Snowflake | INVENTORY | PRODUCT_ID | Lookup DimProduct.HashPK using PRODUCT_ID (OpenMetadata: number(38,0), FK to products). | NO |  | Foreign key to product dimension |
| FactInventorySnapshot | WarehouseHashFK | INT | Foreign Key | Snowflake | INVENTORY | STORE_ID | Lookup DimWarehouse.HashPK using STORE_ID (OpenMetadata: store location for the inventory row). | NO |  | Foreign key to warehouse dimension |
| FactInventorySnapshot | QuantityOnHand | INT | Attribute | Snowflake | INVENTORY | QUANTITY_ON_HAND | Cast source number(38,0) to INT. | NO |  | Total quantity physically in warehouse |
| FactInventorySnapshot | QuantityReserved | INT | Attribute | Snowflake |  |  | No matching column on INVENTORY in OpenMetadata; extend when a source is catalogued. | NO |  | Quantity reserved for pending orders |
| FactInventorySnapshot | QuantityAvailable | INT | Attribute | Snowflake | INVENTORY | QUANTITY_ON_HAND | Derived: QUANTITY_ON_HAND minus reserved quantity; reserved quantity is not on INVENTORY in OpenMetadata—supply from another source or rule. | NO |  | Quantity available for new orders (OnHand - Reserved) |
| FactInventorySnapshot | QuantityOnOrder | INT | Attribute | Snowflake |  |  | No matching column on INVENTORY in OpenMetadata; extend when a source is catalogued. | NO |  | Quantity on incoming purchase orders |
| FactInventorySnapshot | QuantityInTransit | INT | Attribute | Snowflake |  |  | No matching column on INVENTORY in OpenMetadata; extend when a source is catalogued. | NO |  | Quantity currently in transit to warehouse |
| FactInventorySnapshot | ReorderPoint | INT | Attribute | Snowflake | INVENTORY | REORDER_LEVEL | Cast source number(38,0) to INT; OpenMetadata describes REORDER_LEVEL as minimum quantity that triggers reorder (ReorderPoint glossary). | NO |  | Inventory level that triggers reorder |
| FactInventorySnapshot | SafetyStockLevel | INT | Attribute | Snowflake |  |  | No matching column on INVENTORY in OpenMetadata; extend when a source is catalogued. | NO |  | Minimum safety stock level |
| FactInventorySnapshot | DaysOfSupply | INT | Attribute | Snowflake |  |  | No matching column on INVENTORY in OpenMetadata; extend when a source is catalogued. | YES |  | Estimated days of supply based on avg demand |
| FactInventorySnapshot | UnitCost | DECIMAL(19,4) | Attribute | Snowflake | PRODUCTS | COST_PRICE | Join INVENTORY to PRODUCTS on PRODUCT_ID; map COST_PRICE (OpenMetadata: number(10,2)); cast or widen to DECIMAL(19,4). | NO |  | Current unit cost for valuation |
| FactInventorySnapshot | InventoryValue | DECIMAL(19,4) | Attribute | Snowflake | INVENTORY, PRODUCTS | QUANTITY_ON_HAND, COST_PRICE | Join INVENTORY to PRODUCTS on PRODUCT_ID; compute QUANTITY_ON_HAND multiplied by COST_PRICE as DECIMAL(19,4). INVENTORY.STOCK_VALUE exists in OpenMetadata as a stored monetary value; target defines quantity times unit cost. | NO |  | Total inventory value (QuantityOnHand x UnitCost) |
| FactInventorySnapshot | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| FactInventorySnapshot | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | High-water mark on bronze `INVENTORY.UPDATED_AT` (timestamp_ntz) |  | Source: `snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.INVENTORY`; join `PRODUCTS` on `PRODUCT_ID` for cost; dimensions DimDate (snapshot as-of), DimProduct, DimWarehouse |  |  |

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
