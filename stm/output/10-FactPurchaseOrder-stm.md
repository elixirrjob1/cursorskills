## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Retail Dimensional |
| **System / Module** | Retail Dimensional |
| **STM Version** | 1.0 |
| **Author** | fillip |
| **Date Created** | 2026-04-13 |
| **Last Updated** | 2026-04-14 |
| **Approved By** |  |

---

## 2. Business Context
**Purpose / Use Case:**  
> Purchase order tracking with milestone dates and quantities at each stage. Row is updated as PO progresses.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | PURCHASE_ORDER_ITEMS |  |  | Bronze replica via Fivetran. |
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | PURCHASE_ORDERS |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | FactPurchaseOrder |  | One row per purchase order line item / PurchaseOrderHashPK |  | Accumulating Snapshot Fact | Purchase order tracking with milestone dates and quantities at each stage. Row is updated as PO progresses. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | PurchaseOrderLineNumber | Architecture.Raw | Architecture |
| Column | PurchaseOrderLineNumber | Criticality.TransactionalCore | Criticality |
| Column | PurchaseOrderLineNumber | PII.None | PII |
| Column | PurchaseOrderLineNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | QuantityOrdered | Architecture.Raw | Architecture |
| Column | QuantityOrdered | Criticality.TransactionalCore | Criticality |
| Column | QuantityOrdered | PII.None | PII |
| Column | ProductHashFK | Architecture.Raw | Architecture |
| Column | ProductHashFK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashFK | PII.None | PII |
| Column | UnitCost | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | UnitCost | Criticality.TransactionalCore | Criticality |
| Column | UnitCost | PII.None | PII |
| Column | UnitCost | Privacy.AnonymousAggregate | Privacy |
| Column | UnitCost | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitCost | Retention.FinancialStatutory | Retention |
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Silver | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.PseudonymisedTransactional | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | PurchaseOrderNumber | Criticality.TransactionalCore | Criticality |
| Column | PurchaseOrderNumber | PII.None | PII |
| Column | PurchaseOrderNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WarehouseHashFK | Criticality.TransactionalCore | Criticality |
| Column | WarehouseHashFK | PII.None | PII |
| Column | DateOrderedHashFK | Criticality.TransactionalCore | Criticality |
| Column | DateOrderedHashFK | PII.None | PII |
| Column | DateExpectedHashFK | Criticality.Operational | Criticality |
| Column | DateExpectedHashFK | PII.None | PII |
| Column | SupplierHashFK | Criticality.TransactionalCore | Criticality |
| Column | SupplierHashFK | PII.None | PII |
| Column | SupplierHashFK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | OrderStatus | Criticality.TransactionalCore | Criticality |
| Column | OrderStatus | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.OrderLine | Order line | A single product entry on an order specifying the item, quantity, price, and any applicable discount. |
| Column | ProductHashFK | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | UnitCost | RetailDomainGlossary.CostPrice | Cost price | The amount the retailer pays the supplier per unit, before any rebates, allowances, or landed-cost adjustments. |
| Table |  | RetailDomainGlossary.PurchaseOrder | Purchase order | A commitment document from the retailer to a supplier specifying products, quantities, prices, and delivery expectations. |
| Column | WarehouseHashFK | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | SupplierHashFK | RetailDomainGlossary.Supplier | Supplier | An external party that provides goods to the retailer, typically under negotiated commercial terms. |
| Column | OrderStatus | RetailDomainGlossary.OrderStatus | Order status | The current state of an order in its lifecycle—placed, confirmed, picking, shipped, delivered, returned, or cancelled. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| FactPurchaseOrder | PurchaseOrderHashPK | INT | Primary Key | Snowflake | PURCHASE_ORDER_ITEMS | PO_ID, PO_ITEM_ID | Surrogate key from composite natural key: join line to header on PO_ID; derive hash or warehouse key from PO_ID + PO_ITEM_ID per ETL standard. | NO |  | Surrogate primary key for purchase order fact |
| FactPurchaseOrder | ProductHashFK | INT | Foreign Key | Snowflake | PURCHASE_ORDER_ITEMS | PRODUCT_ID | Lookup DimProduct on PRODUCT_ID; populate ProductHashFK. | NO |  | Foreign key to product dimension |
| FactPurchaseOrder | SupplierHashFK | INT | Foreign Key | Snowflake | PURCHASE_ORDERS | SUPPLIER_ID | Join PURCHASE_ORDER_ITEMS to PURCHASE_ORDERS on PO_ID; lookup DimSupplier on SUPPLIER_ID. | NO |  | Foreign key to supplier dimension |
| FactPurchaseOrder | WarehouseHashFK | INT | Foreign Key | Snowflake | PURCHASE_ORDERS | STORE_ID | Join on PO_ID; lookup receiving warehouse dimension on STORE_ID (store associated with the PO per catalogue). | NO |  | Foreign key to receiving warehouse dimension |
| FactPurchaseOrder | DateOrderedHashFK | INT | Foreign Key | Snowflake | PURCHASE_ORDERS | ORDER_DATE | Join on PO_ID; lookup date dimension on ORDER_DATE. | NO |  | Foreign key to date dimension (order placed date) |
| FactPurchaseOrder | DateExpectedHashFK | INT | Foreign Key | Snowflake | PURCHASE_ORDERS | EXPECTED_DATE | Join on PO_ID; lookup date dimension on EXPECTED_DATE. | YES |  | Foreign key to date dimension (expected delivery date) |
| FactPurchaseOrder | DateShippedHashFK | INT | Foreign Key | Snowflake |  |  | OpenMetadata catalogue for PURCHASE_ORDERS and PURCHASE_ORDER_ITEMS has no supplier ship date column; populate from another source or leave NULL. | YES |  | Foreign key to date dimension (supplier ship date) |
| FactPurchaseOrder | DateReceivedHashFK | INT | Foreign Key | Snowflake |  |  | OpenMetadata catalogue for PURCHASE_ORDERS and PURCHASE_ORDER_ITEMS has no warehouse received date column; populate from another source or leave NULL. | YES |  | Foreign key to date dimension (warehouse received date) |
| FactPurchaseOrder | DateInvoicedHashFK | INT | Foreign Key | Snowflake |  |  | OpenMetadata catalogue for PURCHASE_ORDERS and PURCHASE_ORDER_ITEMS has no invoice date column; populate from another source or leave NULL. | YES |  | Foreign key to date dimension (invoice received date) |
| FactPurchaseOrder | PurchaseOrderNumber | VARCHAR(20) | Attribute | Snowflake | PURCHASE_ORDERS | PO_ID | Join on PO_ID; cast PO_ID (number(38,0)) to VARCHAR for degenerate PO identifier; catalogue has no separate PO number text column. | NO |  | Degenerate dimension - source PO number |
| FactPurchaseOrder | PurchaseOrderLineNumber | INT | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | PO_ITEM_ID | Cast PO_ITEM_ID (number(38,0)) to INT; catalogue line key (no separate line sequence column). | NO |  | Line item number within the PO |
| FactPurchaseOrder | OrderStatus | VARCHAR(20) | Attribute | Snowflake | PURCHASE_ORDERS | STATUS | Join on PO_ID; map STATUS (text(256)) to target VARCHAR(20) per value map / truncation rules. | NO |  | Current status (Ordered, Shipped, Received, Invoiced, Closed, Cancelled) |
| FactPurchaseOrder | QuantityOrdered | INT | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | QUANTITY | Cast QUANTITY (number(38,0)) to INT. | NO |  | Quantity originally ordered |
| FactPurchaseOrder | QuantityShipped | INT | Attribute | Snowflake |  |  | No quantity-shipped column on PURCHASE_ORDERS or PURCHASE_ORDER_ITEMS in OpenMetadata catalogue. | YES |  | Quantity shipped by supplier |
| FactPurchaseOrder | QuantityReceived | INT | Attribute | Snowflake |  |  | No quantity-received column on PURCHASE_ORDERS or PURCHASE_ORDER_ITEMS in OpenMetadata catalogue. | YES |  | Quantity received at warehouse |
| FactPurchaseOrder | QuantityAccepted | INT | Attribute | Snowflake |  |  | No quantity-accepted column on PURCHASE_ORDERS or PURCHASE_ORDER_ITEMS in OpenMetadata catalogue. | YES |  | Quantity accepted after inspection |
| FactPurchaseOrder | QuantityRejected | INT | Attribute | Snowflake |  |  | No quantity-rejected column on PURCHASE_ORDERS or PURCHASE_ORDER_ITEMS in OpenMetadata catalogue. | YES |  | Quantity rejected during receiving |
| FactPurchaseOrder | UnitCost | DECIMAL(19,4) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | UNIT_COST | Cast UNIT_COST (number(10,2)) to DECIMAL(19,4). | NO |  | Agreed unit cost on purchase order |
| FactPurchaseOrder | OrderAmount | DECIMAL(19,4) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | QUANTITY, UNIT_COST | QUANTITY * UNIT_COST; cast result to DECIMAL(19,4). | NO |  | Total order line amount (QuantityOrdered x UnitCost) |
| FactPurchaseOrder | ShippedAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  | No shipped quantity or amount columns in OpenMetadata catalogue for these tables. | YES |  | Total shipped amount |
| FactPurchaseOrder | ReceivedAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  | No received quantity or amount columns in OpenMetadata catalogue for these tables. | YES |  | Total received amount |
| FactPurchaseOrder | InvoicedAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  | No invoiced amount column in OpenMetadata catalogue for these tables. | YES |  | Total invoiced amount |
| FactPurchaseOrder | DaysToShip | INT | Attribute | Snowflake |  |  | Target definition uses DateShipped − DateOrdered; ship date not present on PURCHASE_ORDERS or PURCHASE_ORDER_ITEMS in catalogue. | YES |  | Days from order to ship (DateShipped - DateOrdered) |
| FactPurchaseOrder | DaysInTransit | INT | Attribute | Snowflake |  |  | Target definition uses DateReceived − DateShipped; those dates not on PURCHASE_ORDERS or PURCHASE_ORDER_ITEMS in catalogue. | YES |  | Days in transit (DateReceived - DateShipped) |
| FactPurchaseOrder | DaysToReceive | INT | Attribute | Snowflake |  |  | Target definition uses DateReceived − DateOrdered; receive date not in catalogue. | YES |  | Total days to receive (DateReceived - DateOrdered) |
| FactPurchaseOrder | DaysToInvoice | INT | Attribute | Snowflake |  |  | Target definition uses DateInvoiced − DateReceived; those dates not in catalogue. | YES |  | Days from receipt to invoice (DateInvoiced - DateReceived) |
| FactPurchaseOrder | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| FactPurchaseOrder | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was last updated |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | High-water mark on source UPDATED_AT (e.g. GREATEST(PURCHASE_ORDERS.UPDATED_AT, PURCHASE_ORDER_ITEMS.UPDATED_AT) per joined grain); merge/upsert accumulating snapshot rows |  | Bronze PURCHASE_ORDER_ITEMS joined to PURCHASE_ORDERS on PO_ID |  |  |

---

## 9. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-14 | fillip | Initial generation from target data model and analyzer schema JSON |  |
| 1.1 | 2026-04-14 | fillip | Section 7–8 enriched from OpenMetadata snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO (PURCHASE_ORDERS, PURCHASE_ORDER_ITEMS) |  |

---

## 10. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
