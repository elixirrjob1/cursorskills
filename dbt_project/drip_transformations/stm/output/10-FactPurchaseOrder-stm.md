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
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Table |  | Privacy.PseudonymisedTransactional | Privacy |
| Column | PurchaseOrderLineNumber | Architecture.Enriched | Architecture |
| Column | PurchaseOrderLineNumber | Criticality.TransactionalCore | Criticality |
| Column | PurchaseOrderLineNumber | PII.None | PII |
| Column | PurchaseOrderLineNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | QuantityOrdered | Architecture.Enriched | Architecture |
| Column | QuantityOrdered | Criticality.TransactionalCore | Criticality |
| Column | QuantityOrdered | PII.None | PII |
| Column | ProductHashFK | Architecture.Enriched | Architecture |
| Column | ProductHashFK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashFK | PII.None | PII |
| Column | UnitCost | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | UnitCost | Criticality.TransactionalCore | Criticality |
| Column | UnitCost | PII.None | PII |
| Column | UnitCost | Privacy.AnonymousAggregate | Privacy |
| Column | UnitCost | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitCost | Retention.FinancialStatutory | Retention |
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
| Column | DateShippedHashFK | Architecture.Enriched | Architecture |
| Column | DateShippedHashFK | Certification.Gold | Certification |
| Column | DateShippedHashFK | Criticality.Operational | Criticality |
| Column | DateShippedHashFK | PII.None | PII |
| Column | DateReceivedHashFK | Architecture.Enriched | Architecture |
| Column | DateReceivedHashFK | Certification.Gold | Certification |
| Column | DateReceivedHashFK | Criticality.Operational | Criticality |
| Column | DateReceivedHashFK | PII.None | PII |
| Column | DateInvoicedHashFK | Architecture.Enriched | Architecture |
| Column | DateInvoicedHashFK | Certification.Gold | Certification |
| Column | DateInvoicedHashFK | Criticality.Operational | Criticality |
| Column | DateInvoicedHashFK | PII.None | PII |
| Column | QuantityShipped | Architecture.Enriched | Architecture |
| Column | QuantityShipped | Certification.Gold | Certification |
| Column | QuantityShipped | Criticality.StockReplenishment | Criticality |
| Column | QuantityShipped | PII.None | PII |
| Column | QuantityReceived | Architecture.Enriched | Architecture |
| Column | QuantityReceived | Certification.Gold | Certification |
| Column | QuantityReceived | Criticality.StockReplenishment | Criticality |
| Column | QuantityReceived | PII.None | PII |
| Column | QuantityAccepted | Architecture.Enriched | Architecture |
| Column | QuantityAccepted | Certification.Gold | Certification |
| Column | QuantityAccepted | Criticality.StockReplenishment | Criticality |
| Column | QuantityAccepted | PII.None | PII |
| Column | QuantityRejected | Architecture.Enriched | Architecture |
| Column | QuantityRejected | Certification.Gold | Certification |
| Column | QuantityRejected | Criticality.StockReplenishment | Criticality |
| Column | QuantityRejected | PII.None | PII |
| Column | ShippedAmount | Architecture.Enriched | Architecture |
| Column | ShippedAmount | Certification.Gold | Certification |
| Column | ShippedAmount | Criticality.TransactionalCore | Criticality |
| Column | ShippedAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | ShippedAmount | Retention.FinancialStatutory | Retention |
| Column | ShippedAmount | Tier.Tier1 | Tier |
| Column | ShippedAmount | PII.None | PII |
| Column | ReceivedAmount | Architecture.Enriched | Architecture |
| Column | ReceivedAmount | Certification.Gold | Certification |
| Column | ReceivedAmount | Criticality.TransactionalCore | Criticality |
| Column | ReceivedAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | ReceivedAmount | Retention.FinancialStatutory | Retention |
| Column | ReceivedAmount | Tier.Tier1 | Tier |
| Column | ReceivedAmount | PII.None | PII |
| Column | InvoicedAmount | Architecture.Enriched | Architecture |
| Column | InvoicedAmount | Certification.Gold | Certification |
| Column | InvoicedAmount | Criticality.TransactionalCore | Criticality |
| Column | InvoicedAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | InvoicedAmount | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | InvoicedAmount | Retention.FinancialStatutory | Retention |
| Column | InvoicedAmount | Tier.Tier1 | Tier |
| Column | InvoicedAmount | PII.None | PII |
| Column | DaysToShip | Architecture.Enriched | Architecture |
| Column | DaysToShip | Certification.Gold | Certification |
| Column | DaysToShip | Criticality.Analytical | Criticality |
| Column | DaysToShip | PII.None | PII |
| Column | DaysInTransit | Architecture.Enriched | Architecture |
| Column | DaysInTransit | Certification.Gold | Certification |
| Column | DaysInTransit | Criticality.Analytical | Criticality |
| Column | DaysInTransit | PII.None | PII |
| Column | DaysToReceive | Architecture.Enriched | Architecture |
| Column | DaysToReceive | Certification.Gold | Certification |
| Column | DaysToReceive | Criticality.Analytical | Criticality |
| Column | DaysToReceive | PII.None | PII |
| Column | DaysToInvoice | Architecture.Enriched | Architecture |
| Column | DaysToInvoice | Certification.Gold | Certification |
| Column | DaysToInvoice | Criticality.Analytical | Criticality |
| Column | DaysToInvoice | PII.None | PII |
| Column | EtlBatchId | Architecture.Enriched | Architecture |
| Column | EtlBatchId | Certification.Gold | Certification |
| Column | EtlBatchId | Criticality.Operational | Criticality |
| Column | EtlBatchId | Retention.OperationalTransient | Retention |
| Column | EtlBatchId | PII.None | PII |
| Column | LoadTimestamp | Architecture.Enriched | Architecture |
| Column | LoadTimestamp | Certification.Gold | Certification |
| Column | LoadTimestamp | Criticality.Operational | Criticality |
| Column | LoadTimestamp | Retention.OperationalTransient | Retention |
| Column | LoadTimestamp | PII.None | PII |

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
| Column | DateReceivedHashFK | RetailDomainGlossary.GoodsReceipt | Goods receipt | The event of accepting inbound goods against a purchase order or transfer, updating inventory and triggering payables workflows. |
| Column | DateInvoicedHashFK | RetailDomainGlossary.Invoice | Invoice | A document from a supplier requesting payment for goods delivered, matched against purchase orders and goods receipts. |
| Column | QuantityReceived | RetailDomainGlossary.GoodsReceipt | Goods receipt | The event of accepting inbound goods against a purchase order or transfer, updating inventory and triggering payables workflows. |
| Column | QuantityAccepted | RetailDomainGlossary.ThreeWayMatch | Three-way match | Verification that the purchase order, goods receipt, and supplier invoice agree on quantities and values before approving payment. |
| Column | QuantityRejected | RetailDomainGlossary.ThreeWayMatch | Three-way match | Verification that the purchase order, goods receipt, and supplier invoice agree on quantities and values before approving payment. |
| Column | ReceivedAmount | RetailDomainGlossary.GoodsReceipt | Goods receipt | The event of accepting inbound goods against a purchase order or transfer, updating inventory and triggering payables workflows. |
| Column | InvoicedAmount | RetailDomainGlossary.Invoice | Invoice | A document from a supplier requesting payment for goods delivered, matched against purchase orders and goods receipts. |
| Column | DaysToShip | RetailDomainGlossary.SupplierLeadTime | Supplier lead time | The elapsed time between placing a purchase order and receiving the goods at the designated location. |
| Column | DaysInTransit | RetailDomainGlossary.SupplierLeadTime | Supplier lead time | The elapsed time between placing a purchase order and receiving the goods at the designated location. |
| Column | DaysToReceive | RetailDomainGlossary.SupplierLeadTime | Supplier lead time | The elapsed time between placing a purchase order and receiving the goods at the designated location. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| FactPurchaseOrder | PurchaseOrderHashPK | NUMBER(19,0) | Primary Key | Snowflake | PURCHASE_ORDER_ITEMS | PO_ID \| PO_ITEM_ID | HASH(COALESCE(CAST(PO_ID AS VARCHAR), '#@#@#@#@#') \|\| '\|' \|\| COALESCE(CAST(PO_ITEM_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Surrogate primary key for purchase order fact |
| FactPurchaseOrder | ProductHashFK | NUMBER(19,0) | Foreign Key | Snowflake | PURCHASE_ORDER_ITEMS | PRODUCT_ID | HASH(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to product dimension |
| FactPurchaseOrder | SupplierHashFK | NUMBER(19,0) | Foreign Key | Snowflake | PURCHASE_ORDERS | SUPPLIER_ID | HASH(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to supplier dimension |
| FactPurchaseOrder | WarehouseHashFK | NUMBER(19,0) | Foreign Key | Snowflake | PURCHASE_ORDERS | STORE_ID | HASH(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to receiving warehouse dimension |
| FactPurchaseOrder | DateOrderedHashFK | NUMBER(19,0) | Foreign Key | Snowflake | PURCHASE_ORDERS | ORDER_DATE | HASH(COALESCE(CAST(ORDER_DATE AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to date dimension (order placed date) |
| FactPurchaseOrder | DateExpectedHashFK | NUMBER(19,0) | Foreign Key | Snowflake | PURCHASE_ORDERS | EXPECTED_DATE | IFF(EXPECTED_DATE IS NULL, NULL, HASH(COALESCE(CAST(EXPECTED_DATE AS VARCHAR), '#@#@#@#@#'))) | YES |  | Foreign key to date dimension (expected delivery date) |
| FactPurchaseOrder | DateShippedHashFK | NUMBER(19,0) | Foreign Key | Snowflake |  |  | IFF({SOURCE_COL} IS NULL, NULL, HASH(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'))) | YES |  | Foreign key to date dimension (supplier ship date) |
| FactPurchaseOrder | DateReceivedHashFK | NUMBER(19,0) | Foreign Key | Snowflake |  |  | IFF({SOURCE_COL} IS NULL, NULL, HASH(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'))) | YES |  | Foreign key to date dimension (warehouse received date) |
| FactPurchaseOrder | DateInvoicedHashFK | NUMBER(19,0) | Foreign Key | Snowflake |  |  | IFF({SOURCE_COL} IS NULL, NULL, HASH(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'))) | YES |  | Foreign key to date dimension (invoice received date) |
| FactPurchaseOrder | PurchaseOrderNumber | VARCHAR(20) | Attribute | Snowflake | PURCHASE_ORDERS | PO_ID | CAST(PO_ID AS VARCHAR(20)) | NO |  | Degenerate dimension - source PO number |
| FactPurchaseOrder | PurchaseOrderLineNumber | INT | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | PO_ITEM_ID |  | NO |  | Line item number within the PO |
| FactPurchaseOrder | OrderStatus | VARCHAR(20) | Attribute | Snowflake | PURCHASE_ORDERS | STATUS |  | NO |  | Current status (Ordered, Shipped, Received, Invoiced, Closed, Cancelled) |
| FactPurchaseOrder | QuantityOrdered | INT | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | QUANTITY |  | NO |  | Quantity originally ordered |
| FactPurchaseOrder | QuantityShipped | INT | Attribute | Snowflake |  |  |  | YES |  | Quantity shipped by supplier |
| FactPurchaseOrder | QuantityReceived | INT | Attribute | Snowflake |  |  |  | YES |  | Quantity received at warehouse |
| FactPurchaseOrder | QuantityAccepted | INT | Attribute | Snowflake |  |  |  | YES |  | Quantity accepted after inspection |
| FactPurchaseOrder | QuantityRejected | INT | Attribute | Snowflake |  |  |  | YES |  | Quantity rejected during receiving |
| FactPurchaseOrder | UnitCost | DECIMAL(19,4) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | UNIT_COST |  | NO |  | Agreed unit cost on purchase order |
| FactPurchaseOrder | OrderAmount | DECIMAL(19,4) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | QUANTITY, UNIT_COST | QUANTITY * UNIT_COST | NO |  | Total order line amount (QuantityOrdered x UnitCost) |
| FactPurchaseOrder | ShippedAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | YES |  | Total shipped amount |
| FactPurchaseOrder | ReceivedAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | YES |  | Total received amount |
| FactPurchaseOrder | InvoicedAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | YES |  | Total invoiced amount |
| FactPurchaseOrder | DaysToShip | INT | Attribute | Snowflake |  |  |  | YES |  | Days from order to ship (DateShipped - DateOrdered) |
| FactPurchaseOrder | DaysInTransit | INT | Attribute | Snowflake |  |  |  | YES |  | Days in transit (DateReceived - DateShipped) |
| FactPurchaseOrder | DaysToReceive | INT | Attribute | Snowflake |  |  |  | YES |  | Total days to receive (DateReceived - DateOrdered) |
| FactPurchaseOrder | DaysToInvoice | INT | Attribute | Snowflake |  |  |  | YES |  | Days from receipt to invoice (DateInvoiced - DateReceived) |
| FactPurchaseOrder | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| FactPurchaseOrder | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was last updated |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | CDC / Timestamp-based (PURCHASE_ORDERS.UPDATED_AT, PURCHASE_ORDER_ITEMS.UPDATED_AT) | Daily |  | Retry failed batch; log errors | dbt |

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
