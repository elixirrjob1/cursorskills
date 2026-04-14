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
> Sales transaction line items capturing all retail sales activity. Returns are modeled as negative quantities.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | SALES_ORDER_ITEMS |  |  | Bronze replica via Fivetran. |
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | SALES_ORDERS |  |  |  |
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | PRODUCTS |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | FactSales |  | One row per sales transaction line item / SalesHashPK |  | Transaction Fact | Sales transaction line items capturing all retail sales activity. Returns are modeled as negative quantities. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | TransactionLineNumber | Criticality.TransactionalCore | Criticality |
| Column | TransactionLineNumber | PII.None | PII |
| Column | TransactionLineNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | Quantity | Architecture.Raw | Architecture |
| Column | Quantity | Criticality.TransactionalCore | Criticality |
| Column | Quantity | PII.None | PII |
| Column | ProductHashFK | Criticality.TransactionalCore | Criticality |
| Column | ProductHashFK | PII.None | PII |
| Column | UnitPrice | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | UnitPrice | Criticality.TransactionalCore | Criticality |
| Column | UnitPrice | PII.None | PII |
| Column | UnitPrice | Privacy.AnonymousAggregate | Privacy |
| Column | UnitPrice | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitPrice | Retention.FinancialStatutory | Retention |
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Silver | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.Sensitive | PII |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | Privacy.PseudonymisedTransactional | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | TransactionNumber | Criticality.TransactionalCore | Criticality |
| Column | TransactionNumber | PII.None | PII |
| Column | TransactionNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | StoreHashFK | Criticality.TransactionalCore | Criticality |
| Column | StoreHashFK | PII.None | PII |
| Column | DateHashFK | Architecture.Raw | Architecture |
| Column | DateHashFK | Criticality.TransactionalCore | Criticality |
| Column | DateHashFK | Lifecycle.Active | Lifecycle |
| Column | DateHashFK | PII.None | PII |
| Column | DateHashFK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DateHashFK | Retention.FinancialStatutory | Retention |
| Column | DateHashFK | Tier.Tier1 | Tier |
| Column | CustomerHashFK | Criticality.TransactionalCore | Criticality |
| Column | CustomerHashFK | PII.NonSensitive | PII |
| Column | CustomerHashFK | PersonalData.Personal | PersonalData |
| Column | EmployeeHashFK | Criticality.TransactionalCore | Criticality |
| Column | EmployeeHashFK | PII.None | PII |
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.None | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier1 | Tier |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.OrderLine | Order line | A single product entry on an order specifying the item, quantity, price, and any applicable discount. |
| Column | ProductHashFK | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |
| Column | UnitPrice | RetailDomainGlossary.SellingPrice | Selling price | The amount charged to the customer for a product at the point of sale, before or after promotional adjustments. |
| Table |  | RetailDomainGlossary.SalesOrder | Sales order | A customer-facing order recording one or more items requested for purchase, along with delivery and payment expectations. |
| Column | StoreHashFK | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |
| Column | CustomerHashFK | RetailDomainGlossary.CustomerOrderRelationship | Customer-order relationship | The association between a customer and their orders, enabling purchase history, loyalty accrual, and service enquiries. |
| Table |  | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| FactSales | SalesHashPK | INT | Primary Key | Snowflake | SALES_ORDER_ITEMS | SALES_ORDER_ITEM_ID | Generate surrogate key (hash or sequence) from natural line key SALES_ORDER_ITEM_ID (number(38,0)). | NO |  | Surrogate primary key for sales fact |
| FactSales | DateHashFK | INT | Foreign Key | Snowflake | SALES_ORDERS | ORDER_DATE | Join SALES_ORDER_ITEMS to SALES_ORDERS on SALES_ORDER_ID; CAST(ORDER_DATE AS DATE) for role-playing date; lookup DimDate for DateHashFK. | NO |  | Foreign key to date dimension (transaction date) |
| FactSales | ProductHashFK | INT | Foreign Key | Snowflake | SALES_ORDER_ITEMS | PRODUCT_ID | Lookup DimProduct on natural key PRODUCT_ID (number(38,0)). | NO |  | Foreign key to product dimension |
| FactSales | StoreHashFK | INT | Foreign Key | Snowflake | SALES_ORDERS | STORE_ID | Join SALES_ORDER_ITEMS to SALES_ORDERS on SALES_ORDER_ID; lookup DimStore on STORE_ID. | NO |  | Foreign key to store dimension |
| FactSales | CustomerHashFK | INT | Foreign Key | Snowflake | SALES_ORDERS | CUSTOMER_ID | Join on SALES_ORDER_ID; lookup DimCustomer on CUSTOMER_ID (nullable in source for walk-in). | YES |  | Foreign key to customer dimension (NULL for anonymous sales) |
| FactSales | EmployeeHashFK | INT | Foreign Key | Snowflake | SALES_ORDERS | SALES_REP_EMPLOYEE_ID | Join on SALES_ORDER_ID; lookup DimEmployee on SALES_REP_EMPLOYEE_ID (catalogue: sales representative for order ownership). EMPLOYEE_ID on the same table identifies the employee who processed the order—use only if business rules map “associate” to that role instead. | YES |  | Foreign key to employee dimension (sales associate) |
| FactSales | TransactionNumber | VARCHAR(20) | Attribute | Snowflake | SALES_ORDERS | SALES_ORDER_ID | CAST(SALES_ORDER_ID AS VARCHAR(20)) for degenerate display; carry forward without dimension lookup. | NO |  | Degenerate dimension - source transaction ID |
| FactSales | TransactionLineNumber | INT | Attribute | Snowflake | SALES_ORDER_ITEMS | SALES_ORDER_ITEM_ID | CAST number(38,0) to INT. Bronze has no separate per-order line sequence; SALES_ORDER_ITEM_ID is the stable line identifier (or derive ROW_NUMBER() OVER (PARTITION BY SALES_ORDER_ID ORDER BY SALES_ORDER_ITEM_ID) if a 1..n line index is required). | NO |  | Line item number within the transaction |
| FactSales | TransactionType | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Transaction type (Sale, Return, Exchange) |
| FactSales | Quantity | INT | Attribute | Snowflake | SALES_ORDER_ITEMS | QUANTITY | CAST source number(38,0) QUANTITY to INT; negative quantities for returns must come from source data if the ERP stores them that way. | NO |  | Quantity sold (negative for returns) |
| FactSales | UnitPrice | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS | UNIT_PRICE | Cast number(15,2) to DECIMAL(19,4) as needed. | NO |  | Unit selling price at time of transaction |
| FactSales | UnitCost | DECIMAL(19,4) | Attribute | Snowflake | PRODUCTS | PRODUCT_ID, COST_PRICE | Join SALES_ORDER_ITEMS to PRODUCTS on PRODUCT_ID; take PRODUCTS.COST_PRICE as unit cost (number(10,2)); cast to DECIMAL(19,4). Catalogue describes current product cost—not a guaranteed historical snapshot at sale time. | NO |  | Unit cost at time of transaction |
| FactSales | GrossAmount | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS | QUANTITY, UNIT_PRICE | Compute QUANTITY * UNIT_PRICE; cast to DECIMAL(19,4). | NO |  | Gross amount before discounts (Quantity x UnitPrice) |
| FactSales | DiscountAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Total discount amount applied |
| FactSales | TaxAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Tax amount charged |
| FactSales | NetAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Net amount after discounts and tax |
| FactSales | CostAmount | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS, PRODUCTS | QUANTITY, COST_PRICE | Join on PRODUCT_ID; compute QUANTITY * COST_PRICE; cast to DECIMAL(19,4). | NO |  | Total cost of goods sold |
| FactSales | ProfitAmount | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS, PRODUCTS | QUANTITY, UNIT_PRICE, COST_PRICE | Join PRODUCTS on PRODUCT_ID; compute (QUANTITY * UNIT_PRICE) - (QUANTITY * COST_PRICE); cast to DECIMAL(19,4). Line-level proxy for margin when discount/tax columns are absent in bronze. | NO |  | Profit amount (NetAmount - CostAmount) |
| FactSales | IsPromotion | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if item was sold under a promotion |
| FactSales | PromotionCode | VARCHAR(20) | Attribute | Snowflake |  |  |  | YES |  | Promotion code if applicable |
| FactSales | PaymentMethod | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Payment method used (Cash, Credit, Debit, GiftCard) |
| FactSales | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| FactSales | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | High-water mark on bronze `SALES_ORDER_ITEMS.UPDATED_AT` and `SALES_ORDERS.UPDATED_AT` (timestamp_ntz); include lines whose header changed via join on `SALES_ORDER_ID` |  | Join `SALES_ORDER_ITEMS` to `SALES_ORDERS`; optional join to `PRODUCTS` for cost |  | Source: `snowflake_fivetran.DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.SALES_ORDER_ITEMS`, `SALES_ORDERS`, `PRODUCTS` |

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
