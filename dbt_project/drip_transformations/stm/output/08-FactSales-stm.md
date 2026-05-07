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
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.Sensitive | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier1 | Tier |
| Table |  | PersonalData.Personal | PersonalData |
| Column | TransactionLineNumber | Criticality.TransactionalCore | Criticality |
| Column | TransactionLineNumber | PII.None | PII |
| Column | TransactionLineNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | Quantity | Architecture.Enriched | Architecture |
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
| Column | TransactionNumber | Criticality.TransactionalCore | Criticality |
| Column | TransactionNumber | PII.None | PII |
| Column | TransactionNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | StoreHashFK | Criticality.TransactionalCore | Criticality |
| Column | StoreHashFK | PII.None | PII |
| Column | DateHashFK | Architecture.Enriched | Architecture |
| Column | DateHashFK | Criticality.TransactionalCore | Criticality |
| Column | DateHashFK | Lifecycle.Active | Lifecycle |
| Column | DateHashFK | PII.None | PII |
| Column | DateHashFK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DateHashFK | Retention.FinancialStatutory | Retention |
| Column | DateHashFK | Tier.Tier1 | Tier |
| Column | EmployeeHashFK | Criticality.TransactionalCore | Criticality |
| Column | EmployeeHashFK | PII.None | PII |
| Column | CustomerHashFK | Criticality.TransactionalCore | Criticality |
| Column | CustomerHashFK | PII.NonSensitive | PII |
| Column | CustomerHashFK | PersonalData.Personal | PersonalData |
| Column | UnitCost | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | UnitCost | Criticality.TransactionalCore | Criticality |
| Column | UnitCost | PII.None | PII |
| Column | UnitCost | Privacy.AnonymousAggregate | Privacy |
| Column | UnitCost | QualityTrust.SystemOfRecord | QualityTrust |
| Column | UnitCost | Retention.FinancialStatutory | Retention |
| Column | TransactionType | Architecture.Enriched | Architecture |
| Column | TransactionType | Certification.Gold | Certification |
| Column | TransactionType | Criticality.TransactionalCore | Criticality |
| Column | TransactionType | Lifecycle.Active | Lifecycle |
| Column | TransactionType | PII.None | PII |
| Column | TransactionType | Privacy.AnonymousAggregate | Privacy |
| Column | TransactionType | QualityTrust.SystemOfRecord | QualityTrust |
| Column | TransactionType | Retention.FinancialStatutory | Retention |
| Column | TransactionType | Tier.Tier1 | Tier |
| Column | DiscountAmount | Architecture.Enriched | Architecture |
| Column | DiscountAmount | Certification.Gold | Certification |
| Column | DiscountAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | DiscountAmount | Criticality.TransactionalCore | Criticality |
| Column | DiscountAmount | Lifecycle.Active | Lifecycle |
| Column | DiscountAmount | PII.None | PII |
| Column | DiscountAmount | Privacy.AnonymousAggregate | Privacy |
| Column | DiscountAmount | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DiscountAmount | Retention.FinancialStatutory | Retention |
| Column | DiscountAmount | Tier.Tier1 | Tier |
| Column | TaxAmount | Architecture.Enriched | Architecture |
| Column | TaxAmount | Certification.Gold | Certification |
| Column | TaxAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | TaxAmount | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | TaxAmount | Criticality.TransactionalCore | Criticality |
| Column | TaxAmount | Lifecycle.Active | Lifecycle |
| Column | TaxAmount | PII.None | PII |
| Column | TaxAmount | Privacy.AnonymousAggregate | Privacy |
| Column | TaxAmount | QualityTrust.SystemOfRecord | QualityTrust |
| Column | TaxAmount | Retention.FinancialStatutory | Retention |
| Column | TaxAmount | Tier.Tier1 | Tier |
| Column | NetAmount | Architecture.Enriched | Architecture |
| Column | NetAmount | Certification.Gold | Certification |
| Column | NetAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | NetAmount | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | NetAmount | Criticality.TransactionalCore | Criticality |
| Column | NetAmount | Lifecycle.Active | Lifecycle |
| Column | NetAmount | PII.None | PII |
| Column | NetAmount | Privacy.AnonymousAggregate | Privacy |
| Column | NetAmount | QualityTrust.SystemOfRecord | QualityTrust |
| Column | NetAmount | Retention.FinancialStatutory | Retention |
| Column | NetAmount | Tier.Tier1 | Tier |
| Column | ProfitAmount | Architecture.Enriched | Architecture |
| Column | ProfitAmount | Certification.Gold | Certification |
| Column | ProfitAmount | ComplianceLegal.SOX | ComplianceLegal |
| Column | ProfitAmount | Criticality.TransactionalCore | Criticality |
| Column | ProfitAmount | Lifecycle.Active | Lifecycle |
| Column | ProfitAmount | PII.None | PII |
| Column | ProfitAmount | Privacy.AnonymousAggregate | Privacy |
| Column | ProfitAmount | QualityTrust.SystemOfRecord | QualityTrust |
| Column | ProfitAmount | Retention.FinancialStatutory | Retention |
| Column | ProfitAmount | Tier.Tier1 | Tier |
| Column | IsPromotion | Architecture.Enriched | Architecture |
| Column | IsPromotion | Certification.Gold | Certification |
| Column | IsPromotion | Criticality.TransactionalCore | Criticality |
| Column | IsPromotion | Lifecycle.Active | Lifecycle |
| Column | IsPromotion | PII.None | PII |
| Column | IsPromotion | Privacy.AnonymousAggregate | Privacy |
| Column | IsPromotion | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsPromotion | Retention.FinancialStatutory | Retention |
| Column | IsPromotion | Tier.Tier2 | Tier |
| Column | PromotionCode | Architecture.Enriched | Architecture |
| Column | PromotionCode | Certification.Gold | Certification |
| Column | PromotionCode | Criticality.TransactionalCore | Criticality |
| Column | PromotionCode | Lifecycle.Active | Lifecycle |
| Column | PromotionCode | PII.None | PII |
| Column | PromotionCode | Privacy.AnonymousAggregate | Privacy |
| Column | PromotionCode | QualityTrust.SystemOfRecord | QualityTrust |
| Column | PromotionCode | Retention.FinancialStatutory | Retention |
| Column | PromotionCode | Tier.Tier2 | Tier |
| Column | PaymentMethod | Architecture.Enriched | Architecture |
| Column | PaymentMethod | Certification.Gold | Certification |
| Column | PaymentMethod | ComplianceLegal.PCIDSS | ComplianceLegal |
| Column | PaymentMethod | Criticality.TransactionalCore | Criticality |
| Column | PaymentMethod | Lifecycle.Active | Lifecycle |
| Column | PaymentMethod | PII.None | PII |
| Column | PaymentMethod | Privacy.AnonymousAggregate | Privacy |
| Column | PaymentMethod | QualityTrust.SystemOfRecord | QualityTrust |
| Column | PaymentMethod | Retention.FinancialStatutory | Retention |
| Column | PaymentMethod | Tier.Tier1 | Tier |
| Column | EtlBatchId | Architecture.Enriched | Architecture |
| Column | EtlBatchId | Certification.Gold | Certification |
| Column | EtlBatchId | Criticality.Operational | Criticality |
| Column | EtlBatchId | Lifecycle.Active | Lifecycle |
| Column | EtlBatchId | PII.None | PII |
| Column | EtlBatchId | Privacy.AnonymousAggregate | Privacy |
| Column | EtlBatchId | QualityTrust.SystemOfRecord | QualityTrust |
| Column | EtlBatchId | Retention.OperationalTransient | Retention |
| Column | EtlBatchId | Tier.Tier5 | Tier |
| Column | LoadTimestamp | Architecture.Enriched | Architecture |
| Column | LoadTimestamp | Certification.Gold | Certification |
| Column | LoadTimestamp | Criticality.Operational | Criticality |
| Column | LoadTimestamp | Lifecycle.Active | Lifecycle |
| Column | LoadTimestamp | PII.None | PII |
| Column | LoadTimestamp | Privacy.AnonymousAggregate | Privacy |
| Column | LoadTimestamp | QualityTrust.SystemOfRecord | QualityTrust |
| Column | LoadTimestamp | Retention.OperationalTransient | Retention |
| Column | LoadTimestamp | Tier.Tier5 | Tier |

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
| Column | UnitCost | RetailDomainGlossary.CostPrice | Cost price | The amount the retailer pays the supplier per unit, before any rebates, allowances, or landed-cost adjustments. |
| Column | TransactionType | RetailDomainGlossary.Sale | Sale | The transfer of goods to a customer in exchange for payment, typically recorded at the line-item level. |
| Column | TransactionType | RetailDomainGlossary.Return | Return | The process of accepting goods back from a customer or returning goods to a supplier, often tied to refunds, exchanges, or credit notes. |
| Column | TransactionType | RetailDomainGlossary.Exchange | Exchange | Replacing a returned item with a different product or variant rather than issuing a refund. |
| Column | DiscountAmount | RetailDomainGlossary.Promotion | Promotion | A time-bound commercial offer—such as a discount, multi-buy, or loyalty bonus—designed to stimulate demand for specific products. |
| Column | NetAmount | RetailDomainGlossary.NetSales | Net sales | Total sales revenue after deducting returns, refunds, and allowances. |
| Column | ProfitAmount | RetailDomainGlossary.GrossMargin | Gross margin | Revenue from sales minus cost of goods sold, expressed as a value or percentage. |
| Column | IsPromotion | RetailDomainGlossary.Promotion | Promotion | A time-bound commercial offer—such as a discount, multi-buy, or loyalty bonus—designed to stimulate demand for specific products. |
| Column | PromotionCode | RetailDomainGlossary.Promotion | Promotion | A time-bound commercial offer—such as a discount, multi-buy, or loyalty bonus—designed to stimulate demand for specific products. |
| Column | PaymentMethod | RetailDomainGlossary.PaymentMethod | Payment method | The instrument used to effect payment, such as cash, credit card, debit card, mobile wallet, or gift card. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| FactSales | SalesHashPK | NUMBER(19,0) | Primary Key | Snowflake | SALES_ORDER_ITEMS | SALES_ORDER_ID, SALES_ORDER_ITEM_ID | HASH(COALESCE(CAST(SALES_ORDER_ID AS VARCHAR), '#@#@#@#@#') \|\| '\|' \|\| COALESCE(CAST(SALES_ORDER_ITEM_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Surrogate primary key for sales fact |
| FactSales | DateHashFK | NUMBER(19,0) | Foreign Key | Snowflake | SALES_ORDERS | ORDER_DATE | HASH(COALESCE(CAST(CAST(ORDER_DATE AS DATE) AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to date dimension (transaction date) |
| FactSales | ProductHashFK | NUMBER(19,0) | Foreign Key | Snowflake | SALES_ORDER_ITEMS | PRODUCT_ID | HASH(COALESCE(CAST(PRODUCT_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to product dimension |
| FactSales | StoreHashFK | NUMBER(19,0) | Foreign Key | Snowflake | SALES_ORDERS | STORE_ID | HASH(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Foreign key to store dimension |
| FactSales | CustomerHashFK | NUMBER(19,0) | Foreign Key | Snowflake | SALES_ORDERS | CUSTOMER_ID | IFF(CUSTOMER_ID IS NULL, NULL, HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'))) | YES |  | Foreign key to customer dimension (NULL for anonymous sales) |
| FactSales | EmployeeHashFK | NUMBER(19,0) | Foreign Key | Snowflake | SALES_ORDERS | EMPLOYEE_ID | IFF(EMPLOYEE_ID IS NULL, NULL, HASH(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'))) | YES |  | Foreign key to employee dimension (sales associate) |
| FactSales | TransactionNumber | VARCHAR(20) | Attribute | Snowflake | SALES_ORDERS | SALES_ORDER_ID | CAST(SALES_ORDER_ID AS VARCHAR(20)) | NO |  | Degenerate dimension - source transaction ID |
| FactSales | TransactionLineNumber | INT | Attribute | Snowflake | SALES_ORDER_ITEMS | SALES_ORDER_ITEM_ID |  | NO |  | Line item number within the transaction |
| FactSales | TransactionType | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Transaction type (Sale, Return, Exchange) |
| FactSales | Quantity | INT | Attribute | Snowflake | SALES_ORDER_ITEMS | QUANTITY |  | NO |  | Quantity sold (negative for returns) |
| FactSales | UnitPrice | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS | UNIT_PRICE |  | NO |  | Unit selling price at time of transaction |
| FactSales | UnitCost | DECIMAL(19,4) | Attribute | Snowflake | PRODUCTS | COST_PRICE |  | NO |  | Unit cost at time of transaction |
| FactSales | GrossAmount | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS | QUANTITY, UNIT_PRICE | CAST(QUANTITY * UNIT_PRICE AS DECIMAL(19,4)) | NO |  | Gross amount before discounts (Quantity x UnitPrice) |
| FactSales | DiscountAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Total discount amount applied |
| FactSales | TaxAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Tax amount charged |
| FactSales | NetAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Net amount after discounts and tax |
| FactSales | CostAmount | DECIMAL(19,4) | Attribute | Snowflake | SALES_ORDER_ITEMS, PRODUCTS | SALES_ORDER_ITEMS.QUANTITY, PRODUCTS.COST_PRICE | CAST(SALES_ORDER_ITEMS.QUANTITY * PRODUCTS.COST_PRICE AS DECIMAL(19,4)) | NO |  | Total cost of goods sold |
| FactSales | ProfitAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | NO |  | Profit amount (NetAmount - CostAmount) |
| FactSales | IsPromotion | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if item was sold under a promotion |
| FactSales | PromotionCode | VARCHAR(20) | Attribute | Snowflake |  |  |  | YES |  | Promotion code if applicable |
| FactSales | PaymentMethod | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Payment method used (Cash, Credit, Debit, GiftCard) |
| FactSales | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| FactSales | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | UPDATED_AT-based CDC |  |  |  |  |

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
