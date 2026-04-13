## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Retail Operations Target Model |
| **System / Module** | Retail Operations Target Model |
| **STM Version** | 1.0 |
| **Author** | Cursor |
| **Date Created** | 2026-04-09 |
| **Last Updated** |  |
| **Approved By** |  |

---

## 2. Business Context
**Purpose / Use Case:**  
> Target sales order line table preserving sold quantities, unit prices, and explicit quantity-unit fields for downstream commercial reporting.

**Stakeholders:**  
- **Business Owner(s):**  
- **Technical Owner(s):**  
- **Data Consumer(s):**  

**Dependencies / Related Documentation:**  
- Requirements Document:  
- ERD / Data Model:  retail-operations-source-model-2026-04-09.md  
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
| DRIP_DATA_INTELLIGENCE | GOLD | sales_order_items |  | One row per sales_order_item_id / sales_order_item_id |  | Target Table (Source-Aligned) | Target sales order line table preserving sold quantities, unit prices, and explicit quantity-unit fields for downstream commercial reporting. |

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
| Column | sales_order_item_id | Criticality.TransactionalCore | Criticality |
| Column | sales_order_item_id | PII.None | PII |
| Column | sales_order_item_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | sales_order_id | Criticality.TransactionalCore | Criticality |
| Column | sales_order_id | PII.None | PII |
| Column | product_id | Criticality.TransactionalCore | Criticality |
| Column | product_id | PII.None | PII |
| Column | quantity | Architecture.Raw | Architecture |
| Column | quantity | Criticality.TransactionalCore | Criticality |
| Column | quantity | PII.None | PII |
| Column | unit_price | Criticality.TransactionalCore | Criticality |
| Column | unit_price | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | unit_price | PII.None | PII |
| Column | unit_price | Privacy.AnonymousAggregate | Privacy |
| Column | unit_price | QualityTrust.SystemOfRecord | QualityTrust |
| Column | unit_price | Retention.FinancialStatutory | Retention |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | PII.None | PII |
| Column | created_at | Architecture.Raw | Architecture |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | sold_qty_value | Architecture.Raw | Architecture |
| Column | sold_qty_value | Criticality.TransactionalCore | Criticality |
| Column | sold_qty_value | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.SalesOrder | SalesOrder | A customer-facing order recording one or more items requested for purchase, along with delivery and payment expectations. **Type:** business_entity \| **Usage:** Fulfilment, delivery scheduling, and revenue recognition.

Inferred as the customer-side specialisation of Order.

Review status: draft. |
| Table |  | RetailDomainGlossary.OrderLine | OrderLine | A single product entry on an order specifying the item, quantity, price, and any applicable discount. **Type:** business_entity \| **Usage:** Picking, invoicing, and line-level margin analysis.

Inferred; orders are composed of lines.

Review status: draft. |
| Table |  | RetailDomainGlossary.Quantity | Quantity |  |
| Table |  | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |
| Table |  | RetailDomainGlossary.SellingPrice | SellingPrice | The amount charged to the customer for a product at the point of sale, before or after promotional adjustments. **Type:** business_attribute \| **Usage:** POS configuration, margin analysis, and competitive pricing.

Inferred; counterpart to cost price.

Review status: draft. |
| Column | sales_order_id | RetailDomainGlossary.SalesOrder | SalesOrder | A customer-facing order recording one or more items requested for purchase, along with delivery and payment expectations. **Type:** business_entity \| **Usage:** Fulfilment, delivery scheduling, and revenue recognition.

Inferred as the customer-side specialisation of Order.

Review status: draft. |
| Column | product_id | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. **Type:** business_entity \| **Usage:** Merchandising, assortment planning, pricing, and inventory management.

Review status: draft. |
| Column | quantity | RetailDomainGlossary.Quantity | Quantity |  |
| Column | unit_price | RetailDomainGlossary.SellingPrice | SellingPrice | The amount charged to the customer for a product at the point of sale, before or after promotional adjustments. **Type:** business_attribute \| **Usage:** POS configuration, margin analysis, and competitive pricing.

Inferred; counterpart to cost price.

Review status: draft. |
| Column | sold_qty_value | RetailDomainGlossary.Quantity | Quantity |  |
| Column | sold_qty_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| sales_order_items | sales_order_item_id | bigint | Attribute | Snowflake | SALES_ORDER_ITEMS | SALES_ORDER_ITEM_ID | Source type: number(38,0) | NO |  | Unique identifier for each sales order item in the sales_order_items table. |
| sales_order_items | sales_order_id | bigint | Attribute | Snowflake | SALES_ORDER_ITEMS | SALES_ORDER_ID | Source type: number(38,0) | NO |  | References the unique identifier of the associated sales order in the `sales_orders` table, establishing a relationship between sales order items and their parent sales orders. |
| sales_order_items | product_id | bigint | Attribute | Snowflake | SALES_ORDER_ITEMS | PRODUCT_ID | Source type: number(38,0) | NO |  | Identifies the product associated with a sales order item, referencing the `products.product_id` column. |
| sales_order_items | quantity | integer | Attribute | Snowflake | SALES_ORDER_ITEMS | QUANTITY | Source type: number(38,0) | NO |  | The `quantity` column in the `sales_order_items` table stores the non-null integer value representing the number of units of a product included in a specific sales order item. |
| sales_order_items | unit_price | numeric(15,2) | Attribute | Snowflake | SALES_ORDER_ITEMS | UNIT_PRICE | Source type: number(15,2) | NO |  | The `unit_price` column in the `sales_order_items` table stores the per-unit selling price of a product in the sales order, represented as a non-nullable numeric value with two decimal places. |
| sales_order_items | created_at | datetime2 | Attribute | Snowflake | SALES_ORDER_ITEMS | CREATED_AT | Source type: timestamp_ntz | NO |  | Records the timestamp when a sales_order item entry is created, ensuring accurate tracking of creation times; this field is mandatory and uses the datetime2 data type. |
| sales_order_items | updated_at | datetime2 | Attribute | Snowflake | SALES_ORDER_ITEMS | UPDATED_AT | Source type: timestamp_ntz | NO |  | The `updated_at` column in the `sales_order_items` table stores the non-nullable timestamp of the most recent update to a sales order item record. |
| sales_order_items | sold_qty_value | numeric(10,2) | Attribute | Snowflake | SALES_ORDER_ITEMS | SOLD_QTY_VALUE | Source type: number(10,2) | YES |  | The `sold_qty_value` column stores the numeric value representing the quantity of a product sold in a sales order item, allowing up to two decimal places, and can be null. |
| sales_order_items | sold_qty_unit | nvarchar(16) | Attribute | Snowflake | SALES_ORDER_ITEMS | SOLD_QTY_UNIT | Source type: text(32) | YES |  | Sold quantity unit (ea/box). |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | SALES_ORDER_ITEM_ID must not be NULL (primary key) | NOT NULL | SALES_ORDER_ITEM_ID IS NOT NULL | Reject record |  |
| DQ2 | SALES_ORDER_ITEM_ID must be unique | Uniqueness | COUNT(DISTINCT SALES_ORDER_ITEM_ID) = COUNT(*) | Reject record |  |
| DQ3 | PRODUCT_ID referential integrity check | Referential Integrity | All PRODUCT_ID values exist in referenced parent table | Flag / quarantine |  |
| DQ4 | SALES_ORDER_ID referential integrity check | Referential Integrity | All SALES_ORDER_ID values exist in referenced parent table | Flag / quarantine |  |
|  |  |  |  |  |  |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | Delta load using UPDATED_AT |  |  |  |  |
|  |  |  |  |  |  |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-13 | Cursor | Initial generation from target data model and analyzer schema JSON |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
