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
> Target purchase order line table preserving ordered quantities, unit costs, and explicit quantity-unit fields for procurement analytics and traceability.

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
| DRIP_DATA_INTELLIGENCE | GOLD | purchase_order_items |  | One row per po_item_id / po_item_id |  | Target Table (Source-Aligned) | Target purchase order line table preserving ordered quantities, unit costs, and explicit quantity-unit fields for procurement analytics and traceability. |

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
| Column | po_item_id | Architecture.Raw | Architecture |
| Column | po_item_id | Criticality.TransactionalCore | Criticality |
| Column | po_item_id | PII.None | PII |
| Column | po_item_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | po_id | Criticality.TransactionalCore | Criticality |
| Column | po_id | PII.None | PII |
| Column | product_id | Architecture.Raw | Architecture |
| Column | product_id | Criticality.TransactionalCore | Criticality |
| Column | product_id | PII.None | PII |
| Column | quantity | Architecture.Raw | Architecture |
| Column | quantity | Criticality.TransactionalCore | Criticality |
| Column | quantity | PII.None | PII |
| Column | unit_cost | Criticality.TransactionalCore | Criticality |
| Column | unit_cost | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | unit_cost | PII.None | PII |
| Column | unit_cost | Privacy.AnonymousAggregate | Privacy |
| Column | unit_cost | QualityTrust.SystemOfRecord | QualityTrust |
| Column | unit_cost | Retention.FinancialStatutory | Retention |
| Column | created_at | Criticality.Operational | Criticality |
| Column | created_at | PII.None | PII |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | ordered_qty_value | Architecture.Raw | Architecture |
| Column | ordered_qty_value | Criticality.TransactionalCore | Criticality |
| Column | ordered_qty_value | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.OrderLine | OrderLine | A single product entry on an order specifying the item, quantity, price, and any applicable discount. **Type:** business_entity \| **Usage:** Picking, invoicing, and line-level margin analysis.

Inferred; orders are composed of lines.

Review status: draft. |
| Table |  | RetailDomainGlossary.Quantity | Quantity |  |
| Table |  | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |
| Table |  | RetailDomainGlossary.PurchaseOrder | PurchaseOrder | A commitment document from the retailer to a supplier specifying products, quantities, prices, and delivery expectations. **Type:** identifier \| **Usage:** Order tracking, receiving, and three-way match with invoices.

Inferred; standard procurement artifact.

Review status: draft. |
| Column | po_id | RetailDomainGlossary.PurchaseOrder | PurchaseOrder | A commitment document from the retailer to a supplier specifying products, quantities, prices, and delivery expectations. **Type:** identifier \| **Usage:** Order tracking, receiving, and three-way match with invoices.

Inferred; standard procurement artifact.

Review status: draft. |
| Column | product_id | RetailDomainGlossary.Product | Product | A sellable item or SKU identified for catalog, pricing, and inventory purposes. **Type:** business_entity \| **Usage:** Merchandising, assortment planning, pricing, and inventory management.

Review status: draft. |
| Column | quantity | RetailDomainGlossary.Quantity | Quantity |  |
| Column | unit_cost | RetailDomainGlossary.CostPrice | CostPrice | The amount the retailer pays the supplier per unit, before any rebates, allowances, or landed-cost adjustments. **Type:** business_attribute \| **Usage:** Margin calculation, price setting, and supplier negotiation.

Inferred; necessary for financial monitoring.

Review status: draft. |
| Column | ordered_qty_value | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |
| Column | ordered_qty_unit | RetailDomainGlossary.UnitOfMeasure | UnitOfMeasure | The standard quantity designation for a product (e.g. each, pack, kilogram, litre) used in ordering, selling, and inventory. **Type:** business_attribute \| **Usage:** Purchase-order quantities, POS scanning, and stock counting.

Inferred; essential for inventory and purchasing accuracy.

Review status: draft. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| purchase_order_items | po_item_id | bigint | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | PO_ITEM_ID | Source type: number(38,0) | NO |  | Unique identifier for each purchase order item in the system, serving as the primary key for the `purchase_order_items` table. |
| purchase_order_items | po_id | bigint | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | PO_ID | Source type: number(38,0) | NO |  | Represents the unique identifier of the associated purchase order, linking purchase order items to their parent purchase order via a foreign key relationship to `purchase_orders.po_id`. |
| purchase_order_items | product_id | bigint | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | PRODUCT_ID | Source type: number(38,0) | NO |  | Identifier linking each purchase order item to a specific product in the products table. |
| purchase_order_items | quantity | integer | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | QUANTITY | Source type: number(38,0) | NO |  | The `quantity` column in the `purchase_order_items` table stores the non-null integer value representing the number of units of a product included in a specific purchase order item. |
| purchase_order_items | unit_cost | numeric(10,2) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | UNIT_COST | Source type: number(10,2) | NO |  | Represents the per-unit cost of a product in a purchase order, stored as a non-null numeric value with up to 10 digits and 2 decimal places. |
| purchase_order_items | created_at | datetime2 | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | CREATED_AT | Source type: timestamp_ntz | NO |  | The `created_at` column records the non-nullable timestamp indicating when each purchase order item record was created. |
| purchase_order_items | updated_at | datetime2 | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | UPDATED_AT | Source type: timestamp_ntz | NO |  | Records the timestamp of the last update made to a purchase order item, ensuring accurate tracking of modifications. |
| purchase_order_items | ordered_qty_value | numeric(10,2) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | ORDERED_QTY_VALUE | Source type: number(10,2) | YES |  | The `ordered_qty_value` column stores the numeric quantity value (up to two decimal places) of items ordered in a purchase order, which can be null. |
| purchase_order_items | ordered_qty_unit | nvarchar(16) | Attribute | Snowflake | PURCHASE_ORDER_ITEMS | ORDERED_QTY_UNIT | Source type: text(32) | YES |  | Ordered quantity unit (ea/box). |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | PO_ITEM_ID must not be NULL (primary key) | NOT NULL | PO_ITEM_ID IS NOT NULL | Reject record |  |
| DQ2 | PO_ITEM_ID must be unique | Uniqueness | COUNT(DISTINCT PO_ITEM_ID) = COUNT(*) | Reject record |  |
| DQ3 | PO_ID referential integrity check | Referential Integrity | All PO_ID values exist in referenced parent table | Flag / quarantine |  |
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
