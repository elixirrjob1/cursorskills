## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Consulting Practice Financials |
| **System / Module** | Consulting Practice Financials |
| **STM Version** | 1.0 |
| **Author** | Data Architect |
| **Date Created** | 2026-03-25 |
| **Last Updated** |  |
| **Approved By** |  |

---

## 2. Business Context
**Purpose / Use Case:**  
> Billing classification dimension combining billable status, billing category, and overtime flags. Reduces fact table width by consolidating low-cardinality billing attributes.

**Stakeholders:**  
- **Business Owner(s):**  
- **Technical Owner(s):**  
- **Data Consumer(s):**  

**Dependencies / Related Documentation:**  
- Requirements Document:  
- ERD / Data Model:  consulting-practice-financials-data-model-2026-03-25.md  
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
| DRIP_DATA_INTELLIGENCE | GOLD | DimBillingType |  | BillingTypeHashPK |  | Dimension (Junk Dimension) | Billing classification dimension combining billable status, billing category, and overtime flags. Reduces fact table width by consolidating low-cardinality billing attributes. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification | Classification Definition | Tag Definition |
|-------|--------|---------|----------------|---------------------------|----------------|
|  |  |  |  |  |  |

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
| DimBillingType | BillingTypeHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for billing type |
| DimBillingType | BillingTypeCode | VARCHAR(10) | Attribute |  |  |  |  | NO |  | Natural key code for billing type |
| DimBillingType | IsBillable | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating time is billable to client |
| DimBillingType | BillingCategory | VARCHAR(30) | Attribute |  |  |  |  | NO |  | Category of work performed |
| DimBillingType | IsOvertime | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating overtime hours |
| DimBillingType | BillingMultiplier | DECIMAL(5,2) | Attribute |  |  |  |  | NO |  | Rate multiplier (1.0 = standard, 1.5 = overtime) |
| DimBillingType | BillingDescription | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Description for billing display |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | BillingCategory valid values: Consulting, Development, Project Management, Training, Travel, Administrative, Business Development, Internal |  |  |
| BR2 | Business Rule | BillingMultiplier: 1.0 for standard, 1.5 for overtime, 0.0 for non-billable |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
|  |  |  |  |  |  |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
|  |  |  |  |  |  |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-09 | Data Architect | Initial generation from target data model and analyzer schema JSON |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
