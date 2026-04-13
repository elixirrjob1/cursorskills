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
> Expense classification hierarchy for categorizing project expenses. Includes category and subcategory levels for roll-up analysis.

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
| DRIP_DATA_INTELLIGENCE | GOLD | DimExpenseCategory |  | ExpenseCategoryHashPK |  | Dimension (SCD Type 1) | Expense classification hierarchy for categorizing project expenses. Includes category and subcategory levels for roll-up analysis. |

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
| DimExpenseCategory | ExpenseCategoryHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for expense category |
| DimExpenseCategory | ExpenseCategoryHashBK | VARCHAR(20) | Business Key |  |  |  |  | NO |  | Business key (expense code) |
| DimExpenseCategory | CategoryName | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Top-level expense category name |
| DimExpenseCategory | SubcategoryName | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Subcategory name |
| DimExpenseCategory | CategoryCode | VARCHAR(10) | Attribute |  |  |  |  | NO |  | Category code for GL mapping |
| DimExpenseCategory | ExpenseDescription | VARCHAR(200) | Attribute |  |  |  |  | YES |  | Detailed description of expense type |
| DimExpenseCategory | IsBillable | BOOLEAN | Attribute |  |  |  |  | NO |  | Default billable flag for category |
| DimExpenseCategory | RequiresReceipt | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating receipt requirement |
| DimExpenseCategory | RequiresApproval | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating approval requirement |
| DimExpenseCategory | ApprovalThreshold | DECIMAL(10,2) | Attribute |  |  |  |  | YES |  | Amount threshold requiring additional approval |
| DimExpenseCategory | GLAccountCode | VARCHAR(20) | Attribute |  |  |  |  | NO |  | General ledger account mapping |
| DimExpenseCategory | IsActive | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating active category |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | CategoryName valid values: Travel, Meals, Lodging, Transportation, Materials, Software, Professional Services, Other |  |  |
| BR2 | Business Rule | IsBillable defaults based on category but can be overridden at expense level |  |  |

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
