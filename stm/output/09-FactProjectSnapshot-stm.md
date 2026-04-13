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
> Monthly project financial snapshot capturing cumulative and period metrics for project health monitoring. Enables trend analysis, variance reporting, and forecasting.

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
| DRIP_DATA_INTELLIGENCE | GOLD | FactProjectSnapshot |  | One row per project per month / ProjectSnapshotHashPK |  | Fact (Periodic Snapshot) | Monthly project financial snapshot capturing cumulative and period metrics for project health monitoring. Enables trend analysis, variance reporting, and forecasting. |

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
| FactProjectSnapshot | ProjectSnapshotHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for snapshot fact |
| FactProjectSnapshot | SnapshotDateHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to date dimension (snapshot month-end) |
| FactProjectSnapshot | ProjectHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to project dimension |
| FactProjectSnapshot | SnapshotMonth | DATE | Attribute |  |  |  |  | NO |  | First day of snapshot month |
| FactProjectSnapshot | BudgetHours | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Total budgeted hours for project |
| FactProjectSnapshot | BudgetAmount | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Total budget amount for project |
| FactProjectSnapshot | CumulativeHoursWorked | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Total hours worked to date |
| FactProjectSnapshot | CumulativeBillableHours | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Total billable hours to date |
| FactProjectSnapshot | CumulativeNonBillableHours | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Total non-billable hours to date |
| FactProjectSnapshot | CumulativeRevenue | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Total revenue recognized to date |
| FactProjectSnapshot | CumulativeCost | DECIMAL(15,2) | Measure |  |  |  | CumulativeLaborCost + CumulativeExpenseCost | NO |  | Total cost incurred to date (labor + expenses) |
| FactProjectSnapshot | CumulativeLaborCost | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Total labor cost to date |
| FactProjectSnapshot | CumulativeExpenseCost | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Total expense cost to date |
| FactProjectSnapshot | CumulativeMargin | DECIMAL(15,2) | Measure |  |  |  | CumulativeRevenue - CumulativeCost | NO |  | Total margin to date (Revenue - Cost) |
| FactProjectSnapshot | PeriodHoursWorked | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Hours worked in snapshot period |
| FactProjectSnapshot | PeriodBillableHours | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Billable hours in snapshot period |
| FactProjectSnapshot | PeriodRevenue | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Revenue recognized in snapshot period |
| FactProjectSnapshot | PeriodCost | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Cost incurred in snapshot period |
| FactProjectSnapshot | PeriodMargin | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Margin for snapshot period |
| FactProjectSnapshot | BudgetConsumedPercent | DECIMAL(5,4) | Measure |  |  |  | CumulativeCost / BudgetAmount | NO |  | Percentage of budget consumed |
| FactProjectSnapshot | HoursConsumedPercent | DECIMAL(5,4) | Measure |  |  |  | CumulativeHoursWorked / BudgetHours | NO |  | Percentage of budgeted hours consumed |
| FactProjectSnapshot | ProjectedFinalCost | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Estimated cost at completion |
| FactProjectSnapshot | ProjectedFinalMargin | DECIMAL(15,2) | Measure |  |  |  |  | NO |  | Estimated margin at completion |
| FactProjectSnapshot | VarianceToBudget | DECIMAL(15,2) | Measure |  |  |  | BudgetAmount - ProjectedFinalCost | NO |  | Variance from budget (negative = over budget) |
| FactProjectSnapshot | EstimateToComplete | DECIMAL(15,2) | Attribute |  |  |  |  | NO |  | Estimated remaining cost to complete |
| FactProjectSnapshot | LoadTimestamp | TIMESTAMP | Audit/Metadata |  |  |  |  | NO |  | ETL load timestamp |
| FactProjectSnapshot | EtlBatchId | INT | Audit/Metadata |  |  |  |  | NO |  | ETL batch identifier |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | CumulativeMargin = CumulativeRevenue - CumulativeCost |  |  |
| BR2 | Business Rule | CumulativeCost = CumulativeLaborCost + CumulativeExpenseCost |  |  |
| BR3 | Business Rule | BudgetConsumedPercent = CumulativeCost / BudgetAmount |  |  |
| BR4 | Business Rule | HoursConsumedPercent = CumulativeHoursWorked / BudgetHours |  |  |
| BR5 | Business Rule | VarianceToBudget = BudgetAmount - ProjectedFinalCost |  |  |
| TX6 | SnapshotDateHashFK Transformation | Foreign key to date dimension (snapshot month-end) | Lookup and populate the referenced target dimension key. |  |
| TX7 | ProjectHashFK Transformation | Foreign key to project dimension | Lookup and populate the referenced target dimension key. |  |
| TX8 | CumulativeCost Transformation | Total cost incurred to date (labor + expenses) | CumulativeLaborCost + CumulativeExpenseCost |  |
| TX9 | CumulativeMargin Transformation | Total margin to date (Revenue - Cost) | CumulativeRevenue - CumulativeCost |  |
| TX10 | BudgetConsumedPercent Transformation | Percentage of budget consumed | CumulativeCost / BudgetAmount |  |
| TX11 | HoursConsumedPercent Transformation | Percentage of budgeted hours consumed | CumulativeHoursWorked / BudgetHours |  |
| TX12 | VarianceToBudget Transformation | Variance from budget (negative = over budget) | BudgetAmount - ProjectedFinalCost |  |

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
