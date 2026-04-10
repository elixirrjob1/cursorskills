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
> Project expense tracking fact table capturing all costs incurred for projects. Supports full expense lifecycle from submission through reimbursement and client billing.

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
|  |  |  |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
|  |  | FactProjectExpense |  | One row per expense transaction / ProjectExpenseHashPK |  | Fact (Transaction) | Project expense tracking fact table capturing all costs incurred for projects. Supports full expense lifecycle from submission through reimbursement and client billing. |

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
| FactProjectExpense | ProjectExpenseHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for expense fact |
| FactProjectExpense | ExpenseDateHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to date dimension (expense incurred date) |
| FactProjectExpense | SubmissionDateHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to date dimension (expense submitted date) |
| FactProjectExpense | ConsultantHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to consultant dimension (who incurred) |
| FactProjectExpense | ProjectHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to project dimension |
| FactProjectExpense | ExpenseCategoryHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to expense category dimension |
| FactProjectExpense | ExpenseId | VARCHAR(30) | Attribute |  |  |  | Carry forward the source transaction identifier without a separate dimension lookup. | NO |  | Degenerate dimension - source system expense ID |
| FactProjectExpense | ExpenseDescription | VARCHAR(500) | Attribute |  |  |  |  | YES |  | Description of the expense |
| FactProjectExpense | VendorName | VARCHAR(100) | Attribute |  |  |  |  | YES |  | Vendor or merchant name |
| FactProjectExpense | ExpenseAmount | DECIMAL(12,2) | Measure |  |  |  |  | NO |  | Total expense amount in original currency |
| FactProjectExpense | CurrencyCode | VARCHAR(3) | Attribute |  |  |  |  | NO |  | ISO currency code of expense |
| FactProjectExpense | ExchangeRate | DECIMAL(10,6) | Measure |  |  |  |  | NO |  | Exchange rate to base currency |
| FactProjectExpense | ExpenseAmountBase | DECIMAL(12,2) | Measure |  |  |  | ExpenseAmount * ExchangeRate | NO |  | Expense amount in base currency |
| FactProjectExpense | TaxAmount | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Tax portion of expense |
| FactProjectExpense | IsBillable | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating expense is billable to client |
| FactProjectExpense | BillableAmount | DECIMAL(12,2) | Measure |  |  |  | ExpenseAmountBase * (1 + MarkupPercent) when IsBillable = true | NO |  | Amount to be billed to client |
| FactProjectExpense | MarkupPercent | DECIMAL(5,4) | Measure |  |  |  |  | NO |  | Markup percentage applied (0.10 = 10%) |
| FactProjectExpense | ReimbursementStatus | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Status of reimbursement to consultant |
| FactProjectExpense | BillingStatus | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Status of billing to client |
| FactProjectExpense | HasReceipt | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating receipt is attached |
| FactProjectExpense | ApprovalStatus | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Expense approval status |
| FactProjectExpense | ApprovedBy | VARCHAR(100) | Attribute |  |  |  |  | YES |  | Name of approver |
| FactProjectExpense | ApprovalDate | DATE | Attribute |  |  |  |  | YES |  | Date expense was approved |
| FactProjectExpense | LoadTimestamp | TIMESTAMP | Audit/Metadata |  |  |  |  | NO |  | ETL load timestamp |
| FactProjectExpense | EtlBatchId | INT | Audit/Metadata |  |  |  |  | NO |  | ETL batch identifier |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | ExpenseAmountBase = ExpenseAmount * ExchangeRate |  |  |
| BR2 | Business Rule | BillableAmount = ExpenseAmountBase * (1 + MarkupPercent) when IsBillable = true |  |  |
| BR3 | Business Rule | ReimbursementStatus valid values: Pending, Approved, Paid, Rejected |  |  |
| BR4 | Business Rule | BillingStatus valid values: Not Billable, Pending, Invoiced, Paid |  |  |
| BR5 | Business Rule | ApprovalStatus valid values: Draft, Submitted, Approved, Rejected |  |  |
| TX6 | ExpenseDateHashFK Transformation | Foreign key to date dimension (expense incurred date) | Lookup and populate the referenced target dimension key. |  |
| TX7 | SubmissionDateHashFK Transformation | Foreign key to date dimension (expense submitted date) | Lookup and populate the referenced target dimension key. |  |
| TX8 | ConsultantHashFK Transformation | Foreign key to consultant dimension (who incurred) | Lookup and populate the referenced target dimension key. |  |
| TX9 | ProjectHashFK Transformation | Foreign key to project dimension | Lookup and populate the referenced target dimension key. |  |
| TX10 | ExpenseCategoryHashFK Transformation | Foreign key to expense category dimension | Lookup and populate the referenced target dimension key. |  |
| TX11 | ExpenseId Transformation | Degenerate dimension - source system expense ID | Carry forward the source transaction identifier without a separate dimension lookup. |  |
| TX12 | ExpenseAmountBase Transformation | Expense amount in base currency | ExpenseAmount * ExchangeRate |  |
| TX13 | BillableAmount Transformation | Amount to be billed to client | ExpenseAmountBase * (1 + MarkupPercent) when IsBillable = true |  |

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
