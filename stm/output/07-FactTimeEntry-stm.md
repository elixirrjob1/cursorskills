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
> Core time tracking fact table recording consultant hours worked. Grain is one row per consultant per day per project, allowing detailed utilization analysis and billing calculations.

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
|  |  | FactTimeEntry |  | One row per consultant per day per project / TimeEntryHashPK |  | Fact (Transaction) | Core time tracking fact table recording consultant hours worked. Grain is one row per consultant per day per project, allowing detailed utilization analysis and billing calculations. |

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
| FactTimeEntry | TimeEntryHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for time entry fact |
| FactTimeEntry | DateHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to date dimension (entry date) |
| FactTimeEntry | ConsultantHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to consultant dimension |
| FactTimeEntry | ProjectHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to project dimension |
| FactTimeEntry | BillingTypeHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to billing type dimension |
| FactTimeEntry | TimeEntryId | VARCHAR(30) | Attribute |  |  |  | Carry forward the source transaction identifier without a separate dimension lookup. | NO |  | Degenerate dimension - source system entry ID |
| FactTimeEntry | HoursWorked | DECIMAL(5,2) | Measure |  |  |  | BillableHours + NonBillableHours | NO |  | Total hours worked for this entry |
| FactTimeEntry | BillableHours | DECIMAL(5,2) | Measure |  |  |  |  | NO |  | Hours that are billable to client |
| FactTimeEntry | NonBillableHours | DECIMAL(5,2) | Measure |  |  |  |  | NO |  | Hours that are not billable |
| FactTimeEntry | BillRate | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Billing rate applied for this entry |
| FactTimeEntry | CostRate | DECIMAL(10,2) | Measure |  |  |  |  | NO |  | Cost rate for this consultant |
| FactTimeEntry | BilledAmount | DECIMAL(12,2) | Measure |  |  |  | BillableHours * BillRate | NO |  | Calculated billable amount (BillableHours * BillRate) |
| FactTimeEntry | CostAmount | DECIMAL(12,2) | Measure |  |  |  | HoursWorked * CostRate | NO |  | Calculated cost amount (HoursWorked * CostRate) |
| FactTimeEntry | MarginAmount | DECIMAL(12,2) | Measure |  |  |  | BilledAmount - CostAmount | NO |  | Calculated margin (BilledAmount - CostAmount) |
| FactTimeEntry | LoadTimestamp | TIMESTAMP | Audit/Metadata |  |  |  |  | NO |  | ETL load timestamp |
| FactTimeEntry | EtlBatchId | INT | Audit/Metadata |  |  |  |  | NO |  | ETL batch identifier |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | HoursWorked = BillableHours + NonBillableHours |  |  |
| BR2 | Business Rule | BilledAmount = BillableHours * BillRate |  |  |
| BR3 | Business Rule | CostAmount = HoursWorked * CostRate |  |  |
| BR4 | Business Rule | MarginAmount = BilledAmount - CostAmount |  |  |
| TX5 | DateHashFK Transformation | Foreign key to date dimension (entry date) | Lookup and populate the referenced target dimension key. |  |
| TX6 | ConsultantHashFK Transformation | Foreign key to consultant dimension | Lookup and populate the referenced target dimension key. |  |
| TX7 | ProjectHashFK Transformation | Foreign key to project dimension | Lookup and populate the referenced target dimension key. |  |
| TX8 | BillingTypeHashFK Transformation | Foreign key to billing type dimension | Lookup and populate the referenced target dimension key. |  |
| TX9 | TimeEntryId Transformation | Degenerate dimension - source system entry ID | Carry forward the source transaction identifier without a separate dimension lookup. |  |
| TX10 | HoursWorked Transformation | Total hours worked for this entry | BillableHours + NonBillableHours |  |
| TX11 | BilledAmount Transformation | Calculated billable amount (BillableHours * BillRate) | BillableHours * BillRate |  |
| TX12 | CostAmount Transformation | Calculated cost amount (HoursWorked * CostRate) | HoursWorked * CostRate |  |
| TX13 | MarginAmount Transformation | Calculated margin (BilledAmount - CostAmount) | BilledAmount - CostAmount |  |

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
