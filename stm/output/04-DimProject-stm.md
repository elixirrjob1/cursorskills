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
> Project master dimension containing engagement details, financials, and status information. Links to client dimension. Supports historical tracking for changes in status, budget, or project manager.

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
| DRIP_DATA_INTELLIGENCE | GOLD | DimProject | Type 2 on ProjectStatus, ProjectManager, BudgetHours, BudgetAmount | ProjectHashPK |  | Dimension (SCD Type 2) | Project master dimension containing engagement details, financials, and status information. Links to client dimension. Supports historical tracking for changes in status, budget, or project manager. |

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
| DimProject | ProjectHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for project dimension |
| DimProject | ProjectHashBK | VARCHAR(20) | Business Key |  |  |  |  | NO |  | Business key (project code from PM system) |
| DimProject | ClientHashFK | INT | Foreign Key |  |  |  | Lookup and populate the referenced target dimension key. | NO |  | Foreign key to client dimension |
| DimProject | ProjectName | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Project display name |
| DimProject | ProjectCode | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Internal project code |
| DimProject | ProjectType | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Type of engagement |
| DimProject | ProjectStatus | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Current project status |
| DimProject | ProjectManager | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Assigned project manager name |
| DimProject | EngagementPartner | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Partner responsible for engagement |
| DimProject | ServiceLine | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Service line classification |
| DimProject | StartDate | DATE | Attribute |  |  |  |  | NO |  | Project start date |
| DimProject | PlannedEndDate | DATE | Attribute |  |  |  |  | YES |  | Originally planned end date |
| DimProject | ActualEndDate | DATE | Attribute |  |  |  |  | YES |  | Actual completion date |
| DimProject | ContractType | VARCHAR(30) | Attribute |  |  |  |  | NO |  | Contract billing type |
| DimProject | ContractValue | DECIMAL(15,2) | Attribute |  |  |  |  | YES |  | Total contract value (for fixed-price) |
| DimProject | BudgetHours | DECIMAL(10,2) | Attribute |  |  |  |  | YES |  | Budgeted hours for project |
| DimProject | BudgetAmount | DECIMAL(15,2) | Attribute |  |  |  |  | YES |  | Total budget amount |
| DimProject | IsActive | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating active project status |
| DimProject | EffectiveDate | DATE | Attribute |  |  |  | Populate when a new SCD Type 2 version becomes effective. | NO |  | SCD Type 2 row effective start date |
| DimProject | ExpirationDate | DATE | Attribute |  |  |  | Populate with the end date of the current SCD Type 2 version. | NO |  | SCD Type 2 row expiration date |
| DimProject | IsCurrent | BOOLEAN | Attribute |  |  |  | Set to indicate whether the row is the current SCD Type 2 version. | NO |  | SCD Type 2 current row flag |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | ProjectType valid values: Strategy, Implementation, Assessment, Training, Support, Advisory |  |  |
| BR2 | Business Rule | ProjectStatus valid values: Proposed, Active, On Hold, Completed, Cancelled |  |  |
| BR3 | Business Rule | ContractType valid values: Time and Materials, Fixed Price, Retainer, Milestone-Based |  |  |
| TX4 | ClientHashFK Transformation | Foreign key to client dimension | Lookup and populate the referenced target dimension key. |  |
| TX5 | EffectiveDate Transformation | SCD Type 2 row effective start date | Populate when a new SCD Type 2 version becomes effective. |  |
| TX6 | ExpirationDate Transformation | SCD Type 2 row expiration date | Populate with the end date of the current SCD Type 2 version. |  |
| TX7 | IsCurrent Transformation | SCD Type 2 current row flag | Set to indicate whether the row is the current SCD Type 2 version. |  |

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
