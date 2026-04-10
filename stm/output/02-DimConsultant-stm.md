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
> Employee/consultant master dimension containing professional details, organizational hierarchy, and employment attributes. Supports historical tracking via SCD Type 2 for changes in title, department, or rate.

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
|  |  | DimConsultant | Type 2 on Title, Level, Department, StandardBillRate, StandardCostRate | ConsultantHashPK |  | Dimension (Conformed, SCD Type 2) | Employee/consultant master dimension containing professional details, organizational hierarchy, and employment attributes. Supports historical tracking via SCD Type 2 for changes in title, department, or rate. |

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
| DimConsultant | ConsultantHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for consultant dimension |
| DimConsultant | ConsultantHashBK | VARCHAR(20) | Business Key |  |  |  |  | NO |  | Business key (employee ID from HR system) |
| DimConsultant | FirstName | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Consultant first name |
| DimConsultant | LastName | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Consultant last name |
| DimConsultant | FullName | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Computed full name (FirstName + LastName) |
| DimConsultant | Email | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Corporate email address |
| DimConsultant | Title | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Current job title |
| DimConsultant | Level | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Consultant level (Analyst, Consultant, Senior, Manager, Director, Partner) |
| DimConsultant | Department | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Department or practice area |
| DimConsultant | PracticeArea | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Specialty practice area |
| DimConsultant | OfficeLocation | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Primary office location |
| DimConsultant | HireDate | DATE | Attribute |  |  |  |  | NO |  | Original hire date |
| DimConsultant | TerminationDate | DATE | Attribute |  |  |  |  | YES |  | Termination date if applicable |
| DimConsultant | StandardBillRate | DECIMAL(10,2) | Attribute |  |  |  |  | NO |  | Standard hourly billing rate |
| DimConsultant | StandardCostRate | DECIMAL(10,2) | Attribute |  |  |  |  | NO |  | Standard hourly cost rate |
| DimConsultant | TargetUtilization | DECIMAL(5,4) | Attribute |  |  |  |  | NO |  | Target utilization percentage (0.75 = 75%) |
| DimConsultant | IsActive | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating active employment status |
| DimConsultant | EffectiveDate | DATE | Attribute |  |  |  | Populate when a new SCD Type 2 version becomes effective. | NO |  | SCD Type 2 row effective start date |
| DimConsultant | ExpirationDate | DATE | Attribute |  |  |  | Populate with the end date of the current SCD Type 2 version. | NO |  | SCD Type 2 row expiration date |
| DimConsultant | IsCurrent | BOOLEAN | Attribute |  |  |  | Set to indicate whether the row is the current SCD Type 2 version. | NO |  | SCD Type 2 current row flag |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Level valid values: Analyst, Consultant, Senior Consultant, Manager, Senior Manager, Director, Partner |  |  |
| BR2 | Business Rule | TargetUtilization typically ranges from 0.60 to 0.90 |  |  |
| TX3 | EffectiveDate Transformation | SCD Type 2 row effective start date | Populate when a new SCD Type 2 version becomes effective. |  |
| TX4 | ExpirationDate Transformation | SCD Type 2 row expiration date | Populate with the end date of the current SCD Type 2 version. |  |
| TX5 | IsCurrent Transformation | SCD Type 2 current row flag | Set to indicate whether the row is the current SCD Type 2 version. |  |

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
