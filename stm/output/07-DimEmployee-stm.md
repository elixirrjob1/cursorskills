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
> Employee master for tracking sales associates and other staff involved in transactions.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | EMPLOYEES |  |  | Bronze replica via Fivetran. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimEmployee | Type 1 | EmployeeHashPK |  | Dimension | Employee master for tracking sales associates and other staff involved in transactions. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | Criticality.Operational | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.Sensitive | PII |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | Privacy.IdentifiedLoyaltyMember | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | EmployeeHashBK | Criticality.TransactionalCore | Criticality |
| Column | EmployeeHashBK | PII.None | PII |
| Column | EmployeeHashBK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | HomeStoreHashFK | Criticality.Operational | Criticality |
| Column | HomeStoreHashFK | PII.None | PII |
| Column | JobTitle | Criticality.Operational | Criticality |
| Column | JobTitle | PII.None | PII |
| Column | LastName | Criticality.Operational | Criticality |
| Column | LastName | PII.NonSensitive | PII |
| Column | LastName | PersonalData.Personal | PersonalData |
| Column | HireDate | Criticality.Operational | Criticality |
| Column | HireDate | PII.None | PII |
| Column | HireDate | QualityTrust.SystemOfRecord | QualityTrust |
| Column | FirstName | PII.Sensitive | PII |
| Column | FirstName | PersonalData.Personal | PersonalData |
| Column | EmailAddress | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | EmailAddress | PII.Sensitive | PII |
| Column | EmailAddress | PersonalData.Personal | PersonalData |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.OperationalActivity | Operational activity | Day-to-day execution work such as receiving, stocking, fulfilment, and store operations that maintain service levels. |
| Column | HomeStoreHashFK | RetailDomainGlossary.StoreLocation | Store location | A distinct site used to scope inventory, sales, and operational activity within the retail network. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimEmployee | EmployeeHashPK | INT | Primary Key | Snowflake | EMPLOYEES | EMPLOYEE_ID | Generate surrogate/hash primary key deterministically from natural key EMPLOYEE_ID. | NO |  | Surrogate primary key for employee dimension |
| DimEmployee | EmployeeHashBK | VARCHAR(20) | Business Key | Snowflake | EMPLOYEES | EMPLOYEE_ID | Cast EMPLOYEE_ID (Snowflake number(38,0)) to VARCHAR(20). | NO |  | Natural business key (employee ID from HR system) |
| DimEmployee | FirstName | VARCHAR(50) | Attribute | Snowflake | EMPLOYEES | FIRST_NAME | Trim; truncate or cast from source text(256) to VARCHAR(50). | NO |  | Employee first name |
| DimEmployee | LastName | VARCHAR(50) | Attribute | Snowflake | EMPLOYEES | LAST_NAME | Trim; truncate or cast from source text(256) to VARCHAR(50). | NO |  | Employee last name |
| DimEmployee | FullName | VARCHAR(100) | Attribute | Snowflake | EMPLOYEES | FIRST_NAME, LAST_NAME | Concatenate FIRST_NAME + ' ' + LAST_NAME (apply trim on each part); ensure final length fits VARCHAR(100). | NO |  | Concatenated full name for display |
| DimEmployee | EmailAddress | VARCHAR(100) | Attribute | Snowflake | EMPLOYEES | EMAIL | Truncate or cast from source text(900) to VARCHAR(100). | YES |  | Employee email address |
| DimEmployee | JobTitle | VARCHAR(50) | Attribute | Snowflake | EMPLOYEES | ROLE | Truncate or cast from source text(256) to VARCHAR(50); ROLE holds job position/function per catalogue. | NO |  | Current job title |
| DimEmployee | Department | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Department name |
| DimEmployee | HireDate | DATE | Attribute | Snowflake | EMPLOYEES | HIRE_DATE | Direct map from source DATE. | NO |  | Date employee was hired |
| DimEmployee | TerminationDate | DATE | Attribute | Snowflake |  |  |  | YES |  | Date employee was terminated (NULL if active) |
| DimEmployee | ManagerEmployeeHashFK | INT | Foreign Key | Snowflake |  |  | Lookup and populate the referenced target dimension key. | YES |  | Foreign key to manager employee record |
| DimEmployee | HomeStoreHashFK | INT | Foreign Key | Snowflake | EMPLOYEES | STORE_ID | Lookup DimStore surrogate key using STORE_ID as the store natural key (references stores per catalogue). | YES |  | Foreign key to employee primary store location |
| DimEmployee | IsActive | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if employee is currently employed |
| DimEmployee | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimEmployee | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | High-water mark / change capture on source `EMPLOYEES.UPDATED_AT` |  | DimStore available for `STORE_ID` → `HomeStoreHashFK` resolution |  |  |

---

## 9. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-14 | fillip | Initial generation from target data model and analyzer schema JSON |  |

---

## 10. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
