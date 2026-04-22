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
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
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
| DimEmployee | EmployeeHashPK | INT | Primary Key | Snowflake | EMPLOYEES | EMPLOYEE_ID | CAST(SHA2_BINARY(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) | NO |  | Surrogate primary key for employee dimension |
| DimEmployee | EmployeeHashBK | VARCHAR(20) | Business Key | Snowflake | EMPLOYEES | EMPLOYEE_ID | CAST(SHA2_BINARY(COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) | NO |  | Natural business key (employee ID from HR system) |
| DimEmployee | FirstName | VARCHAR(50) | Attribute | Snowflake | EMPLOYEES | FIRST_NAME |  | NO |  | Employee first name |
| DimEmployee | LastName | VARCHAR(50) | Attribute | Snowflake | EMPLOYEES | LAST_NAME |  | NO |  | Employee last name |
| DimEmployee | FullName | VARCHAR(100) | Attribute | Snowflake | EMPLOYEES | FIRST_NAME, LAST_NAME | TRIM(FIRST_NAME) \|\| ' ' \|\| TRIM(LAST_NAME) | NO |  | Concatenated full name for display |
| DimEmployee | EmailAddress | VARCHAR(100) | Attribute | Snowflake | EMPLOYEES | EMAIL |  | YES |  | Employee email address |
| DimEmployee | JobTitle | VARCHAR(50) | Attribute | Snowflake | EMPLOYEES | ROLE |  | NO |  | Current job title |
| DimEmployee | Department | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Department name |
| DimEmployee | HireDate | DATE | Attribute | Snowflake | EMPLOYEES | HIRE_DATE |  | NO |  | Date employee was hired |
| DimEmployee | TerminationDate | DATE | Attribute | Snowflake |  |  |  | YES |  | Date employee was terminated (NULL if active) |
| DimEmployee | ManagerEmployeeHashFK | INT | Foreign Key | Snowflake |  |  | IFF({SOURCE_COL} IS NULL, NULL, CAST(SHA2_BINARY(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))) | YES |  | Foreign key to manager employee record |
| DimEmployee | HomeStoreHashFK | INT | Foreign Key | Snowflake | EMPLOYEES | STORE_ID | IFF(STORE_ID IS NULL, NULL, CAST(SHA2_BINARY(COALESCE(CAST(STORE_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32))) | YES |  | Foreign key to employee primary store location |
| DimEmployee | IsActive | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if employee is currently employed |
| DimEmployee | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimEmployee | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | UPDATED_AT-based CDC |  |  |  |  |

---

## 9. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-16 | fillip | Initial generation from target data model and analyzer schema JSON |  |

---

## 10. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
