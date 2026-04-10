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
> Target employee master table preserving employee identifiers, store relationships, personal attributes, role data, and audit timestamps in a source-aligned target structure.

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
|  |  |  |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
|  |  | employees |  | One row per employee_id / employee_id |  | Target Table (Source-Aligned) | Target employee master table preserving employee identifiers, store relationships, personal attributes, role data, and audit timestamps in a source-aligned target structure. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | Criticality.Operational | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | PII.Sensitive | PII |
| Table |  | Privacy.IdentifiedLoyaltyMember | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier2 | Tier |
| Column | employee_id | Criticality.TransactionalCore | Criticality |
| Column | employee_id | PII.None | PII |
| Column | employee_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | store_id | Criticality.Operational | Criticality |
| Column | store_id | PII.None | PII |
| Column | first_name | PersonalData.Personal | PersonalData |
| Column | first_name | PII.Sensitive | PII |
| Column | last_name | PersonalData.Personal | PersonalData |
| Column | last_name | PII.NonSensitive | PII |
| Column | last_name | Criticality.Operational | Criticality |
| Column | email | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | email | PersonalData.Personal | PersonalData |
| Column | email | PII.Sensitive | PII |
| Column | role | Criticality.Operational | Criticality |
| Column | role | PII.None | PII |
| Column | hire_date | Criticality.Operational | Criticality |
| Column | hire_date | PII.None | PII |
| Column | hire_date | QualityTrust.SystemOfRecord | QualityTrust |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | PII.None | PII |
| Column | created_at | QualityTrust.SystemOfRecord | QualityTrust |
| Column | created_at | Retention.TransientOperational | Retention |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |
| Column | updated_at | QualityTrust.SystemOfRecord | QualityTrust |
| Column | updated_at | Retention.TransientOperational | Retention |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Customer | Customer |  |
| Table |  | RetailDomainGlossary.PhysicalStore | PhysicalStore |  |
| Table |  | RetailDomainGlossary.OperationalActivity | OperationalActivity |  |
| Column | store_id | RetailDomainGlossary.StoreLocation | StoreLocation |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| employees | employee_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each employee in the retail operations system. |
| employees | store_id | bigint | Attribute |  |  |  |  | NO |  | Associates each employee with a specific store, referencing the `store_id` column in the `stores` table. |
| employees | first_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | Stores the given name of an employee as a non-null, case-insensitive Unicode string. |
| employees | last_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | The "last_name" column in the "employees" table stores the non-nullable family name of an employee as a case-insensitive Unicode string. |
| employees | email | nvarchar(450) | Attribute |  |  |  |  | NO |  | Stores the unique email address of each employee, used as a mandatory contact identifier. |
| employees | role | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | Indicates the job position or function of an employee within the organization, stored as a non-nullable text value. |
| employees | hire_date | date | Attribute |  |  |  |  | NO |  | The `hire_date` column in the `employees` table records the non-nullable date an employee was hired, used for tracking employment start dates. |
| employees | created_at | datetime2 | Attribute |  |  |  |  | NO |  | The `created_at` column records the non-nullable timestamp indicating when an employee record was initially created in the system. |
| employees | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | Tracks the timestamp of the most recent update to an employee record, ensuring accurate change history. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Incremental candidates: updated_at |  |  |
| BR2 | Business Rule | Contains sensitive or personal data fields according to the analyzer. |  |  |

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
| 1.0 | 2026-04-10 | Cursor | Initial generation from target data model and analyzer schema JSON |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
