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
> Target customer master table preserving customer identifiers, names, contact details, and audit timestamps in an analyzer-compatible structure.

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
|  |  | customers |  | One row per customer_id / customer_id |  | Target Table (Source-Aligned) | Target customer master table preserving customer identifiers, names, contact details, and audit timestamps in an analyzer-compatible structure. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | PII.Sensitive | PII |
| Table |  | Privacy.IdentifiedLoyaltyMember | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier1 | Tier |
| Column | customer_id | Criticality.TransactionalCore | Criticality |
| Column | customer_id | PII.None | PII |
| Column | customer_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | first_name | PersonalData.Personal | PersonalData |
| Column | first_name | PII.Sensitive | PII |
| Column | first_name | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | first_name | Criticality.TransactionalCore | Criticality |
| Column | last_name | PersonalData.Personal | PersonalData |
| Column | last_name | PII.Sensitive | PII |
| Column | last_name | Criticality.TransactionalCore | Criticality |
| Column | email | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | email | PersonalData.Personal | PersonalData |
| Column | email | PII.Sensitive | PII |
| Column | phone | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | phone | PersonalData.Personal | PersonalData |
| Column | phone | PII.Sensitive | PII |
| Column | created_at | Criticality.TransactionalCore | Criticality |
| Column | created_at | PII.None | PII |
| Column | created_at | Retention.TransientOperational | Retention |
| Column | created_at | QualityTrust.SystemOfRecord | QualityTrust |
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
| Table |  | RetailDomainGlossary.CustomerIdentifier | CustomerIdentifier |  |
| Column | customer_id | RetailDomainGlossary.CustomerIdentifier | CustomerIdentifier |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| customers | customer_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each customer in the retail operations system. |
| customers | first_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | Stores the given name of a customer as a non-null, case-insensitive Unicode string. |
| customers | last_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | The "last_name" column in the "customers" table stores the non-nullable family name of a customer as a case-insensitive Unicode string. |
| customers | email | nvarchar(450) | Attribute |  |  |  |  | YES |  | Stores the email address of the customer, allowing null values, for contact purposes. |
| customers | phone | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | YES |  | Stores the customer's phone number as an optional text field for contact purposes. |
| customers | created_at | datetime2 | Attribute |  |  |  |  | NO |  | The `created_at` column records the non-nullable timestamp indicating when a customer record was initially created in the system. |
| customers | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | Tracks the timestamp of the most recent update to a customer's record, ensuring accurate change history. |

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
