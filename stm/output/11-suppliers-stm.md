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
> Target supplier master table preserving supplier identifiers, names, contact details, and audit timestamps in a source-aligned target design.

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
|  |  | suppliers |  | One row per supplier_id / supplier_id |  | Target Table (Source-Aligned) | Target supplier master table preserving supplier identifiers, names, contact details, and audit timestamps in a source-aligned target design. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Raw | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Table |  | Criticality.Operational | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | PII.Sensitive | PII |
| Table |  | Privacy.IdentifiedLoyaltyMember | Privacy |
| Table |  | QualityTrust.SupplierProvided | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | supplier_id | Criticality.TransactionalCore | Criticality |
| Column | supplier_id | PII.None | PII |
| Column | supplier_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | name | Criticality.Operational | Criticality |
| Column | name | PersonalData.Personal | PersonalData |
| Column | name | PII.NonSensitive | PII |
| Column | contact_name | PersonalData.Personal | PersonalData |
| Column | contact_name | PII.Sensitive | PII |
| Column | email | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | email | PersonalData.Personal | PersonalData |
| Column | email | PII.Sensitive | PII |
| Column | email | Criticality.Operational | Criticality |
| Column | phone | PersonalData.Personal | PersonalData |
| Column | phone | PII.Sensitive | PII |
| Column | phone | Criticality.Operational | Criticality |
| Column | created_at | Criticality.Operational | Criticality |
| Column | created_at | PII.None | PII |
| Column | created_at | Retention.TransientOperational | Retention |
| Column | updated_at | Criticality.Operational | Criticality |
| Column | updated_at | PII.None | PII |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Supplier | Supplier |  |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| suppliers | supplier_id | bigint | Attribute |  |  |  |  | NO |  | Unique identifier for each supplier, serving as the primary key in the suppliers table. |
| suppliers | name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | NO |  | The "name" column in the "suppliers" table is a non-nullable nvarchar field intended to store the name of the supplier, though sample data indicates it is currently unused. |
| suppliers | contact_name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | YES |  | The "contact_name" column in the "suppliers" table stores the name of the primary contact person for a supplier, allowing null values. |
| suppliers | email | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | YES |  | Stores the email address of the supplier, allowing null values. |
| suppliers | phone | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute |  |  |  |  | YES |  | The "phone" column in the "suppliers" table stores the contact phone number of a supplier as a nullable string. |
| suppliers | created_at | datetime2 | Attribute |  |  |  |  | NO |  | Records the timestamp when a supplier entry is initially created in the system, stored as a non-nullable datetime value. |
| suppliers | updated_at | datetime2 | Attribute |  |  |  |  | NO |  | The `updated_at` column stores the non-nullable timestamp of the most recent update to a supplier record in the `suppliers` table. |

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
