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
> Target store master table preserving store identifiers, reference codes, address details, contact fields, and audit timestamps.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | See field-level mapping |  |  | Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | stores |  | One row per store_id / store_id |  | Target Table (Source-Aligned) | Target store master table preserving store identifiers, reference codes, address details, contact fields, and audit timestamps. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Bronze | Certification |
| Table |  | Criticality.Operational | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | PII.Sensitive | PII |
| Table |  | Privacy.AnonymousAggregate | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | store_id | Criticality.TransactionalCore | Criticality |
| Column | store_id | PII.None | PII |
| Column | store_id | QualityTrust.SystemOfRecord | QualityTrust |
| Column | name | Criticality.Operational | Criticality |
| Column | name | PersonalData.Personal | PersonalData |
| Column | name | PII.NonSensitive | PII |
| Column | code | Criticality.Operational | Criticality |
| Column | code | PII.None | PII |
| Column | address | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | address | PersonalData.Personal | PersonalData |
| Column | address | PII.Sensitive | PII |
| Column | city | PII.None | PII |
| Column | postal_code | ComplianceLegal.TaxVAT | ComplianceLegal |
| Column | postal_code | PII.NonSensitive | PII |
| Column | phone | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | phone | PersonalData.Personal | PersonalData |
| Column | phone | PII.Sensitive | PII |
| Column | created_at | Criticality.Operational | Criticality |
| Column | created_at | PII.None | PII |
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
| Table |  | RetailDomainGlossary.PhysicalStore | PhysicalStore | A brick-and-mortar location where customers browse and purchase goods and where local inventory and staff are managed. **Type:** business_entity \| **Usage:** Store network planning, staffing, compliance, and location-level P&L.

Review status: draft. |
| Table |  | RetailDomainGlossary.StoreLocation | StoreLocation | A distinct site used to scope inventory, sales, and operational activity within the retail network. **Type:** business_entity \| **Usage:** Inventory allocation, replenishment triggers, and cross-location performance comparison.

Review status: draft. |
| Table |  | RetailDomainGlossary.StoreCode | StoreCode | A unique short identifier assigned to each store location for use in systems, reporting, and logistics addressing. **Type:** identifier \| **Usage:** Location-level filtering, delivery routing, and store-comparison reports.

Inferred from multi-location operations.

Review status: draft. |
| Column | name | RetailDomainGlossary.PhysicalStore | PhysicalStore | A brick-and-mortar location where customers browse and purchase goods and where local inventory and staff are managed. **Type:** business_entity \| **Usage:** Store network planning, staffing, compliance, and location-level P&L.

Review status: draft. |
| Column | code | RetailDomainGlossary.StoreCode | StoreCode | A unique short identifier assigned to each store location for use in systems, reporting, and logistics addressing. **Type:** identifier \| **Usage:** Location-level filtering, delivery routing, and store-comparison reports.

Inferred from multi-location operations.

Review status: draft. |
| Column | address | RetailDomainGlossary.StoreLocation | StoreLocation | A distinct site used to scope inventory, sales, and operational activity within the retail network. **Type:** business_entity \| **Usage:** Inventory allocation, replenishment triggers, and cross-location performance comparison.

Review status: draft. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| stores | store_id | bigint | Attribute | Snowflake | STORES | STORE_ID | Source type: number(38,0) | NO |  | Unique identifier for each store in the retail operations system. |
| stores | name | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | STORES | NAME | Source type: text(256) | NO |  | The "name" column in the "stores" table is a non-nullable nvarchar field intended to store the name of a retail store. |
| stores | code | nvarchar(450) | Attribute | Snowflake | STORES | CODE | Source type: text(900) | NO |  | Unique alphanumeric identifier for each store, used for internal reference and operations. |
| stores | address | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | STORES | ADDRESS | Source type: text(256) | YES |  | The "address" column in the "stores" table stores the street address of a retail store location as a nullable Unicode string. |
| stores | city | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | STORES | CITY | Source type: text(256) | YES |  | Stores.city: The nullable nvarchar column stores the name of the city where each store is located. |
| stores | state | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | STORES | STATE | Source type: text(256) | YES |  | The "state" column in the "stores" table stores the name of the state or province where a store is located, allowing null values. |
| stores | postal_code | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | STORES | POSTAL_CODE | Source type: text(256) | YES |  | The "postal_code" column in the "stores" table stores the postal or ZIP code of a store location as a nullable nvarchar value. |
| stores | phone | nvarchar collate "sql_latin1_general_cp1_ci_as" | Attribute | Snowflake | STORES | PHONE | Source type: text(256) | YES |  | The "phone" column in the "stores" table stores the contact phone number of a store as a nullable nvarchar value. |
| stores | created_at | datetime2 | Attribute | Snowflake | STORES | CREATED_AT | Source type: timestamp_ntz | NO |  | The `created_at` column stores the non-nullable timestamp indicating when a store record was initially created in the system. |
| stores | updated_at | datetime2 | Attribute | Snowflake | STORES | UPDATED_AT | Source type: timestamp_ntz | NO |  | The `updated_at` column records the timestamp of the most recent update to a store's record and cannot be null. |

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
| DQ1 | STORE_ID must not be NULL (primary key) | NOT NULL | STORE_ID IS NOT NULL | Reject record |  |
| DQ2 | STORE_ID must be unique | Uniqueness | COUNT(DISTINCT STORE_ID) = COUNT(*) | Reject record |  |
|  |  |  |  |  |  |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | Delta load using UPDATED_AT |  |  |  |  |
|  |  |  |  |  |  |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-13 | Cursor | Initial generation from target data model and analyzer schema JSON |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
