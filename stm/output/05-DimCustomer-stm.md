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
> Customer master with loyalty program information and acquisition tracking. Supports SCD Type 2.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | CUSTOMERS |  |  | Bronze replica via Fivetran. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimCustomer | Type 2 | CustomerHashPK |  | Dimension | Customer master with loyalty program information and acquisition tracking. Supports SCD Type 2. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Table |  | Criticality.TransactionalCore | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.Sensitive | PII |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | Privacy.IdentifiedLoyaltyMember | Privacy |
| Table |  | QualityTrust.SystemOfRecord | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier1 | Tier |
| Column | CustomerHashBK | Criticality.TransactionalCore | Criticality |
| Column | CustomerHashBK | PII.None | PII |
| Column | CustomerHashBK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | PhoneNumber | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | PhoneNumber | PII.Sensitive | PII |
| Column | PhoneNumber | PersonalData.Personal | PersonalData |
| Column | LastName | Criticality.TransactionalCore | Criticality |
| Column | LastName | PII.Sensitive | PII |
| Column | LastName | PersonalData.Personal | PersonalData |
| Column | AcquisitionDate | Criticality.TransactionalCore | Criticality |
| Column | AcquisitionDate | PII.None | PII |
| Column | AcquisitionDate | QualityTrust.SystemOfRecord | QualityTrust |
| Column | AcquisitionDate | Retention.TransientOperational | Retention |
| Column | FirstName | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | FirstName | Criticality.TransactionalCore | Criticality |
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
| Table |  | RetailDomainGlossary.Customer | Customer | The buyer of goods in a retail context, whether identified through a loyalty programme or anonymous at the point of sale. |
| Column | CustomerHashBK | RetailDomainGlossary.CustomerIdentifier | Customer identifier | A unique key assigned to a known customer, often linked to a loyalty card or account registration. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimCustomer | CustomerHashPK | INT | Primary Key | Snowflake | CUSTOMERS | CUSTOMER_ID | CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) | NO |  | Surrogate primary key for customer dimension |
| DimCustomer | CustomerHashBK | VARCHAR(20) | Business Key | Snowflake | CUSTOMERS | CUSTOMER_ID | CAST(SHA2(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 256) AS BINARY(32)) | NO |  | Natural business key (customer ID from CRM) |
| DimCustomer | FirstName | VARCHAR(50) | Attribute | Snowflake | CUSTOMERS | FIRST_NAME |  | NO |  | Customer first name |
| DimCustomer | LastName | VARCHAR(50) | Attribute | Snowflake | CUSTOMERS | LAST_NAME |  | NO |  | Customer last name |
| DimCustomer | FullName | VARCHAR(100) | Attribute | Snowflake | CUSTOMERS | FIRST_NAME, LAST_NAME | TRIM(FIRST_NAME) \|\| ' ' \|\| TRIM(LAST_NAME) | NO |  | Concatenated full name for display |
| DimCustomer | EmailAddress | VARCHAR(100) | Attribute | Snowflake | CUSTOMERS | EMAIL |  | YES |  | Primary email address |
| DimCustomer | PhoneNumber | VARCHAR(20) | Attribute | Snowflake | CUSTOMERS | PHONE |  | YES |  | Primary phone number |
| DimCustomer | StreetAddress | VARCHAR(200) | Attribute | Snowflake |  |  |  | YES |  | Mailing street address |
| DimCustomer | City | VARCHAR(50) | Attribute | Snowflake |  |  |  | YES |  | Customer city |
| DimCustomer | StateProvince | VARCHAR(50) | Attribute | Snowflake |  |  |  | YES |  | Customer state or province |
| DimCustomer | PostalCode | VARCHAR(20) | Attribute | Snowflake |  |  |  | YES |  | Customer postal code |
| DimCustomer | Country | VARCHAR(50) | Attribute | Snowflake |  |  |  | YES |  | Customer country |
| DimCustomer | CustomerType | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Customer classification (Individual, Business, Wholesale) |
| DimCustomer | AcquisitionChannel | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | How customer was acquired (Online, Store, Referral, Advertising) |
| DimCustomer | AcquisitionDate | DATE | Attribute | Snowflake | CUSTOMERS | CREATED_AT | CAST(CREATED_AT AS DATE) | NO |  | Date customer was first acquired |
| DimCustomer | LoyaltyTier | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Current loyalty tier (Bronze, Silver, Gold, Platinum) |
| DimCustomer | LoyaltyPoints | INT | Attribute | Snowflake |  |  |  | NO |  | Current loyalty points balance |
| DimCustomer | LoyaltyJoinDate | DATE | Attribute | Snowflake |  |  |  | YES |  | Date customer joined loyalty program |
| DimCustomer | IsActive | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if customer account is active |
| DimCustomer | EffectiveDate | DATE | Attribute | Snowflake |  |  |  | NO |  | Start date when this version became effective |
| DimCustomer | ExpirationDate | DATE | Attribute | Snowflake |  |  |  | YES |  | End date when this version expired (NULL if current) |
| DimCustomer | IsCurrent | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if this is the current active version |
| DimCustomer | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimCustomer | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | CDC / UPDATED_AT | Scheduled |  | Standard error handling and retry | dbt |

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
