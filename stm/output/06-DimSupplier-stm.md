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
> Supplier master with contact information and payment terms.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | SUPPLIERS |  |  | Bronze replica via Fivetran. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimSupplier | Type 1 | SupplierHashPK |  | Dimension | Supplier master with contact information and payment terms. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Architecture.Enriched | Architecture |
| Table |  | Certification.Gold | Certification |
| Table |  | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Table |  | Criticality.Operational | Criticality |
| Table |  | Lifecycle.Active | Lifecycle |
| Table |  | PII.Sensitive | PII |
| Table |  | PersonalData.Personal | PersonalData |
| Table |  | Privacy.IdentifiedLoyaltyMember | Privacy |
| Table |  | QualityTrust.SupplierProvided | QualityTrust |
| Table |  | Retention.FinancialStatutory | Retention |
| Table |  | Tier.Tier3 | Tier |
| Column | SupplierHashBK | Criticality.TransactionalCore | Criticality |
| Column | SupplierHashBK | PII.None | PII |
| Column | SupplierHashBK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | ContactName | Criticality.Operational | Criticality |
| Column | ContactName | PII.NonSensitive | PII |
| Column | ContactName | PersonalData.Personal | PersonalData |
| Column | ContactPhone | Criticality.Operational | Criticality |
| Column | ContactPhone | PII.Sensitive | PII |
| Column | ContactPhone | PersonalData.Personal | PersonalData |
| Column | SupplierName | Criticality.Operational | Criticality |
| Column | SupplierName | PII.NonSensitive | PII |
| Column | SupplierName | PersonalData.Personal | PersonalData |
| Column | ContactEmail | ComplianceLegal.GDPRCCPA | ComplianceLegal |
| Column | ContactEmail | Criticality.Operational | Criticality |
| Column | ContactEmail | PII.Sensitive | PII |
| Column | ContactEmail | PersonalData.Personal | PersonalData |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Supplier | Supplier | An external party that provides goods to the retailer, typically under negotiated commercial terms. |
| Column | SupplierHashBK | RetailDomainGlossary.SupplierCode | Supplier code | A unique identifier assigned to each supplier in the retailer's master data. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimSupplier | SupplierHashPK | INT | Primary Key | Snowflake | SUPPLIERS | SUPPLIER_ID | SHA2(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) | NO |  | Surrogate primary key for supplier dimension |
| DimSupplier | SupplierHashBK | VARCHAR(20) | Business Key | Snowflake | SUPPLIERS | SUPPLIER_ID | SHA2(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#'), 256) | NO |  | Natural business key (supplier ID from procurement system) |
| DimSupplier | SupplierName | VARCHAR(100) | Attribute | Snowflake | SUPPLIERS | NAME |  | NO |  | Legal supplier name |
| DimSupplier | SupplierDBAName | VARCHAR(100) | Attribute | Snowflake |  |  |  | YES |  | Supplier doing-business-as name |
| DimSupplier | ContactName | VARCHAR(100) | Attribute | Snowflake | SUPPLIERS | CONTACT_NAME |  | YES |  | Primary contact person name |
| DimSupplier | ContactEmail | VARCHAR(100) | Attribute | Snowflake | SUPPLIERS | EMAIL |  | YES |  | Primary contact email |
| DimSupplier | ContactPhone | VARCHAR(20) | Attribute | Snowflake | SUPPLIERS | PHONE |  | YES |  | Primary contact phone |
| DimSupplier | StreetAddress | VARCHAR(200) | Attribute | Snowflake |  |  |  | NO |  | Supplier street address |
| DimSupplier | City | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Supplier city |
| DimSupplier | StateProvince | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Supplier state or province |
| DimSupplier | PostalCode | VARCHAR(20) | Attribute | Snowflake |  |  |  | NO |  | Supplier postal code |
| DimSupplier | Country | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Supplier country |
| DimSupplier | PaymentTermsCode | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Payment terms code (NET30, NET60, 2/10NET30, etc.) |
| DimSupplier | PaymentTermsDescription | VARCHAR(50) | Attribute | Snowflake |  |  |  | NO |  | Payment terms description |
| DimSupplier | PaymentTermsDays | INT | Attribute | Snowflake |  |  |  | NO |  | Number of days for payment |
| DimSupplier | LeadTimeDays | INT | Attribute | Snowflake |  |  |  | NO |  | Standard lead time in days |
| DimSupplier | MinimumOrderAmount | DECIMAL(19,4) | Attribute | Snowflake |  |  |  | YES |  | Minimum order amount if applicable |
| DimSupplier | IsActive | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if supplier is currently active |
| DimSupplier | IsPreferred | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if supplier is a preferred vendor |
| DimSupplier | EtlBatchId | INT | Audit/Metadata | Snowflake |  |  |  | NO |  | ETL batch identifier that loaded this record |
| DimSupplier | LoadTimestamp | TIMESTAMP | Audit/Metadata | Snowflake |  |  |  | NO |  | Timestamp when record was loaded |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Incremental | CDC / UPDATED_AT | Scheduled | Source bronze table available | Retry failed batch; log and alert on consecutive failures | dbt |

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
