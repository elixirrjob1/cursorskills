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
| Column | SupplierDBAName | Architecture.Enriched | Architecture |
| Column | SupplierDBAName | Certification.Gold | Certification |
| Column | SupplierDBAName | PII.None | PII |
| Column | SupplierDBAName | Privacy.Non-Personal | Privacy |
| Column | SupplierDBAName | Criticality.Operational | Criticality |
| Column | SupplierDBAName | QualityTrust.SupplierProvided | QualityTrust |
| Column | StreetAddress | Architecture.Enriched | Architecture |
| Column | StreetAddress | Certification.Gold | Certification |
| Column | StreetAddress | PII.None | PII |
| Column | StreetAddress | Privacy.Non-Personal | Privacy |
| Column | StreetAddress | Criticality.Operational | Criticality |
| Column | StreetAddress | QualityTrust.SupplierProvided | QualityTrust |
| Column | City | Architecture.Enriched | Architecture |
| Column | City | Certification.Gold | Certification |
| Column | City | PII.None | PII |
| Column | City | Privacy.Non-Personal | Privacy |
| Column | City | Criticality.Operational | Criticality |
| Column | City | QualityTrust.SupplierProvided | QualityTrust |
| Column | StateProvince | Architecture.Enriched | Architecture |
| Column | StateProvince | Certification.Gold | Certification |
| Column | StateProvince | PII.None | PII |
| Column | StateProvince | Privacy.Non-Personal | Privacy |
| Column | StateProvince | Criticality.Operational | Criticality |
| Column | StateProvince | QualityTrust.SupplierProvided | QualityTrust |
| Column | PostalCode | Architecture.Enriched | Architecture |
| Column | PostalCode | Certification.Gold | Certification |
| Column | PostalCode | PII.None | PII |
| Column | PostalCode | Privacy.Non-Personal | Privacy |
| Column | PostalCode | Criticality.Operational | Criticality |
| Column | PostalCode | QualityTrust.SupplierProvided | QualityTrust |
| Column | Country | Architecture.Enriched | Architecture |
| Column | Country | Certification.Gold | Certification |
| Column | Country | PII.None | PII |
| Column | Country | Privacy.Non-Personal | Privacy |
| Column | Country | Criticality.Operational | Criticality |
| Column | Country | QualityTrust.SupplierProvided | QualityTrust |
| Column | PaymentTermsCode | Architecture.Enriched | Architecture |
| Column | PaymentTermsCode | Certification.Gold | Certification |
| Column | PaymentTermsCode | PII.None | PII |
| Column | PaymentTermsCode | Criticality.Operational | Criticality |
| Column | PaymentTermsCode | QualityTrust.SupplierProvided | QualityTrust |
| Column | PaymentTermsDescription | Architecture.Enriched | Architecture |
| Column | PaymentTermsDescription | Certification.Gold | Certification |
| Column | PaymentTermsDescription | PII.None | PII |
| Column | PaymentTermsDescription | Criticality.Operational | Criticality |
| Column | PaymentTermsDescription | QualityTrust.SupplierProvided | QualityTrust |
| Column | PaymentTermsDays | Architecture.Enriched | Architecture |
| Column | PaymentTermsDays | Certification.Gold | Certification |
| Column | PaymentTermsDays | PII.None | PII |
| Column | PaymentTermsDays | Criticality.Operational | Criticality |
| Column | PaymentTermsDays | QualityTrust.SupplierProvided | QualityTrust |
| Column | LeadTimeDays | Architecture.Enriched | Architecture |
| Column | LeadTimeDays | Certification.Gold | Certification |
| Column | LeadTimeDays | PII.None | PII |
| Column | LeadTimeDays | Criticality.Operational | Criticality |
| Column | LeadTimeDays | QualityTrust.SupplierProvided | QualityTrust |
| Column | MinimumOrderAmount | Architecture.Enriched | Architecture |
| Column | MinimumOrderAmount | Certification.Gold | Certification |
| Column | MinimumOrderAmount | PII.None | PII |
| Column | MinimumOrderAmount | Criticality.Operational | Criticality |
| Column | MinimumOrderAmount | QualityTrust.SupplierProvided | QualityTrust |
| Column | IsActive | Architecture.Enriched | Architecture |
| Column | IsActive | Certification.Gold | Certification |
| Column | IsActive | PII.None | PII |
| Column | IsActive | Criticality.Operational | Criticality |
| Column | IsPreferred | Architecture.Enriched | Architecture |
| Column | IsPreferred | Certification.Gold | Certification |
| Column | IsPreferred | PII.None | PII |
| Column | IsPreferred | Criticality.Operational | Criticality |
| Column | EtlBatchId | Architecture.Enriched | Architecture |
| Column | EtlBatchId | Certification.Gold | Certification |
| Column | EtlBatchId | PII.None | PII |
| Column | EtlBatchId | Criticality.Operational | Criticality |
| Column | LoadTimestamp | Architecture.Enriched | Architecture |
| Column | LoadTimestamp | Certification.Gold | Certification |
| Column | LoadTimestamp | PII.None | PII |
| Column | LoadTimestamp | Criticality.Operational | Criticality |

---

## 6. Glossary Terms
Definitions are included only when they are present in the analyzer JSON.

| Scope | Column | Term FQN | Term Name | Definition |
|-------|--------|----------|-----------|------------|
| Table |  | RetailDomainGlossary.Supplier | Supplier | An external party that provides goods to the retailer, typically under negotiated commercial terms. |
| Column | SupplierHashBK | RetailDomainGlossary.SupplierCode | Supplier code | A unique identifier assigned to each supplier in the retailer's master data. |
| Column | LeadTimeDays | RetailDomainGlossary.SupplierLeadTime | Supplier lead time | The elapsed time between placing a purchase order and receiving the goods at the designated location. |

---

## 7. Field-Level Mapping Matrix
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| DimSupplier | SupplierHashPK | NUMBER(19,0) | Primary Key | Snowflake | SUPPLIERS | SUPPLIER_ID | HASH(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Surrogate primary key for supplier dimension |
| DimSupplier | SupplierHashBK | NUMBER(19,0) | Business Key | Snowflake | SUPPLIERS | SUPPLIER_ID | HASH(COALESCE(CAST(SUPPLIER_ID AS VARCHAR), '#@#@#@#@#')) | NO |  | Natural business key (supplier ID from procurement system) |
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
