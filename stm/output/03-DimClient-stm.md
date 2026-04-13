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
> Client organization master containing company details, industry classification, and relationship attributes. Supports historical tracking for changes in account status or industry classification.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | See field-level mapping |  |  | Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimClient | Type 2 on AccountStatus, Industry, AccountManager | ClientHashPK |  | Dimension (SCD Type 2) | Client organization master containing company details, industry classification, and relationship attributes. Supports historical tracking for changes in account status or industry classification. |

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
| DimClient | ClientHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for client dimension |
| DimClient | ClientHashBK | VARCHAR(20) | Business Key |  |  |  |  | NO |  | Business key (client ID from CRM system) |
| DimClient | ClientName | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Legal company name |
| DimClient | ClientShortName | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Common/short name for reporting |
| DimClient | Industry | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Primary industry classification |
| DimClient | IndustrySector | VARCHAR(50) | Attribute |  |  |  |  | NO |  | Industry sector grouping |
| DimClient | CompanySize | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Size classification (Small, Medium, Large, Enterprise) |
| DimClient | AccountStatus | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Account status (Prospect, Active, Inactive, Former) |
| DimClient | AccountManager | VARCHAR(100) | Attribute |  |  |  |  | NO |  | Assigned account manager name |
| DimClient | BillingAddress | VARCHAR(200) | Attribute |  |  |  |  | YES |  | Client billing address |
| DimClient | BillingCity | VARCHAR(50) | Attribute |  |  |  |  | YES |  | Billing city |
| DimClient | BillingState | VARCHAR(50) | Attribute |  |  |  |  | YES |  | Billing state/province |
| DimClient | BillingCountry | VARCHAR(50) | Attribute |  |  |  |  | YES |  | Billing country |
| DimClient | PaymentTerms | VARCHAR(20) | Attribute |  |  |  |  | NO |  | Standard payment terms (Net 30, Net 45, etc.) |
| DimClient | RelationshipStartDate | DATE | Attribute |  |  |  |  | NO |  | Date client relationship began |
| DimClient | EffectiveDate | DATE | Attribute |  |  |  | Populate when a new SCD Type 2 version becomes effective. | NO |  | SCD Type 2 row effective start date |
| DimClient | ExpirationDate | DATE | Attribute |  |  |  | Populate with the end date of the current SCD Type 2 version. | NO |  | SCD Type 2 row expiration date |
| DimClient | IsCurrent | BOOLEAN | Attribute |  |  |  | Set to indicate whether the row is the current SCD Type 2 version. | NO |  | SCD Type 2 current row flag |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | AccountStatus valid values: Prospect, Active, Inactive, Former |  |  |
| BR2 | Business Rule | CompanySize valid values: Small (<100 employees), Medium (100-999), Large (1000-9999), Enterprise (10000+) |  |  |
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
