## 1. Document Information
| Field | Description |
|-------|-------------|
| **Project Name** | Retail Dimensional |
| **System / Module** | Retail Dimensional |
| **STM Version** | 2.0 |
| **Author** | fillip |
| **Date Created** | 2026-04-13 |
| **Last Updated** | 2026-04-21 |
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
| ERP | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | CUSTOMERS | Near real-time via Fivetran | Data Engineering | Fivetran **history mode** — source already exposes per-version rows with `_FIVETRAN_START` / `_FIVETRAN_END` / `_FIVETRAN_ACTIVE`. Hard deletes surface as rows whose `_FIVETRAN_END` is not the sentinel '9999-12-31 23:59:59.999'. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimCustomer | Type 2 | CustomerHashPK | n/a (Snowflake) | Dimension | Customer master with loyalty program information and acquisition tracking. Supports SCD Type 2. |

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

### Data Condition 1
> Per-version rows coming from Fivetran history-mode replication of `BRONZE_ERP__DBO.CUSTOMERS`. Each version of a customer row arrives as a distinct record with its own `_FIVETRAN_START` / `_FIVETRAN_END` window.

| Source System | Source Table | Source Column(s) | Column Alias | Transformation / Business Rule | Partition Field Rank | Order By Field Rank |
| -- | -- | -- | -- | -- | -- | -- |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | CUSTOMER_ID | CUSTOMER_ID |  | 1 | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | FIRST_NAME | FIRST_NAME |  | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | LAST_NAME | LAST_NAME |  | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | EMAIL | EMAIL |  | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | PHONE | PHONE |  | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | CREATED_AT | CREATED_AT |  | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | UPDATED_AT | UPDATED_AT |  | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | _FIVETRAN_START | EffectiveStartDateTimeUTC | Use `_FIVETRAN_START` directly (already `TIMESTAMP_TZ`, UTC). Partition-level start of each SCD2 version. | | 1 |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | _FIVETRAN_END | EffectiveEndDateTimeRaw | Use `_FIVETRAN_END` directly. Sentinel `'9999-12-31 23:59:59.999'` means current/open version. | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | _FIVETRAN_ACTIVE | IsFivetranActive | Passthrough — `TRUE` means current/open version for this CUSTOMER_ID. | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | _FIVETRAN_SYNCED | InsertedDateTimeUTC | Pipeline sync timestamp (audit). | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | 'Data Condition 1' | DataCondition | Hard code as 'Data Condition 1'. | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | 'ERP' | SourceSystemCode | Hard code as 'ERP'. | | |
| ERP | BRONZE_ERP__DBO.CUSTOMERS | '' | FileName | Hard code as empty string — Fivetran-sourced, no file origin. | | |

### Final
| Target Table | Target Column | Data Type | Field Type | Source System | Source Table | Source Column(s) | Transformation / Business Rule | Nullable? | Default / Fallback | Description |
|--------------|---------------|-----------|------------|---------------|--------------|------------------|--------------------------------|-----------|--------------------|-------------|
| GOLD.DimCustomer | CustomerHashPK | NUMBER(19,0) | Primary Key | Derived from Data Condition 1 | Derived from Data Condition 1 | CUSTOMER_ID | `HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 'ERP')` (TX1) | NO |  | Surrogate primary key for customer dimension. |
| GOLD.DimCustomer | CustomerHashBK | NUMBER(19,0) | Business Key | Derived from Data Condition 1 | Derived from Data Condition 1 | CUSTOMER_ID | `HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR), '#@#@#@#@#'), 'ERP')` (TX1) | NO |  | Natural business key (customer ID from CRM). Note: CRM uses CUSTOMER_ID as both PK and BK. |
| GOLD.DimCustomer | AcquisitionChannel | VARCHAR(50) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | How customer was acquired (Online, Store, Referral, Advertising). |
| GOLD.DimCustomer | AcquisitionDate | DATE | Attribute | ERP | CUSTOMERS | CREATED_AT | `CAST(CREATED_AT AS DATE)` | YES |  | Date customer was first acquired. |
| GOLD.DimCustomer | City | VARCHAR(50) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Customer city. |
| GOLD.DimCustomer | Country | VARCHAR(50) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Customer country. |
| GOLD.DimCustomer | CustomerType | VARCHAR(20) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Customer classification (Individual, Business, Wholesale). |
| GOLD.DimCustomer | EmailAddress | VARCHAR(100) | Attribute | ERP | CUSTOMERS | EMAIL | Direct mapping. | YES |  | Primary email address. |
| GOLD.DimCustomer | FirstName | VARCHAR(50) | Attribute | ERP | CUSTOMERS | FIRST_NAME | Direct mapping. | YES |  | Customer first name. |
| GOLD.DimCustomer | FullName | VARCHAR(100) | Attribute | ERP | CUSTOMERS | FIRST_NAME, LAST_NAME | `TRIM(COALESCE(FIRST_NAME,'')) \|\| ' ' \|\| TRIM(COALESCE(LAST_NAME,''))` | YES |  | Concatenated full name for display. |
| GOLD.DimCustomer | IsActive | BOOLEAN | Attribute |  |  |  | Not available in source — assume `TRUE` for all current versions, `FALSE` for rows whose Fivetran history was closed-out without a successor (future enhancement). | NO | TRUE | True if customer account is active. |
| GOLD.DimCustomer | LastName | VARCHAR(50) | Attribute | ERP | CUSTOMERS | LAST_NAME | Direct mapping. | YES |  | Customer last name. |
| GOLD.DimCustomer | LoyaltyJoinDate | DATE | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Date customer joined loyalty program. |
| GOLD.DimCustomer | LoyaltyPoints | INT | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Current loyalty points balance. |
| GOLD.DimCustomer | LoyaltyTier | VARCHAR(20) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Current loyalty tier (Bronze, Silver, Gold, Platinum). |
| GOLD.DimCustomer | PhoneNumber | VARCHAR(20) | Attribute | ERP | CUSTOMERS | PHONE | Direct mapping. | YES |  | Primary phone number. |
| GOLD.DimCustomer | PostalCode | VARCHAR(20) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Customer postal code. |
| GOLD.DimCustomer | StateProvince | VARCHAR(50) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Customer state or province. |
| GOLD.DimCustomer | StreetAddress | VARCHAR(200) | Attribute |  |  |  | Not available in source — set to NULL. | YES |  | Mailing street address. |
| GOLD.DimCustomer | EffectiveStartDateTime | TIMESTAMP_TZ | Type 2 Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_START | `_FIVETRAN_START` passthrough. | NO |  | Start of Type 2 version validity window. |
| GOLD.DimCustomer | EffectiveEndDateTime | TIMESTAMP_TZ | Type 2 Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_END | `_FIVETRAN_END` passthrough — `'9999-12-31 23:59:59.999'` for current versions. | NO | '9999-12-31 23:59:59.999 UTC' | End of Type 2 version validity window. |
| GOLD.DimCustomer | CurrentFlagYN | VARCHAR(1) | Type 2 Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_ACTIVE | `IFF(_FIVETRAN_ACTIVE, 'Y', 'N')` | NO | 'N' | Flag indicating if this is the current active version (Y) or historical (N). |
| GOLD.DimCustomer | CreatedDateTime | TIMESTAMP_TZ | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_START | `_FIVETRAN_START` (BR1). | NO |  | Timestamp when the record version was created in the target. |
| GOLD.DimCustomer | ModifiedDateTime | TIMESTAMP_TZ | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_SYNCED | `_FIVETRAN_SYNCED` (BR2). | NO |  | Timestamp when the record version was last modified. |
| GOLD.DimCustomer | SourceSystemCode | VARCHAR(5) | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | 'ERP' | Hard code. | NO | 'ERP' | Source system identifier. |
| GOLD.DimCustomer | SourceCustomerPK | VARCHAR(40) | Source | Derived from Data Condition 1 | Derived from Data Condition 1 | CUSTOMER_ID | `CAST(CUSTOMER_ID AS VARCHAR)` (TX2). | NO |  | Original CUSTOMER_ID from source system (pre-hash). |
| GOLD.DimCustomer | SourceCustomerBK | VARCHAR(40) | Source | Derived from Data Condition 1 | Derived from Data Condition 1 | CUSTOMER_ID | `CAST(CUSTOMER_ID AS VARCHAR)` (TX4). | NO |  | Original CUSTOMER_ID used as business key (pre-hash). |
| GOLD.DimCustomer | FileName | VARCHAR(255) | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | '' | Hard code as empty string — Fivetran-sourced (BR3). | NO | '' | Source file name for audit (Fivetran: none). |
| GOLD.DimCustomer | StageInsertedDateTimeUTC | TIMESTAMP_TZ | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | _FIVETRAN_SYNCED | `_FIVETRAN_SYNCED` (BR4). | NO |  | UTC timestamp when record was synced into the bronze staging table by Fivetran. |
| GOLD.DimCustomer | Hashbytes | BINARY | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | All attributes | SHA2_BINARY over alphabetically-sorted business attributes, pipe-separated, NULL replaced with '#@#@#@#@#' (BR5, TX6). | YES |  | Hash over business attributes used for change-detection in Type 2 SCD. |
| GOLD.DimCustomer | DataCondition | VARCHAR(50) | Metadata | Derived from Data Condition 1 | Derived from Data Condition 1 | 'Data Condition 1' | Hard code. | NO | 'Data Condition 1' | Indicates which Data Condition the record originated from. |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Standard Attribute Fields | `CreatedDateTime` should be included on all objects as a `TIMESTAMP_TZ` datatype. |  |  |
| BR2 | Standard Attribute Fields | `ModifiedDateTime` should be included on all objects as a `TIMESTAMP_TZ` datatype. |  |  |
| BR3 | Standard Audit/Metadata Fields | `FileName` should be included on all objects as a `VARCHAR(255)` field. For Fivetran-sourced tables this is hard-coded to an empty string (no file concept). |  |  |
| BR4 | Standard Audit/Metadata Fields | `StageInsertedDateTimeUTC` should be included on all objects as a `TIMESTAMP_TZ`. Sourced from `_FIVETRAN_SYNCED`. |  |  |
| BR5 | Standard Audit/Metadata Fields | `Hashbytes` should be included on all objects as a `BINARY` column, sourced from a SHA2_256 hash of all business-attribute fields (excluding PK/FK/BK and all metadata columns). |  |  |
| BR6 | Standard Audit/Metadata Fields | Not applicable — Fivetran history mode replaces the need for an explicit `InsertedDateTimeUTC` per-record attribute. |  |  |
| BR7 | Standard Audit/Metadata Fields | Not applicable — Fivetran-sourced tables have no `InsertedByUser` semantic. |  |  |
| BR8 | Primary Key Cardinality | All objects should include only 1 `*HashPK` field. |  |  |
| BR9 | Foreign Key Cardinality | All objects should include 0 or more `*HashFK` fields. DimCustomer has 0. |  |  |
| BR10 | Business Key Cardinality | All objects should include 0 or more `*HashBK` fields. DimCustomer has 1 (`CustomerHashBK`). |  |  |
| BR11 | Type 2 Data Conditions | SCD Type = Type 2 objects require at least 1 Data Condition and exactly 1 Final section. |  | Fivetran history mode supplies the full version stream in a single Data Condition. |
| BR12 | Type 2 Metadata | SCD Type = Type 2 objects require `EffectiveStartDateTime`, `EffectiveEndDateTime`, and `CurrentFlagYN` fields. |  |  |
| TX1 | Hash[PFB]K Transformation | All `*HashPK`, `*HashFK`, `*HashBK` values must be computed as `HASH(cast_fields, 'ERP')` with `SourceSystemCode` as the last argument. Returns a Snowflake 64-bit signed `NUMBER`. | `HASH(COALESCE(CAST(CUSTOMER_ID AS VARCHAR),'#@#@#@#@#'),'ERP')` |  |
| TX2 | Source*PK Transformation | `Source[xxx]PK` stores the pre-hash source key(s), concatenated with `;` separator. | `CAST(CUSTOMER_ID AS VARCHAR)` |  |
| TX3 | Source*FK Transformation | `Source[xxx]FK` stores pre-hash foreign-key value(s), `;` separator. | n/a for DimCustomer |  |
| TX4 | Source*BK Transformation | `Source[xxx]BK` stores pre-hash business-key value(s), `;` separator. | `CAST(CUSTOMER_ID AS VARCHAR)` |  |
| TX5 | stg Deduplication logic | Fivetran history mode already delivers one row per version per PK. No stg dedup needed. Optional hash-based `LAG` filter included as safety net. |  |  |
| TX6 | Hashbytes Transformation | Individual fields are concatenated together with `'\|'` separator and NULL-replaced with `'#@#@#@#@#'`, sorted alphabetically by source field name, then SHA2_256-hashed into a `BINARY`. |  |  |

---

## 9. Data Quality & Validation Rules
| Rule ID | Description | Check Type | Threshold / Condition | Action on Failure | Owner |
|---------|-------------|------------|-----------------------|-------------------|-------|
| DQ1 | Multiple records from the source mapping view with the same (CustomerHashPK, EffectiveStartDateTime) | SourceDupes | > 0 | Alert Owner | Data Engineering |
| DQ2 | Multiple records from the target with the same (CustomerHashPK, EffectiveStartDateTime) | TargetDupes | > 0 | Alert Owner | Data Engineering |
| DQ3 | CustomerHashPK appears in source mapping view but not in target table | SourceNotInTarget | > 0 | Alert Owner | Data Engineering |
| DQ4 | CustomerHashPK appears in target table but not in source mapping view | TargetNotInSource | > 0 | Alert Owner | Data Engineering |
| DQ5 | Hashbytes differ in source mapping view compared to target for the same (CustomerHashPK, EffectiveStartDateTime) | HashDiff | > 0 | Alert Owner | Data Engineering |

---

## 10. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
| Full refresh | `dbt run -m vw_DimCustomer DimCustomer` — view rebuilt from Fivetran history every run; enriched table `table` materialization (full rebuild) to preserve Type 2 correctness. | Scheduled (per dbt Cloud job) | `BRONZE_ERP__DBO.CUSTOMERS` loaded | Standard dbt error handling; failed tests skip downstream | dbt Cloud |

---

## 11. Version Control & Governance
| Version | Date | Author | Changes | Approved By |
|---------|------|--------|---------|-------------|
| 1.0 | 2026-04-16 | fillip | Initial generation from target data model and analyzer schema JSON. |  |
| 2.0 | 2026-04-21 | fillip | Restructured to follow Ajay Kalyan's Type 2 SCD pattern: Data Conditions, Final mapping, BR1–BR12, TX1–TX6. Switched to Fivetran history-mode-aware `_FIVETRAN_START`/`_FIVETRAN_END`/`_FIVETRAN_ACTIVE` for Type 2 metadata. Renamed EffectiveDate→EffectiveStartDateTime, ExpirationDate→EffectiveEndDateTime, IsCurrent→CurrentFlagYN. Dropped EtlBatchId; added SourceSystemCode, SourceCustomerPK/BK, FileName, StageInsertedDateTimeUTC, Hashbytes, DataCondition. |  |

---

## 12. Sign-Off
- **Business Owner Approval:** _____________________  
- **Data Engineering Lead Approval:** _____________________  
- **QA / Testing Approval:** _____________________  
