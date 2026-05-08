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
> Standard date dimension with fiscal calendar support. Used as a role-playing dimension across all fact tables.

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
| Snowflake | DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO | See field-level mapping |  |  | Immediate technical source is Snowflake bronze; original lineage comes from the analyzer source system. |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
| DRIP_DATA_INTELLIGENCE | GOLD | DimDate | Type 0 (static) | DateHashPK |  | Conformed Dimension | Standard date dimension with fiscal calendar support. Used as a role-playing dimension across all fact tables. |

---

## 5. Classification Tags
| Scope | Column | Tag FQN | Classification |
|-------|--------|---------|----------------|
| Table |  | Certification.Gold | Certification |
| Table |  | Architecture.Enriched | Architecture |
| Column | DateHashPK | Architecture.Enriched | Architecture |
| Column | DateHashPK | Certification.Gold | Certification |
| Column | DateHashPK | PII.None | PII |
| Column | DateHashPK | Privacy.Non-Personal | Privacy |
| Column | DateHashPK | Lifecycle.Active | Lifecycle |
| Column | DateHashPK | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DateHashPK | Retention.PermanentRecord | Retention |
| Column | DateHashPK | Criticality.Reference | Criticality |
| Column | DateValue | Architecture.Enriched | Architecture |
| Column | DateValue | Certification.Gold | Certification |
| Column | DateValue | PII.None | PII |
| Column | DateValue | Privacy.Non-Personal | Privacy |
| Column | DateValue | Lifecycle.Active | Lifecycle |
| Column | DateValue | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DateValue | Retention.PermanentRecord | Retention |
| Column | DateValue | Criticality.Reference | Criticality |
| Column | DateKey | Architecture.Enriched | Architecture |
| Column | DateKey | Certification.Gold | Certification |
| Column | DateKey | PII.None | PII |
| Column | DateKey | Privacy.Non-Personal | Privacy |
| Column | DateKey | Lifecycle.Active | Lifecycle |
| Column | DateKey | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DateKey | Retention.PermanentRecord | Retention |
| Column | DateKey | Criticality.Reference | Criticality |
| Column | DayOfWeek | Architecture.Enriched | Architecture |
| Column | DayOfWeek | Certification.Gold | Certification |
| Column | DayOfWeek | PII.None | PII |
| Column | DayOfWeek | Privacy.Non-Personal | Privacy |
| Column | DayOfWeek | Lifecycle.Active | Lifecycle |
| Column | DayOfWeek | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DayOfWeek | Retention.PermanentRecord | Retention |
| Column | DayOfWeek | Criticality.Reference | Criticality |
| Column | DayName | Architecture.Enriched | Architecture |
| Column | DayName | Certification.Gold | Certification |
| Column | DayName | PII.None | PII |
| Column | DayName | Privacy.Non-Personal | Privacy |
| Column | DayName | Lifecycle.Active | Lifecycle |
| Column | DayName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DayName | Retention.PermanentRecord | Retention |
| Column | DayName | Criticality.Reference | Criticality |
| Column | DayOfMonth | Architecture.Enriched | Architecture |
| Column | DayOfMonth | Certification.Gold | Certification |
| Column | DayOfMonth | PII.None | PII |
| Column | DayOfMonth | Privacy.Non-Personal | Privacy |
| Column | DayOfMonth | Lifecycle.Active | Lifecycle |
| Column | DayOfMonth | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DayOfMonth | Retention.PermanentRecord | Retention |
| Column | DayOfMonth | Criticality.Reference | Criticality |
| Column | DayOfYear | Architecture.Enriched | Architecture |
| Column | DayOfYear | Certification.Gold | Certification |
| Column | DayOfYear | PII.None | PII |
| Column | DayOfYear | Privacy.Non-Personal | Privacy |
| Column | DayOfYear | Lifecycle.Active | Lifecycle |
| Column | DayOfYear | QualityTrust.SystemOfRecord | QualityTrust |
| Column | DayOfYear | Retention.PermanentRecord | Retention |
| Column | DayOfYear | Criticality.Reference | Criticality |
| Column | WeekOfYear | Architecture.Enriched | Architecture |
| Column | WeekOfYear | Certification.Gold | Certification |
| Column | WeekOfYear | PII.None | PII |
| Column | WeekOfYear | Privacy.Non-Personal | Privacy |
| Column | WeekOfYear | Lifecycle.Active | Lifecycle |
| Column | WeekOfYear | QualityTrust.SystemOfRecord | QualityTrust |
| Column | WeekOfYear | Retention.PermanentRecord | Retention |
| Column | WeekOfYear | Criticality.Reference | Criticality |
| Column | MonthNumber | Architecture.Enriched | Architecture |
| Column | MonthNumber | Certification.Gold | Certification |
| Column | MonthNumber | PII.None | PII |
| Column | MonthNumber | Privacy.Non-Personal | Privacy |
| Column | MonthNumber | Lifecycle.Active | Lifecycle |
| Column | MonthNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | MonthNumber | Retention.PermanentRecord | Retention |
| Column | MonthNumber | Criticality.Reference | Criticality |
| Column | MonthName | Architecture.Enriched | Architecture |
| Column | MonthName | Certification.Gold | Certification |
| Column | MonthName | PII.None | PII |
| Column | MonthName | Privacy.Non-Personal | Privacy |
| Column | MonthName | Lifecycle.Active | Lifecycle |
| Column | MonthName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | MonthName | Retention.PermanentRecord | Retention |
| Column | MonthName | Criticality.Reference | Criticality |
| Column | MonthAbbrev | Architecture.Enriched | Architecture |
| Column | MonthAbbrev | Certification.Gold | Certification |
| Column | MonthAbbrev | PII.None | PII |
| Column | MonthAbbrev | Privacy.Non-Personal | Privacy |
| Column | MonthAbbrev | Lifecycle.Active | Lifecycle |
| Column | MonthAbbrev | QualityTrust.SystemOfRecord | QualityTrust |
| Column | MonthAbbrev | Retention.PermanentRecord | Retention |
| Column | MonthAbbrev | Criticality.Reference | Criticality |
| Column | QuarterNumber | Architecture.Enriched | Architecture |
| Column | QuarterNumber | Certification.Gold | Certification |
| Column | QuarterNumber | PII.None | PII |
| Column | QuarterNumber | Privacy.Non-Personal | Privacy |
| Column | QuarterNumber | Lifecycle.Active | Lifecycle |
| Column | QuarterNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | QuarterNumber | Retention.PermanentRecord | Retention |
| Column | QuarterNumber | Criticality.Reference | Criticality |
| Column | QuarterName | Architecture.Enriched | Architecture |
| Column | QuarterName | Certification.Gold | Certification |
| Column | QuarterName | PII.None | PII |
| Column | QuarterName | Privacy.Non-Personal | Privacy |
| Column | QuarterName | Lifecycle.Active | Lifecycle |
| Column | QuarterName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | QuarterName | Retention.PermanentRecord | Retention |
| Column | QuarterName | Criticality.Reference | Criticality |
| Column | YearNumber | Architecture.Enriched | Architecture |
| Column | YearNumber | Certification.Gold | Certification |
| Column | YearNumber | PII.None | PII |
| Column | YearNumber | Privacy.Non-Personal | Privacy |
| Column | YearNumber | Lifecycle.Active | Lifecycle |
| Column | YearNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | YearNumber | Retention.PermanentRecord | Retention |
| Column | YearNumber | Criticality.Reference | Criticality |
| Column | YearMonth | Architecture.Enriched | Architecture |
| Column | YearMonth | Certification.Gold | Certification |
| Column | YearMonth | PII.None | PII |
| Column | YearMonth | Privacy.Non-Personal | Privacy |
| Column | YearMonth | Lifecycle.Active | Lifecycle |
| Column | YearMonth | QualityTrust.SystemOfRecord | QualityTrust |
| Column | YearMonth | Retention.PermanentRecord | Retention |
| Column | YearMonth | Criticality.Reference | Criticality |
| Column | YearQuarter | Architecture.Enriched | Architecture |
| Column | YearQuarter | Certification.Gold | Certification |
| Column | YearQuarter | PII.None | PII |
| Column | YearQuarter | Privacy.Non-Personal | Privacy |
| Column | YearQuarter | Lifecycle.Active | Lifecycle |
| Column | YearQuarter | QualityTrust.SystemOfRecord | QualityTrust |
| Column | YearQuarter | Retention.PermanentRecord | Retention |
| Column | YearQuarter | Criticality.Reference | Criticality |
| Column | FiscalMonthNumber | Architecture.Enriched | Architecture |
| Column | FiscalMonthNumber | Certification.Gold | Certification |
| Column | FiscalMonthNumber | PII.None | PII |
| Column | FiscalMonthNumber | Privacy.Non-Personal | Privacy |
| Column | FiscalMonthNumber | Lifecycle.Active | Lifecycle |
| Column | FiscalMonthNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | FiscalMonthNumber | Retention.PermanentRecord | Retention |
| Column | FiscalMonthNumber | Criticality.Reference | Criticality |
| Column | FiscalQuarterNumber | Architecture.Enriched | Architecture |
| Column | FiscalQuarterNumber | Certification.Gold | Certification |
| Column | FiscalQuarterNumber | PII.None | PII |
| Column | FiscalQuarterNumber | Privacy.Non-Personal | Privacy |
| Column | FiscalQuarterNumber | Lifecycle.Active | Lifecycle |
| Column | FiscalQuarterNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | FiscalQuarterNumber | Retention.PermanentRecord | Retention |
| Column | FiscalQuarterNumber | Criticality.Reference | Criticality |
| Column | FiscalYearNumber | Architecture.Enriched | Architecture |
| Column | FiscalYearNumber | Certification.Gold | Certification |
| Column | FiscalYearNumber | PII.None | PII |
| Column | FiscalYearNumber | Privacy.Non-Personal | Privacy |
| Column | FiscalYearNumber | Lifecycle.Active | Lifecycle |
| Column | FiscalYearNumber | QualityTrust.SystemOfRecord | QualityTrust |
| Column | FiscalYearNumber | Retention.PermanentRecord | Retention |
| Column | FiscalYearNumber | Criticality.Reference | Criticality |
| Column | IsWeekend | Architecture.Enriched | Architecture |
| Column | IsWeekend | Certification.Gold | Certification |
| Column | IsWeekend | PII.None | PII |
| Column | IsWeekend | Privacy.Non-Personal | Privacy |
| Column | IsWeekend | Lifecycle.Active | Lifecycle |
| Column | IsWeekend | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsWeekend | Retention.PermanentRecord | Retention |
| Column | IsWeekend | Criticality.Reference | Criticality |
| Column | IsHoliday | Architecture.Enriched | Architecture |
| Column | IsHoliday | Certification.Gold | Certification |
| Column | IsHoliday | PII.None | PII |
| Column | IsHoliday | Privacy.Non-Personal | Privacy |
| Column | IsHoliday | Lifecycle.Active | Lifecycle |
| Column | IsHoliday | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsHoliday | Retention.PermanentRecord | Retention |
| Column | IsHoliday | Criticality.Reference | Criticality |
| Column | HolidayName | Architecture.Enriched | Architecture |
| Column | HolidayName | Certification.Gold | Certification |
| Column | HolidayName | PII.None | PII |
| Column | HolidayName | Privacy.Non-Personal | Privacy |
| Column | HolidayName | Lifecycle.Active | Lifecycle |
| Column | HolidayName | QualityTrust.SystemOfRecord | QualityTrust |
| Column | HolidayName | Retention.PermanentRecord | Retention |
| Column | HolidayName | Criticality.Reference | Criticality |
| Column | IsLastDayOfMonth | Architecture.Enriched | Architecture |
| Column | IsLastDayOfMonth | Certification.Gold | Certification |
| Column | IsLastDayOfMonth | PII.None | PII |
| Column | IsLastDayOfMonth | Privacy.Non-Personal | Privacy |
| Column | IsLastDayOfMonth | Lifecycle.Active | Lifecycle |
| Column | IsLastDayOfMonth | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsLastDayOfMonth | Retention.PermanentRecord | Retention |
| Column | IsLastDayOfMonth | Criticality.Reference | Criticality |
| Column | IsLastDayOfQuarter | Architecture.Enriched | Architecture |
| Column | IsLastDayOfQuarter | Certification.Gold | Certification |
| Column | IsLastDayOfQuarter | PII.None | PII |
| Column | IsLastDayOfQuarter | Privacy.Non-Personal | Privacy |
| Column | IsLastDayOfQuarter | Lifecycle.Active | Lifecycle |
| Column | IsLastDayOfQuarter | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsLastDayOfQuarter | Retention.PermanentRecord | Retention |
| Column | IsLastDayOfQuarter | Criticality.Reference | Criticality |
| Column | IsLastDayOfYear | Architecture.Enriched | Architecture |
| Column | IsLastDayOfYear | Certification.Gold | Certification |
| Column | IsLastDayOfYear | PII.None | PII |
| Column | IsLastDayOfYear | Privacy.Non-Personal | Privacy |
| Column | IsLastDayOfYear | Lifecycle.Active | Lifecycle |
| Column | IsLastDayOfYear | QualityTrust.SystemOfRecord | QualityTrust |
| Column | IsLastDayOfYear | Retention.PermanentRecord | Retention |
| Column | IsLastDayOfYear | Criticality.Reference | Criticality |

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
| DimDate | DateHashPK | NUMBER(19,0) | Primary Key | Snowflake |  |  | HASH(COALESCE(CAST({SOURCE_COL} AS VARCHAR), '#@#@#@#@#')) | NO |  | Surrogate primary key for date dimension |
| DimDate | DateValue | DATE | Attribute | Snowflake |  |  |  | NO |  | Actual calendar date value |
| DimDate | DateKey | INT | Attribute | Snowflake |  |  |  | NO |  | Integer date key in YYYYMMDD format for partitioning |
| DimDate | DayOfWeek | INT | Attribute | Snowflake |  |  |  | NO |  | Day of week number (1=Sunday, 7=Saturday) |
| DimDate | DayName | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Full name of day (Sunday, Monday, etc.) |
| DimDate | DayOfMonth | INT | Attribute | Snowflake |  |  |  | NO |  | Day number within the month (1-31) |
| DimDate | DayOfYear | INT | Attribute | Snowflake |  |  |  | NO |  | Day number within the year (1-366) |
| DimDate | WeekOfYear | INT | Attribute | Snowflake |  |  |  | NO |  | ISO week number within the year (1-53) |
| DimDate | MonthNumber | INT | Attribute | Snowflake |  |  |  | NO |  | Month number (1-12) |
| DimDate | MonthName | VARCHAR(10) | Attribute | Snowflake |  |  |  | NO |  | Full name of month (January, February, etc.) |
| DimDate | MonthAbbrev | VARCHAR(3) | Attribute | Snowflake |  |  |  | NO |  | Three-letter month abbreviation (Jan, Feb, etc.) |
| DimDate | QuarterNumber | INT | Attribute | Snowflake |  |  |  | NO |  | Calendar quarter number (1-4) |
| DimDate | QuarterName | VARCHAR(2) | Attribute | Snowflake |  |  |  | NO |  | Quarter label (Q1, Q2, Q3, Q4) |
| DimDate | YearNumber | INT | Attribute | Snowflake |  |  |  | NO |  | Four-digit calendar year |
| DimDate | YearMonth | VARCHAR(7) | Attribute | Snowflake |  |  |  | NO |  | Year and month in YYYY-MM format |
| DimDate | YearQuarter | VARCHAR(7) | Attribute | Snowflake |  |  |  | NO |  | Year and quarter in YYYY-Q# format |
| DimDate | FiscalMonthNumber | INT | Attribute | Snowflake |  |  |  | NO |  | Fiscal month number (1-12) |
| DimDate | FiscalQuarterNumber | INT | Attribute | Snowflake |  |  |  | NO |  | Fiscal quarter number (1-4) |
| DimDate | FiscalYearNumber | INT | Attribute | Snowflake |  |  |  | NO |  | Fiscal year number |
| DimDate | IsWeekend | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if Saturday or Sunday |
| DimDate | IsHoliday | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if a recognized holiday |
| DimDate | HolidayName | VARCHAR(50) | Attribute | Snowflake |  |  |  | YES |  | Name of holiday if applicable |
| DimDate | IsLastDayOfMonth | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if last day of calendar month |
| DimDate | IsLastDayOfQuarter | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if last day of calendar quarter |
| DimDate | IsLastDayOfYear | BOOLEAN | Attribute | Snowflake |  |  |  | NO |  | True if last day of calendar year |

---

## 8. Load Strategy
| Load Type | Method | Frequency | Dependencies | Error Handling / Recovery | Orchestration Tool |
|-----------|--------|-----------|--------------|---------------------------|--------------------|
|  |  |  |  |  |  |

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
