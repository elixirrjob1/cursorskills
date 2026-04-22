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
