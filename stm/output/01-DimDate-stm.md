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
> Standard calendar dimension supporting time-based analysis across all fact tables. Used as role-playing dimension for various date contexts (time entry date, expense date, snapshot date, project start/end dates).

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
|  |  |  |  |  |  |

---

## 4. Target Schema Definition
| Target Database | Schema | Table Name | SCD Type | Grain / Primary Key | Distribution | Table Type | Notes |
|-----------------|--------|------------|----------|----------------------|-------------|------------|-------|
|  |  | DimDate |  | DateHashPK |  | Dimension (Conformed, Role-Playing) | Standard calendar dimension supporting time-based analysis across all fact tables. Used as role-playing dimension for various date contexts (time entry date, expense date, snapshot date, project start/end dates). |

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
| DimDate | DateHashPK | INT | Primary Key |  |  |  |  | NO |  | Surrogate primary key for date dimension |
| DimDate | DateValue | DATE | Attribute |  |  |  |  | NO |  | Actual calendar date value |
| DimDate | DateKey | INT | Attribute |  |  |  |  | NO |  | Natural key in YYYYMMDD format |
| DimDate | DayOfWeek | INT | Attribute |  |  |  |  | NO |  | Day of week number (1=Sunday, 7=Saturday) |
| DimDate | DayName | VARCHAR(10) | Attribute |  |  |  |  | NO |  | Full name of day (Monday, Tuesday, etc.) |
| DimDate | DayOfMonth | INT | Attribute |  |  |  |  | NO |  | Day number within the month (1-31) |
| DimDate | DayOfYear | INT | Attribute |  |  |  |  | NO |  | Day number within the year (1-366) |
| DimDate | WeekOfYear | INT | Attribute |  |  |  |  | NO |  | ISO week number (1-53) |
| DimDate | MonthNumber | INT | Attribute |  |  |  |  | NO |  | Month number (1-12) |
| DimDate | MonthName | VARCHAR(10) | Attribute |  |  |  |  | NO |  | Full name of month (January, February, etc.) |
| DimDate | MonthShortName | VARCHAR(3) | Attribute |  |  |  |  | NO |  | Abbreviated month name (Jan, Feb, etc.) |
| DimDate | QuarterNumber | INT | Attribute |  |  |  |  | NO |  | Quarter number (1-4) |
| DimDate | QuarterName | VARCHAR(2) | Attribute |  |  |  |  | NO |  | Quarter label (Q1, Q2, Q3, Q4) |
| DimDate | CalendarYear | INT | Attribute |  |  |  |  | NO |  | Four-digit calendar year |
| DimDate | FiscalMonth | INT | Attribute |  |  |  |  | NO |  | Fiscal month number based on fiscal calendar |
| DimDate | FiscalQuarter | INT | Attribute |  |  |  |  | NO |  | Fiscal quarter number |
| DimDate | FiscalYear | INT | Attribute |  |  |  |  | NO |  | Fiscal year number |
| DimDate | IsWeekend | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating Saturday or Sunday |
| DimDate | IsHoliday | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating company holiday |
| DimDate | IsWorkingDay | BOOLEAN | Attribute |  |  |  |  | NO |  | Flag indicating regular working day |

---

## 8. Transformation & Business Rules
| Rule ID | Name | Description | Example / Formula | Notes |
|---------|------|-------------|-------------------|-------|
| BR1 | Business Rule | Date range typically covers 5 years historical + 2 years future |  |  |

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
