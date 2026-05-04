-- =============================================================================
-- PII Dynamic Masking Setup — DRIP_DATA_INTELLIGENCE
-- =============================================================================
-- Source of truth: OpenMetadata PII.Sensitive tags (queried 2026-05-04)
--
-- What this script does:
--   1. Creates a GOVERNANCE schema to house masking policies
--   2. Creates the PII_VIEWER role — members see plain-text PII
--   3. Grants PII_VIEWER to SYSADMIN and FIVETRAN_DRIP_USER
--   4. Creates a single VARCHAR masking policy (all PII cols are VARCHAR)
--   5. Applies the policy to all 21 PII.Sensitive-tagged columns in:
--        - BRONZE_ERP__DBO  (source tables: CUSTOMERS, EMPLOYEES, STORES, SUPPLIERS)
--        - DBT_PROD_ENRICHED (gold dims: DimCustomer, DimEmployee, DimStore, DimSupplier)
--
-- Required Snowflake privileges:
--   Run sections 1-3 as SECURITYADMIN or ACCOUNTADMIN
--   Run sections 4-6 as SYSADMIN (or the schema owner)
--
-- NOTE on column name case in DBT_PROD_ENRICHED:
--   dbt view aliases use quoted mixed-case (e.g. "EmailAddress"), so enriched
--   table columns are case-sensitive and must be referenced with double-quotes.
-- =============================================================================

USE DATABASE DRIP_DATA_INTELLIGENCE;
USE WAREHOUSE FIVETRAN_DRIP_WH;

-- =============================================================================
-- 1. Governance schema
-- =============================================================================
USE ROLE SYSADMIN;

CREATE SCHEMA IF NOT EXISTS DRIP_DATA_INTELLIGENCE.GOVERNANCE
  COMMENT = 'Masking policies and governance objects for the Drip platform';

-- PUBLIC needs USAGE on the schema so the policy can be evaluated at query time
GRANT USAGE ON SCHEMA DRIP_DATA_INTELLIGENCE.GOVERNANCE TO ROLE PUBLIC;

-- =============================================================================
-- 2. PII_VIEWER role
-- =============================================================================
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS PII_VIEWER
  COMMENT = 'Members of this role see PII columns in plain text. All other users see *** PII MASKED ***.';

-- Admins automatically inherit PII_VIEWER
GRANT ROLE PII_VIEWER TO ROLE SYSADMIN;

-- Fivetran ingestion account must see unmasked data to write bronze correctly
GRANT ROLE PII_VIEWER TO USER FIVETRAN_DRIP_USER;

-- Uncomment when the dbt transformation service account is created (PA-95):
-- GRANT ROLE PII_VIEWER TO USER DBT_DRIP_USER;

-- =============================================================================
-- 3. VARCHAR masking policy
-- =============================================================================
USE ROLE SYSADMIN;

CREATE OR REPLACE MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR
  AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
      WHEN IS_ROLE_IN_SESSION('PII_VIEWER') THEN val
      ELSE '*** PII MASKED ***'
    END
  COMMENT = 'Applied to all PII.Sensitive VARCHAR columns. Bypass requires PII_VIEWER role.';

-- =============================================================================
-- 4. Apply masking — BRONZE_ERP__DBO  (Fivetran source tables)
-- =============================================================================

-- CUSTOMERS (4 PII columns)
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS
  MODIFY COLUMN FIRST_NAME SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS
  MODIFY COLUMN LAST_NAME  SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS
  MODIFY COLUMN EMAIL      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.CUSTOMERS
  MODIFY COLUMN PHONE      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- EMPLOYEES (2 PII columns — LAST_NAME and FULLNAME not tagged in OpenMetadata)
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.EMPLOYEES
  MODIFY COLUMN FIRST_NAME SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.EMPLOYEES
  MODIFY COLUMN EMAIL      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- STORES (2 PII columns)
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.STORES
  MODIFY COLUMN ADDRESS    SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.STORES
  MODIFY COLUMN PHONE      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- SUPPLIERS (2 PII columns)
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.SUPPLIERS
  MODIFY COLUMN PHONE      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.BRONZE_ERP__DBO.SUPPLIERS
  MODIFY COLUMN EMAIL      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- =============================================================================
-- 5. Apply masking — DBT_PROD_ENRICHED  (gold dimension tables)
--    Column names are quoted (case-sensitive) because dbt view aliases use
--    double-quoted identifiers (e.g. EMAIL AS "EmailAddress").
-- =============================================================================

-- DimCustomer (6 PII columns)
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimCustomer"
  MODIFY COLUMN "EmailAddress"  SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimCustomer"
  MODIFY COLUMN "FirstName"     SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimCustomer"
  MODIFY COLUMN "FullName"      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimCustomer"
  MODIFY COLUMN "LastName"      SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimCustomer"
  MODIFY COLUMN "PhoneNumber"   SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimCustomer"
  MODIFY COLUMN "StreetAddress" SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- DimEmployee (2 PII columns)
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimEmployee"
  MODIFY COLUMN "FirstName"    SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimEmployee"
  MODIFY COLUMN "EmailAddress" SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- DimStore (1 PII column)
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimStore"
  MODIFY COLUMN "StreetAddress" SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- DimSupplier (2 PII columns)
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimSupplier"
  MODIFY COLUMN "ContactEmail" SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;
ALTER TABLE DRIP_DATA_INTELLIGENCE.DBT_PROD_ENRICHED."DimSupplier"
  MODIFY COLUMN "ContactPhone" SET MASKING POLICY DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR;

-- =============================================================================
-- 6. Verify — list all columns with the masking policy applied
-- =============================================================================
SELECT
    object_database,
    object_schema,
    object_name,
    column_name,
    masking_policy_name,
    masking_policy_database,
    masking_policy_schema
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'DRIP_DATA_INTELLIGENCE.GOVERNANCE.MASK_PII_VARCHAR'
))
ORDER BY object_schema, object_name, column_name;
