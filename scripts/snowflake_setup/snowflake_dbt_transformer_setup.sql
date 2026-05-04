-- dbt Transformer Snowflake user/role + warehouse setup (TEMPLATE — do not paste secrets here)
-- Placeholders (e.g. SNOWFLAKE_DBT_USER) are filled from repo .env by:
--   python scripts/snowflake_setup/run_snowflake_sql_pat.py --render-from-env --sql-file scripts/snowflake_setup/snowflake_dbt_transformer_setup.sql
--
-- Purpose: separate service account dedicated to running dbt transformations.
--          Fivetran account (FIVETRAN_DRIP_USER) handles ingestion only.
--          This account handles transformation only.
--
-- Env vars required: SNOWFLAKE_DBT_ROLE, SNOWFLAKE_DBT_USER, SNOWFLAKE_DBT_PASSWORD,
--                    SNOWFLAKE_DBT_WAREHOUSE, SNOWFLAKE_DRIP_DATABASE (falls back to SNOWFLAKE_DATABASE)

begin;

set dbt_role     = '{{SNOWFLAKE_DBT_ROLE}}';
set dbt_user     = '{{SNOWFLAKE_DBT_USER}}';
set dbt_password = '{{SNOWFLAKE_DBT_PASSWORD}}';
set dbt_wh       = '{{SNOWFLAKE_DBT_WAREHOUSE}}';
set db_name      = '{{SNOWFLAKE_DATABASE}}';

-- ── Role ──────────────────────────────────────────────────────────────────────
use role securityadmin;

create role if not exists identifier($dbt_role);
grant role identifier($dbt_role) to role SYSADMIN;

-- ── User ──────────────────────────────────────────────────────────────────────
create user if not exists identifier($dbt_user)
  password         = $dbt_password
  default_role     = $dbt_role
  default_warehouse = $dbt_wh
  comment          = 'dbt transformation service account';

grant role identifier($dbt_role) to user identifier($dbt_user);

-- ── Warehouse ────────────────────────────────────────────────────────────────
use role sysadmin;

create warehouse if not exists identifier($dbt_wh)
  warehouse_size  = xsmall
  warehouse_type  = standard
  auto_suspend    = 60
  auto_resume     = true
  initially_suspended = true
  comment         = 'dedicated warehouse for dbt transformation runs';

grant usage on warehouse identifier($dbt_wh) to role identifier($dbt_role);

-- ── Database access ───────────────────────────────────────────────────────────
-- dbt needs to create schemas and objects across the whole database
grant usage on database identifier($db_name)               to role identifier($dbt_role);
grant create schema    on database identifier($db_name)    to role identifier($dbt_role);

-- Read access to all existing + future schemas (covers bronze / staging layer)
grant usage on all schemas in database identifier($db_name)           to role identifier($dbt_role);
grant usage on future schemas in database identifier($db_name)        to role identifier($dbt_role);
grant select on all tables in database identifier($db_name)           to role identifier($dbt_role);
grant select on future tables in database identifier($db_name)        to role identifier($dbt_role);
grant select on all views in database identifier($db_name)            to role identifier($dbt_role);
grant select on future views in database identifier($db_name)         to role identifier($dbt_role);

-- Write access — dbt creates tables/views in its output schemas
grant create table, create view, create stage, create file format, create sequence
  on all schemas in database identifier($db_name)          to role identifier($dbt_role);
grant create table, create view, create stage, create file format, create sequence
  on future schemas in database identifier($db_name)       to role identifier($dbt_role);

grant insert, update, delete, truncate
  on all tables in database identifier($db_name)           to role identifier($dbt_role);
grant insert, update, delete, truncate
  on future tables in database identifier($db_name)        to role identifier($dbt_role);

commit;
