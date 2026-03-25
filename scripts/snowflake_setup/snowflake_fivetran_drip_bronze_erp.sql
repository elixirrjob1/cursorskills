-- Fivetran Snowflake user/role + bronze schema (TEMPLATE — do not paste secrets here)
-- Placeholders {{VAR}} are filled from repo .env by:
--   python scripts/snowflake_setup/render_snowflake_fivetran_drip_sql.py
--   python scripts/snowflake_setup/run_snowflake_sql_pat.py --render-from-env --sql-file scripts/snowflake_setup/snowflake_fivetran_drip_bronze_erp.sql
-- Docs: https://fivetran.com/docs/destinations/snowflake/setup-guide
--
-- Env (see .env.example): SNOWFLAKE_FIVETRAN_PASSWORD (required to render), plus optional
-- SNOWFLAKE_FIVETRAN_ROLE, SNOWFLAKE_FIVETRAN_USER, SNOWFLAKE_FIVETRAN_WAREHOUSE,
-- SNOWFLAKE_DRIP_DATABASE, SNOWFLAKE_BRONZE_SCHEMA.
--
-- After this script:
-- 1) Fivetran destination: database / user / role / warehouse match these values.
-- 2) Per connector: destination schema = bronze_erp (or your SNOWFLAKE_BRONZE_SCHEMA).

begin;

set role_name = '{{SNOWFLAKE_FIVETRAN_ROLE}}';
set user_name = '{{SNOWFLAKE_FIVETRAN_USER}}';
set user_password = '{{SNOWFLAKE_FIVETRAN_PASSWORD}}';

set warehouse_name = '{{SNOWFLAKE_FIVETRAN_WAREHOUSE}}';

use role securityadmin;

create role if not exists identifier($role_name);
grant role identifier($role_name) to role SYSADMIN;

create user if not exists identifier($user_name)
  password = $user_password
  default_role = $role_name
  default_warehouse = $warehouse_name;

grant role identifier($role_name) to user identifier($user_name);

alter user identifier($user_name) set BINARY_INPUT_FORMAT = 'BASE64';
alter user identifier($user_name) set TIMESTAMP_INPUT_FORMAT = 'AUTO';

use role sysadmin;

create warehouse if not exists identifier($warehouse_name)
  warehouse_size = xsmall
  warehouse_type = standard
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;

grant usage on warehouse identifier($warehouse_name) to role identifier($role_name);

grant usage, monitor, create schema on database {{SNOWFLAKE_DRIP_DATABASE}} to role identifier($role_name);

create schema if not exists {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}";
grant usage on schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}" to role identifier($role_name);
grant create table, create stage, create file format, create pipe, create stream, create task
  on schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}" to role identifier($role_name);

use role securityadmin;

grant select, insert, update, delete, truncate on future tables in schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}"
  to role identifier($role_name);
grant select, insert, update, delete, truncate on all tables in schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}"
  to role identifier($role_name);

grant read on future stages in schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}" to role identifier($role_name);
grant write on future stages in schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}" to role identifier($role_name);
grant read on all stages in schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}" to role identifier($role_name);
grant write on all stages in schema {{SNOWFLAKE_DRIP_DATABASE}}."{{SNOWFLAKE_BRONZE_SCHEMA}}" to role identifier($role_name);

use role ACCOUNTADMIN;
grant create integration on account to role identifier($role_name);

commit;
