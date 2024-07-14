use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;

-- create schema with managed access using the SYSADMIN role
create or replace schema EXT with managed access;
create or replace schema STG with managed access;
create or replace schema DWH with managed access;
create or replace schema MGMT with managed access;
create or replace schema ORCHESTRATION with managed access;

-- using the USERADMIN role (because this role has the create or replace role privilege)
use role USERADMIN;

-- create the access roles for full access and for read-only access
create or replace role BAKERY_FULL;

-- to make the script repeatable:
-- must use the SECURITYADMIN role (because this role has the MANAGE GRANTS privilege) 
-- to drop the BAKERY_READ role (because this role has future grants)
use role SECURITYADMIN;
drop role if exists BAKERY_READ;

-- go back to the USERADMIN role
use role USERADMIN;
create or replace role BAKERY_READ;

-- create the functional roles
create or replace role DATA_ENGINEER;
create or replace role DATA_ANALYST;

-- using the SECURITYADMIN role (because this role has the MANAGE GRANTS privilege)
use role SECURITYADMIN;

-- grant privileges to each of the access roles

-- grant full privileges on database BAKERY_DB to the BAKERY_FULL role
grant usage on database BAKERY_DB to role BAKERY_FULL;
grant usage on all schemas in database BAKERY_DB to role BAKERY_FULL;
grant all on schema BAKERY_DB.EXT to role BAKERY_FULL;
grant all on schema BAKERY_DB.STG to role BAKERY_FULL;
grant all on schema BAKERY_DB.DWH to role BAKERY_FULL;
grant all on schema BAKERY_DB.MGMT to role BAKERY_FULL;
grant all on schema BAKERY_DB.ORCHESTRATION to role BAKERY_FULL;

-- grant read-only privileges on database BAKERY_DB to the BAKERY_READ role
grant usage on database BAKERY_DB to role BAKERY_READ;
grant usage on all schemas in database BAKERY_DB to role BAKERY_READ;
-- grant read-only privileges on the MGMT schema to the BAKERY_READ role
grant select on all tables in schema BAKERY_DB.MGMT to role BAKERY_READ;
grant select on all views in schema BAKERY_DB.MGMT to role BAKERY_READ;

-- grant future privileges
grant select on future tables in schema BAKERY_DB.MGMT to role BAKERY_READ;
grant select on future views in schema BAKERY_DB.MGMT to role BAKERY_READ;

-- grant access roles to functional roles
-- grant the BAKERY_FULL role to the DATA_ENGINEER role
grant role BAKERY_FULL to role DATA_ENGINEER;
-- grant the BAKERY_READ role to the DATA_ANALYST role
grant role BAKERY_READ to role DATA_ANALYST;

-- grant both functional roles to the SYSADMIN role
grant role DATA_ENGINEER to role SYSADMIN;
grant role DATA_ANALYST to role SYSADMIN;

-- grant usage on the BAKERY_WH warehouse to the functional roles
grant usage on warehouse BAKERY_WH to role DATA_ENGINEER;
grant usage on warehouse BAKERY_WH to role DATA_ANALYST;

-- grant privileges that allow the DATA_ENGINEER role to use the GIT integration
use role SYSADMIN;
grant usage on database ADMIN_DB to role DATA_ENGINEER;
grant usage on schema ADMIN_DB.GIT_INTEGRATION to role DATA_ENGINEER;
grant read on git repository ADMIN_DB.GIT_INTEGRATION.SF_DE to role DATA_ENGINEER;
grant write on git repository ADMIN_DB.GIT_INTEGRATION.SF_DE to role DATA_ENGINEER;

-- grant the DATA_ENGINEER role the privilege to execute tasks
use role ACCOUNTADMIN;
grant execute task on account to role DATA_ENGINEER;

-- create a notification integration for sending emails
create or replace notification integration PIPELINE_EMAIL_INT
  type = email
  enabled = true;

-- grant usage on the integration to the DATA_ENGINEER role
grant usage on integration PIPELINE_EMAIL_INT to role DATA_ENGINEER;