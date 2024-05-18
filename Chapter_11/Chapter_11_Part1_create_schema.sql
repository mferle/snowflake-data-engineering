-- refer to Chapter_10_Part1_role_based_access_control.sql

use role SYSADMIN;
use database BAKERY_DB;

-- create schema with managed access using the SYSADMIN role
create schema EXT with managed access;
create schema STG with managed access;
create schema DWH with managed access;
create schema MGMT with managed access;

-- using the SECURITYADMIN role (because this role has the MANAGE GRANTS privilege)
use role SECURITYADMIN;

-- grant full privileges on all schemas to the BAKERY_FULL role
grant all on schema BAKERY_DB.EXT to role BAKERY_FULL;
grant all on schema BAKERY_DB.STG to role BAKERY_FULL;
grant all on schema BAKERY_DB.DWH to role BAKERY_FULL;
grant all on schema BAKERY_DB.MGMT to role BAKERY_FULL;

-- grant read-only privileges on the MGMT schema to the BAKERY_READ role
grant select on all tables in schema BAKERY_DB.MGMT to role BAKERY_READ;
grant select on all views in schema BAKERY_DB.MGMT to role BAKERY_READ;

-- grant future privileges
grant select on future tables in schema BAKERY_DB.MGMT to role BAKERY_READ;
grant select on future views in schema BAKERY_DB.MGMT to role BAKERY_READ;