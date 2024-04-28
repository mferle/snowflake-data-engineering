-- refer to Chapter_10_Part1_role_based_access_control.sql

use role SYSADMIN;
use database BAKERY_DB;

-- create schema with managed access using the SYSADMIN role
create schema ORCHESTRATION with managed access;

-- using the SECURITYADMIN role (because this role has the MANAGE GRANTS privilege)
use role SECURITYADMIN;

-- grant full privileges the ORCHESTRATION schema to the BAKERY_FULL role
grant all on schema BAKERY_DB.ORCHESTRATION to role BAKERY_FULL;
