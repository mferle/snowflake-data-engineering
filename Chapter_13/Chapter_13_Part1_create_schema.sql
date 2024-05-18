-- refer to Chapter_10_Part1_role_based_access_control.sql

use role SYSADMIN;
use database BAKERY_DB;

-- create schema with managed access using the SYSADMIN role
create schema ORCHESTRATION with managed access;

-- grant full privileges on the ORCHESTRATION schema to the BAKERY_FULL role using the SECURITYADMIN role
use role SECURITYADMIN;
grant all on schema BAKERY_DB.ORCHESTRATION to role BAKERY_FULL;
