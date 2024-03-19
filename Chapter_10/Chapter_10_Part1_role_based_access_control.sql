use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;

-- create schemas with managed access
create schema EXT with managed access;
create schema STG with managed access;
create schema DWH with managed access;
create schema MGMT with managed access;

-- using the USERADMIN role (because this role has the CREATE ROLE privilege)
use role USERADMIN;

-- create the access roles for full access and for read-only access
create role BAKERY_FULL;
create role BAKERY_READ;

-- create the functional roles
create role DATA_ENGINEER;
create role DATA_ANALYST;

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

-- grant read-only privileges on database BAKERY_DB to the BAKERY_READ role
grant usage on database BAKERY_DB to role BAKERY_READ;
grant usage on all schemas in database BAKERY_DB to role BAKERY_READ;
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

-- grant the functional roles to the users who perform those business functions
-- in this exercise we grant both functional roles to our current user to be able to test them

set my_current_user = current_user();
grant role DATA_ENGINEER to user IDENTIFIER($my_current_user);
grant role DATA_ANALYST to user IDENTIFIER($my_current_user);

-- grant usage on the BAKERY_WH warehouse to the functional roles
grant usage on warehouse BAKERY_WH to role DATA_ENGINEER;
grant usage on warehouse BAKERY_WH to role DATA_ANALYST;

-- to test, create a table in the MGMT schema using the DATA_ENGINEER role
-- since the DATA_ENGINEER role has full access, it should be allowed to create the table
-- the table will be owned by SYSADMIN because it is in a managed access schema owned by SYSADMIN
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema MGMT;
create table TEST_TABLE(id integer);
insert into TEST_TABLE values(1);

-- then switch to the DATA_ANALYST role and select from the test table
-- it should return data because the role was granted select privileges on future tables in the MGMT schema
use role DATA_ANALYST;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema MGMT;
select * from TEST_TABLE;

-- attempt to drop the test table still using the DATA_ANALYST role
drop table TEST_TABLE; -- does not succeed because the data analyst has only select privileges

-- switch to the DATA_ENGINEER role
use role DATA_ENGINEER;
drop table TEST_TABLE; -- succeeds because the data engineer has full privileges

