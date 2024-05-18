use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;

-- create schemas with managed access
create schema RAW with managed access;
create schema RPT with managed access;

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
grant all on schema BAKERY_DB.RAW to role BAKERY_FULL;
grant all on schema BAKERY_DB.RPT to role BAKERY_FULL;

-- grant read-only privileges on database BAKERY_DB to the BAKERY_READ role
grant usage on database BAKERY_DB to role BAKERY_READ;
grant usage on all schemas in database BAKERY_DB to role BAKERY_READ;
grant select on all tables in schema BAKERY_DB.RPT to role BAKERY_READ;
grant select on all views in schema BAKERY_DB.RPT to role BAKERY_READ;

-- grant future privileges
grant select on future tables in schema BAKERY_DB.RPT to role BAKERY_READ;
grant select on future views in schema BAKERY_DB.RPT to role BAKERY_READ;

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

-- to test, use the DATA_ENGINEER role to create a table in the RAW schema and insert some sample values
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema RAW;

create table EMPLOYEE (
  id integer,
  name varchar,
  home_address varchar,
  department varchar,
  hire_date date
);

insert into EMPLOYEE values
(1001, 'William Jones', '5170 Arcu St.', 'Bread', '2020-02-01'),
(1002, 'Alexander North', '261 Ipsum Rd.', 'Pastry', '2021-04-01'),
(1003, 'Jennifer Navarro', '880 Dictum Ave.', 'Pastry', '2019-08-01'),
(1004, 'Sandra Perkins', '55 Velo St.', 'Bread', '2022-05-01');

-- use the DATA_ANALYST role to select from the table in the RAW schema
use role DATA_ANALYST;
select * from RAW.EMPLOYEE;
-- should not succeed because the DATA_ANALYST has no privileges in the RAW schema

-- switch to the DATA_ENGINEER role and create a view in the RPT schema
use role DATA_ENGINEER;
create view RPT.EMPLOYEE as 
select id, name, home_address, department, hire_date
from RAW.EMPLOYEE;

-- switch to the DATA_ANALYST role and select from the view in the RPT schema
use role DATA_ANALYST;
select * from RPT.EMPLOYEE;
-- should return values