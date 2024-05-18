use role SYSADMIN;
use database BAKERY_DB;

-- create schema with managed access using the SYSADMIN role
create schema DG with managed access;
grant all on schema DG to role BAKERY_FULL;

use role USERADMIN;
-- create the functional roles
create role DATA_ANALYST_BREAD;
create role DATA_ANALYST_PASTRY;

-- grant the BAKERY_READ access role to functional roles
grant role BAKERY_READ to role DATA_ANALYST_BREAD;
grant role BAKERY_READ to role DATA_ANALYST_PASTRY;

-- grant the functional roles to the users who perform those business functions
-- in this exercise we grant both functional roles to our current user to be able to test them

set my_current_user = current_user();
grant role DATA_ANALYST_BREAD to user IDENTIFIER($my_current_user);
grant role DATA_ANALYST_PASTRY to user IDENTIFIER($my_current_user);

-- grant usage on the BAKERY_WH warehouse to the functional roles
use role SYSADMIN;
grant usage on warehouse BAKERY_WH to role DATA_ANALYST_BREAD;
grant usage on warehouse BAKERY_WH to role DATA_ANALYST_PASTRY;

-- to keep the exercise simple, the DATA_ENGINEER role creates and applies row access policies
-- grant privileges to create and apply row access policies to the DATA_ENGINEER role
use role ACCOUNTADMIN;
grant create row access policy on schema BAKERY_DB.DG to role DATA_ENGINEER;
grant apply row access policy on account to role DATA_ENGINEER;

-- use the DATA_ENGINEER role to create the row access policy
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DG;

create row access policy DG.RAP_BUSINES_UNIT 
as (DEPARTMENT varchar) 
returns boolean -> 
  case
-- return TRUE when the role is the creator of the row access policy
    when (is_role_in_session('DATA_ENGINEER'))
      then TRUE
-- grant access based on the mapping of role and department
    when (is_role_in_session('DATA_ANALYST_BREAD')) and DEPARTMENT = 'Bread'
      then TRUE
    when (is_role_in_session('DATA_ANALYST_PASTRY')) and DEPARTMENT = 'Pastry'
      then TRUE
-- otherwise return FALSE
    else FALSE
  end;

-- apply the row access policy to the EMPLOYEE view in the RPT schema
alter view BAKERY_DB.RPT.EMPLOYEE add row access policy RAP_BUSINES_UNIT on (DEPARTMENT); 

-- test to verify that the row access policy is working as expected
-- the DATA_ANALYST_BREAD role should see only the data in the 'Bread' department
use role DATA_ANALYST_BREAD;
select * from BAKERY_DB.RPT.EMPLOYEE;

-- the DATA_ANALYST_PASTRY role should see only the data in the 'Pastry' department
use role DATA_ANALYST_PASTRY;
select * from BAKERY_DB.RPT.EMPLOYEE;

