-- to keep the exercise simple, the DATA_ENGINEER role creates and applies masking policies
-- grant privileges to create and apply masking policies to the DATA_ENGINEER role
use role ACCOUNTADMIN;
grant create masking policy on schema BAKERY_DB.DG to role DATA_ENGINEER;
grant apply masking policy on account to role DATA_ENGINEER;

-- use the DATA_ENGINEER role to create the masking policy
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DG;

create masking policy ADDRESS_MASK 
as (addr varchar) 
returns varchar ->
  case
    when current_role() in ('DATA_ENGINEER') then addr
    else '***'
  end;

-- apply the masking policy to the EMPLOYEE view in the RPT schema
alter view BAKERY_DB.RPT.EMPLOYEE 
modify column HOME_ADDRESS 
set masking policy ADDRESS_MASK;

-- to test, use one of the data analyst roles
-- should return masked data
use role DATA_ANALYST_BREAD;
select * from BAKERY_DB.RPT.EMPLOYEE;

-- then use the DATA_ENGINEER role
-- should return unmasked data
use role DATA_ENGINEER;
select * from BAKERY_DB.RPT.EMPLOYEE;

