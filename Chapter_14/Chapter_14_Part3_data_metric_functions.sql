-- grant privileges to work with data metric functions
use role ACCOUNTADMIN;
grant database role SNOWFLAKE.DATA_METRIC_USER to role DATA_ENGINEER;
grant EXECUTE DATA METRIC FUNCTION on account to role DATA_ENGINEER;
grant application role SNOWFLAKE.DATA_QUALITY_MONITORING_VIEWER 
  to role DATA_ENGINEER;

-- continue to use the DATA_ENGINEER role
use role DATA_ENGINEER;

-- add the data metric schedule to the PARTNER_TBL table
alter table DWH.PARTNER_TBL set DATA_METRIC_SCHEDULE = '5 MINUTE';

-- add a data metric function to the RATING column
alter table DWH.PARTNER_TBL
  add data metric function SNOWFLAKE.CORE.NULL_COUNT 
  on (rating);

-- after about 5 minutes, check the output in the DATA_QUALITY_MONITORING_RESULTS table
select measurement_time, table_name, metric_name, argument_names, value
from SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
order by measurement_time desc;

-- create a custom data metric function in the DQ schema
create data metric function DQ.INVALID_CATEGORY(
  T table(CAT varchar))
  returns integer 
as
$$
  select count(*) 
  from T
  where CAT not in ('Bread', 'Pastry')
$$;

-- add the data metric schedule to the PRODUCT_TBL table
alter table DWH.PRODUCT_TBL set DATA_METRIC_SCHEDULE = '5 MINUTE';

alter table DWH.PRODUCT_TBL
  add data metric function DQ.INVALID_CATEGORY 
  on (category);

-- after about 5 minutes, check the output in the DATA_QUALITY_MONITORING_RESULTS table
select measurement_time, table_name, metric_name, argument_names, value
from SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
order by measurement_time desc;

-- unset the data metric schedule so that it doesn't execute every 5 minutes
alter table DWH.PARTNER_TBL unset DATA_METRIC_SCHEDULE;
alter table DWH.PRODUCT_TBL unset DATA_METRIC_SCHEDULE;

-- set the data metric schedule to trigger on DML changes
alter table DWH.PARTNER_TBL 
  set DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
alter table DWH.PRODUCT_TBL 
  set DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- view the data metric schedule on a table
show parameters like 'DATA_METRIC_SCHEDULE' in table DWH.PRODUCT_TBL;

-- view the data metric functions associated with a table
select metric_name, ref_entity_name, ref_entity_domain, ref_arguments, schedule 
from table(
  INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'BAKERY_DB.DWH.PRODUCT_TBL', 
    ref_entity_domain => 'table'
  )
);
