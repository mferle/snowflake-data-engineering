-- grant privilege to execute alerts
use role ACCOUNTADMIN;
grant execute alert on account to role DATA_ENGINEER;

-- use the DATA_ENGINEER role to create an alert in the DQ schema
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DQ;

-- construct a query that sums the values reported by the data metric functions on all tables in the DWH schema within the last hour
-- adding a filter that the query returns data only if the sum of the values is greater than 0
-- Listing 14.3.
select sum(value)
  from SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
  where table_database = 'BAKERY_DB'
  and table_schema = 'DWH'
  and measurement_time > dateadd('hour', -1, current_timestamp())
  having sum(value) > 0;

-- create an alert that sends an email when the previous query returns data
create alert DATA_QUALITY_MONITORING_ALERT
  warehouse = BAKERY_WH
  schedule = '5 minute'
if (exists(
  select sum(value)
  from SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
  where table_database = 'BAKERY_DB'
  and table_schema = 'DWH'
  and measurement_time > dateadd('hour', -1, current_timestamp())
  having sum(value) > 0
))
then 
  call SYSTEM$SEND_EMAIL(
  'PIPELINE_EMAIL_INT',
  'firstname.lastname@youremail.com', -- substitute you email address
  'Data quality monitoring alert',
  'Data metric functions reported invalid values since ' || to_char(dateadd('hour', -1, current_timestamp()), 'YYYY-MM-DD HH24:MI:SS') || '.'
);

-- resume the alert
alter alert DATA_QUALITY_MONITORING_ALERT resume;

-- check the execution status of the alert
select * from table(information_schema.alert_history())
order by scheduled_time desc;

-- suspend the alert
alter alert DATA_QUALITY_MONITORING_ALERT suspend;
-- change the schedule to execute every hour
alter alert DATA_QUALITY_MONITORING_ALERT set schedule = '60 minute';
