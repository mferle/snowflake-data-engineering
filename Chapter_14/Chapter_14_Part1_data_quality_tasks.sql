use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema STG;

-- insert a new partner into the PARTNER staging table
insert into STG.PARTNER values
(114, 'Country Market', '12 Meadow Lane', null, '2023-10-10');

-- resume the pipeline
alter task ORCHESTRATION.PIPELINE_START_TASK resume;

-- after 10 minutes, you should receive two emails, one when the pipeline started and one when the pipeline completed
-- you can also check the task_history() table function
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- suspend the pipeline
alter task ORCHESTRATION.PIPELINE_START_TASK resume;

-- create DQ schema
-- refer to Chapter_10_Part1_role_based_access_control.sql

use role SYSADMIN;
use database BAKERY_DB;

-- create schema with managed access using the SYSADMIN role
create schema DQ with managed access;

-- grant full privileges on the DQ schema to the BAKERY_FULL role using the SECURITYADMIN role
use role SECURITYADMIN;
grant all on schema BAKERY_DB.DQ to role BAKERY_FULL;

use role DATA_ENGINEER;
use schema DQ;
-- create a table to store data quality information
create or replace table DQ_LOG (
  run_group_id varchar, --CURRENT_TASK_GRAPH_RUN_GROUP_ID
  root_task_name varchar, --CURRENT_ROOT_TASK_NAME
  task_name varchar, --CURRENT_TASK_NAME
  log_ts timestamp,
  database_name varchar,
  table_name varchar,
  schema_name varchar,
  dq_rule_name varchar,
  error_cnt number
);

-- create the PARTNER_DQ_TASK
create or replace task ORCHESTRATION.PARTNER_DQ_TASK
  warehouse = BAKERY_WH
  schedule = '10 M'
as
  declare
    error_cnt integer;
  begin
    error_cnt := (select count(*) from BAKERY_DB.DWH.PARTNER_TBL where rating is null);
    if (error_cnt > 0) then
      insert into DQ.DQ_LOG
      select
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
        current_timestamp(),
        'BAKERY_DB',
        'DWH',
        'PARTNER_TBL',
        'Null values in the RATING column',
        :error_cnt;
    end if;
  end;

-- execute the task manually to test
execute task ORCHESTRATION.PARTNER_DQ_TASK;

-- check the task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- view the data inserted into the DQ_LOG table
select * from DQ.DQ_LOG;

-- unset the schedule from the task and make it dependent on the INSERT_PARTNER_TASK
alter task ORCHESTRATION.PARTNER_DQ_TASK unset schedule;
alter task ORCHESTRATION.PARTNER_DQ_TASK 
  add after ORCHESTRATION.INSERT_PARTNER_TASK;