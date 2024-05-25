use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema ORCHESTRATION;

-- select rows where the CATEGORY is not one of the allowed values ('Bread', 'Pastry')
--Listing 14.2
select * 
from DWH.PRODUCT_TBL 
where category not in ('Bread', 'Pastry');

-- create the PRODUCT_DQ_TASK
-- schedule it every 10 minutes initially so you can execute it manually to test
create or replace task ORCHESTRATION.PRODUCT_DQ_TASK
  warehouse = BAKERY_WH
  schedule = '10 M'
as
  declare
    error_cnt integer;
    error_info variant;
  begin
    select array_agg(product_id) into error_info 
    from BAKERY_DB.DWH.PRODUCT_TBL 
    where category not in ('Bread', 'Pastry');

    error_cnt := array_size(error_info);

    if (error_cnt > 0) then
      insert into DQ.DQ_LOG
      select
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
        current_timestamp(),
        'BAKERY_DB',
        'DWH',
        'PRODUCT_TBL',
        'Invalid values in the CATEGORY column',
        :error_cnt,
        :error_info;
    end if;
  end;

-- execute the task manually to test
execute task PRODUCT_DQ_TASK;

select *
from table(information_schema.task_history())
order by scheduled_time desc;

select * from DQ.DQ_LOG;

-- unset the schedule from the task and make it dependent on the INSERT_PRODUCT_TASK
alter task PRODUCT_DQ_TASK unset schedule;
alter task PRODUCT_DQ_TASK 
  add after INSERT_PRODUCT_TASK;

-- resume the task so it will run in the pipeline
alter task PRODUCT_DQ_TASK resume;

-- before executing the pipeline, update the rows in the staging table so the streams have data
update STG.PARTNER set valid_from = '2023-10-11' where partner_id = 114;
update STG.PRODUCT set valid_from = '2023-10-11' where product_id = 14;

-- execute the pipeline manually
execute task ORCHESTRATION.PIPELINE_START_TASK;

-- you can check the task_history() table function
select *
from table(information_schema.task_history())
order by scheduled_time desc;
-- you should also receive two emails, one when the pipeline started and one when the pipeline completed

-- check the DQ_LOG table
select * from DQ.DQ_LOG order by log_ts desc;
