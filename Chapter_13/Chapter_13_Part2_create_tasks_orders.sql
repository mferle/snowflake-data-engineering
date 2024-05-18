-- this chapter is a continuation of Chapter 11
-- all scripts in Chapter 11 must be executed before continuing

-- use the ACCOUNTADMIN role to grant the execute task privilege to the DATA_ENGINEER role
use role ACCOUNTADMIN;
grant execute task on account to role DATA_ENGINEER;

-- continue working with the DATA_ENGINEER role
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema ORCHESTRATION;

-- create a task that performs the COPY INTO operation from the stage into the table
create or replace task COPY_ORDERS_TASK
  warehouse = BAKERY_WH
  schedule = '10 M'
as
  copy into EXT.JSON_ORDERS_EXT
  from (
    select 
      $1, 
      metadata$filename, 
      current_timestamp() 
    from @EXT.JSON_ORDERS_STAGE
  )
  on_error = abort_statement;

-- execute the task once to verify that it is working
execute task COPY_ORDERS_TASK;

-- view the task history
-- Listing 13.1
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

-- create a task that inserts data from the stream into the staging table
create or replace task INSERT_ORDERS_STG_TASK
  warehouse = 'BAKERY_WH'
  after COPY_ORDERS_TASK
when
  system$stream_has_data('EXT.JSON_ORDERS_STREAM')
as
  insert into STG.JSON_ORDERS_TBL_STG
  select 
    customer_orders:"Customer"::varchar as customer, 
    customer_orders:"Order date"::date as order_date, 
    CO.value:"Delivery date"::date as delivery_date,
    DO.value:"Baked good type":: varchar as baked_good_type,
    DO.value:"Quantity"::number as quantity,
    source_file_name,
    load_ts
  from EXT.JSON_ORDERS_STREAM,
  lateral flatten (input => customer_orders:"Orders") CO,
  lateral flatten (input => CO.value:"Orders by day") DO;

-- if you wish to test the task, remove the dependency on the COPY_ORDERS_TASK, execute the task, then add the dependency again
-- remove the dependency
alter task INSERT_ORDERS_STG_TASK remove after COPY_ORDERS_TASK;
-- execute the task manually
execute task INSERT_ORDERS_STG_TASK;
-- view the task history
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;
-- add the dependency again
alter task INSERT_ORDERS_STG_TASK add after COPY_ORDERS_TASK;
-- execute the task once to verify that it is working

-- enable the child and parent tasks
alter task INSERT_ORDERS_STG_TASK resume;
alter task COPY_ORDERS_TASK resume;

-- upload the json file Orders_2023-09-07.json to the object storage location 
-- wait until the tasks execute on schedule

-- view the task history
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

-- suspend the task when you are done testing
alter task COPY_ORDERS_TASK suspend;
