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
