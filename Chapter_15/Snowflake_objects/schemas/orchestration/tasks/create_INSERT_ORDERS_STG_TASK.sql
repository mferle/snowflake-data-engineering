-- create a task that inserts data from the stream into the staging table
create or replace task BAKERY_DB.ORCHESTRATION.INSERT_ORDERS_STG_TASK
  warehouse = 'BAKERY_WH'
  after BAKERY_DB.ORCHESTRATION.COPY_ORDERS_TASK
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
