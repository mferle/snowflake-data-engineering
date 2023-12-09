use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
use warehouse BAKERY_WH;
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema TRANSFORM;
use schema TRANSFORM;

-- create a view that combines data from individual staging tables
create view ORDERS_COMBINED_STG as
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from bakery_db.orders.ORDERS_STG
union all
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from bakery_db.external_orders.ORDERS_BISTRO_STG
union all
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from bakery_db.external_json_orders.ORDERS_PARK_INN_STG;

-- create target table that will store historical orders combined from all sources
use database BAKERY_DB;
use schema TRANSFORM;
use schema TRANSFORM;create or replace table CUSTOMER_ORDERS_COMBINED (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- merge combined staging data into the target table
-- Listing 4.5 
merge into CUSTOMER_ORDERS_COMBINED tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, tgt.source_file_name = src.source_file_name, tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp())
;

-- create a stored procedure that executes the previous MERGE statement
-- Listing 4.6 
use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
  merge into CUSTOMER_ORDERS_COMBINED tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, 
    tgt.source_file_name = src.source_file_name, 
    tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, 
    baked_good_type, quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date,
    src.baked_good_type, src.quantity, src.source_file_name,
    current_timestamp());
end;
$$
;

-- execute the stored procedure
call LOAD_CUSTOMER_ORDERS();

-- modify the stored procedure: add return string
use database BAKERY_DB;
use schema TRANSFORM;
-- Listing 4.8
create procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
  merge into CUSTOMER_ORDERS_COMBINED tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, 
    tgt.source_file_name = src.source_file_name, 
    tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, 
    baked_good_type, quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date,
    src.baked_good_type, src.quantity, src.source_file_name,
    current_timestamp());
  return 'Load completed. ' || SQLROWCOUNT || ' rows affected.';
end;
$$
;

-- execute the stored procedure
call LOAD_CUSTOMER_ORDERS();

-- modify the stored procedure: add exception handling
-- Listing 4.9
use database BAKERY_DB;
use schema TRANSFORM;
create or replace procedure LOAD_CUSTOMER_ORDERS()
returns varchar
language sql
as
$$
begin
  merge into CUSTOMER_ORDERS_COMBINED tgt
using ORDERS_COMBINED_STG as src
on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, 
    tgt.source_file_name = src.source_file_name, 
    tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, 
    baked_good_type, quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date,
    src.baked_good_type, src.quantity, src.source_file_name,
    current_timestamp());
  return 'Load completed. ' || SQLROWCOUNT || ' rows affected.';
exception
  when other then
    return 'Load failed with error message: ' || SQLERRM;
end;
$$
;

-- execute the stored procedure
call LOAD_CUSTOMER_ORDERS();
