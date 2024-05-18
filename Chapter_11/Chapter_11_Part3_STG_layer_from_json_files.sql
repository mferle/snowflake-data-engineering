use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema STG;

-- create a view in the STG schema flattening the json into a relational structure
-- refer to Chapter 4 for an explanation of how the view is constructed
create view JSON_ORDERS_STG as
select 
  E.customer_orders:"Customer"::varchar as customer, 
  E.customer_orders:"Order date"::date as order_date, 
  CO.value:"Delivery date"::date as delivery_date,
  DO.value:"Baked good type":: varchar as baked_good_type,
  DO.value:"Quantity"::number as quantity,
  source_file_name,
  load_ts
from EXT.JSON_ORDERS_EXT E,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO;

-- view data in the view
select *
from JSON_ORDERS_STG;
