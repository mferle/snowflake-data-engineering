-- this chapter is a continuation of Chapter 10
-- all scripts in Chapter 10 must be executed before continuing

use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema EXT;

-- delete all files from the object storage location used in the JSON_ORDERS_STAGE stage
-- upload the json file Orders_2023-09-05.json to the object storage location 

-- recreate the table to remove any data from previous exercises
create or replace table JSON_ORDERS_EXT (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- create a stream on the table
create stream JSON_ORDERS_STREAM 
on table JSON_ORDERS_EXT;

-- view data in the stream
select * from JSON_ORDERS_STREAM;
-- the stream should be empty

-- view files in the stage
list @JSON_ORDERS_STAGE;

-- copy data from the stage into the JSON_ORDERS_EXT table
copy into JSON_ORDERS_EXT
from (
  select 
    $1, 
    metadata$filename, 
    current_timestamp() 
  from @JSON_ORDERS_STAGE
)
on_error = abort_statement
;
-- the output from the copy command should indicate that data from the Orders_2023-09-05.json file was copied into the table

-- check the data in the stream again
select * from JSON_ORDERS_STREAM;
-- the stream should contain the newly uploaded file


-- create a staging table in the STG schema that will store the flattened semi-structured data from the extraction layer
create table STG.JSON_ORDERS_TBL_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- insert the flattened data from the stream into the staging table
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

-- check the data in the table:
select * from STG.JSON_ORDERS_TBL_STG;
-- should show 8 rows

-- check the data in the stream again
select * from JSON_ORDERS_STREAM;
-- the stream should now be empty because it was consumed by the insert statement

-- repeat with another file
-- upload the json file Orders_2023-09-06.json to the object storage location 

-- view files in the stage
list @JSON_ORDERS_STAGE;

-- copy data from the stage into the JSON_ORDERS_EXT table
copy into JSON_ORDERS_EXT
from (
  select 
    $1, 
    metadata$filename, 
    current_timestamp() 
  from @JSON_ORDERS_STAGE
)
on_error = abort_statement
;
-- the output from the copy command should indicate that data from the Orders_2023-09-06.json file was copied into the table

-- check the data in the stream again
select * from JSON_ORDERS_STREAM;
-- the stream should contain the newly uploaded file

-- perform the insert statement again
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

-- should insert 4 rows
