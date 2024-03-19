use role ACCOUNTADMIN;
-- create a storage integration object named PARK_INN_INTEGRATION as described in Chapter 4 
-- if you created the storage integration already in Chapter 4, no need to recreate it
-- grant usage on the storage integration object to the DATA_ENGINEER role
grant usage on integration PARK_INN_INTEGRATION to role DATA_ENGINEER;

-- use the DATA_ENGINEER role going forward
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema EXT;

-- create an external stage named JSON_ORDERS_STAGE using the PARK_INN_INTEGRATION as described in Chapter 4
-- upload the json files Orders_2023-09-01.json and Orders_2023-09-04.json to the object storage location used in the stage

-- view files in the stage
list @JSON_ORDERS_STAGE;

-- create the extract table for the orders in raw (json) format
create table JSON_ORDERS_EXT (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- load data from the stage into the extract table
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

select * from JSON_ORDERS_EXT;