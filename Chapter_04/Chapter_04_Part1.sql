-- create a storage integration
-- using Amazon S3
-- refer to Chapter 3 for Microsoft Azure
use role ACCOUNTADMIN;

create storage integration PARK_INN_INTEGRATION
  type = external_stage
  storage_provider = 'S3'
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::567890987654:role/Snowflake-demo'
  storage_allowed_locations = ('s3://parkinnorders001/');

-- describe the storage integration and take note of the following parameters:
-- - STORAGE_AWS_IAM_USER_ARN
-- - STORAGE_AWS_EXTERNAL_ID
describe storage integration PARK_INN_INTEGRATION;

-- grant usage on storage integration so that the SYSADMIN role can use it
grant usage on integration PARK_INN_INTEGRATION to role SYSADMIN;

-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
use warehouse BAKERY_WH;
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema EXTERNAL_JSON_ORDERS;
use schema EXTERNAL_JSON_ORDERS;

-- create an external stage using the storage integration
create stage PARK_INN_STAGE
  storage_integration = PARK_INN_INTEGRATION
  url = 's3://parkinnorders001/'
  file_format = (type = json);

-- view files in the external stage
list @PARK_INN_STAGE;

-- view data in the staged file
select $1 from @PARK_INN_STAGE;

-- create staging table for restaurant orders in raw (json) format
use database BAKERY_DB;
use schema EXTERNAL_JSON_ORDERS;
create table ORDERS_PARK_INN_RAW_STG (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- load data from the stage into the staging table
copy into ORDERS_PARK_INN_RAW_STG
from (
  select 
    $1, 
    metadata$filename, 
    current_timestamp() 
  from @PARK_INN_STAGE
)
on_error = abort_statement
;

-- view data in the staging table
select * 
from ORDERS_PARK_INN_RAW_STG;

-- select the values from the first level keys
-- Listing 4.2 
select 
  customer_orders:"Customer"::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  customer_orders:"Orders"
from ORDERS_PARK_INN_RAW_STG;

-- select the values from the second level keys using LATERAL FLATTEN
-- Listing 4.3 
select 
  customer_orders:"Customer"::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  value:"Delivery date"::date as delivery_date,
  value:"Orders by day"
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders");

-- select the values from the third level keys using another LATERAL FLATTEN
-- Listing 4.4 
select 
  customer_orders:"Customer"::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  CO.value:"Delivery date"::date as delivery_date,
  DO.value:"Baked good type":: varchar as baked_good_type,
  DO.value:"Quantity"::number as quantity
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO;

-- create a view to represent a relational staging table using the previous query

use database BAKERY_DB;
use schema EXTERNAL_JSON_ORDERS;
create view ORDERS_PARK_INN_STG as
select 
  customer_orders:"Customer"::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  CO.value:"Delivery date"::date as delivery_date,
  DO.value:"Baked good type":: varchar as baked_good_type,
  DO.value:"Quantity"::number as quantity,
  source_file_name,
  load_ts
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Orders by day") DO;

-- view data in the view
select *
from ORDERS_PARK_INN_STG;