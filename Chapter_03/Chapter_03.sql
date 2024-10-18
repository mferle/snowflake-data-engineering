-- create a storage integration
-- using Microsoft Azure
-- refer to Chapter 4 for Amazon S3
use role ACCOUNTADMIN;

create storage integration BISTRO_INTEGRATION
  type = external_stage
  storage_provider = 'AZURE'
  enabled = true
  azure_tenant_id = '1234abcd-xxx-56efgh78' --use your own Tenant ID
  storage_allowed_locations = ('azure://bakeryorders001.blob.core.windows.net/orderfiles/');

-- describe the storage integration and take note of the following parameters:
-- - AZURE_CONSENT_URL
-- - AZURE_MULTI_TENANT_APP_NAME
describe storage integration BISTRO_INTEGRATION;

-- grant usage on storage integration so that the SYSADMIN role can use it
grant usage on integration BISTRO_INTEGRATION to role SYSADMIN;

-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema EXTERNAL_ORDERS;
use schema EXTERNAL_ORDERS;

-- create an external stage using the storage integration
-- Listing 3.1 
create stage BISTRO_STAGE
  storage_integration = BISTRO_INTEGRATION
  url = 'azure://bakeryorders001.blob.core.windows.net/orderfiles';

-- Upload a sample file named Orders_2023-08-04.csv to the storage container

-- view files in the external stage
list @BISTRO_STAGE;

-- create an external stage using a SAS token
create stage BISTRO_SAS_STAGE
  URL = 'azure://bakeryorders001.blob.core.windows.net/orderfiles'
  CREDENTIALS=(AZURE_SAS_TOKEN = '?sv=2023-...%3D'); --generate and use your own SAS token

-- view files in the external stage
list @BISTRO_SAS_STAGE;

-- create a named file format
create file format ORDERS_CSV_FORMAT
  type = csv
  field_delimiter = ','
  skip_header = 1;

-- create the external stage by adding the file format
create or replace stage BISTRO_STAGE
  storage_integration = BISTRO_INTEGRATION
  url = 'azure://bakeryorders001.blob.core.windows.net/orderfiles'
  file_format = ORDERS_CSV_FORMAT;

-- create staging table for restaurant orders
use database BAKERY_DB;
use schema EXTERNAL_ORDERS;
create table ORDERS_BISTRO_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- load data from the stage into the staging table
-- Listing 3.3 
copy into ORDERS_BISTRO_STG
from (
  select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() 
  from @BISTRO_STAGE
)
file_format = ORDERS_CSV_FORMAT
on_error = abort_statement
;

-- view data in the staging table
select * from ORDERS_BISTRO_STG;

-- view load history for the table
select *
from information_schema.load_history
where schema_name = 'EXTERNAL_ORDERS' and table_name = 'ORDERS_BISTRO_STG'
order by last_load_time desc;

-- add a directory table to the stage
alter stage BISTRO_STAGE
set directory = (enable = true);

-- manually refresh the directory
alter stage BISTRO_STAGE refresh;

-- query the directory table
select * 
from directory (@BISTRO_STAGE);

-- Upload additional CSV files to the container in the 202308 path

-- load data from the stage into the staging table by specifying a path
copy into ORDERS_BISTRO_STG
from (
  select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() 
  from @BISTRO_STAGE/202308
)
file_format = ORDERS_CSV_FORMAT
on_error = abort_statement
;

-- create an external table 
-- Listing 3.4 
use database BAKERY_DB;
use schema EXTERNAL_ORDERS;
create external table ORDERS_BISTRO_EXT (
  customer varchar as (VALUE:c1::varchar),
  order_date date as (VALUE:c2::date),
  delivery_date date as (VALUE:c3::date),
  baked_good_type varchar as (VALUE:c4::varchar),
  quantity number as (VALUE:c5::number),
  source_file_name varchar as metadata$filename
)
location = @BISTRO_STAGE
auto_refresh = FALSE
file_format = ORDERS_CSV_FORMAT;

-- query the external table
select * 
from ORDERS_BISTRO_EXT;

-- refresh the external table
alter external table ORDERS_BISTRO_EXT refresh;

-- create a materialized view
use database BAKERY_DB;
use schema EXTERNAL_ORDERS;
create materialized view ORDERS_BISTRO_MV as
select customer, order_date, delivery_date, 
  baked_good_type, quantity, source_file_name
from ORDERS_BISTRO_EXT;

-- query the materialized view
select * 
from ORDERS_BISTRO_MV;
