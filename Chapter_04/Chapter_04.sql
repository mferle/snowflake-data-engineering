-- create a storage integration
use role ACCOUNTADMIN;

create storage integration PARK_INN_INTEGRATION
  type = external_stage
  storage_provider = 'AZURE'
  enabled = true
  azure_tenant_id = '1234abcd-xxx-56efgh78' --use your own Tenant ID
  storage_allowed_locations = ('azure://parkinnorders.blob.core.windows.net/orderjsonfiles/');

-- describe the storage integration and take note of the following parameters:
-- - AZURE_CONSENT_URL
-- - AZURE_MULTI_TENANT_APP_NAME
describe integration PARK_INN_INTEGRATION;

-- grant usage on storage integration so that the SYSADMIN role can use it
grant usage on integration PARK_INN_INTEGRATION to role SYSADMIN;

-- create a new schema in the BAKERY_DB database (see Chapter 2)

use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema EXTERNAL_JSON_ORDERS;

-- create an external stage using the storage integration
create or replace stage PARK_INN_STAGE
  storage_integration = PARK_INN_INTEGRATION
  url = 'azure://parkinnorders.blob.core.windows.net/orderjsonfiles'
  file_format = (type = json);

-- view files in the external stage
list @PARK_INN_STAGE;

select $1 from @PARK_INN_STAGE;

-- create staging table for restaurant orders in raw (json) format
create or replace table ORDERS_PARK_INN_RAW_STG (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- load data from the stage into the staging table
copy into ORDERS_PARK_INN_RAW_STG
from (
  select $1, metadata$filename, current_timestamp() 
  from @PARK_INN_STAGE
)
on_error = abort_statement
;

-- view data in the staging table
select * 
from ORDERS_PARK_INN_RAW_STG;

select customer_orders
from ORDERS_PARK_INN_RAW_STG;

select 
  customer_orders:Customer::varchar as customer, 
  customer_orders:"Order date"::date as order_date, 
  CO.value:"Delivery date"::date as delivery_date,
  DO.value:"Baked good type":: varchar as baked_good_type,
  DO.value:"Quantity"::number as quantity
from ORDERS_PARK_INN_RAW_STG,
lateral flatten (input => customer_orders:"Orders") CO,
lateral flatten (input => CO.value:"Daily orders") DO;

