-- create a storage integration
use role ACCOUNTADMIN;

create storage integration SPEEDY_INTEGRATION
  type = external_stage
  storage_provider = 'AZURE'
  enabled = true
  azure_tenant_id = '1234abcd-xxx-56efgh78' --use your own Tenant ID
  storage_allowed_locations = ('azure://speedyorders001.blob.core.windows.net/speedyservicefiles/');;

-- describe the storage integration and take note of the following parameters:
-- - AZURE_CONSENT_URL
-- - AZURE_MULTI_TENANT_APP_NAME
describe integration SPEEDY_INTEGRATION;

-- grant usage on storage integration so that the SYSADMIN role can use it
grant usage on integration SPEEDY_INTEGRATION to role SYSADMIN;

-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema DELIVERY_ORDERS;
use schema DELIVERY_ORDERS;

-- create an external stage using the storage integration
create stage SPEEDY_STAGE
  storage_integration = SPEEDY_INTEGRATION
  url = 'azure://speedyorders001.blob.core.windows.net/speedyservicefiles/'
  file_format = (type = json);

-- view files in the external stage
list @SPEEDY_STAGE;

-- view data in the staged files
select $1 from @SPEEDY_STAGE;

-- extract the ORDER_ID and ORDER_DATETIME columns from the JSON, but leave ITEMS as variant without parsing
select 
  $1:"Order id",
  $1:"Order datetime",
  $1:"Items",
  metadata$filename, 
  current_timestamp() 
from @SPEEDY_STAGE;

-- create staging table for delivery orders
create table SPEEDY_ORDERS_RAW_STG (
  order_id varchar,
  order_datetime timestamp,
  items variant,
  source_file_name varchar,
  load_ts timestamp
);

-- configure event grid messages for blob storage events
-- - enable the event grid resource provider
-- - create a storage queue and take note of the queue URL
-- - create an event grid subscription with an event grid system topic for the "Blob Created" event

-- create a notification integration
use role ACCOUNTADMIN;
CREATE NOTIFICATION INTEGRATION SPEEDY_QUEUE_INTEGRATION
ENABLED = true
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://speedyorders001.queue.core.windows.net/speedyordersqueue'
AZURE_TENANT_ID = '1234abcd-xxx-56efgh78';

-- describe the storage integration and take note of the following parameters:
-- - AZURE_CONSENT_URL
-- - AZURE_MULTI_TENANT_APP_NAME
describe notification integration SPEEDY_QUEUE_INTEGRATION;

-- grant usage on notification integration so that the SYSADMIN role can use it
grant usage on integration SPEEDY_QUEUE_INTEGRATION to role SYSADMIN;

-- create the snowpipe
use role SYSADMIN;
use database BAKERY_DB;
use schema DELIVERY_ORDERS;

create pipe SPEEDY_PIPE
  auto_ingest = true
  integration = 'SPEEDY_QUEUE_INTEGRATION'
  as
  copy into SPEEDY_ORDERS_RAW_STG
  from (
    select 
      $1:"Order id",
      $1:"Order datetime",
      $1:"Items",
      metadata$filename, 
      current_timestamp() 
    from @SPEEDY_STAGE
  );

-- load historical data from files that existed in the external stage before Event Grid messages were configured
alter pipe SPEEDY_PIPE refresh;

-- view data in the staging table
select * 
from SPEEDY_ORDERS_RAW_STG;

-- check the status of the pipe
select system$pipe_status('SPEEDY_PIPE');

-- view the copy history in the last hour
select *
from table(information_schema.copy_history(
  table_name => 'SPEEDY_ORDERS_RAW_STG', 
  start_time => dateadd(hours, -1, current_timestamp())));

-- select the values from the second level keys
select
  order_id,
  order_datetime,
  value:"Item"::varchar as baked_good_type,
  value:"Quantity"::number as quantity
from SPEEDY_ORDERS_RAW_STG,
lateral flatten (input => items);

-- create a dynamic table that materializes the output of the previous query
create dynamic table SPEEDY_ORDERS
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
  select
  order_id,
  order_datetime,
  value:"Item"::varchar as baked_good_type,
  value:"Quantity"::number as quantity,
  source_file_name,
  load_ts
from SPEEDY_ORDERS_RAW_STG,
lateral flatten (input => items);

-- query the data in the dynamic table
select *
from SPEEDY_ORDERS
order by order_datetime desc;

-- query the dynamic table refresh history
select *
from table(information_schema.dynamic_table_refresh_history())
order by refresh_start_time desc;