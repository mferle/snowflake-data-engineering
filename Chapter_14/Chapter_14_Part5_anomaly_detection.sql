-- use the DATA_ENGINEER role to generate sample data in the STG schema
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema STG;

-- generate random data representing supermarket orders
create or replace table STG.COUNTRY_MARKET_ORDERS as
with raw_data as (
  select
    dateadd('day', uniform(1, 180, random()), '2023-11-01'::date) as delivery_date,
    uniform(1, 14, random()) as product_id,
    uniform(500, 1000, random()) as quantity
  from table(generator(rowcount => 10000))
)
select 
  'Country Market' as customer, 
  delivery_date, 
  product_id, 
  sum(quantity) as quantity
from raw_data
group by all;

-- simulate data anomalies
-- eg. unusually low quantities between March 10 and March 15
update STG.COUNTRY_MARKET_ORDERS 
  set quantity = 0.2*quantity 
  where delivery_date between '2024-03-10' and '2024-03-15';
-- missing data on March 21 and 22
update STG.COUNTRY_MARKET_ORDERS 
  set quantity = 0 
  where delivery_date between '2024-03-21' and '2024-03-22';

-- view the quantity distribution as a line chart
select * from STG.COUNTRY_MARKET_ORDERS;

-- grant the create anomaly detection privilege to the DATA_ENGINEER role
use role ACCOUNTADMIN;
grant create SNOWFLAKE.ML.ANOMALY_DETECTION 
  on schema BAKERY_DB.DQ
  to role DATA_ENGINEER;

-- continue working with the DATA_ENGINEER role in the DQ schema
use role DATA_ENGINEER;
use schema DQ;

-- historical data before March 1 on which the model trains
create or replace view ORDERS_HISTORICAL_DATA as
  select delivery_date::timestamp as delivery_ts, 
    sum(quantity) as quantity
  from STG.COUNTRY_MARKET_ORDERS
  where delivery_date < '2024-03-01'
  group by delivery_ts;

-- new data after March 1 on which the model looks for anomalies based on historical trends
create or replace view ORDERS_NEW_DATA as
  select delivery_date::timestamp as delivery_ts, 
    sum(quantity) as quantity
  from STG.COUNTRY_MARKET_ORDERS
  where delivery_date >= '2024-03-01'
  group by delivery_ts;

-- train the model on historical data
create or replace SNOWFLAKE.ML.ANOMALY_DETECTION orders_model(
  input_data => SYSTEM$REFERENCE('VIEW', 'ORDERS_HISTORICAL_DATA'),
  timestamp_colname => 'delivery_ts',
  target_colname => 'quantity',
  label_colname => '');

-- calculate anomalies on new data
call orders_model!DETECT_ANOMALIES(
  input_data => SYSTEM$REFERENCE('VIEW', 'ORDERS_NEW_DATA'),
  timestamp_colname =>'delivery_ts',
  target_colname => 'quantity'
);

-- save the output to a table
create or replace table ORDERS_MODEL_ANOMALIES as 
select * from table(result_scan(last_query_id()));
