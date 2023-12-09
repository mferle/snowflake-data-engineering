-- initial setup: create database, schema and virtual warehouse
use role SYSADMIN;
create database BAKERY_DB;
create schema ORDERS;
create warehouse BAKERY_WH with warehouse_size = 'XSMALL';

-- create named internal stage
use database BAKERY_DB;
use schema ORDERS;
create stage ORDERS_STAGE;
-- view the contents of the stage (will be empty upon creation)
list @ORDERS_STAGE;

-- manually upload file Orders_2023-07-07.csv into the internal stage using the Snowsight user interface

-- view the contents of the stage again (should show the file that was just uploaded)
list @ORDERS_STAGE;

-- then view data in the internal stage
select $1, $2, $3, $4, $5 from @ORDERS_STAGE;

-- create staging table
use database BAKERY_DB;
use schema ORDERS;
create table ORDERS_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- copy data from the internal stage to the staging table using parameters:
-- - file_format to specify that the header line is to be skipped
-- - on_error to specify that the statement is to be aborted if an error is encountered
-- - purge the csv file from the internal stage after loading data
-- Listing 2.1 
use database BAKERY_DB;
use schema ORDERS;
copy into ORDERS_STG
from (
  select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() 
  from @ORDERS_STAGE
)
file_format = (type = csv, skip_header = 1)
on_error = abort_statement
purge = true;

-- view the data that was loaded
select * from ORDERS_STG;

-- view the contents of the stage again (should be empty again because the file was purged after loading)
list @ORDERS_STAGE;

-- create the target table
use database BAKERY_DB;
use schema ORDERS;
create or replace table CUSTOMER_ORDERS (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);


-- merge data from the staging table into the target table
-- Listing 2.2  
-- the target table
merge into CUSTOMER_ORDERS tgt
-- the source table
using ORDERS_STG as src 
-- the columns that ensure uniqueness
on src.customer = tgt.customer 
  and src.delivery_date = tgt.delivery_date 
  and src.baked_good_type = tgt.baked_good_type
-- update the target table with the values from the source table
when matched then 
  update set tgt.quantity = src.quantity, 
    tgt.source_file_name = src.source_file_name, 
    tgt.load_ts = current_timestamp()
-- insert new values from the source table into the target table
when not matched then
  insert (customer, order_date, delivery_date, baked_good_type, 
    quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date, 
    src.baked_good_type, src.quantity, src.source_file_name,
    current_timestamp());


-- view data after merging
select * from CUSTOMER_ORDERS order by delivery_date desc;

-- create summary table
use database BAKERY_DB;
use schema ORDERS;
create table SUMMARY_ORDERS(
  delivery_date date,
  baked_good_type varchar,
  total_quantity number
);

-- construct a SQL query that summarizes the customer order data by delivery date, and baked good type
select delivery_date, baked_good_type, sum(quantity) as total_quantity
  from CUSTOMER_ORDERS
  group by all;

-- truncate summary table
truncate table SUMMARY_ORDERS;

-- insert summarized data into the summary table
-- Listing 2.3 
insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
  select delivery_date, baked_good_type, sum(quantity) as total_quantity
  from CUSTOMER_ORDERS
  group by all;

-- view data in the summary table
select * from SUMMARY_ORDERS order by delivery_date;

-- create task that executes the previous steps on schedule:
-- - truncates the staging table
-- - loads data from the internal stage into the staging table using the COPY command
-- - merges data from the staging table into the target table
-- - truncates the summary table
-- - inserts summarized data into the summary table
-- - executes every 10 minutes (for testing) - later will be rescheduled to run once every evening
use database BAKERY_DB;
use schema ORDERS;
create or replace task PROCESS_ORDERS
  warehouse = BAKERY_WH
  schedule = '10 M'
as
begin
  truncate table ORDERS_STG;
  copy into ORDERS_STG
  from (
    select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() 
    from @ORDERS_STAGE
  )
  file_format = (type = csv, skip_header = 1)
  on_error = abort_statement
  purge = true;

  merge into CUSTOMER_ORDERS tgt
  using ORDERS_STG as src
  on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
  when matched then 
    update set tgt.quantity = src.quantity, tgt.source_file_name = src.source_file_name, tgt.load_ts = current_timestamp()
  when not matched then
    insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
    values(src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp());

  truncate table SUMMARY_ORDERS;
  insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
    select delivery_date, baked_good_type, sum(quantity) as total_quantity
    from CUSTOMER_ORDERS
    group by all;
end;

-- grant EXECUTE TASK privilege to user who will be executing the task
use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

-- manually execute task to test
execute task PROCESS_ORDERS;

-- view all previous and scheduled task executions
-- Listing 2.4 
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;
  
-- when the task is created it is initially suspended, must be manually resumed
alter task PROCESS_ORDERS resume;

-- change the task schedule to run at 11PM using UTC timezone
-- must suspend task first and resume after changing the schedule
alter task PROCESS_ORDERS suspend;

alter task PROCESS_ORDERS
set schedule = 'USING CRON 0 23 * * * UTC';

alter task PROCESS_ORDERS resume;

-- when done, suspend the task so that it doesn't continue to execute and consume credits
alter task PROCESS_ORDERS suspend;
