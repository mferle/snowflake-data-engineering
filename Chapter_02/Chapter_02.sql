-- initial setup: create database, schema and warehouse
use role SYSADMIN;
create database BAKERY_DB;
create schema ORDERS;
create warehouse BAKERY_WH with warehouse_size = 'XSMALL';

-- create named internal stage
create stage orders_stage;
-- view contents of the stage (will be empty upon creation)
list @orders_stage;

-- create staging table
create or replace table ORDERS_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- upload a csv file to the internal stage using the Snowsight UI
-- then view data in the internal stage
select $1, $2, $3, $4, $5 from @ORDERS_STAGE;

-- truncate the staging table before loading data to remove any data from previous loads
truncate table orders_stg;

-- copy data from the internal stage to the staging table using parameters:
-- - file_format to specify that the header line is to be skipped
-- - on_error to specify that the statement is to be aborted if an error is encountered
-- - purge the csv file from the internal stage after loading data
copy into ORDERS_STG
from (select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() from @ORDERS_STAGE)
file_format = (type = csv, skip_header = 1)
on_error = abort_statement
purge = true;

-- view the data that was loaded
select * from ORDERS_STG;

-- construct a SQL query that eliminites duplicate records: 
-- - for each customer, delivery date and baked good type take the latest quantity by order date
select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts
from ORDERS_STG
qualify row_number() over (partition by customer, delivery_date, baked_good_type order by order_date desc) = 1;

-- create the target table
create or replace table customer_orders(
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- merge data from the staging table into the target table
-- using the SQL query that eliminates duplicate records so that only the latest data is taken into consideration
merge into CUSTOMER_ORDERS tgt
using (
  select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name
  from orders_stg
  qualify row_number() over (partition by customer, delivery_date, baked_good_type order by order_date desc) = 1
) as src
on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
when matched then 
  update set tgt.quantity = src.quantity, tgt.source_file_name = src.source_file_name, tgt.load_ts = current_timestamp()
when not matched then
  insert (customer, order_date, delivery_date, baked_good_type, quantity, source_file_name, load_ts)
  values(src.customer, src.order_date, src.delivery_date, src.baked_good_type, src.quantity, src.source_file_name, current_timestamp())
;

-- view data after merging
select * from CUSTOMER_ORDERS order by delivery_date desc;

-- construct a SQL query that summarizes the customer order data by delivery date, and baked good type
select delivery_date, baked_good_type, sum(quantity) as total_quantity
    from CUSTOMER_ORDERS
    group by all;

-- create summary table
create or replace table SUMMARY_ORDERS(
  delivery_date date,
  baked_good_type varchar,
  total_quantity number,
);

-- truncate summary table
truncate table SUMMARY_ORDERS;
-- insert summarized data into the summary table
insert into SUMMARY_ORDERS(delivery_date, baked_good_type, total_quantity)
  select delivery_date, baked_good_type, sum(quantity) as total_quantity
  from CUSTOMER_ORDERS
  group by all;

-- create task that executes the previous steps on schedule:
-- - truncates the staging table
-- - loads data from the internal stage into the staging table using the COPY command
-- - merges data from the staging table into the target table
-- - truncates the summary table
-- - inserts summarized data into the summary table
-- - executes every 10 minutes (for testing) - later will be rescheduled to run once every evening
create or replace task PROCESS_ORDERS
warehouse = BAKERY_WH
  schedule = '5 M'
as
begin
  truncate table orders_stg;
  copy into ORDERS_STG
  from (select $1, $2, $3, $4, $5, metadata$filename, current_timestamp() from @ORDERS_STAGE)
  file_format = (type = csv, skip_header = 1)
  on_error = abort_statement
  purge = true;

  merge into CUSTOMER_ORDERS tgt
  using (
    select customer, order_date, delivery_date, baked_good_type, quantity, source_file_name
    from orders_stg
    qualify row_number() over (partition by customer, delivery_date, baked_good_type order by order_date desc) = 1
  ) as src
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

-- when the task is created it is initially suspended, must be manually resumed
alter task PROCESS_ORDERS resume;

-- view all previous and scheduled task executions
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

-- when done, suspend the task so that it doesn't continue to execute and consume credits
alter task PROCESS_ORDERS suspend;
