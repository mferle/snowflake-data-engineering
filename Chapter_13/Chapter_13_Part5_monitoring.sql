use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema ORCHESTRATION;

-- create a table to store logging information
create or replace table PIPELINE_LOG (
  run_group_id varchar, --CURRENT_TASK_GRAPH_RUN_GROUP_ID
  root_task_name varchar, --CURRENT_ROOT_TASK_NAME
  task_name varchar, --CURRENT_TASK_NAME
  log_ts timestamp,
  rows_processed number
);

-- recreate the COPY_ORDERS_TASK with these changes:
-- - set a schedule so that we can test it (we will modify to unset the schedule and add a dependency on the root task later)
-- - enclose the body of the task with the BEGIN and END keywords
-- - add an INSERT statement to insert data into the logging table
create or replace task COPY_ORDERS_TASK
  warehouse = BAKERY_WH
  schedule = '10 M'
as
  begin
    copy into EXT.JSON_ORDERS_EXT
    from (
      select 
        $1, 
        metadata$filename, 
        current_timestamp() 
      from @EXT.JSON_ORDERS_STAGE
    )
    on_error = abort_statement;

    insert into PIPELINE_LOG
    select
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
      current_timestamp(),
      :SQLROWCOUNT;
  end;

-- execute the task manually
execute task COPY_ORDERS_TASK;

-- check the TASK_HISTORY()
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- verify that data was inserted into the logging table
select * from PIPELINE_LOG;

-- alter the task to unset the schedule and add a dependency on the root task
alter task PIPELINE_START_TASK suspend;
alter task COPY_ORDERS_TASK unset schedule;
alter task COPY_ORDERS_TASK 
  add after PIPELINE_START_TASK;

-- recreate the INSERT_ORDERS_STG_TASK and insert data into the logging table
create or replace task INSERT_ORDERS_STG_TASK
  warehouse = 'BAKERY_WH'
  after COPY_ORDERS_TASK
when
  system$stream_has_data('EXT.JSON_ORDERS_STREAM')
as
  begin
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

    insert into PIPELINE_LOG
    select
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
      current_timestamp(),
      :SQLROWCOUNT;
  end;

-- recreate the INSERT_PRODUCT_TASK and insert data into the logging table
create or replace task INSERT_PRODUCT_TASK
  warehouse = BAKERY_WH
  after PIPELINE_START_TASK
when
  system$stream_has_data('STG.PRODUCT_STREAM')
as
  begin
    insert into DWH.PRODUCT_TBL
    select product_id, product_name, category, 
      min_quantity, price, valid_from
    from STG.PRODUCT_STREAM
    where METADATA$ACTION = 'INSERT';

    insert into PIPELINE_LOG
    select
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
      current_timestamp(),
      :SQLROWCOUNT;
  end;
  
-- recreate the INSERT_PARTNER_TASK and insert data into the logging table
create or replace task INSERT_PARTNER_TASK
  warehouse = BAKERY_WH
  after PIPELINE_START_TASK
when
  system$stream_has_data('STG.PARTNER_STREAM')
as
  begin
    insert into DWH.PARTNER_TBL
    select partner_id, partner_name, address, rating, valid_from
    from STG.PARTNER_STREAM
    where METADATA$ACTION = 'INSERT';

    insert into PIPELINE_LOG
    select
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
      SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
      current_timestamp(),
      :SQLROWCOUNT;
  end;

-- recreate the finalizer task by constructing a return_message string with the logging information from all tasks in the current run
create or replace task PIPELINE_END_TASK
  warehouse = BAKERY_WH
  finalize = PIPELINE_START_TASK
as
  declare
    return_message varchar := '';
  begin
    let log_cur cursor for
      select task_name, rows_processed 
      from PIPELINE_LOG 
      where run_group_id = 
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');

    for log_rec in log_cur loop
      return_message := return_message ||
        'Task: '|| log_rec.task_name || 
        ' Rows processed: ' || log_rec.rows_processed ||  '\n';
    end loop;
  
    call SYSTEM$SEND_EMAIL(
      'PIPELINE_EMAIL_INT',
      'firstname.lastname@youremail.com',    
      'Daily pipeline end',
      'The daily pipeline finished at ' || current_timestamp || '.' ||
        '\n\n' || :return_message

    );
  end;

-- add data to the sources
-- upload the Orders_2023-09-08.json file to the cloud storage location
-- insert partner data
insert into STG.PARTNER values(
  113, 'Lazy Brunch', '1012 Astoria Avenue', 'A', '2023-09-01'
);
-- update product data
update STG.PRODUCT set min_quantity = 5 where product_id = 5;

-- resume all tasks
alter task PIPELINE_END_TASK resume;
alter task INSERT_PRODUCT_TASK resume;
alter task INSERT_PARTNER_TASK resume;
alter task INSERT_ORDERS_STG_TASK resume;
alter task COPY_ORDERS_TASK resume;
alter task PIPELINE_START_TASK resume;

-- execute the root task manually
execute task PIPELINE_START_TASK;

 -- check the TASK_HISTORY()
select *
from table(information_schema.task_history())
order by scheduled_time desc;

-- view data in the logging table
select * from PIPELINE_LOG order by log_ts desc;

-- suspend the pipeline so it doesn't continue to consume resources and send emails
alter task PIPELINE_START_TASK suspend;