use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema ORCHESTRATION;

-- create the root task
create or replace task PIPELINE_START_TASK
  warehouse = BAKERY_WH
  schedule = '10 M'
as
  call SYSTEM$SEND_EMAIL(
    'PIPELINE_EMAIL_INT',
    'firstname.lastname@youremail.com', -- substitute you email address
    'Daily pipeline start',
    'The daily pipeline started at ' || current_timestamp || '.'
);

-- create a task that inserts the product data from the stream to the target table
create or replace task INSERT_PRODUCT_TASK
  warehouse = BAKERY_WH
  after PIPELINE_START_TASK
when
  system$stream_has_data('STG.PRODUCT_STREAM')
as
  insert into DWH.PRODUCT_TBL
  select product_id, product_name, category, 
    min_quantity, price, valid_from
  from STG.PRODUCT_STREAM
  where METADATA$ACTION = 'INSERT';

-- create a task that inserts the partner data from the stream to the target table
create or replace task INSERT_PARTNER_TASK
  warehouse = BAKERY_WH
  after PIPELINE_START_TASK
when
  system$stream_has_data('STG.PARTNER_STREAM')
as
  insert into DWH.PARTNER_TBL
  select partner_id, partner_name, address, rating, valid_from
  from PARTNER_STREAM
  where METADATA$ACTION = 'INSERT';

-- create the finalizer task
create task PIPELINE_END_TASK
  warehouse = BAKERY_WH
  finalize = PIPELINE_START_TASK
as
  call SYSTEM$SEND_EMAIL(
    'PIPELINE_EMAIL_INT',
    'firstname.lastname@youremail.com', -- substitute you email address
    'Daily pipeline end',
    'The daily pipeline finished at ' || current_timestamp || '.'
);

-- modify the COPY_ORDERS_TASK to remove the schedule and to run after the PIPELINE_START_TASK
alter task COPY_ORDERS_TASK suspend;
alter task COPY_ORDERS_TASK unset schedule;
alter task COPY_ORDERS_TASK 
  add after PIPELINE_START_TASK;

-- resume all tasks
alter task PIPELINE_END_TASK resume;
alter task INSERT_PRODUCT_TASK resume;
alter task INSERT_PARTNER_TASK resume;
alter task INSERT_ORDERS_STG_TASK resume;
alter task COPY_ORDERS_TASK resume;
alter task PIPELINE_START_TASK resume;

-- suspend the pipeline so it doesn't continue to consume resources and send emails
alter task PIPELINE_START_TASK suspend;
