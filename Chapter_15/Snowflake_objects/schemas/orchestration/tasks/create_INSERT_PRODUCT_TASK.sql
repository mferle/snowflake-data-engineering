use database {{curr_db_name}};

-- create a task that inserts the product data from the stream to the target table
create or replace task ORCHESTRATION.INSERT_PRODUCT_TASK
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