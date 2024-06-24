-- create a task that performs the COPY INTO operation from the stage into the table
create or replace task BAKERY_DB.ORCHESTRATION.COPY_ORDERS_TASK
  warehouse = BAKERY_WH
  after BAKERY_DB.ORCHESTRATION.PIPELINE_START_TASK
as
  copy into EXT.JSON_ORDERS_EXT
  from (
    select 
      $1, 
      metadata$filename, 
      current_timestamp() 
    from @EXT.JSON_ORDERS_STAGE
  )
  on_error = abort_statement;