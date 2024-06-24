-- create a task that inserts the partner data from the stream to the target table
create or replace task BAKERY_DB.ORCHESTRATION.INSERT_PARTNER_TASK
  warehouse = BAKERY_WH
  after BAKERY_DB.ORCHESTRATION.PIPELINE_START_TASK
when
  system$stream_has_data('STG.PARTNER_STREAM')
as
  insert into DWH.PARTNER_TBL
  select partner_id, partner_name, address, rating, valid_from
  from PARTNER_STREAM
  where METADATA$ACTION = 'INSERT';
