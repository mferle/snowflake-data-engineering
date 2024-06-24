-- create the finalizer task
create or replace task BAKERY_DB.ORCHESTRATION.PIPELINE_END_TASK
  warehouse = BAKERY_WH
  finalize = BAKERY_DB.ORCHESTRATION.PIPELINE_START_TASK
as
  call SYSTEM$SEND_EMAIL(
    'PIPELINE_EMAIL_INT',
    'firstname.lastname@youremail.com', -- substitute you email address
    'Daily pipeline end',
    'The daily pipeline finished at ' || current_timestamp || '.'
);