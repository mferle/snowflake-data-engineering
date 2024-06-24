use database BAKERY_DB;
-- suspend the pipeline so it doesn't continue to consume resources and send emails
alter task ORCHESTRATION.PIPELINE_START_TASK suspend;
