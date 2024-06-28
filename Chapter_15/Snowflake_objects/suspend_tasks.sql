use database BAKERY_DB;
-- suspend the pipeline so it doesn't continue to consume resources and send emails
alter task ORCHESTRATION.PIPELINE_START_TASK suspend;

-- snow sql -q "alter git repository ADMIN_DB.GIT_INTEGRATION.SF_DE fetch"
-- snow sql -q "execute immediate from @ADMIN_DB.GIT_INTEGRATION.SF_DE/branches/main/Chapter_15/Snowflake_objects/suspend_tasks.sql"