use database {{curr_db_name}};
-- EXT schema
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/ext/stages/create_JSON_ORDERS_STAGE.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/ext/tables/create_JSON_ORDERS_EXT.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/ext/streams/create_JSON_ORDERS_STREAM.sql';

-- STG schema
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/stg/tables/create_JSON_ORDERS_TBL_STG.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/stg/tables/create_PARTNER.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/stg/tables/create_PRODUCT.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/stg/streams/create_PARTNER_STREAM.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/stg/streams/create_PRODUCT_STREAM.sql';

-- DWH schema
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/dwh/tables/create_PARTNER_TBL.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/dwh/tables/create_PRODUCT_TBL.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/dwh/dynamic_tables/create_ORDERS_TBL.sql';

-- MGMT schema
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/mgmt/dynamic_tables/create_ORDERS_SUMMARY_TBL.sql';

-- ORCHESTRATION schema
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/orchestration/tasks/create_PIPELINE_START_TASK.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/orchestration/tasks/create_COPY_ORDERS_TASK.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/orchestration/tasks/create_INSERT_ORDERS_STG_TASK.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/orchestration/tasks/create_INSERT_PARTNER_TASK.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/orchestration/tasks/create_INSERT_PRODUCT_TASK.sql';


-- snow sql -q "alter git repository ADMIN_DB.GIT_INTEGRATION.SF_DE_IA fetch"
-- snow sql --database ADMIN_DB --schema GIT_INTEGRATION -q "execute immediate from @SF_DE_IA/branches/wip/Chapter_15/Snowflake_objects/deploy_objects.sql using (curr_db_name => 'BAKERY_DB')"