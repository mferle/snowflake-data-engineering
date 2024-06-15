use database {{curr_db_name}};
-- EXT schema
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/ext/stages/Create_JSON_ORDERS_STAGE.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/ext/tables/Create_JSON_ORDERS_EXT.sql';
execute immediate from '../Snowflake_objects/databases/bakery_db/schemas/ext/streams/Create_JSON_ORDERS_STREAM.sql';
-- STG schema






-- snow sql -q "alter git repository ADMIN_DB.GIT_INTEGRATION.SF_DE_IA fetch"
-- snow sql --database ADMIN_DB --schema GIT_INTEGRATION -q "execute immediate from @SF_DE_IA/branches/wip/Chapter_15/Snowflake_objects/deploy_objects.sql using (curr_db_name => 'BAKERY_DB') using (curr_db_name => 'BAKERY_DB')"