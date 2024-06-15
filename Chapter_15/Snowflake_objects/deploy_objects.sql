-- EXT schema
execute immediate from '../databases/bakery_db/schemas/ext/stages/Create_JSON_ORDERS_STAGE.sql';
execute immediate from '../databases/bakery_db/schemas/ext/tables/Create_JSON_ORDERS_EXT.sql';
execute immediate from '../databases/bakery_db/schemas/ext/streams/Create_JSON_ORDERS_STREAM.sql';
-- STG schema






-- snow sql -q "alter git repository ADMIN_DB.GIT_INTEGRATION.SF_DE_IA fetch"
-- snow sql -q "execute immediate from @SF_DE_IA/branches/wip/Chapter_15/Snowflake_objects/deploy_objects.sql"