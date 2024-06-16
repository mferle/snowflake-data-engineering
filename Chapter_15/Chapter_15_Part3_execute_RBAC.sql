-- execute the RBAC script using the SYSADMIN role
use role SYSADMIN;
use database ADMIN_DB;
use schema GIT_INTEGRATION;

alter git repository SF_DE_IA fetch;

execute immediate from @SF_DE_IA/branches/main/Chapter_15/Snowflake_scripts/RBAC_setup.sql;