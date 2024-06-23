-- execute the RBAC script using the SYSADMIN role
use role SYSADMIN;
use database ADMIN_DB;
use schema GIT_INTEGRATION;

alter git repository SF_DE_IA fetch;

execute immediate from @SF_DE_IA/branches/main/Chapter_15/Chapter_15_Part2_RBAC.sql;

-- grant the functional roles to the users who perform those business functions
-- in this exercise we grant both functional roles to our current user to be able to test them

grant role DATA_ENGINEER to user <your username>;
grant role DATA_ANALYST to user <your username>;