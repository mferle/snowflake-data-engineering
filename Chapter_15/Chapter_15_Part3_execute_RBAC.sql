-- execute the RBAC script using the SYSADMIN role
use role SYSADMIN;
use database ADMIN_DB;
use schema GIT_INTEGRATION;

alter git repository SF_DE fetch;

execute immediate from @SF_DE/branches/main/Chapter_15/Chapter_15_Part2_RBAC.sql;

-- grant the functional roles to the users who perform those business functions
-- in this exercise we grant both functional roles to ourself to be able to test them
use role USERADMIN;
grant role DATA_ENGINEER to user <your username>;
grant role DATA_ANALYST to user <your username>;

-- switch to the DATA_ENGINEER role and verify that you can list files from the Git repository stage
use role DATA_ENGINEER;
use database ADMIN_DB;
use schema GIT_INTEGRATION;
ls @SF_DE/branches/main;
