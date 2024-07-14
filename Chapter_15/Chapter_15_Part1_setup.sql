use role SYSADMIN;
create database ADMIN_DB;
create schema GIT_INTEGRATION;

-- continue to use the SYSADMIN role to keep the exercise simple
-- otherwise, create a custom role and grant it the CREATE SECRET ON SCHEMA privilege
-- create the git secret
create or replace secret GIT_SECRET
  type = password
  username = <your Git username>
  password = <your password>
;

-- use the ACCOUNTADMIN role to create an API integration to keep the exercise simple
-- otherwise, create a custom role and grant it the CREATE INTEGRATION ON ACCOUNT privilege
use role ACCOUNTADMIN;
-- create an API integration with the Git account
create or replace api integration GIT_API_INTEGRATION
  API_PROVIDER = git_https_api
-- replace your Git account in the next line (example 'https://github.com/mferle')
  API_ALLOWED_PREFIXES = ('https://<your Git host>/<your Git account>')
  ALLOWED_AUTHENTICATION_SECRETS = (GIT_SECRET)
  ENABLED = TRUE;

-- grant usage on the integration to the SYSADMIN role
grant usage on integration GIT_API_INTEGRATION to role SYSADMIN;

-- continue to use the SYSADMIN role to keep the exercise simple
-- otherwise, create a custom role and grant it the CREATE GIT REPOSITORY ON SCHEMA privilege
use role SYSADMIN;
-- create a Git repository using the API integration and the secret
create or replace git repository SF_DE
  api_integration = GIT_API_INTEGRATION
  git_credentials = GIT_SECRET
-- replace the URL to your repository in the next line
  ORIGIN = 'https://<your Git host>/<your Git account>/snowflake-data-engineering.git'; 

-- fetch the latest from the Git repository
alter git repository SF_DE fetch;

-- show branches in the Git repository
show git branches in SF_DE;

-- list the files in the repository main branch
ls @SF_DE/branches/main;
