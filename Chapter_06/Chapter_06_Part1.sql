-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema SNOWPARK;
use schema SNOWPARK;

-- Section 6.5	Ingesting Data from a CSV File into a Snowflake Table
create stage ORDERS_STAGE;
