use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema STG;

-- create tables in the STG schema, simulating tables populated from the source system using a data integration tool or custom solution
create or alter table PARTNER (
partner_id integer,
partner_name varchar,
address varchar,
rating varchar,
valid_from date
);