use database BAKERY_DB;
use schema STG;

-- create a stream on the PRODUCT table
create stream PRODUCT_STREAM on table PRODUCT;