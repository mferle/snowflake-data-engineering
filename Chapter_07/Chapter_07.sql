-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema SNOWPARK;
use schema SNOWPARK;


--https://api.yelp.com/v3/businesses/search
--https://api.yelp.com/v3/businesses/augustiner-keller-münchen-2/reviews
--https://api.yelp.com/v3/businesses/boulangerie-julien-paris-3/reviews
