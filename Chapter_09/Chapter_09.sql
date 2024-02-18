-- This chapter assumes that you have the “Sample Harmonized Data for Top CPG Retailers and Distributors” listing from the Snowflake Marketplace (see Chapter 8)

-- use the RETAIL_ANALYSIS schema in the BAKERY_DB database (see Chapter 8)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema if not exists RETAIL_ANALYSIS;
use schema RETAIL_ANALYSIS;

-- you must have the RETAILER_SALES table (see Chapter 8)

-- create a set of virtual warehouses in increasing sizes
create warehouse BAKERY_WH_XSMALL with warehouse_size = 'xsmall';
create warehouse BAKERY_WH_SMALL with warehouse_size = 'small';
create warehouse BAKERY_WH_MEDIUM with warehouse_size = 'medium';
create warehouse BAKERY_WH_LARGE with warehouse_size = 'large';

-- construct a complex query that 
-- - selects the total sold quantity of each product in each store
-- - adds a condition to include only stores which sell more than 100 distinct products
-- - sorts the results by the distance
-- Listing 9.1
select 
  store_id, 
  distance_km, 
  product_id, 
  sum(sales_quantity) as total_quantity
from RETAILER_SALES
where store_id in (
  select store_id 
  from (
    select store_id, 
      count(distinct product_id) as product_cnt
    from RETAILER_SALES
    group by store_id
    having product_cnt > 100
  )
)
group by store_id, distance_km, product_id
order by distance_km;

-- use the extra small warehouse
use warehouse BAKERY_WH_XSMALL;
-- execute the query above (Listing 9.1)
-- open the query profile after executing
-- take note of the Total execution time and Bytes spilled to local storage statistics

-- use the small warehouse
use warehouse BAKERY_WH_SMALL;
-- execute the query above (Listing 9.1)
-- open the query profile after executing
-- notice that the query results were reused

-- set the session so that it doesn't reuse query results - for testing only
alter session set use_cached_result = false;

-- still using the small warehouse, execute the query above (Listing 9.1)
-- open the query profile after executing
-- take note of the Total execution time and Bytes spilled to local storage statistics

-- use the medium warehouse
use warehouse BAKERY_WH_MEDIUM;
-- execute the query above (Listing 9.1)
-- open the query profile after executing
-- take note of the Total execution time and Bytes spilled to local storage statistics

-- use the large warehouse
use warehouse BAKERY_WH_LARGE;
-- execute the query above (Listing 9.1)
-- open the query profile after executing
-- take note of the Total execution time and Bytes spilled to local storage statistics


-- reduce spilling
-- use the extra small warehouse
use warehouse BAKERY_WH_XSMALL;

-- add filter to the previous query (Listing 9.1) to select only stores that within 1000 km
select 
  store_id, 
  distance_km, 
  product_id, 
  sum(sales_quantity) as total_quantity
from RETAILER_SALES
where store_id in (
  select store_id 
  from (
    select store_id, 
      count(distinct product_id) as product_cnt
    from RETAILER_SALES
    group by store_id
    having product_cnt > 100
  )
)
and distance_km < 1000
group by store_id, distance_km, product_id
order by distance_km;

-- open the query profile after executing
-- examine the Bytes spilled to local storage statistic - it should be less than before adding the filter

-- count the number of records
select count(*) from RETAILER_SALES;
-- open the query profile after executing
-- note that it is a metadata operation