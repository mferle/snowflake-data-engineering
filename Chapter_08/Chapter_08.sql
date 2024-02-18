use role ACCOUNTADMIN;
-- Get the “Sample Harmonized Data for Top CPG Retailers and Distributors” listing from the Snowflake Marketplace
-- Save the listing into a database named CPG_RETAILERS_AND_DISTRIBUTORS

-- grant usage on the database to the SYSADMIN role if you haven't granted it already when getting the data
grant imported privileges on database CPG_RETAILERS_AND_DISTRIBUTORS to role SYSADMIN;

-- query that selects individual stores, converts their latitude and longitude into a geography data type
-- and calculates the distance in kilometers from Dayton, Ohio
-- results are limited to 5 rows for better performance while developing the query
select distinct
  store_id, 
  store_latitude,
  store_longitude, 
  TO_GEOGRAPHY(
    'Point('||store_longitude||' '||store_latitude||')'
  ) as store_loc_geo,
  ST_DISTANCE(
    TO_GEOGRAPHY('Point(-84.19 39.76)'), store_loc_geo
  )/1000 as distance_km
from CPG_RETAILERS_AND_DISTRIBUTORS.PUBLIC.HARMONIZED_RETAILER_SALES
limit 5;

-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
use warehouse BAKERY_WH;
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema RETAIL_ANALYSIS;
use schema RETAIL_ANALYSIS;

-- create a table by selecting all columns from HARMONIZED_RETAIL_SALES from the shared database
-- adding each store’s location in a geography data type and the distance between the store and Dayton, Ohio
create table RETAILER_SALES as 
select *, 
  TO_GEOGRAPHY(
    'Point('||store_longitude||' '||store_latitude||')'
  ) as store_loc_geo,
  ST_DISTANCE(
    TO_GEOGRAPHY('Point(-84.19 39.76)'), store_loc_geo
  )/1000 as distance_km
from CPG_RETAILERS_AND_DISTRIBUTORS.PUBLIC.HARMONIZED_RETAILER_SALES;

-- select top 100 stores that are closest to the bakery's location
select distinct 
  store_id, 
  distance_km
from RETAILER_SALES
order by distance_km
limit 100;

-- chose store_id 392366678147865718 to perform further analysis

-- select each product sold in the chosen store and the total quantity sold
-- Listing 8.1
select 
  product_id, 
  sum(sales_quantity) as tot_quantity
from RETAILER_SALES
where store_id = 392366678147865718
group by product_id;

-- count the rows in the entire table and the number of filtered rows
select 
  'Total rows' as filtering_type, 
  count(*) as row_cnt 
from retailer_sales
union all
select 
  'Filtered rows' as filtering_type, 
  count(*) as row_cnt 
from retailer_sales 
where store_id = 392366678147865718;

-- view clustering information
select SYSTEM$CLUSTERING_INFORMATION('retailer_sales', '(store_id)');

-- add a clustering key
alter table RETAILER_SALES cluster by (store_id);

-- monitor the clustering process
-- grant privilege first
use role ACCOUNTADMIN;
grant MONITOR USAGE ON ACCOUNT to role SYSADMIN;
use role SYSADMIN;
select *
  from table(information_schema.automatic_clustering_history(
  date_range_start=>dateadd(D, -1, current_date),
  table_name=>'BAKERY_DB.RETAIL_ANALYSIS.RETAILER_SALES'));

-- execute the query from Listing 8.1 again
select 
  product_id, 
  sum(sales_quantity) as tot_quantity
from RETAILER_SALES
where store_id = 392366678147865718
group by product_id;

-- view the clustering information again
select SYSTEM$CLUSTERING_INFORMATION('retailer_sales', '(store_id)');

-- sum of the sold quantity of a chosen product in each store
-- Listing 8.4
select store_id, sum(sales_quantity) as tot_quantity
from RETAILER_SALES
where product_id = 4120371332641752996
group by store_id;

-- add search optimization
alter table RETAILER_SALES add search optimization on equality(product_id);

-- view the search optimization parameters
show tables like 'RETAILER_SALES';

-- grant the GOVERNANCE_VIEWER database role to SYSADMIN
use role ACCOUNTADMIN;
grant database role SNOWFLAKE.GOVERNANCE_VIEWER to role SYSADMIN;
use role SYSADMIN;

-- select the longest running queries from query history
select 
  query_id, 
  query_text, 
  partitions_scanned, 
  partitions_total, 
  total_elapsed_time
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where TO_DATE(start_time) > DATEADD(day,-1,TO_DATE(CURRENT_TIMESTAMP()))
order by total_elapsed_time desc
limit 50;
