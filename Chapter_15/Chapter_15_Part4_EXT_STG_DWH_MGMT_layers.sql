-- -----------------------------------------------------------------------------------------
-- EXT layer
-- -----------------------------------------------------------------------------------------

-- create a storage integration object named PARK_INN_INTEGRATION as described in Chapter 4 
-- if you created the storage integration already in Chapter 4, no need to recreate it
-- grant usage on the storage integration object to the DATA_ENGINEER role
--use role ACCOUNTADMIN;
--grant usage on integration PARK_INN_INTEGRATION to role DATA_ENGINEER;

-- use the DATA_ENGINEER role going forward
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema EXT;

-- create an external stage named JSON_ORDERS_STAGE using the PARK_INN_INTEGRATION as described in Chapter 4
-- be sure to create the external stage with the JSON file format, eg. file_format = (type = json)

-- quick and dirty if you don't have the storage integration object: create an internal stage
create stage JSON_ORDERS_STAGE file_format = (type = json);

-- upload the json files Orders_2023-09-01.json and Orders_2023-09-04.json to the object storage location used in the stage

-- create the extract table for the orders in raw (json) format
create table JSON_ORDERS_EXT (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);

-- create a stream on the table
create stream JSON_ORDERS_STREAM 
on table JSON_ORDERS_EXT;

-- -----------------------------------------------------------------------------------------
-- STG layer
-- -----------------------------------------------------------------------------------------

use schema STG;

-- create a staging table in the STG schema that will store the flattened semi-structured data from the extraction layer
create table STG.JSON_ORDERS_TBL_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);

-- create tables in the STG schema, simulating tables populated from the source system using a data integration tool or custom solution
create table PARTNER (
partner_id integer,
partner_name varchar,
address varchar,
rating varchar,
valid_from date
);

insert into PARTNER values
(101, 'Coffee Pocket', '501 Courtney Wells', 'A', '2023-06-01'),
(102, 'Lily''s Coffee', '2825 Joshua Forest', 'A', '2023-06-01'),
(103, 'Crave Coffee', '538 Hayden Port', 'B', '2023-06-01'),
(104, 'Best Burgers', '790 Friedman Valley', 'A', '2023-06-01'),
(105, 'Page One Fast Food', '44864 Amber Walk', 'B', '2023-06-01'),
(106, 'Jimmy''s Diner', '2613 Scott Mountains', 'A', '2023-06-01'),
(107, 'Metro Fine Foods', '520 Castillo Valley', 'A', '2023-06-01'),
(108, 'New Bistro', '494 Terry Spurs', 'A', '2023-06-01'),
(109, 'Park Inn', '3692 Nelson Turnpike', 'A', '2023-06-01'),
(110, 'Chef Supplies', '870 Anthony Hill', 'A', '2023-06-01'),
(111, 'Farm Fresh', '23633 Melanie Ranch', 'A', '2023-06-01'),
(112, 'Murphy Mill', '700 Darren Centers', 'A', '2023-06-01');

-- create a stream on the table in the staging layer
create stream PRODUCT_STREAM on table PRODUCT;

create table PRODUCT (
product_id integer,
product_name varchar,
category varchar,
min_quantity integer,
price number(18,2),
valid_from date
);

insert into PRODUCT values
(1, 'Baguette', 'Bread', 2, 2.5, '2023-06-01'),
(2, 'Bagel', 'Bread', 6, 1.3, '2023-06-01'), 
(3, 'English Muffin', 'Bread', 6, 1.2, '2023-06-01'), 
(4, 'Croissant', 'Pastry', 4, 2.1, '2023-06-01'), 
(5, 'White Loaf', 'Bread', 1, 1.8, '2023-06-01'), 
(6, 'Hamburger Bun', 'Bread', 10, 0.9, '2023-06-01'), 
(7, 'Rye Loaf', 'Bread', 1, 3.2, '2023-06-01'), 
(8, 'Whole Wheat Loaf', 'Bread', 1, 2.8, '2023-06-01'), 
(9, 'Muffin', 'Pastry', 12, 3.0, '2023-06-01'), 
(10, 'Cinnamon Bun', 'Pastry', 6, 3.4, '2023-06-01'), 
(11, 'Blueberry Muffin', 'Pastry', 12, 3.6, '2023-06-01'), 
(12, 'Chocolate Muffin', 'Pastry', 12, 3.6, '2023-06-01'); 

-- create a stream on the table in the staging layer
create stream PARTNER_STREAM on table PARTNER;

-- -----------------------------------------------------------------------------------------
-- DWH layer
-- -----------------------------------------------------------------------------------------

use schema DWH;

-- create a table in the data warehouse layer and populate initially with the data from the staging layer
create table PRODUCT_TBL as select * from STG.PRODUCT;

-- create a table in the data warehouse layer and populate initially with the data from the staging layer
create table PARTNER_TBL as select * from STG.PARTNER;

-- create a dynamic table ORDERS_TBL in the DWH schema that normalizes the data from the STG schema
create dynamic table ORDERS_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select PT.partner_id, PRD.product_id, ORD.delivery_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_TBL_STG ORD
inner join STG.PARTNER PT
  on PT.partner_name = ORD.customer
inner join STG.PRODUCT PRD
  on PRD.product_name = ORD.baked_good_type;

-- -----------------------------------------------------------------------------------------
-- MGMT layer
-- -----------------------------------------------------------------------------------------
use schema MGMT;

create dynamic table ORDERS_SUMMARY_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select ORD.delivery_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from dwh.ORDERS_TBL ORD
left join (select * from dwh.PRODUCT_VALID_TS where valid_to = '9999-12-31') PRD
on ORD.product_id = PRD.product_id
group by all;