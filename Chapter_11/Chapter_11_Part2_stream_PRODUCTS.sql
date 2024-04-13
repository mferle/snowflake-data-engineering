use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- create a table in the data warehouse layer and populate initially with the data from the staging layer
create table PRODUCTS_TBL as select * from STG.PRODUCTS;
select * from PRODUCTS_TBL;

-- create a stream on the table in the staging layer
use schema STG;
create stream PRODUCTS_STREAM on table PRODUCTS;

-- make some changes in the staging table: one update and one insert
update PRODUCTS
  set category = 'Pastry', valid_from = '2023-08-08'
  where product_id = 3;
  
insert into PRODUCTS values
  (13, 'Sourdough Bread', 'Bread', 1, 3.6, '2023-08-08');

-- view the contents of the stream
select * from PRODUCTS_STREAM;

-- consume the stream by inserting into the target table
insert into DWH.PRODUCTS_TBL
select product_id, product_name, category, min_quantity, price, valid_from
from PRODUCTS_STREAM
where METADATA$ACTION = 'INSERT';

-- check that the stream is now empty
select * from PRODUCTS_STREAM;

-- view data in the target table
select * from DWH.PRODUCTS_TBL;

-- create a view in the data warehouse layer that calculates the end timestamp of the validity interval
create view DWH.PRODUCTS_VALID_TS as
select 
  product_id, 
  product_name, 
  category, 
  min_quantity,
  price,
  valid_from,
  NVL(
    LEAD(valid_from) over (partition by product_id order by valid_from),
    '9999-12-31'
  ) as valid_to
from DWH.PRODUCTS_TBL
order by product_id;

select * from DWH.PRODUCTS_VALID_TS;
