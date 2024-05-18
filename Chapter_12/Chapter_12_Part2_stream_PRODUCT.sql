use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- create a table in the data warehouse layer and populate initially with the data from the staging layer
create table PRODUCT_TBL as select * from STG.PRODUCT;
select * from PRODUCT_TBL;

-- create a stream on the table in the staging layer
use schema STG;
create stream PRODUCT_STREAM on table PRODUCT;

-- make some changes in the staging table: one update and one insert
update PRODUCT
  set category = 'Pastry', valid_from = '2023-08-08'
  where product_id = 3;
  
insert into PRODUCT values
  (13, 'Sourdough Bread', 'Bread', 1, 3.6, '2023-08-08');

-- view the contents of the stream
select * from PRODUCT_STREAM;

-- consume the stream by inserting into the target table
insert into DWH.PRODUCT_TBL
select product_id, product_name, category, min_quantity, price, valid_from
from PRODUCT_STREAM
where METADATA$ACTION = 'INSERT';

-- check that the stream is now empty
select * from PRODUCT_STREAM;

-- view data in the target table
select * from DWH.PRODUCT_TBL;

-- create a view in the data warehouse layer that calculates the end timestamp of the validity interval
create view DWH.PRODUCT_VALID_TS as
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
from DWH.PRODUCT_TBL
order by product_id;

select * from DWH.PRODUCT_VALID_TS;
