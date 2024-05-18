use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- create views PARTNER and PRODUCT in the DWH schema that select data from the STG schema
create view PARTNER as
select partner_id, partner_name, address, rating
from STG.PARTNER;

create view PRODUCT as
select product_id, product_name, category, min_quantity, price, valid_from
from STG.PRODUCT;

-- create view ORDERS in the DWH schema that adds primary keys from the PARTNER and PRODUCT tables
create view ORDERS as
select PT.partner_id, PRD.product_id, ORD.delivery_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_STG ORD
inner join STG.PARTNER PT
  on PT.partner_name = ORD.customer
inner join STG.PRODUCT PRD
  on PRD.product_name = ORD.baked_good_type;

select * from ORDERS;
