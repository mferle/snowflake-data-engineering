use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- create views PARTNERS and PRODUCTS in the DWH schema that select data from the STG schema
create view PARTNERS as
select partner_id, partner_name, address, rating
from STG.PARTNERS;

create view PRODUCTS as
select product_id, product_name, category, min_quantity, price, valid_from
from STG.PRODUCTS;

-- create view ORDERS in the DWH schema that adds primary keys from the PARTNERS and PRODUCTS tables
create view ORDERS as
select PT.partner_id, PRD.product_id, ORD.delivery_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_STG ORD
left join STG.PARTNERS PT
  on PT.partner_name = ORD.customer
left join STG.PRODUCTS PRD
  on PRD.product_name = ORD.baked_good_type;

select * from ORDERS;
