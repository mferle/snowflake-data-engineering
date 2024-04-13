-- normalize data in the data warehouse layer
use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- construct a query that adds primary keys from the PARTNERS and PRODUCTS tables to the orders
-- Listing 11.1.
select PT.partner_id, PRD.product_id, ORD.delivery_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_TBL_STG ORD
left join STG.PARTNERS PT
  on PT.partner_name = ORD.customer
left join STG.PRODUCTS PRD
  on PRD.product_name = ORD.baked_good_type;

-- create a dynamic table ORDERS_TBL in the DWH schema using the previous query
create dynamic table ORDERS_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select PT.partner_id, PRD.product_id, ORD.delivery_date, 
  ORD.order_date, ORD.quantity  
from STG.JSON_ORDERS_TBL_STG ORD
left join STG.PARTNERS PT
  on PT.partner_name = ORD.customer
left join STG.PRODUCTS PRD
  on PRD.product_name = ORD.baked_good_type;

select * from ORDERS_TBL;

-- summarize data for reporting
-- Listing 11.2.
select ORD.delivery_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from dwh.ORDERS_TBL ORD
left join dwh.PRODUCTS_TBL PRD
on ORD.product_id = PRD.product_id
group by all;

-- select products that are valid currently
select * from DWH.PRODUCTS_VALID_TS
where valid_to = '9999-12-31';

-- select products that were valid on August 1, 2023
select * from DWH.PRODUCTS_VALID_TS
where valid_from <= '2023-08-01' and valid_to > '2023-08-01';

-- summarize data for reporting by taking the product category that is valid currently
-- Listing 11.3.
select ORD.delivery_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from dwh.ORDERS_TBL ORD
left join (select * from dwh.PRODUCTS_VALID_TS where valid_to = '9999-12-31') PRD
on ORD.product_id = PRD.product_id
group by all;

use schema MGMT;
create dynamic table ORDERS_SUMMARY_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select ORD.delivery_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from dwh.ORDERS_TBL ORD
left join (select * from dwh.PRODUCTS_VALID_TS where valid_to = '9999-12-31') PRD
on ORD.product_id = PRD.product_id
group by all;

select * from ORDERS_SUMMARY_TBL;
