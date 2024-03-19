use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema MGMT;

-- create view in the MGMT schema that summarizes orders by delivery date and baked good type, adding the baked good category
create view ORDERS_SUMMARY as
select ORD.delivery_date, PRD.product_name, PRD.category, sum(ORD.quantity) as total_quantity
from dwh.ORDERS ORD
left join dwh.PRODUCTS PRD
on ORD.product_id = PRD.product_id
group by all;

-- use the DATA_ANALYST role to select data from the summary view
use role DATA_ANALYST;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema MGMT;

select * from ORDERS_SUMMARY;
