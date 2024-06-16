-- create a dynamic table in the MGMT schema that summarizes order information for reporting
create or replace dynamic table MGMT.ORDERS_SUMMARY_TBL
  target_lag = '1 minute'
  warehouse = BAKERY_WH
  as 
select ORD.delivery_date, PRD.product_name, PRD.category, 
  sum(ORD.quantity) as total_quantity
from DWH.ORDERS_TBL ORD
left join (select * from DWH.PRODUCT_VALID_TS where valid_to = '9999-12-31') PRD
on ORD.product_id = PRD.product_id
group by all;