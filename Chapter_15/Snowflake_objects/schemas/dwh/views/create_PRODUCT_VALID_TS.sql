-- create a view in the data warehouse layer that calculates the end timestamp of the validity interval
create or replace view DWH.PRODUCT_VALID_TS as
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