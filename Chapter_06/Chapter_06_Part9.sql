-- view the data in the ORDERS_STG staging table
use schema SNOWPARK;
select * from ORDERS_STG;

--Listing 6.15
-- join the orders table with the date dimension using SQL
use schema SNOWPARK;
select 
  customer, order_date, delivery_date, baked_good_type, quantity, 
  day, holiday_flg
from ORDERS_STG
left join DIM_DATE
  on delivery_date = day;

-- view the data in the ORDERS_HOLIDAY_FLG view
use schema SNOWPARK;
select * from ORDERS_HOLIDAY_FLG;
