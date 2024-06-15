-- create a stream on the JSON_ORDERS_EXT table
create or replace stream EXT.JSON_ORDERS_STREAM 
on table EXT.JSON_ORDERS_EXT;
