-- create the extract table for the orders in raw (json) format
create or alter table EXT.JSON_ORDERS_EXT (
  customer_orders variant,
  source_file_name varchar,
  load_ts timestamp
);