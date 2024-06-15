-- create a staging table in the STG schema that will store the flattened semi-structured data from the extraction layer
create or alter table STG.JSON_ORDERS_TBL_STG (
  customer varchar,
  order_date date,
  delivery_date date,
  baked_good_type varchar,
  quantity number,
  source_file_name varchar,
  load_ts timestamp
);