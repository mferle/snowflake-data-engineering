-- create an external stage named JSON_ORDERS_STAGE using the PARK_INN_INTEGRATION as described in Chapter 4
-- be sure to create the external stage with the JSON file format, eg. file_format = (type = json)

--create stage EXT.JSON_ORDERS_STAGE
--  storage_integration = PARK_INN_INTEGRATION
--  url = 'azure://bakeryorders001.blob.core.windows.net/orderfiles'
--  file_format = (type = json);

-- quick alternative if you don't have the storage integration object: create an internal stage
create or replace stage EXT.JSON_ORDERS_STAGE file_format = (type = json);