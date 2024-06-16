-- create a table in the data warehouse layer and populate initially with the data from the staging layer
create or replace table DWH.PRODUCT_TBL as 
select * from STG.PRODUCT;