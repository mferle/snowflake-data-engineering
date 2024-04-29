use role DATA_ENGINEER;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema DWH;

-- create a table in the data warehouse layer and populate initially with the data from the staging layer
create table PARTNERS_TBL as select * from STG.PARTNERS;
select * from PARTNERS_TBL;

-- create a stream on the table in the staging layer
use schema STG;
create stream PARTNERS_STREAM on table PARTNERS;

-- make some changes in the staging table: one update
update PARTNERS
  set rating = 'A', valid_from = '2023-08-08'
  where partner_id = 103;
  
-- view the contents of the stream
select * from PARTNERS_STREAM;

-- consume the stream by inserting into the target table
insert into DWH.PARTNERS_TBL
select partner_id, partner_name, address, rating, valid_from
from PARTNERS_STREAM
where METADATA$ACTION = 'INSERT';

-- check that the stream is now empty
select * from PARTNERS_STREAM;

-- view data in the target table
select * from DWH.PARTNERS_TBL;

-- create a view in the data warehouse layer that calculates the end timestamp of the validity interval
create view DWH.PARTNERS_VALID_TS as
select 
  partner_id, 
  partner_name, 
  address, 
  rating,
  valid_from,
  NVL(
    LEAD(valid_from) over (partition by partner_id order by valid_from),
    '9999-12-31'
  ) as valid_to
from DWH.PARTNERS_TBL
order by partner_id;

select * from DWH.PARTNERS_VALID_TS;
