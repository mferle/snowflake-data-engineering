-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
create warehouse if not exists BAKERY_WH with warehouse_size = 'XSMALL';
create database if not exists BAKERY_DB;
use database BAKERY_DB;
create schema REVIEWS;
use schema REVIEWS;

-- use role ACCOUNTADMIN to grant privilege
use role ACCOUNTADMIN;
-- grant CREATE_NETWORK RULE, CREATE SECRET, and CREATE INTEGRATION privileges to role SYSADMIN
grant create network rule on schema REVIEWS to role SYSADMIN;
grant create secret on schema REVIEWS to role SYSADMIN;
grant create integration on account to role SYSADMIN;
-- switch back to the SYSADMIN role
use role SYSADMIN;

-- create a network rule
use role SYSADMIN;
create network rule YELP_API_NETWORK_RULE
  mode = EGRESS
  type = HOST_PORT
  value_list = ('api.yelp.com');

-- create a secret
create secret YELP_API_TOKEN
  type = GENERIC_STRING
  secret_string = 'ab12DE...89XYZ';

-- grant usage on the secret to a custom role if that role will be using the secret 
grant usage on secret YELP_API_TOKEN to role <custom_role>;

-- create an external access integration
create external access integration YELP_API_INTEGRATION
  allowed_network_rules = (YELP_API_NETWORK_RULE)
  allowed_authentication_secrets = (YELP_API_TOKEN)
  enabled = TRUE;

-- create a UDF that calls the API endpoint using the external access integration and the secret
--Listing 7.1
create or replace function GET_CUSTOMER_REVIEWS(business_alias varchar)
returns variant
language python
runtime_version = 3.10
handler = 'get_reviews'
external_access_integrations = (YELP_API_INTEGRATION)
secrets = ('yelp_api_token' = YELP_API_TOKEN)
packages = ('requests')
AS
--Listing 7.2
$$
import _snowflake
import requests

def get_reviews(business_alias):
  api_key = _snowflake.get_generic_secret_string('yelp_api_token')
  url = f'''https://api.yelp.com/v3/businesses/{business_alias}/reviews'''
  response = requests.get(
    url=url, 
    headers = {'Authorization': 'Bearer ' + api_key}
  )
  return response.json()
$$;

-- select from the UDF
select GET_CUSTOMER_REVIEWS('boulangerie-julien-paris-3');

-- select the value from the "reviews key"
--Listing 7.3
select GET_CUSTOMER_REVIEWS('boulangerie-julien-paris-3'):"reviews";

-- flatten the values of the "rating", "time_created", and "text" keys
--Listing 7.5
select 
  value:"rating"::number as rating, 
  value:"time_created"::timestamp as time_created, 
  value:"text"::varchar as customer_review
from table(flatten(input => 
  GET_CUSTOMER_REVIEWS('boulangerie-julien-paris-3'):"reviews"
));

-- create a table to store the customer reviews
use schema REVIEWS;
create table CUSTOMER_REVIEWS (
  rating number,
  time_created timestamp,
  customer_review varchar
);

-- insert the result of the previous query into the table
insert into CUSTOMER_REVIEWS
select 
  value:"rating"::number as rating, 
  value:"time_created"::timestamp as time_created, 
  regexp_replace(value:"text"::varchar, 
    '[^a-zA-Z0-9 .,!?-]+')::varchar as customer_review
from table(flatten(
  input => GET_CUSTOMER_REVIEWS('boulangerie-julien-paris-3'):"reviews"
));


-- select data from the table
select * from CUSTOMER_REVIEWS;