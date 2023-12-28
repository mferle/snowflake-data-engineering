-- use the SYSADMIN role and the REVIEwS schema in the BAKERY_DB database
use role SYSADMIN;
use schema REVIEWS;

-- create a network rule
create network_rule OPENAI_API_NETWORK_RULE
  mode = EGRESS
  type = HOST_PORT
  value_list = ('api.openai.com');

-- create a secret
create secret OPENAI_API_TOKEN
  type = GENERIC_STRING
  secret_string = 'ghCD...GOwy';

-- create an external access integration
create external access integration OPENAI_API_INTEGRATION
allowed_network_rules = (OPENAI_API_NETWORK_RULE)
allowed_authentication_secrets = (OPENAI_API_TOKEN)
enabled = TRUE;

-- create a UDF that calls the API endpoint using the external access integration and the secret
--Listing 7.6 
create or replace function REVIEW_SENTIMENT(customer_review varchar)
returns variant
language python
runtime_version = 3.10
handler = 'get_sentiment'
external_access_integrations = (OPENAI_API_INTEGRATION)
secrets = ('openai_api_token' = OPENAI_API_TOKEN)
packages = ('requests')
AS
--Listing 7.7
$$
import _snowflake
import requests

def get_sentiment(customer_review):
  api_key = _snowflake.get_generic_secret_string('openai_api_token')
  
  url = 'https://api.openai.com/v1/chat/completions'
  
  prompt = 'Given a customer review, classify it as positive or negative. \
    If in doubt, classify it as unknown. \
    Return a value from this list: [positive, negative, unknown]. \
    This is the review: ' + customer_review

  data = '{"model": "gpt-3.5-turbo", \
    "messages": [{"role": "user", "content": "' + prompt + '"}], \
    "temperature": 0.7}'
  
  response = requests.post(
    url = url, 
    headers = {'Authorization': 'Bearer ' + api_key, 
    'Content-Type': 'application/json'}, 
    data = data)

  return response.json()
$$;

-- test the UDF by using it in a SELECT statement and providing a parameter
select REVIEW_SENTIMENT('The service was excellent!');

-- extract the sentiment from the JSON and test with different parameters
select REVIEW_SENTIMENT('The service was excellent!'):"choices"[0]."message"."content"::varchar as sentiment;
select REVIEW_SENTIMENT('The bagel was stale.'):"choices"[0]."message"."content"::varchar as sentiment;
select REVIEW_SENTIMENT('It was raining all day.'):"choices"[0]."message"."content"::varchar as sentiment;

-- use the UDF when selecting from the CUSTOMER_REVIEWS table
select 
  customer_review,
  REVIEW_SENTIMENT(
    customer_review
  ):"choices"[0]."message"."content"::varchar as sentiment
from CUSTOMER_REVIEWS;
