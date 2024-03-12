-- grant the privilege to use Snowflake Cortex functions to the SYSADMIN role
use role ACCOUNTADMIN;
grant database role SNOWFLAKE.CORTEX_USER to role SYSADMIN;

use role SYSADMIN;
use warehouse BAKERY_WH;
use database BAKERY_DB;
use schema REVIEWS;

-- create this UDF as a simulation for REVIEW_SENTIMENT if not using OpenAI's API
-- the function returns the sentiment in the same JSON structure using the Snowflake Cortex SENTIMENT function
create or replace function REVIEW_SENTIMENT_SIMULATION(customer_review varchar)
returns variant
AS
$$
with cortex_sentiment as (select SNOWFLAKE.CORTEX.SENTIMENT(customer_review) as sentiment_num)
select parse_json('{"choices":[{
  "finish_reason": "stop",
  "index": 0,
  "logprobs": null,
  "message": {
  "content": "' ||
  case when sentiment_num < -0.1 then 'negative'
    when sentiment_num > 0.1 then 'positive'
    else 'unknown' 
    end ||
  '","role": "assistant"}}]}')  
from cortex_sentiment
$$;