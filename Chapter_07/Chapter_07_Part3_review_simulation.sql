use role SYSADMIN;
use schema REVIEWS;

-- create this UDF as a simulation for REVIEW_SENTIMENT if not using OpenAI's API
-- the function returns a random sentiment in the same JSON structure
create or replace function REVIEW_SENTIMENT_SIMULATION(customer_review varchar)
returns variant
AS
$$
with random_sentiment as (SELECT uniform(1, 3, random()) as sentiment_num)
select parse_json('{"choices":[{
  "finish_reason": "stop",
  "index": 0,
  "logprobs": null,
  "message": {
  "content": "' ||
  case when sentiment_num = 1 then 'positive'
    when sentiment_num = 2 then 'negative'
    else 'unknown' 
    end ||
  '","role": "assistant"}}]}')  
from random_sentiment
$$;