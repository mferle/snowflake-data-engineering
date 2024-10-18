-- grant the SNOWFLAKE.CORTEX_USER to the SYSADMIN role
use role ACCOUNTADMIN;
grant database role SNOWFLAKE.CORTEX_USER to role SYSADMIN;

-- use the SYSADMIN role and the REVIEWS schema in the BAKERY_DB database
use role SYSADMIN;
use database BAKERY_DB;
use schema REVIEWS;

-- get the sentiment score from different examples of text
select SNOWFLAKE.CORTEX.SENTIMENT('The service was excellent!');
select SNOWFLAKE.CORTEX.SENTIMENT('The bagel was stale.');
select SNOWFLAKE.CORTEX.SENTIMENT('I went to the bakery for lunch.');

-- map the sentiment score to Positive, Negative, and Neutral
select 
  rating,
  time_created,
  customer_review,
  SNOWFLAKE.CORTEX.SENTIMENT(customer_review) as sentiment_score,
  case
    when sentiment_score < -0.7 then 'Negative'
    when sentiment_score < 0.4 then 'Neutral'
    else 'Positive'
  end as sentiment
from CUSTOMER_REVIEWS;
