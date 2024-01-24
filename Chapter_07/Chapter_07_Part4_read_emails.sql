-- create a new schema in the BAKERY_DB database (see Chapter 2)
use role SYSADMIN;
use database BAKERY_DB;
use schema REVIEWS;

-- create a stored procedure that calls the API endpoint using the external access integration and the secret
-- it then converts the resulting CSV output into a data frame and saves it to a table
--Listing 7.8 
create or replace procedure READ_EMAIL_PROC(email_content varchar)
returns table()
language python
runtime_version = 3.10
handler = 'get_order_info_from_email'
external_access_integrations = (OPENAI_API_INTEGRATION)
secrets = ('openai_api_token' = OPENAI_API_TOKEN)
packages = ('requests', 'snowflake-snowpark-python')
AS
$$
import _snowflake
import requests
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, DateType, StringType, IntegerType

#Listing 7.10 
def get_order_info_from_email(session: snowpark.Session, email_content):

  api_key = _snowflake.get_generic_secret_string('openai_api_token')

#Listing 7.11
  url = 'https://api.openai.com/v1/chat/completions'

#Listing 7.12
  prompt = 'You are a bakery employee, reading customer emails asking for deliveries. \
Please read the email at the end of this text and extract information about the ordered items.  \
Format the information in CSV using the following columns: customer, order_date, delivery_date, item, and quantity. \
Format the date as YYYY-MM-DD. If no year is given, assume the current year. \
Use the current date in the format YYYY-MM-DD for the order date.  \
Items should be in this list: [white loaf, rye loaf, baguette, bagel, croissant, chocolate muffin, blueberry muffin].  \
The content of the email follows this line. ' + email_content

  data = '{"model": "gpt-3.5-turbo", \
    "messages": [{"role": "user", "content": "' + prompt + '"}], \
    "temperature": 0.3}'

  response = requests.post(
    url = url, 
    headers = {'Authorization': 'Bearer ' + api_key, 
    'Content-Type': 'application/json'}, 
    data = data)

  csv_output = response.json()["choices"][0]["message"]["content"]
  
  schema = StructType([ 
        StructField("CUSTOMER", StringType(), False),  
        StructField("ORDER_DATE", DateType(), False),  
        StructField("DELIVERY_DATE", DateType(), False), 
        StructField("ITEM", StringType(), False),  
        StructField("QUANTITY", IntegerType(), False)
    ])

  orders_df = session.create_dataframe([x.split(',') for x in csv_output.split("\n")][1:], schema)
  orders_df.write.mode("append").save_as_table('COLLECTED_ORDERS_FROM_EMAIL')
    
  return orders_df
$$;

-- execute the stored procedure and provide a sample of an email content
call READ_EMAIL_PROC('Hello, please deliver 6 loaves of white bread on Tuesday, September 5. On Wednesday, September 6, we need 16 bagels. Thanks, Lilys Coffee');

-- select from the table to verify that the csv was written to the table
select * from COLLECTED_ORDERS_FROM_EMAIL;

-- a few more sample email contents to test the stored procedure
call READ_EMAIL_PROC('Hi again. At Metro Fine Foods, we are renewing our order for Thursday, September 7. We need 20 baguettes, 16 croissants, and a dozen blueberry muffins. Have a nice day!');

call READ_EMAIL_PROC('Greetings! We loved your French bread last week. Please deliver 10 more tomorrow. Cheers from your friends at Page One Fast Food');

call READ_EMAIL_PROC('Do you deliver pizza? If so, send two this afternoon. If not, then some bagels should do. Best, Jimmys Diner');
