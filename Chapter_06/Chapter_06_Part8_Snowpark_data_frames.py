# import Session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import json package for reading connection parameters
import json

# establish a connection with Snowflake

# read the credentials from a file
credentials = json.load(open('connection_parameters.json'))
# create a dictionary with the connection parameters
connection_parameters_dict = {
    "account": credentials["account"],
    "user": credentials["user"],
    "password": credentials["password"],
    "role": credentials["role"],
    "warehouse": credentials["warehouse"],
    "database": credentials["database"],
    "schema": credentials["schema"]  # optional
}  

# create a session object for the Snowpark session
my_session = Session.builder.configs(connection_parameters_dict).create()

# retrieve tables into data frames
df_orders = my_session.table("ORDERS_STG")
df_dim_date = my_session.table("DIM_DATE")

# join the data frames
df_orders_with_holiday_flg = df_orders.join(df_dim_date, df_orders.delivery_date == df_dim_date.day, 'left')

# create a view from the joined data frames
df_orders_with_holiday_flg.create_or_replace_view("ORDERS_HOLIDAY_FLG")

# close the Snowpark session
my_session.close()
