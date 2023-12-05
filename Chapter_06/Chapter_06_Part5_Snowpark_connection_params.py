# import Session from the snowflake.snowpark package
from snowflake.snowpark import Session

import json

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

my_session = Session.builder.configs(connection_parameters_dict).create()

ts = my_session.sql("select current_timestamp()").collect()
print(ts)

my_session.close()
