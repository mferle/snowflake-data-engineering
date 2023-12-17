#Listing 6.4
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

# create the session
my_session = Session.builder.configs(connection_parameters_dict).create()

# select the current timestamp
ts = my_session.sql("select current_timestamp()").collect()
# print the output to the console
print(ts)

# close the session
my_session.close()
