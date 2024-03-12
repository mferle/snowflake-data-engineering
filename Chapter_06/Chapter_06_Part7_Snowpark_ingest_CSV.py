#Listing 6.11
# import Session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import data types from the snowflake.snowpark package
from snowflake.snowpark.types import StructType, StructField, DateType, StringType, DecimalType
# import json package for reading connection parameters
import json

# assign the source file name to a variable
source_file_name = 'Orders_2023-07-09.csv'

# establish a connection with Snowflake

#Refer to Listing 6.4
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

#Listing 6.12
# put the file into the stage
result = my_session.file.put(source_file_name, "@orders_stage")
print(result)

#Listing 6.13
# define the schema for the csv file
schema_for_csv = StructType(
        [StructField("Customer", StringType()), 
         StructField("Order_date", DateType()),
         StructField("Delivery_date", DateType()),
         StructField("Baked_good_type", StringType()),
         StructField("Quantity", DecimalType())
        ])

#Listing 6.14
# COPY data from the CSV file to the staging table using the session.read method
df = my_session.read.schema(schema_for_csv).csv("@orders_stage")
result = df.copy_into_table("ORDERS_STG", format_type_options = {"skip_header": 1})

# close the Snowpark session
my_session.close()
