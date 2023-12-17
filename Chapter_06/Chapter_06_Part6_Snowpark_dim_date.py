#Listing 6.5
# import Session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import data types from the snowflake.snowpark package
from snowflake.snowpark.types import StructType, StructField, DateType, BooleanType
# import json package for reading connection parameters
import json
# import date and timedelta from the datetime package for generating dates
from datetime import date, timedelta
# install the holidays package using pip or conda
# import the holidays package to determine whether a given date is a holiday
import holidays

#Listing 6.6
# define a function that returns True if p_date is a holiday in p_country
def is_holiday(p_date, p_country):
    # get a list of all holidays in p_country
    all_holidays = holidays.country_holidays(p_country)
    # return True if p_date is a holiday, otherwise return false
    if p_date in all_holidays:
        return True
    else:
        return False

#Listing 6.7
# generate a list of dates starting from start_date followed by as many days as defined in the no_days variable
# define the start date
start_dt = date(2023, 1, 1)
# define number of days
# use the value 5 to generate a sample dimension with 5 days
no_days = 5 
# change the value to 731 to generate dates for 731 days (years 2023 and 2024)
#no_days = 731
# store consecutive dates starting from the start date in a list
dates = [(start_dt + timedelta(days=i)).isoformat() for i in range(no_days)]

#Listing 6.8
# create a list of lists that combines the list of dates with the output of the is_holiday() function
holiday_flags = [[d, is_holiday(d, 'US')] for d in dates]

# print the holiday_flags list of lists locally to check that the data is as expected
print(holiday_flags)

# establish a connection with Snowflake because we need the Snowpark API

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

#Listing 6.9
# create a data frame from the holiday_flags list of lists and define the schema as two columns:
# - column named "day" with data type DateType
# - column named "holiday_flg" with data type BooleanType
df = my_session.create_dataframe(
    holiday_flags, 
    schema = StructType(
        [StructField("day", DateType()), 
         StructField("holiday_flg", BooleanType())])
    )

# print the data frame to verify that it contains the correct data
print(df.collect())

#Listing 6.10
# save the data frame to a Snowflake table named DIM_DATE and overwrite the table if it already exists
df.write.mode("overwrite").save_as_table("DIM_DATE")

# close the Snowpark session
my_session.close()