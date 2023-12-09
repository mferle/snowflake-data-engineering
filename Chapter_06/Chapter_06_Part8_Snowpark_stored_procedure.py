# import Session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import data types from the snowflake.snowpark package
from snowflake.snowpark.types import StringType
# import sproc package for registering stored procedures
from snowflake.snowpark.functions import sproc
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



#create or replace procedure LOAD_CUSTOMER_ORDERS()
@sproc(name="snowpark_load_customer_orders", is_permanent=True, stage_location="@ORDERS_STAGE", replace=True, packages=["snowflake-snowpark-python"])
def snowpark_load_customer_orders(my_session: Session) -> str:
    sql_statement = f'''
        merge into CUSTOMER_ORDERS tgt
        using ORDERS_COMBINED_STG as src
        on src.customer = tgt.customer and src.delivery_date = tgt.delivery_date and src.baked_good_type = tgt.baked_good_type
        when matched then 
        update set tgt.quantity = src.quantity, 
            tgt.load_ts = current_timestamp()
        when not matched then
        insert (customer, order_date, delivery_date, 
            baked_good_type, quantity, load_ts)
        values(src.customer, src.order_date, src.delivery_date,
            src.baked_good_type, src.quantity, current_timestamp())
    '''
    my_session.sql(sql_statement).collect()
    return 'Success'




# close the Snowpark session
my_session.close()
