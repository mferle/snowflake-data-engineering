-- create the stored procedure with parameters
create or replace procedure PROC_IS_HOLIDAY(p_date date, p_country string)
    returns String
    language python
    runtime_version = 3.8
    packages =('holidays==0.29', 'snowflake-snowpark-python==*')
    handler = 'is_holiday'
    comment = 'The procedure returns True if the date is a holiday in the country'
    as '
# The Snowpark package is required for Python Worksheets
import snowflake.snowpark as snowpark
# Adding the holidays package
import holidays

#Listing 6.2
def is_holiday(session: snowpark.Session, p_date, p_country):
    # get a list of all holidays in the US
    all_holidays = holidays.country_holidays(p_country)
    # return TRUE if p_date is a holiday, otherwise return false
    if p_date in all_holidays:
        return True
    else:
        return False
';

-- execute the procedure using different values for the p_date and p_country parameters
call PROC_IS_HOLIDAY('2024-01-01', 'US'); -- returns True
call PROC_IS_HOLIDAY('2024-07-04', 'US'); -- returns True
call PROC_IS_HOLIDAY('2024-07-14', 'US'); -- returns False
call PROC_IS_HOLIDAY('2024-07-14', 'FR'); -- returns TRTrueUE
call PROC_IS_HOLIDAY('2024-07-04', 'FR'); -- returns False
