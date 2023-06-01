# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,udf
import pandas as pd
import json
def model(dbt, session):
    df = session.sql('''select parse_json(f_excel_reader()) as xls_parse''')
    json_object = json.loads(df.collect()[0][0])
    df1 = pd.DataFrame(json_object)
    return df1