# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,udf
import pandas as pd
import json
def model(dbt, session):
    dataframe = session.table('excel_details').collect()
    # print(dataframe[0][0],dataframe[0][1])
    sheet="'"+dataframe[0][1]+"'"
    path="'"+dataframe[0][0]+"'"
    query = '''create or replace FUNCTION excel_reader()
  returns Variant
  language python
  runtime_version=3.8'''+'''
  imports = ('''+path+''')
  packages = ('snowflake-snowpark-python','pandas','xlrd','cachetools')
  handler='compute'
as
$$
import sys
import os
import xlrd
import pandas
import cachetools
def compute():
    pat = os.path.join(sys._xoptions["snowflake_import_directory"], 'Sample_Superstore.xls')
    df = pandas.read_excel(pat,'''+sheet+''',index_col=None)
    parsed = df.to_json(orient="records")
    return parsed
$$'''
    # print(query)
    session.sql(query).collect()

    df = session.sql('''select parse_json(excel_reader()) as xls_parse''')
    # print(type(li[0]))
    json_object = json.loads(df.collect()[0][0])
    # print(json_object[0])
    df1 = pd.DataFrame(json_object)
#session.write_pandas(df1,dataframe[0][1],auto_create_table=True)
    snow_df=session.create_dataframe(df1)
    return snow_df
	
	
