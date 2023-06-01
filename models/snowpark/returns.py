import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,udf
import pandas as pd
import json
import os

def model(dbt, session):
    dbt.config(packages = ["xlrd"])
    filename = os.path.basename('@snowflakeinternalstage/Sample_Superstore.xls')
    staged_file = session.file.get('@snowflakeinternalstage/Sample_Superstore.xls', "/tmp")
    file_path = f"/tmp/{filename}"
    colnames = ['RETURNED', 'ORDERID']
    excel_data_df = pd.read_excel(file_path,sheet_name='Returns',names=colnames)
    return excel_data_df