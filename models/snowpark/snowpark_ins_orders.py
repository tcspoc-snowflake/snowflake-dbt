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
    excel_data_df = pd.read_excel(file_path)
    return excel_data_df