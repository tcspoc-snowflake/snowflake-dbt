{% macro create_f_excel_reader() %}
create or replace FUNCTION f_excel_reader()
  returns Variant
  language python
  runtime_version='3.8'
  imports = {{ var('external_stage') }}
  packages = ('snowflake-snowpark-python','pandas','xlrd','cachetools')
  handler='compute'
as
$$
import sys
import os
import xlrd
import pandas
def compute():
    pat = os.path.join(sys._xoptions["snowflake_import_directory"], 'Sample_Superstore.xls')
    df = pandas.read_excel(pat,index_col=None)
    parsed = df.to_json(orient="records")
    return parsed
$$
{% endmacro %}