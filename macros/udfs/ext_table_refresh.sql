{% macro ext_table_refresh() %}

ALTER EXTERNAL TABLE {{var('external_table')}} REFRESH

{% endmacro %}