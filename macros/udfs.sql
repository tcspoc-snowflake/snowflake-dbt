{% macro udfs(function_name) %}
{%- set fun_name = function_name | string -%}

    {%- if fun_name == "CREATE_FUNCTION" -%}
        {{ create_f_excel_reader() }}
        ;

    {%- elif fun_name == "REFRESH_EXTERNAL_TABLE" -%}
        {{ ext_table_refresh() }}
        ;

    {%- else -%}

    {% endif %}

{% endmacro %}
