--macro to deal with null (for products we have a brand name Nan which is not null but we want to treat it as null)
{% macro clean_null(column_name) %}
    case
        when trim({{ column_name }}) in ('', 'null', 'NULL', 'N/A', 'nan', 'None', '-') then null
        else trim({{ column_name }})
    end
{% endmacro %}