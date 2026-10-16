
{% macro piasters_to_egp(column_name, threshold=1000) %}
    case
        when {{ column_name }} > {{ threshold }} then round({{ column_name }} / 100, 2)
        else {{ column_name }}
    end
{% endmacro %}