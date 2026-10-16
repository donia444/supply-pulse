--ean 13  barcode can be checked or validated from the last digit , since we cant exactly know the barcode we check the last digit generated the existing barcode and we add column as a flag to flag it if right or wrong
{% macro is_valid_barcode(column_name) %}
    case
        when {{ column_name }} is null then null
        when not regexp_like({{ column_name }}, '^[0-9]{13}$') then false
        else (
            mod(10 - mod(
                {% for i in range(1, 13) %}
                to_number(substr({{ column_name }}, {{ i }}, 1)) * {{ 1 if i % 2 == 1 else 3 }} {{ '+' if not loop.last else '' }}
                {% endfor %}
            , 10), 10) = to_number(substr({{ column_name }}, 13, 1))
        )
    end
{% endmacro %}