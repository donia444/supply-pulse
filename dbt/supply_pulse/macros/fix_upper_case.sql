
{% macro fix_upper_case(column_name) %}
    case
        when {{ column_name }} = upper({{ column_name }}) and regexp_like({{ column_name }}, '.*[A-Z].*')
            then replace(
                    regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                        initcap(lower({{ column_name }})),
                        ' G$', ' g'), ' Kg$', ' kg'), ' Ml$', ' ml'), ' Pcs$', ' pcs'),
                    ' X ', ' x ')
        else {{ column_name }}
    end
{% endmacro %}