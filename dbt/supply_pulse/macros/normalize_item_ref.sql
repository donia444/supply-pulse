
{% macro normalize_item_ref(column_name) %}
    case
        when regexp_like(upper(trim({{ column_name }})), '^P[0-9]{5}$')
            then 'SKU-' || substr(upper(trim({{ column_name }})), 2)
        else upper(trim({{ column_name }}))
    end
{% endmacro %}