--to make boolean values true or false standardized and clean any spaces in the column
{% macro to_boolean_clean(column_name) %}
    case
        -- in_stock / available / sold_out / oos are the availability flags of the vendor-item
        -- events, taken from the answer key's dq_manifest
        when lower(trim({{ column_name }})) in ('true', 't', 'yes', 'y', '1', 'in_stock', 'available') then true
        when lower(trim({{ column_name }})) in ('false', 'f', 'no', 'n', '0', 'sold_out', 'oos') then false
        else null
    end
{% endmacro %}