
{% macro normalize_pack_size(column_name) %}
{%- set txt  = "trim(" ~ column_name ~ ")" -%}
{%- set qty  = "to_number(regexp_substr(" ~ txt ~ ", '^[0-9]+([.][0-9]+)?'), 18, 6)" -%}
{%- set unit = "lower(regexp_substr(" ~ txt ~ ", '[a-zA-Z]+$'))" -%}
{%- set base = "(" ~ qty ~ " * iff(" ~ unit ~ " in ('kg', 'l', 'liter'), 1000, 1))" -%}
{%- set is_mass = unit ~ " in ('kg', 'g')" -%}
    case
        when regexp_like({{ txt }}, '^[0-9]+([.][0-9]+)?[ ]*(kg|g|l|liter|ml)$', 'i')
            then iff({{ base }} >= 1000,
                     to_varchar(({{ base }} / 1000)::float) || ' ' || iff({{ is_mass }}, 'kg', 'L'),
                     to_varchar({{ base }}::float)          || ' ' || iff({{ is_mass }}, 'g', 'ml'))
        else {{ txt }}
    end
{% endmacro %}