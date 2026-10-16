{#
  EAN-13: the 13th digit is a check digit computed from the first 12.
  A 13-digit barcode keeps its first 12 digits and gets the CORRECT check digit
  (fixes typos in the last digit, e.g. 6222640418398 -> 6222640418395).
  Anything that is not 13 digits is returned unchanged (and is_valid_barcode flags it).
#}
{% macro ean13_with_check_digit(column_name) %}
    case
        when regexp_like({{ column_name }}, '^[0-9]{13}$') then
            substr({{ column_name }}, 1, 12) || to_varchar(mod(10 - mod(
                {% for i in range(1, 13) %}
                to_number(substr({{ column_name }}, {{ i }}, 1)) * {{ 1 if i % 2 == 1 else 3 }} {{ '+' if not loop.last else '' }}
                {% endfor %}
            , 10), 10))
        else {{ column_name }}
    end
{% endmacro %}
