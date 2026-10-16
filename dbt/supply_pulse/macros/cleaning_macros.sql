{#
  Reusable technical-cleaning rules for the Silver layer.
#}

{% macro clean_text(col) -%}
    case
        when upper(trim(cast({{ col }} as varchar))) in ('', 'NULL', 'NONE', 'NAN', 'N/A', '-') then null
        else trim(cast({{ col }} as varchar))
    end
{%- endmacro %}

{% macro parse_decimal(col) -%}
    try_cast(
        regexp_replace(replace({{ col }}, ',', '.'), '[^0-9.-]', '')
        as decimal(18,4)
    )
{%- endmacro %}

{# Rule: amount in EGP. If the app wrote it in piasters (a whole number with no dot, such as 19500), divide by 100.
   A normal EGP amount in this data always has a dot, for example 195.25. #}
{% macro parse_egp(col) -%}
    case
        when {{ regex_match(col, '^-?[0-9]+$') }} then {{ parse_decimal(col) }} / 100
        else {{ parse_decimal(col) }}
    end
{%- endmacro %}

{# Keep a leading minus sign so bad negative values are not silently turned positive. #}
{% macro parse_int(col) -%}
    try_cast(regexp_substr({{ col }}, '-?[0-9]+') as integer)
{%- endmacro %}

{% macro parse_utc_ts(col) -%}
    case
        when {{ regex_match(col, '^[0-9]{13}$') }}
            then {{ epoch_ms_to_ts(col) }}

        when {{ regex_match(col, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$') }}
            then {{ parse_layout("replace(replace(" ~ col ~ ", 'T', ' '), 'Z', '')", 'iso') }}

        when {{ regex_match(col, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$') }}
            then {{ add_minutes(
                        parse_layout("replace(substr(" ~ col ~ ", 1, 19), 'T', ' ')", 'iso'),
                        "-1 * (case when substr(" ~ col ~ ", 20, 1) = '-' then -1 else 1 end)"
                        ~ " * (cast(substr(" ~ col ~ ", 21, 2) as integer) * 60 + cast(substr(" ~ col ~ ", 24, 2) as integer))"
                    ) }}

        when {{ regex_match(col, '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$') }}
            then {{ cairo_to_utc(parse_layout(col, 'iso')) }}

        when {{ regex_match(col, '^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}$') }}
            then {{ cairo_to_utc(parse_layout(col, 'day_first')) }}

        when {{ regex_match("upper(" ~ col ~ ")", '^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2} (AM|PM)$') }}
            then {{ cairo_to_utc(parse_layout("upper(" ~ col ~ ")", 'month_first_ampm')) }}

        when {{ regex_match(col, '^[0-9]{2}-[A-Za-z]{3}-[0-9]{4} [0-9]{2}:[0-9]{2}$') }}
            then {{ cairo_to_utc(parse_layout("upper(" ~ col ~ ")", 'day_month_name')) }}

        else null
    end
{%- endmacro %}

{% macro clean_vendor_id(col) -%}
    case
        when {{ col }} is null then null
        when regexp_replace({{ col }}, '[^0-9]', '') = '' then null
        else 'V' || lpad(regexp_replace({{ col }}, '[^0-9]', ''), 4, '0')
    end
{%- endmacro %}

{# Kept as a shared helper for the teammate who owns order-items. #}
{% macro clean_item_ref(col) -%}
    case
        when {{ col }} is null then null
        when {{ regex_match("upper(" ~ col ~ ")", '^P[0-9]+$') }}
            then 'SKU-' || substr(upper({{ col }}), 2)
        else upper({{ col }})
    end
{%- endmacro %}
