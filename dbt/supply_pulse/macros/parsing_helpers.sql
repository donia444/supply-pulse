{#
  Parsing helpers used by the vendor, inventory and vendor-item staging models.
  (Originally written by a teammate; renamed / trimmed so they do not clash with
  macros that already exist in this project.)

  Removed on purpose, because the project already has an equivalent:
    clean_text -> macros/cleaning_macros.sql   (also treats 'nan' and '-' as NULL)
    to_bool    -> macros/to_boolean_clean.sql
#}

{# Decimal comma to decimal point: '120,00' -> 120.00. Not safe for '1,200.50'. #}
{% macro to_decimal(col, precision=12, scale=2) -%}
    try_to_decimal(replace(trim({{ col }}), ',', '.'), {{ precision }}, {{ scale }})
{%- endmacro %}

{# Day first (DD/MM/YYYY) or ISO (also works if the column is already a DATE). #}
{% macro to_date_dmy(col) -%}
    coalesce(
        try_to_date(trim({{ col }}), 'DD/MM/YYYY'),
        try_to_date(trim({{ col }}), 'YYYY-MM-DD')
    )
{%- endmacro %}

{#
  Timestamp in UTC, following the project rule:
    - epoch milliseconds and ISO with Z / offset  -> already UTC
    - text without a time zone                    -> Cairo local time, converted to UTC
  Step 1 uses the project's strict parser (parse_utc_ts).
  Step 2 is a fallback for the extra day-first layouts seen in vendor / inventory data
         (DD/MM/YYYY HH24:MI:SS, DD/MM/YYYY, ...), read as Cairo local time.
  Anything that still cannot be parsed stays NULL and is flagged by the staging model.
#}
{% macro parse_ts_any_utc(col) -%}
    coalesce(
        {{ parse_utc_ts(col) }},
        {{ cairo_to_utc(
            "coalesce(
                try_to_timestamp_ntz(trim(" ~ col ~ "), 'DD/MM/YYYY HH24:MI:SS'),
                try_to_timestamp_ntz(trim(" ~ col ~ "), 'DD/MM/YYYY HH24:MI'),
                try_to_timestamp_ntz(trim(" ~ col ~ "), 'DD/MM/YYYY'),
                try_to_timestamp_ntz(trim(" ~ col ~ "), 'YYYY-MM-DD HH24:MI:SS.FF'),
                try_to_timestamp_ntz(trim(" ~ col ~ "), 'YYYY-MM-DD')
            )"
        ) }}
    )
{%- endmacro %}
