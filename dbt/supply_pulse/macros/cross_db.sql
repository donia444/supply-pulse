{#
  Snowflake-specific helper macros.
#}

{% macro regex_match(expr, pattern) -%}
    regexp_like({{ expr }}, '{{ pattern }}')
{%- endmacro %}

{% macro epoch_ms_to_ts(expr) -%}
    to_timestamp_ntz(cast({{ expr }} as bigint), 3)
{%- endmacro %}

{% macro cairo_to_utc(expr) -%}
    convert_timezone('Africa/Cairo', 'UTC', {{ expr }})
{%- endmacro %}

{% macro add_minutes(expr, minutes) -%}
    dateadd(minute, {{ minutes }}, {{ expr }})
{%- endmacro %}

{% macro minutes_between(start_expr, end_expr) -%}
    datediff(minute, {{ start_expr }}, {{ end_expr }})
{%- endmacro %}

{% macro to_date_key(expr) -%}
    coalesce(to_number(to_char({{ expr }}, 'YYYYMMDD')), -1)
{%- endmacro %}

{% macro parse_layout(expr, layout) -%}
    {%- if layout == 'iso' -%}
        try_to_timestamp_ntz({{ expr }}, 'YYYY-MM-DD HH24:MI:SS')
    {%- elif layout == 'day_first' -%}
        try_to_timestamp_ntz({{ expr }}, 'DD/MM/YYYY HH24:MI')
    {%- elif layout == 'month_first_ampm' -%}
        try_to_timestamp_ntz({{ expr }}, 'MM/DD/YYYY HH12:MI AM')
    {%- elif layout == 'day_month_name' -%}
        try_to_timestamp_ntz({{ expr }}, 'DD-MON-YYYY HH24:MI')
    {%- endif -%}
{%- endmacro %}

{% macro proper_case(expr) -%}
    initcap({{ expr }})
{%- endmacro %}

{% macro utc_now() -%}
    sysdate()
{%- endmacro %}

{# Latest moment a real event can have. Set the variable data_cutoff_ts to the time the data was extracted
   (for the course files: 2026-10-01 00:00:00). If it is not set, the current UTC time is used. #}
{% macro cutoff_ts() -%}
    {%- if var('data_cutoff_ts', none) -%} cast('{{ var("data_cutoff_ts") }}' as timestamp) {%- else -%} {{ utc_now() }} {%- endif -%}
{%- endmacro %}
