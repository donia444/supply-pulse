-- Overrides dbt's default schema naming.
-- Default dbt behaviour: <target schema>_<custom schema>  (e.g. DBT_DONIA_silver).
-- dev / prod : use the custom schema as-is (SILVER, GOLD) -> clean names.
-- any other target (e.g. shared): keep the personal prefix so teammates
--   sharing one account never overwrite each other's objects.
-- Models/seeds with no custom schema (seeds) stay in the target schema.
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name in ['dev', 'prod'] -%}
        {{ custom_schema_name | trim | upper }}

    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}