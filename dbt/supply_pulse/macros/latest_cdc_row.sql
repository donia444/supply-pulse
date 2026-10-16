-- Returns the CURRENT state of a CDC event log: one row per key.
-- On the shared account (targets: shared, prod) the BRONZE views hold every
-- Kafka event (r = snapshot, c = insert, u = update, d = delete), so we:
--   1) keep only the latest event per key (newest Kafka timestamp, then offset)
--   2) drop the key entirely when that latest event is a delete ('d')
-- On the personal account (target: dev) the RAW tables have no CDC columns,
-- so the relation is returned untouched.
--
-- Usage:  from {{ latest_cdc_row(source('customer_app', 'customers'), ['customer_id']) }}
{% macro latest_cdc_row(relation, key_columns) -%}

    {%- if target.name in ['shared', 'prod'] -%}
    (
        select *
        from {{ relation }}
        qualify row_number() over (
                    partition by {{ key_columns | join(', ') }}
                    order by kafka_ts desc, try_to_number(kafka_offset::string) desc
                ) = 1
            and cdc_op <> 'd'
    )
    {%- else -%}
        {{ relation }}
    {%- endif -%}

{%- endmacro %}