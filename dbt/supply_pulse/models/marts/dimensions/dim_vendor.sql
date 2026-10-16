-- Dimension: one row per vendor (current state, SCD Type 1), plus the Unknown row ('-1')
-- so facts never lose rows when a vendor cannot be matched.
-- vendor_key = hash of vendor_id (V####), built EXACTLY like fact_orders.vendor_key,
-- so every fact joins to it without any extra mapping.
-- Values that could not be repaired are NULL; dq_flag / dq_issues come from staging.

{{ config(materialized='table') }}

with vendors as (

    select *
    from {{ ref('stg_partners_system__vendors') }}
    where vendor_id is not null

),

dimension as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendor_id']) }}   as vendor_key,
        vendor_id,
        vendor_name,
        vendor_type,
        cuisine,
        integration_type,
        integration_family,
        prices_in_piasters,
        city,
        area,
        commission_pct,
        avg_prep_time_min,
        rating,
        onboarded_on,
        status,
        dq_flag,
        dq_issues
    from vendors

),

unknown_row as (

    select
        '-1'                          as vendor_key,
        'UNKNOWN'                     as vendor_id,
        'Unknown vendor'              as vendor_name,
        cast(null as varchar)         as vendor_type,
        cast(null as varchar)         as cuisine,
        cast(null as varchar)         as integration_type,
        cast(null as varchar)         as integration_family,
        cast(null as boolean)         as prices_in_piasters,
        'Unknown'                     as city,
        cast(null as varchar)         as area,
        cast(null as number(18, 4))   as commission_pct,
        cast(null as integer)         as avg_prep_time_min,
        cast(null as number(18, 4))   as rating,
        cast(null as date)            as onboarded_on,
        cast(null as varchar)         as status,
        false                         as dq_flag,
        cast(null as varchar)         as dq_issues

)

select * from dimension
union all
select * from unknown_row
