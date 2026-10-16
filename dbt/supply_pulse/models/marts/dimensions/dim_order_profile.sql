-- Gold junk dimension
-- Grain: one row per payment_method x status x cancel_reason combination,
-- plus the Unknown row (-1).

with combinations as (

    select distinct
        payment_method,
        status,
        case
            when cancel_reason is not null then cancel_reason
            when status = 'delivered' then 'not_applicable'
            else 'unknown'
        end as cancel_reason
    from {{ ref('stg_customer_app__orders') }}

),

dimension as (

    select
        {{ dbt_utils.generate_surrogate_key(['payment_method', 'status', 'cancel_reason']) }} as order_profile_key,
        payment_method,
        status,
        cancel_reason
    from combinations
    where not (
        payment_method = 'unknown'
        and status = 'unknown'
        and cancel_reason = 'unknown'
    )

),

unknown_row as (

    select
        '-1'      as order_profile_key,
        'unknown' as payment_method,
        'unknown' as status,
        'unknown' as cancel_reason

)

select * from dimension
union all
select * from unknown_row
