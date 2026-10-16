-- Gold dimension
-- Grain: one row per customer account, plus the Unknown row (-1).

with customers as (

    select *
    from {{ ref('stg_customer_app__customers') }}
    where customer_id is not null

),

dimension as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,

        -- one real person may own several accounts (same phone); see stg_customer_app__customers
        person_id,

        {{ to_date_key('signup_ts') }} as signup_date_key,
        full_name,
        phone,
        email,
        city,
        signup_ts,
        is_pro_subscriber
    from customers

),

unknown_row as (

    select
        '-1'                    as customer_key,
        'UNKNOWN'               as customer_id,
        'UNKNOWN'               as person_id,
        -1                      as signup_date_key,
        'Unknown'               as full_name,
        cast(null as varchar)   as phone,
        cast(null as varchar)   as email,
        'Unknown'               as city,
        cast(null as timestamp) as signup_ts,
        false                   as is_pro_subscriber

)

select * from dimension
union all
select * from unknown_row
