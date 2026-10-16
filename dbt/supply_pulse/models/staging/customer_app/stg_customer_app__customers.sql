-- Silver customer model
-- Grain: one row per customer account / customer_id.

with raw_text as (

    select
        upper({{ clean_text('customer_id') }})       as customer_id,
        {{ clean_text('full_name') }}                as full_name,
        {{ clean_text('phone') }}                    as phone,
        lower({{ clean_text('email') }})             as email,
        lower({{ clean_text('city') }})              as city,
        {{ clean_text('signup_ts') }}                as signup_ts,
        lower({{ clean_text('is_pro_subscriber') }}) as is_pro_subscriber
    -- SCD Type 1: on the shared account the source is a CDC event log, so keep
    -- only the latest event per customer_id (deleted customers are dropped).
    -- On the personal account (dev) the macro returns the table unchanged.
    from {{ latest_cdc_row(source('customer_app', 'customers'), ['customer_id']) }}

),

repaired as (

    select
        *,
        case
            when position('@' in email) = 0
                then replace(replace(email, ',', '.'), ' ', '@')
            else replace(replace(email, ',', '.'), '@@', '@')
        end as email_repaired
    from raw_text

),

cleaned as (

    select
        customer_id,

        {{ proper_case("regexp_replace(full_name, ' +', ' ')") }} as full_name,

        case
            when length(regexp_replace(phone, '[^0-9]', '')) >= 10
                then '+20' || right(regexp_replace(phone, '[^0-9]', ''), 10)
        end as phone,

        case
            when {{ regex_match('email_repaired', '^[^@ ]+@[^@ ]+[.][a-z]{2,}$') }}
                then email_repaired
        end as email,

        coalesce(city_map.clean_value, 'Unknown') as city,

        {{ parse_utc_ts('signup_ts') }} as signup_ts_parsed,

        case
            when is_pro_subscriber in ('true', '1', 'y', 'yes') then true
            when is_pro_subscriber in ('false', '0', 'n', 'no') then false
            else null
        end as is_pro_subscriber

    from repaired
    left join {{ ref('map_city') }} as city_map
        on repaired.city = city_map.raw_value

),

deduplicated as (

    -- Remove exact duplicate cleaned rows only.
    -- If the same customer_id has conflicting values, the unique test will expose it
    -- instead of silently choosing a record with an unapproved rule.
    select distinct *
    from cleaned

),

final as (

    select
        customer_id,

        -- person_id: accounts with the same cleaned phone belong to one real person.
        -- The smallest customer_id of the group becomes the person id. No phone -> the account is its own person.
        case
            when phone is not null then min(customer_id) over (partition by phone)
            else customer_id
        end as person_id,

        full_name,
        phone,
        email,
        city,

        case
            when signup_ts_parsed <= {{ cutoff_ts() }} then signup_ts_parsed
        end as signup_ts,

        is_pro_subscriber,

        (
            email is null
            or city = 'Unknown'
            or signup_ts_parsed is null
            or signup_ts_parsed > {{ cutoff_ts() }}
            or is_pro_subscriber is null
        ) as dq_flag

    from deduplicated

)

select * from final