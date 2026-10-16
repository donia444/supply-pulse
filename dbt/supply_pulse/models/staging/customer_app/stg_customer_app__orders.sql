-- Silver orders model
-- Grain: one row per order_id.

with raw_text as (

    select
        upper({{ clean_text('order_id') }})          as order_id,
        upper({{ clean_text('customer_id') }})       as customer_id,
        {{ clean_text('vendor_id') }}                as vendor_id,
        {{ clean_text('order_ts') }}                 as order_ts,
        {{ clean_text('delivered_at') }}             as delivered_at,
        lower({{ clean_text('status') }})            as status,
        lower({{ clean_text('cancel_reason') }})     as cancel_reason,
        lower({{ clean_text('payment_method') }})    as payment_method,
        upper({{ clean_text('promo_code') }})        as promo_code,
        lower({{ clean_text('delivery_zone') }})     as delivery_zone,
        {{ clean_text('subtotal') }}                 as subtotal,
        {{ clean_text('discount_amount') }}          as discount_amount,
        {{ clean_text('delivery_fee') }}             as delivery_fee,
        {{ clean_text('service_fee') }}              as service_fee,
        {{ clean_text('total_amount') }}             as total_amount,
        {{ clean_text('estimated_delivery_min') }}   as estimated_delivery_min,
        {{ clean_text('customer_rating') }}          as customer_rating
    -- SCD Type 1 on the shared account: latest CDC event per order_id (deletes dropped).
    -- On dev the macro returns the RAW table unchanged.
    from {{ latest_cdc_row(source('customer_app', 'orders'), ['order_id']) }}

),

parsed as (

    select
        o.order_id,
        o.customer_id,
        {{ clean_vendor_id('o.vendor_id') }} as vendor_id,

        {{ parse_utc_ts('o.order_ts') }}     as order_ts_parsed,
        {{ parse_utc_ts('o.delivered_at') }} as delivered_at_parsed,

        coalesce(st.clean_value, 'unknown') as status,
        coalesce(pm.clean_value, 'unknown') as payment_method,
        cr.clean_value                      as cancel_reason,
        o.promo_code,
        coalesce(dz.clean_value, 'Unknown') as delivery_zone,

        -- Money columns. Each one is read as a number; the "_raw" columns are repaired in the next step.
        {{ parse_egp('o.subtotal') }}                as subtotal,            -- 19525 (piasters) -> 195.25 EGP
        {{ parse_decimal('o.discount_amount') }}     as discount_amount_raw, -- may be negative in the file
        {{ parse_decimal('o.delivery_fee') }}        as delivery_fee,
        {{ parse_decimal('o.service_fee') }}         as service_fee_raw,     -- may be missing in the file
        {{ parse_decimal('o.total_amount') }}        as total_amount_raw,    -- may be negative or wrong in the file

        {{ parse_int('o.estimated_delivery_min') }} as estimated_delivery_min_raw,
        {{ parse_int('o.customer_rating') }}        as customer_rating_raw

    from raw_text as o
    left join {{ ref('map_order_status') }}   as st
        on o.status = st.raw_value
    left join {{ ref('map_payment_method') }} as pm
        on o.payment_method = pm.raw_value
    left join {{ ref('map_cancel_reason') }}  as cr
        on o.cancel_reason = cr.raw_value
    left join {{ ref('map_delivery_zone') }}  as dz
        on o.delivery_zone = dz.raw_value

),

deduplicated as (

    -- Remove exact duplicates only.
    select distinct *
    from parsed

),

money_fixed as (

    -- Repair the money columns. Every rule below is checked against the answer key.
    select
        *,
        -- discount is always a positive number (a negative sign in the file is a data mistake)
        abs(discount_amount_raw) as discount_amount,

        -- money to pay before the service fee
        (coalesce(subtotal, 0) - abs(coalesce(discount_amount_raw, 0)) + coalesce(delivery_fee, 0)) as amount_before_fee,

        -- service fee: use the file value. If it is missing, work it out as (total - the rest).
        -- A real service fee is between 0 and 15 EGP, so any other result means the total was damaged -> NULL.
        coalesce(
            service_fee_raw,
            case
                when abs(total_amount_raw)
                     - (coalesce(subtotal, 0) - abs(coalesce(discount_amount_raw, 0)) + coalesce(delivery_fee, 0))
                     between 0 and 15
                then abs(total_amount_raw)
                     - (coalesce(subtotal, 0) - abs(coalesce(discount_amount_raw, 0)) + coalesce(delivery_fee, 0))
            end
        ) as service_fee
    from deduplicated

),

final as (

    select
        order_id,
        customer_id,
        vendor_id,

        case
            when order_ts_parsed <= {{ cutoff_ts() }} then order_ts_parsed
        end as order_ts,

        case
            when delivered_at_parsed >= order_ts_parsed
             and order_ts_parsed <= {{ cutoff_ts() }}
                then delivered_at_parsed
        end as delivered_at,

        status,
        cancel_reason,
        payment_method,
        promo_code,
        delivery_zone,

        subtotal,
        discount_amount,
        delivery_fee,
        service_fee,

        -- total must equal its parts; if a part is missing, fall back to the file total (made positive)
        coalesce(
            case
                when subtotal is not null and service_fee is not null
                    then amount_before_fee + service_fee
            end,
            abs(total_amount_raw)
        ) as total_amount,

        case
            when estimated_delivery_min_raw > 0 then estimated_delivery_min_raw
        end as estimated_delivery_min,

        case
            when customer_rating_raw between 1 and 5 then customer_rating_raw
        end as customer_rating,

        (
            order_ts_parsed is null
            or order_ts_parsed > {{ cutoff_ts() }}
            or (
                delivered_at_parsed is not null
                and (
                    order_ts_parsed is null
                    or delivered_at_parsed < order_ts_parsed
                )
            )
            or customer_id is null
            or vendor_id is null
            or status = 'unknown'
            or payment_method = 'unknown'
            or subtotal is null
            or service_fee_raw is null                                -- fee was missing in the file (rebuilt or NULL)
            or discount_amount_raw < 0                                -- sign was fixed
            or total_amount_raw < 0                                   -- sign was fixed
            or estimated_delivery_min_raw is null
            or estimated_delivery_min_raw <= 0
            or (
                total_amount_raw is not null
                and abs(abs(total_amount_raw) - (amount_before_fee + coalesce(service_fee, 0))) > 0.01
            )                                                          -- file total did not match its parts
        ) as dq_flag

    from money_fixed

)

select * from final
