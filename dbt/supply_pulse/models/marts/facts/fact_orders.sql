-- Gold fact
-- Grain: one row per order.

with orders as (

    select *
    from {{ ref('stg_customer_app__orders') }}
    where order_id is not null

),

customers as (

    select
        customer_id,
        customer_key
    from {{ ref('dim_customer') }}

),

-- Line count and item quantity per order, from the cleaned order lines.
-- Lines whose quantity is still invalid (zero/negative, about 40) are counted
-- as lines but do not add to item_quantity.
order_lines as (

    select
        upper(trim(order_id))                                    as order_id,
        count(*)                                                 as line_count,
        sum(case when quantity > 0 then quantity end)            as item_quantity
    from {{ ref('stg_customer_app__order_items') }}
    group by 1

),

order_profiles as (

    select
        order_profile_key,
        payment_method,
        status,
        cancel_reason
    from {{ ref('dim_order_profile') }}

),

prepared as (

    select
        o.*,
        c.customer_key as known_customer_key,

        case
            when o.cancel_reason is not null then o.cancel_reason
            when o.status = 'delivered' then 'not_applicable'
            else 'unknown'
        end as profile_cancel_reason

    from orders as o
    left join customers as c
        on o.customer_id = c.customer_id

),

joined as (

    select
        p.*,
        op.order_profile_key as known_order_profile_key,
        ol.line_count        as order_line_count,
        ol.item_quantity     as order_item_quantity

    from prepared as p
    left join order_profiles as op
        on p.payment_method = op.payment_method
       and p.status = op.status
       and p.profile_cancel_reason = op.cancel_reason
    left join order_lines as ol
        on p.order_id = ol.order_id

)

select
    -- KEYS
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
    order_id,

    coalesce(known_customer_key, '-1') as customer_key,

    case
        when vendor_id is null then '-1'
        else {{ dbt_utils.generate_surrogate_key(['vendor_id']) }}
    end as vendor_key,

    coalesce(known_order_profile_key, '-1') as order_profile_key,

    {{ to_date_key('order_ts') }}     as order_date_key,
    {{ to_date_key('delivered_at') }} as delivered_date_key,

    -- CONTEXT
    order_ts,
    delivered_at,
    promo_code,
    delivery_zone,

    -- MEASURES
    subtotal,
    discount_amount,
    delivery_fee,
    service_fee,
    total_amount,
    estimated_delivery_min,

    case
        when order_ts is not null and delivered_at is not null
            then {{ minutes_between('order_ts', 'delivered_at') }}
    end as delivery_minutes,

    customer_rating,

    -- Filled from the order lines (now merged). An order with no lines gets 0 lines
    -- and a NULL item_quantity (nothing to sum).
    coalesce(order_line_count, 0) as line_count,
    order_item_quantity           as item_quantity,

    -- FLAGS
    coalesce(
        status = 'delivered'
        and order_ts is not null
        and delivered_at is not null
        and estimated_delivery_min is not null
        and {{ minutes_between('order_ts', 'delivered_at') }} <= estimated_delivery_min,
        false
    ) as is_on_time,

    coalesce(
        status = 'cancelled'
        and cancel_reason = 'item_unavailable',
        false
    ) as is_cancelled_item_unavailable,

    coalesce(dq_flag, false)
        or known_customer_key is null
        or known_order_profile_key is null
        as dq_flag

from joined
