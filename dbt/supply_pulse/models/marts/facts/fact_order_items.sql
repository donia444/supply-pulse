-- Fact: one row per order line, following erd_gold_v2.
-- Grain: (order_id, line_number).
-- NOT included yet (their source models do not exist): vendor_key, order_date_key,
-- vendor_item_version_key, was_available_at_order.

{{ config(materialized='table') }}

with order_items as (

    select * from {{ ref('stg_customer_app__order_items') }}

),

dim_item as (

    select item_key, item_ref from {{ ref('dim_item') }}

)

select
    -- keys
    -- ASSUMPTION: fact_orders builds order_key the same way (hash of order_id)
    {{ dbt_utils.generate_surrogate_key(['order_id']) }}     as order_key,
    o.line_number,
    o.order_id,                                              -- degenerate dimension
    -- unmatched items point to the Unknown member of dim_item
    coalesce(d.item_key, '-1')                               as item_key,

    -- measures
    o.quantity,
    o.unit_price                    as unit_price_egp,
    o.vendor_confirmed_price        as vendor_confirmed_price_egp,
    o.line_total                    as line_total_egp,
    -- business rule from the ERD: (vendor_confirmed - unit) x quantity.
    -- Positive = the app charged less than the vendor-confirmed price. NULL if any input is NULL.
    (o.vendor_confirmed_price - o.unit_price) * o.quantity   as price_leakage_egp,

    -- business rule from the ERD: stale when the app price differs from the vendor-confirmed price.
    -- A missing price on either side counts as "not stale" (FALSE) because we cannot prove a difference.
    coalesce(o.unit_price <> o.vendor_confirmed_price, false) as is_stale_price,

    -- data-quality flags and original values, kept so BI can include or exclude repaired rows
    o.quantity_raw,
    o.is_invalid_quantity,
    o.is_quantity_rebuilt,
    o.is_unit_price_rebuilt

from order_items as o
left join dim_item as d
    on o.item_ref = d.item_ref