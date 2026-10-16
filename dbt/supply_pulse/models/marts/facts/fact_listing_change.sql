-- Fact: one row per CDC event on a vendor item (insert, update, delete, snapshot).
-- Grain: event_id. ALL events are kept, including the ones the QA gate rejects,
-- so analysts can count rejections; filter on gate_result = 'PASS' for trusted prices.
--
-- price_delta_egp      : price minus the previous PASSED price of the same vendor item
--                        (NULL for the first price and for deletes)
-- capture_lag_seconds  : seconds between the business change (event_time_utc) and the moment
--                        CDC captured it (NULL for snapshot rows, where the lag is meaningless)
-- change_date_key      : business date (UTC) of the change, same convention as the other facts
-- vendor_key / item_key: looked up from dim_vendor / dim_item; '-1' (Unknown) when not found

{{ config(materialized='table') }}

with events as (

    select
        *,
        lag(iff(gate_result = 'PASS' and op <> 'd', price_egp, null)) ignore nulls over (
            partition by vendor_id, item_ref order by event_time_utc, lsn)   as previous_price_egp
    from {{ ref('int_vendor_items__events') }}

),

vendors as (

    select vendor_id, vendor_key from {{ ref('dim_vendor') }}

),

items as (

    select item_ref, item_key from {{ ref('dim_item') }}

)

select
    -- keys
    e.event_id,
    e.lsn,
    {{ to_date_key('e.event_time_utc') }}                                   as change_date_key,
    coalesce(v.vendor_key, '-1')                                            as vendor_key,
    coalesce(i.item_key, '-1')                                              as item_key,

    -- measures
    e.price_egp,
    e.previous_price_egp,
    iff(e.op = 'd', null, e.price_egp - e.previous_price_egp)               as price_delta_egp,
    e.qty_on_hand,
    iff(e.op = 'r', null, datediff('second', e.event_time_utc, e.captured_at)) as capture_lag_seconds,

    -- context
    e.is_available,
    e.op,
    e.op_name,
    e.vendor_id,
    e.item_ref,
    e.item_type,
    e.currency,
    e.is_piaster_price,
    e.update_source,
    e.event_time_utc,
    e.captured_at,

    -- quality
    e.gate_result,
    e.reject_reason,
    e.is_processable,
    e.dq_flag,
    e.dq_issues
from events as e
left join vendors as v
    on v.vendor_id = e.vendor_id
left join items as i
    on i.item_ref = e.item_ref
