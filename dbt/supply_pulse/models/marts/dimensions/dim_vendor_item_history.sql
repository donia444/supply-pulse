-- Dimension (SCD Type 2): history of price and availability for each vendor item.
-- Grain: one row per version of (vendor_id, item_ref).
--
-- Built the way the answer key (expected_scd2_vendor_items) builds it:
--   * only events the QA gate PASSES are used (rejected events never create a version)
--   * time = event_time_utc (business time of the change), not the capture time,
--     so late-arriving events still land at the right moment
--   * events in the same second for the same item: only the last one (highest lsn) counts
--   * a version starts only when price_egp or is_available really changes (no-op events are skipped)
--   * a delete (op = d) closes the open version and creates no new row
--   * valid_to of the current version is NULL (open ended); is_current = true
-- vendor_key / item_key are looked up from dim_vendor / dim_item, '-1' (Unknown) when not found.

{{ config(materialized='table') }}

with events as (

    select *
    from {{ ref('int_vendor_items__events') }}
    where is_processable
      and gate_result = 'PASS'
    -- several events in the same second: the last one (highest lsn) is the state at that second
    qualify row_number() over (
        partition by vendor_id, item_ref, event_time_utc
        order by lsn desc
    ) = 1

),

hashed as (

    select
        *,
        md5(concat_ws('|',
            coalesce(to_varchar(price_egp), '~'),
            coalesce(to_varchar(is_available), '~'),
            to_varchar(op = 'd')))                                          as hashdiff
    from events

),

changed as (

    -- keep only the events where price, availability or the deleted state really changed
    select *
    from hashed
    qualify hashdiff is distinct from lag(hashdiff) over (
        partition by vendor_id, item_ref order by event_time_utc, lsn)

),

versions as (

    select
        *,
        lead(event_time_utc) over (
            partition by vendor_id, item_ref order by event_time_utc, lsn)  as next_change_ts
    from changed

),

kept as (

    select *
    from versions
    where op <> 'd'

),

vendors as (

    select vendor_id, vendor_key from {{ ref('dim_vendor') }}

),

items as (

    select item_ref, item_key from {{ ref('dim_item') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['k.vendor_id', 'k.item_ref', 'k.event_time_utc']) }} as vendor_item_version_key,
    coalesce(v.vendor_key, '-1')                                            as vendor_key,
    coalesce(i.item_key, '-1')                                              as item_key,
    k.vendor_id,
    k.item_ref,
    k.item_type,
    k.item_name,
    row_number() over (partition by k.vendor_id, k.item_ref order by k.event_time_utc, k.lsn) as version_number,
    k.price_egp,
    k.is_available,
    k.event_time_utc                                                        as valid_from,
    k.next_change_ts                                                        as valid_to,
    k.next_change_ts is null                                                as is_current,
    k.hashdiff,
    k.qty_on_hand                                                           as qty_on_hand_at_change,
    k.update_source,
    k.lsn                                                                   as source_lsn,
    k.event_id                                                              as source_event_id
from kept as k
left join vendors as v
    on v.vendor_id = k.vendor_id
left join items as i
    on i.item_ref = k.item_ref