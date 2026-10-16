-- Intermediate model: vendor-item events with the business meaning added.
-- Grain: one row per CDC event (same as stg_ops_db_cdc__vendor_items_events).
--
-- Adds:
--   price_egp     selling price in EGP. POS_LEGACY vendors (and events marked PT) send piasters -> /100
--   currency      EGP or PT; a missing currency on a POS_LEGACY vendor is PT (answer key)
--   item_type     grocery (SKU-#####) or menu (M#######)
--   gate_result   PASS / REJECT, the same decision the streaming QA gate takes, so the warehouse
--   reject_reason and the dashboard agree on which events are trusted:
--                   NULL_ITEM_NAME      item name missing
--                   NON_POSITIVE_PRICE  price missing, zero or negative
--                   PRICE_OUTLIER       price more than price_outlier_factor (default 3) times away
--                                       from the item's typical (median) price at that vendor.
--                                       Real price changes in the answer key stay within x0.67 .. x1.5;
--                                       fat-finger errors are around x10 or /10.
--                   NEGATIVE_STOCK      qty_on_hand below zero
--                 Deletes (op = d) always pass: they only close the listing.

{%- set outlier_factor = var('price_outlier_factor', 3) %}

with events as (

    select * from {{ ref('stg_ops_db_cdc__vendor_items_events') }}

),

vendors as (

    select vendor_id, prices_in_piasters
    from {{ ref('stg_partners_system__vendors') }}
    where vendor_id is not null

),

priced as (

    select
        e.*,
        coalesce(v.prices_in_piasters, false) or e.currency = 'PT'          as is_piaster_price,
        case
            when e.op = 'd' then null
            when coalesce(v.prices_in_piasters, false) or e.currency = 'PT'
                then round(e.item_price_reported / 100, 2)
            else e.item_price_reported
        end                                                                 as price_egp,
        case
            when e.currency is not null then e.currency
            when v.prices_in_piasters   then 'PT'
            when e.op <> 'd'            then 'EGP'
        end                                                                 as currency_clean,
        case
            when e.item_ref like 'SKU-%' then 'grocery'
            when e.item_ref like 'M%'    then 'menu'
        end                                                                 as item_type
    from events as e
    left join vendors as v
        on e.vendor_id = v.vendor_id

),

with_reference as (

    select
        *,
        -- typical price of this item at this vendor (fat-finger events are rare, so the median ignores them)
        median(iff(op <> 'd' and price_egp > 0, price_egp, null))
            over (partition by vendor_id, item_ref)                         as median_price_egp
    from priced

),

gated as (

    select
        *,
        case
            when op = 'd'                                                   then null
            when item_name is null                                          then 'NULL_ITEM_NAME'
            when price_egp is null or price_egp <= 0                        then 'NON_POSITIVE_PRICE'
            when price_egp > median_price_egp * {{ outlier_factor }}
              or price_egp < median_price_egp / {{ outlier_factor }}        then 'PRICE_OUTLIER'
            when qty_on_hand_reported < 0                                   then 'NEGATIVE_STOCK'
        end                                                                 as reject_reason
    from with_reference

)

select
    event_id,
    lsn,
    op,
    op_name,
    vendor_id,
    store_id,
    item_ref,
    item_type,
    item_name,
    item_price_reported,
    is_piaster_price,
    price_egp,
    median_price_egp,
    currency_clean                                                          as currency,
    is_available,
    qty_on_hand,
    qty_on_hand_reported,
    update_source,
    updated_by,
    event_time_utc,
    captured_at,
    iff(reject_reason is null, 'PASS', 'REJECT')                           as gate_result,
    reject_reason,
    is_processable,
    dq_issues,
    dq_flag
from gated
