-- Staging model: ops_db_cdc.vendor_items_events
-- Grain: one row per CDC EVENT on a store item (price, availability, stock).
-- This model keeps the full event history on purpose (it feeds the SCD Type 2 dimension);
-- only an event delivered twice (same lsn, at-least-once delivery) collapses to one row.
-- Project style: values that cannot be repaired become NULL; dq_flag / dq_issues explain.
--
-- The two accounts name the CDC columns differently, so they are mapped here, in one place:
--   column        dev (RAW)        shared / prod (BRONZE)
--   lsn           LSN              SRC_LSN
--   op            OP               CDC_OP
--   captured_at   CAPTURED_AT_MS   KAFKA_TS  (already a UTC timestamp)
--
-- Repair rules (all taken from the data-quality answer key):
--   store_id      12 -> vendor_id V0012
--   item_price    '157.5 EGP' / '138,0' -> number. The UNIT (EGP or piasters) is decided in
--                 int_vendor_items__events, because it depends on the vendor (POS_LEGACY = piasters)
--   currency      egp / E£ / جنيه -> EGP ; PT = piasters
--   qty_on_hand   '86.0' -> 86 ; a negative value is kept in qty_on_hand_reported for the QA gate
--                 and becomes NULL in qty_on_hand
--   is_available  Y / yes / 0 ... -> boolean
--   updated_at    LOCAL Cairo time ('2026-09-01 00:00:05', '09/01/2026 12:00 AM') or epoch ms -> UTC.
--                 This is the business time of the change (event_time_utc).

{%- set on_team_account = target.name in ['shared', 'prod'] %}

with source as (

    select * from {{ source('ops_db_cdc', 'vendor_items_events') }}

),

renamed as (

    select
        {{ 'src_lsn' if on_team_account else 'lsn' }}                as lsn_raw,
        {{ 'cdc_op'  if on_team_account else 'op' }}                 as op_raw,
        {{ 'kafka_ts' if on_team_account else 'captured_at_ms' }}    as captured_at_raw,
        store_id,
        item_ref,
        item_name,
        item_price,
        currency,
        qty_on_hand,
        is_available,
        updated_by,
        updated_at
    from source

),

typed as (

    select
        -- plain digits only: a value like 1.79E+15 lost its digits, so it becomes NULL and is flagged
        iff(regexp_like(trim(to_varchar(lsn_raw)), '[0-9]{10,}'), try_to_number(trim(to_varchar(lsn_raw))), null) as lsn,
        lower(trim(op_raw))                                          as op,
{% if on_team_account %}
        cast(captured_at_raw as timestamp_ntz)                       as captured_at,      -- Kafka time, UTC
{% else %}
        {{ parse_ts_any_utc('trim(to_varchar(captured_at_raw))') }}  as captured_at,      -- epoch ms -> UTC
{% endif %}
        {{ clean_vendor_id(clean_text('store_id')) }}                as vendor_id,
        {{ clean_text('store_id') }}                                 as store_id,
        {{ normalize_item_ref(clean_text('item_ref')) }}             as item_ref,         -- P00047 -> SKU-00047
        {{ clean_text('item_name') }}                                as item_name,
        {{ parse_decimal(clean_text('item_price')) }}                as item_price_reported,
        lower({{ clean_text('currency') }})                          as currency_raw,
        {{ parse_int(clean_text('qty_on_hand')) }}                   as qty_on_hand_reported,
        {{ to_boolean_clean('is_available') }}                       as is_available,
        lower({{ clean_text('updated_by') }})                        as updated_by,
        {{ parse_ts_any_utc(clean_text('updated_at')) }}             as event_time_utc
    from renamed

),

deduplicated as (

    -- the same CDC event delivered twice has the same lsn: keep one copy
    select *
    from typed
    qualify lsn is null
         or row_number() over (partition by lsn, vendor_id, item_ref, op order by captured_at) = 1

),

enriched as (

    select
        d.*,
        cur.clean_value                                              as currency,
        case d.op
            when 'c' then 'insert'
            when 'u' then 'update'
            when 'd' then 'delete'
            when 'r' then 'snapshot'
        end                                                          as op_name,
        case
            when d.updated_by is null          then null
            when d.updated_by like 'wms%'      then 'wms'
            when d.updated_by like 'partner%'  then 'partner_api'
            when d.updated_by like 'mgr%'      then 'manager'
            else 'other'
        end                                                          as update_source,
        case when d.qty_on_hand_reported >= 0 then d.qty_on_hand_reported end as qty_on_hand,
        -- idempotency key: the same event always gets the same id
        md5(concat_ws('|',
            coalesce(to_varchar(d.lsn), '~'),
            coalesce(d.vendor_id, '~'),
            coalesce(d.item_ref, '~'),
            coalesce(d.op, '~')))                                    as event_id,
        -- events that cannot be placed in the history at all (no order, no time, no key)
        (d.lsn is not null
         and d.op in ('c', 'u', 'd', 'r')
         and d.event_time_utc is not null
         and d.vendor_id is not null
         and d.item_ref is not null)                                 as is_processable
    from deduplicated as d
    left join {{ ref('map_currency') }} as cur
        on d.currency_raw = cur.raw_value

)

select
    *,
    nullif(array_to_string(array_construct_compact(
        iff(lsn is null,                                              'lsn_invalid',              null),
        iff(op is null or op not in ('c', 'u', 'd', 'r'),             'op_invalid',               null),
        iff(event_time_utc is null,                                   'updated_at_unparseable',   null),
        iff(captured_at is null,                                      'captured_at_unparseable',  null),
        iff(vendor_id is null,                                        'store_id_missing',         null),
        iff(item_ref is null,                                         'item_ref_missing',         null),
        iff(op <> 'd' and item_name is null,                          'item_name_missing',        null),
        iff(op <> 'd' and (item_price_reported is null or item_price_reported <= 0),
                                                                      'item_price_not_positive',  null),
        iff(qty_on_hand_reported < 0,                                 'negative_stock_nulled',    null),
        iff(currency_raw is not null and currency is null,            'currency_unmapped',        null),
        iff(op <> 'd' and is_available is null,                       'is_available_missing',     null)
    ), ', '), '')                                                    as dq_issues,
    dq_issues is not null                                            as dq_flag
from enriched
