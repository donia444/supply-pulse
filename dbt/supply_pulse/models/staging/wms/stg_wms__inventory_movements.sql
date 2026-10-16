-- Staging model: wms.inventory_movements
-- Grain: one row per movement_id.
-- Project style: rows are never dropped for bad values; a value that cannot be repaired
-- becomes NULL, and dq_flag / dq_issues say what was repaired or nulled.
--
-- Repair rules (all taken from the data-quality answer key):
--   warehouse_code   wh-0183 / 0102 / WH99 -> WH-0183 / WH-0102 / WH-0099 ; vendor_id = V + same number
--   movement_type    OB / opening / GRN / PICK / damage ... -> mapping seed (5 standard types)
--   quantity         '99 units' -> 99 ; SALE and WASTE are outbound, so they are always negative,
--                    RESTOCK and OPENING_BALANCE always positive, ADJUSTMENT keeps its sign
--   unit_cost_egp    '21,38' -> 21.38
--   product_barcode  '6221182572626.0' -> 6221182572626 ; Excel scientific notation (6.22882E+12)
--                    lost digits, so the approximate value is kept and the real barcode is
--                    recovered from the catalog in fact_inventory_movement
--   movement_ts      local DD/MM/YYYY time, ISO with +03:00, or epoch ms -> UTC
--   expiry_date      YYYY-MM-DD, 'Sep 06, 2026', or a slash date in either day order (the reading that
--                    is not before the movement wins) ; still before the movement -> NULL
--   duplicates       exact copies collapse to one row

with source as (

    -- SCD Type 1 on the shared account: latest CDC event per movement_id (deletes dropped).
    -- On dev the macro returns the RAW table unchanged.
    select * from {{ latest_cdc_row(source('wms', 'inventory_movements'), ['movement_id']) }}

),

-- NULL-like text -> real NULL once; exact duplicate rows collapse here
cleaned_text as (

    select distinct
        upper({{ clean_text('movement_id') }})        as movement_id,
        upper({{ clean_text('warehouse_code') }})     as warehouse_code_raw,
        {{ clean_text('product_barcode') }}           as product_barcode_raw,
        lower({{ clean_text('movement_type') }})      as movement_type_raw,
        {{ clean_text('quantity') }}                  as quantity_raw,
        {{ clean_text('unit_cost_egp') }}             as unit_cost_raw,
        {{ clean_text('expiry_date') }}               as expiry_date_raw,
        {{ clean_text('reference_id') }}              as reference_id,
        {{ clean_text('movement_ts') }}               as movement_ts_raw
    from source

),

parsed as (

    select
        c.*,

        case
            when regexp_replace(c.warehouse_code_raw, '[^0-9]', '') <> ''
                then 'WH-' || lpad(regexp_replace(c.warehouse_code_raw, '[^0-9]', ''), 4, '0')
        end                                                         as warehouse_code,
        {{ clean_vendor_id('c.warehouse_code_raw') }}               as vendor_id,

        coalesce(mt.clean_value, upper(c.movement_type_raw))        as movement_type,
        mt.clean_value is null and c.movement_type_raw is not null  as is_movement_type_unmapped,

        {{ parse_int('c.quantity_raw') }}                           as quantity_parsed,
        {{ parse_decimal('c.unit_cost_raw') }}                      as unit_cost_parsed,

        -- barcode: plain digits, or digits followed by '.0' (stored as a float)
        case
            when regexp_like(c.product_barcode_raw, '^[0-9]{8,14}([.]0+)?$')
                then regexp_substr(c.product_barcode_raw, '^[0-9]+')
        end                                                         as product_barcode,
        -- barcode in scientific notation: keep the approximate number and its precision
        regexp_like(c.product_barcode_raw, '^[0-9]+([.][0-9]+)?[eE][+]?[0-9]+$') as is_barcode_scientific,

        {{ parse_ts_any_utc('c.movement_ts_raw') }}                 as movement_ts_parsed,

        try_to_date(c.expiry_date_raw, 'YYYY-MM-DD')                as expiry_iso,
        try_to_date(c.expiry_date_raw, 'MON DD, YYYY')              as expiry_month_name,
        try_to_date(c.expiry_date_raw, 'DD/MM/YYYY')                as expiry_day_first,
        try_to_date(c.expiry_date_raw, 'MM/DD/YYYY')                as expiry_month_first

    from cleaned_text as c
    left join {{ ref('map_movement_type') }} as mt
        on c.movement_type_raw = mt.raw_value

),

-- Slash dates come in BOTH orders (15/09/2026 and 09/15/2026). When both readings are valid,
-- the expiry is the earliest reading that is not before the movement date
-- ('09/05/2026' moved on 2026-09-01 -> 2026-09-05, not 2026-05-09).
expiry_resolved as (

    select
        *,
        coalesce(
            expiry_iso,
            expiry_month_name,
            case
                when expiry_day_first is not null and expiry_month_first is not null then
                    case
                        when expiry_day_first   >= movement_ts_parsed::date
                         and expiry_month_first >= movement_ts_parsed::date then least(expiry_day_first, expiry_month_first)
                        when expiry_month_first >= movement_ts_parsed::date then expiry_month_first
                        else expiry_day_first
                    end
                else coalesce(expiry_day_first, expiry_month_first)
            end
        )                                                           as expiry_date_parsed
    from parsed

),

repaired as (

    select
        movement_id,
        warehouse_code,
        vendor_id,
        movement_type,
        product_barcode,
        is_barcode_scientific,
        -- approximate barcode value and how far the real barcode can be from it
        -- (6.22882E+12 has 6 significant digits -> the real value is within +/- 5,000,000)
        iff(is_barcode_scientific, try_to_double(product_barcode_raw), null)        as barcode_approx_value,
        iff(is_barcode_scientific,
            0.5 * power(10,
                to_number(regexp_substr(product_barcode_raw, '[eE][+]?([0-9]+)', 1, 1, 'e', 1))
                - (length(replace(regexp_substr(product_barcode_raw, '^[0-9.]+'), '.', '')) - 1)),
            null)                                                                   as barcode_approx_tolerance,

        -- sign convention: outbound types are negative, inbound types positive
        case
            when movement_type in ('SALE', 'WASTE')              then -abs(quantity_parsed)
            when movement_type in ('RESTOCK', 'OPENING_BALANCE') then  abs(quantity_parsed)
            else quantity_parsed
        end                                                         as quantity,

        case when unit_cost_parsed > 0 then unit_cost_parsed end    as unit_cost_egp,

        case when movement_ts_parsed <= {{ cutoff_ts() }} then movement_ts_parsed end as movement_ts,

        case
            when expiry_date_parsed is not null
             and movement_ts_parsed is not null
             and expiry_date_parsed < movement_ts_parsed::date      then null
            else expiry_date_parsed
        end                                                         as expiry_date,

        reference_id,

        nullif(array_to_string(array_construct_compact(
            iff(movement_id is null,                                    'movement_id_missing',          null),
            iff(warehouse_code is null,                                 'warehouse_code_missing',       null),
            iff(is_movement_type_unmapped,                              'movement_type_unmapped',       null),
            iff(quantity_parsed is null or quantity_parsed = 0,         'quantity_invalid',             null),
            iff((movement_type in ('SALE', 'WASTE') and quantity_parsed > 0)
                or (movement_type in ('RESTOCK', 'OPENING_BALANCE') and quantity_parsed < 0),
                                                                        'quantity_sign_fixed',          null),
            iff(unit_cost_parsed is null or unit_cost_parsed <= 0,      'unit_cost_missing',            null),
            iff(is_barcode_scientific,                                  'barcode_scientific_notation',  null),
            iff(product_barcode is null and not is_barcode_scientific,  'barcode_invalid',              null),
            iff(movement_ts_parsed is null,                             'movement_ts_unparseable',      null),
            iff(movement_ts_parsed > {{ cutoff_ts() }},                 'movement_ts_in_future_nulled', null),
            iff(expiry_date_raw is not null and expiry_date_parsed is null, 'expiry_date_unparseable',  null),
            iff(expiry_date_parsed < movement_ts_parsed::date,          'expiry_before_movement_nulled', null)
        ), ', '), '')                                               as dq_issues

    from expiry_resolved

),

deduplicated as (

    -- one row per movement_id (a row without id is kept on its own)
    select *
    from repaired
    qualify movement_id is null
         or row_number() over (partition by movement_id order by movement_ts, product_barcode) = 1

)

select
    *,
    quantity * unit_cost_egp      as total_cost_egp,
    dq_issues is not null         as dq_flag
from deduplicated
