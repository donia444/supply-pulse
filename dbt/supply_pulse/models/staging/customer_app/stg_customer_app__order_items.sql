-- Stage model for customer_app.order_items
-- Grain: one row per (order_id, line_number)

with source as (
    -- SCD Type 1 on the shared account: latest CDC event per order line (deletes dropped).
    -- On dev the macro returns the RAW table unchanged.
    select * from {{ latest_cdc_row(source('customer_app', 'order_items'), ['order_id', 'line_number']) }}
),

-- Exact duplicate rows share the same key, so we keep only one of them.
deduplicated as (
    select * from source
    qualify row_number() over (
        partition by order_id, line_number
        order by order_id
    ) = 1
),

-- Convert every raw text column to its proper type
typed as (
    select
        order_id,
        line_number,
        -- P00012 -> SKU-00012, m0000018 -> M0000018
        {{ normalize_item_ref('item_ref') }}              as item_ref,
        -- Empty/'null'/'N/A' -> NULL, then ALL UPPER names -> Title Case
        {{ fix_upper_case(clean_null('item_name')) }}     as item_name,
        -- "1 pcs" -> 1 (a minus sign is kept)
        try_to_number(regexp_substr(trim(quantity), '^-?[0-9]+')) as quantity,
        -- Decimal comma and "LE" suffix handled by the macro
        {{ to_number_clean('unit_price') }}               as unit_price,
        -- Kept as "raw": the piasters check below needs quantity and prices
        {{ to_number_clean('line_total') }}               as line_total_raw,
        {{ to_number_clean('vendor_confirmed_price') }}   as vendor_confirmed_price
    from deduplicated
),

-- For rows with a zero/negative quantity, line_total / unit_price tells us what the
-- quantity should be (line_total = quantity * unit_price held on 100% of valid rows).
-- Valid quantities are 1..max_valid_quantity (observed max is 4 in this dataset).
-- An implied value of exactly 100/200/300 means line_total is in piasters.
implied as (
    select
        *,
        -- div0null returns NULL (not an error) when unit_price is NULL or 0
        round(div0null(line_total_raw, unit_price))       as implied_qty
    from typed
),

flagged as (
    select
        *,
        -- The ORIGINAL quantity is zero or negative
        quantity <= 0                                     as is_invalid_quantity,

        -- Implied quantity is a normal quantity (1..max).
        -- coalesce(..., false) so rows without unit_price get false, not NULL.
        coalesce(
            quantity <= 0 and unit_price > 0
            and implied_qty between 1 and {{ var('max_valid_quantity', 10) }},
            false)                                        as fix_qty_direct,

        -- Implied quantity is a normal quantity x 100 -> line_total is in piasters
        coalesce(
            quantity <= 0 and unit_price > 0
            and implied_qty between 100 and 100 * {{ var('max_valid_quantity', 10) }}
            and mod(implied_qty, 100) = 0,
            false)                                        as fix_qty_piasters
    from implied
),

cleaned as (
    select
        order_id,
        line_number,
        item_ref,
        item_name,

        -- Original quantity is kept untouched for traceability
        quantity                                          as quantity_raw,

        -- Rebuilt quantity when possible, otherwise the original value
        cast(
            case
                when fix_qty_direct   then implied_qty
                when fix_qty_piasters then implied_qty / 100
                else quantity
            end
        as number(10, 0))                                 as quantity,

        is_invalid_quantity,
        (fix_qty_direct or fix_qty_piasters)              as is_quantity_rebuilt,

        unit_price,

        -- line_total in piasters: either found through the implied quantity
        -- (rebuilt quantity) or by the ratio check (valid quantity)
        case
            when fix_qty_piasters then round(line_total_raw / 100, 2)
            when quantity > 0
             and div0null(
                    line_total_raw,
                    quantity * coalesce(unit_price, vendor_confirmed_price)
                 ) between 90 and 110
            then round(line_total_raw / 100, 2)
            else line_total_raw
        end                                               as line_total,

        vendor_confirmed_price
    from flagged
),

-- Decision 1: rebuild a missing unit_price as line_total / quantity.
-- vendor_confirmed_price is never used to fill this column.
priced as (
    select
        order_id,
        line_number,
        item_ref,
        item_name,
        quantity_raw,
        quantity,
        is_invalid_quantity,
        is_quantity_rebuilt,

        coalesce(
            unit_price,
            case
                when quantity > 0 and line_total is not null
                then round(line_total / quantity, 2)
            end
        )                                                 as unit_price,

        -- True only where we rebuilt the price
        (unit_price is null
         and quantity > 0
         and line_total is not null)                      as is_unit_price_rebuilt,

        line_total,
        vendor_confirmed_price
    from cleaned
)

select * from priced