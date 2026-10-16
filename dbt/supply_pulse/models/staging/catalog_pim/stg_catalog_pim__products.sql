-- Staging model: catalog_pim.products
-- Grain: one row per product_id (barcode duplicates are handled later, in int_item__unioned).
-- Cleans text, price and boolean fields, and standardizes category via a mapping seed.

with source as (

    -- SCD Type 1 on the shared account: latest CDC event per product_id (deletes dropped).
    -- On dev the macro returns the RAW table unchanged.
    select * from {{ latest_cdc_row(source('catalog_pim', 'products'), ['product_id']) }}

),

-- Cleaning pass: NULL-like text -> real NULL, fix upper-case names,
-- standard booleans, normalized pack size, price text -> number
typed as (

    select
        product_id,
        -- a wrong check digit is corrected (answer key: P00055); is_valid_barcode keeps the original verdict
        {{ ean13_with_check_digit('trim(barcode)') }}      as barcode,
        {{ is_valid_barcode('trim(barcode)') }}            as is_valid_barcode,
        {{ fix_upper_case(clean_null('product_name')) }}   as product_name,
        {{ clean_null('brand') }}                          as brand,
        trim(category)                                     as category_raw,   -- original text, kept for auditing
        {{ normalize_pack_size('pack_size') }}             as pack_size,
        {{ to_number_clean('base_price_egp') }}            as base_price_egp,
        {{ to_boolean_clean('is_perishable') }}            as is_perishable,
        {{ to_boolean_clean('is_active') }}                as is_active
    from source

),

-- Mapping seed: lower-cased raw spelling -> one standard category label
category_map as (

    select raw_value, clean_value
    from {{ ref('map_product_category') }}

),

-- Standardize category.
-- LEFT JOIN on purpose: a product whose spelling is missing from the seed
-- must NOT disappear. It gets a NULL category and the staging test fails,
-- which tells us to add the new spelling to the seed.
mapped as (

    select
        t.product_id,
        t.barcode,
        t.is_valid_barcode,
        t.product_name,
        t.brand,
        m.clean_value                                      as category,
        t.category_raw,
        t.pack_size,
        t.base_price_egp,
        t.is_perishable,
        t.is_active
    from typed as t
    left join category_map as m
        on lower(t.category_raw) = m.raw_value

)

select * from mapped