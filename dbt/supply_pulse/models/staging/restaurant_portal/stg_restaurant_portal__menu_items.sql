-- Staging model: restaurant_portal.menu_items
-- Grain: one row per menu_item_id.
-- Cleans text, price and boolean fields, and standardizes menu_section via a mapping seed.

with source as (

    -- SCD Type 1 on the shared account: latest CDC event per menu_item_id (deletes dropped).
    -- On dev the macro returns the RAW table unchanged.
    select * from {{ latest_cdc_row(source('restaurant_portal', 'menu_items'), ['menu_item_id']) }}

),

-- Remove duplicate rows: keep one row per menu_item_id
deduplicated as (

    select * from source
    qualify row_number() over (partition by menu_item_id order by menu_item_id) = 1

),

-- First cleaning pass: NULL-like text -> real NULL, trim spaces,
-- price text -> number, vegetarian flag -> standard boolean
typed as (

    select
        menu_item_id,
        vendor_id,
        {{ clean_null("rtrim(item_name, ' .')") }}  as item_name,
        trim(menu_section)                           as raw_section,
        {{ to_number_clean('price_egp') }}           as price_raw,
        {{ to_boolean_clean('is_vegetarian') }}      as is_vegetarian
    from deduplicated

),

-- Second cleaning pass: fix upper-case names, convert piasters to EGP.
-- The original section text is kept as menu_section_raw for auditing
-- and for the "unmapped values" test.
cleaned as (

    select
        menu_item_id,
        vendor_id,
        {{ fix_upper_case('item_name') }}            as item_name,
        raw_section                                  as menu_section_raw,
        {{ piasters_to_egp('price_raw') }}           as price_egp,
        is_vegetarian
    from typed

),

-- Mapping seed: lower-cased raw spelling -> one standard section label
menu_section_map as (

    select raw_value, clean_value
    from {{ ref('map_menu_section') }}

),

-- Standardize menu_section.
-- LEFT JOIN on purpose: an item whose spelling is missing from the seed
-- must NOT disappear. It gets a NULL menu_section and the staging test fails,
-- which tells us to add the new spelling to the seed.
mapped as (

    select
        c.menu_item_id,
        c.vendor_id,
        c.item_name,
        m.clean_value                                as menu_section,
        c.menu_section_raw,
        c.price_egp,
        c.is_vegetarian
    from cleaned as c
    left join menu_section_map as m
        on lower(c.menu_section_raw) = m.raw_value

)

select * from mapped