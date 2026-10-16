-- Dimension: one row per item (grocery SKU or restaurant menu item), plus one Unknown row.
-- Grain: one row per item_ref. Column names and order follow erd_gold_v2.
-- Built from int_item__unioned, which already removed duplicate barcodes.
-- NULLs in source-specific columns are intentional ("not applicable"), e.g. a menu item has no barcode.

{{ config(materialized='table') }}

with items as (

    select * from {{ ref('int_item__unioned') }}

),

shaped as (

    select
        -- surrogate key: stable hash of the business key, used by fact tables
        {{ dbt_utils.generate_surrogate_key(['item_ref']) }}  as item_key,
        item_ref,                                              -- business key (SKU-##### or M#######)

        -- PLACEHOLDER values: confirm the real item_type values with the team
        case item_source
            when 'catalog_pim'        then 'grocery'
            when 'restaurant_portal'  then 'menu'
        end                                                    as item_type,

        item_name,
        barcode,
        brand,
        category,                                              -- grocery items only
        pack_size,
        price_egp                  as list_price_egp,          -- catalog / menu price, not the order price
        is_perishable,                                         -- grocery items only
        is_active,                                             -- grocery items only
        menu_section,                                          -- menu items only
        is_vegetarian,                                         -- menu items only
        vendor_id                  as owner_vendor_id,         -- menu items only
        is_valid_barcode                                       -- extra column: FALSE = failed EAN-13 check digit
    from items

),

-- Unknown member: fact rows whose item cannot be found point here instead of having a NULL key
unknown_member as (

    select
        '-1'                          as item_key,
        '-1'                          as item_ref,
        'unknown'                     as item_type,
        'Unknown'                     as item_name,
        cast(null as varchar)         as barcode,
        cast(null as varchar)         as brand,
        cast(null as varchar)         as category,
        cast(null as varchar)         as pack_size,
        cast(null as number(10, 2))   as list_price_egp,
        cast(null as boolean)         as is_perishable,
        cast(null as boolean)         as is_active,
        cast(null as varchar)         as menu_section,
        cast(null as boolean)         as is_vegetarian,
        cast(null as varchar)         as owner_vendor_id,
        cast(null as boolean)         as is_valid_barcode

)

select * from shaped
union all
select * from unknown_member