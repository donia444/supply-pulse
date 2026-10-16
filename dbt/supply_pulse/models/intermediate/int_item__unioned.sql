-- Intermediate model: one unified item list (catalog products + restaurant menu items).
-- Grain: one row per item_ref. Duplicate barcodes are removed (keeps the lowest item_ref).
-- NULLs in source-specific columns are intentional ("not applicable").

with products as (

    select * from {{ ref('stg_catalog_pim__products') }}

),

menu_items as (

    select * from {{ ref('stg_restaurant_portal__menu_items') }}

),

-- Both CTEs must expose the SAME columns in the SAME order (UNION ALL matches by position)
products_shaped as (

    select
        {{ normalize_item_ref('product_id') }}  as item_ref,
        'catalog_pim'                           as item_source,
        product_name                            as item_name,
        category                                as category,
        cast(null as varchar)                   as menu_section,   -- not applicable for products
        brand                                   as brand,
        pack_size                               as pack_size,
        barcode                                 as barcode,
        is_valid_barcode                        as is_valid_barcode,
        cast(null as varchar)                   as vendor_id,
        base_price_egp                          as price_egp,
        is_perishable                           as is_perishable,
        cast(null as boolean)                   as is_vegetarian,
        is_active                               as is_active
    from products

),

menu_items_shaped as (

    select
        {{ normalize_item_ref('menu_item_id') }} as item_ref,
        'restaurant_portal'                      as item_source,
        item_name                                as item_name,
        cast(null as varchar)                    as category,      -- not applicable for menu items
        menu_section                             as menu_section,
        cast(null as varchar)                    as brand,
        cast(null as varchar)                    as pack_size,
        cast(null as varchar)                    as barcode,
        cast(null as boolean)                    as is_valid_barcode,
        vendor_id                                as vendor_id,
        price_egp                                as price_egp,
        cast(null as boolean)                    as is_perishable,
        is_vegetarian                            as is_vegetarian,
        cast(null as boolean)                    as is_active
    from menu_items

),

unioned as (

    select * from products_shaped
    union all
    select * from menu_items_shaped

),

-- Keep one row per barcode. Menu items have a NULL barcode, so item_ref keeps each in its own group.
deduplicated as (

    select * from unioned
    qualify row_number() over (partition by coalesce(barcode, item_ref) order by item_ref) = 1

)

select * from deduplicated