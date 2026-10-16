-- Checks every documented data defect (dq_manifest.csv) against the value our models produced.
-- For each source file / column / kind of defect it reports how many were repaired exactly.
-- Run:  dbt compile --select audit_03_cleaning_vs_answer_key
--       then run the SQL from target/compiled/supply_pulse/analyses/ in Snowsight.
--
-- How to read it:
--   match_pct 100       -> the rule repairs this defect exactly like the answer key
--   ours_is_null high   -> the true value cannot be recovered from the data (e.g. a date in the
--                          future, a missing value with no other source); NULL + dq_flag is the
--                          honest result there, so a low match_pct is EXPECTED for those rows
--   different > 0       -> look at sample_* columns: a rule may need fixing
-- Every value is compared in the answer key's own format (UTC 'Z' times for orders/customers,
-- local Cairo time for inventory / vendor-item events, DD/MM/YYYY for vendor onboarding).

with manifest as (

    select
        source_file,
        record_key,
        column_name,
        issue_category,
        issue_detail,
        coalesce(try_parse_json(clean_value)::varchar,
                 iff(clean_value = 'null', null, clean_value))              as clean_value
    from {{ source('answer_key', 'dq_manifest') }}
    where column_name not in ('*row', '*entity')          -- row duplicates are checked by unique tests

),

-- our cleaned values, one row per (file, record, column), written in the answer key's format
ours as (

    -- customers ---------------------------------------------------------------
    select 'customer_app/customers.csv' as source_file, customer_id as record_key, column_name, our_value
    from (
        select
            customer_id,
            full_name,
            phone,
            email,
            city,
            to_char(signup_ts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')               as signup_ts,
            to_varchar(is_pro_subscriber)                                   as is_pro_subscriber
        from {{ ref('stg_customer_app__customers') }}
    ) unpivot include nulls (our_value for column_name in
        (full_name, phone, email, city, signup_ts, is_pro_subscriber))

    union all

    -- orders ------------------------------------------------------------------
    select 'customer_app/orders.csv', order_id, column_name, our_value
    from (
        select
            order_id,
            customer_id,
            vendor_id,
            to_char(order_ts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')                as order_ts,
            to_char(delivered_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')            as delivered_at,
            status,
            cancel_reason,
            payment_method,
            delivery_zone,
            to_varchar(subtotal)                                            as subtotal,
            to_varchar(discount_amount)                                     as discount_amount,
            to_varchar(service_fee)                                         as service_fee,
            to_varchar(total_amount)                                        as total_amount,
            to_varchar(estimated_delivery_min)                              as estimated_delivery_min,
            to_varchar(customer_rating)                                     as customer_rating
        from {{ ref('stg_customer_app__orders') }}
    ) unpivot include nulls (our_value for column_name in
        (customer_id, vendor_id, order_ts, delivered_at, status, cancel_reason, payment_method,
         delivery_zone, subtotal, discount_amount, service_fee, total_amount,
         estimated_delivery_min, customer_rating))

    union all

    -- order items (record key = ORDER_ID#LINE) -------------------------------------
    select 'customer_app/order_items.csv', record_key, column_name, our_value
    from (
        select
            upper(trim(order_id)) || '#' || to_varchar(line_number)        as record_key,
            item_ref,
            item_name,
            to_varchar(quantity)                                            as quantity,
            to_varchar(unit_price)                                          as unit_price,
            to_varchar(line_total)                                          as line_total,
            to_varchar(vendor_confirmed_price)                              as vendor_confirmed_price
        from {{ ref('stg_customer_app__order_items') }}
    ) unpivot include nulls (our_value for column_name in
        (item_ref, item_name, quantity, unit_price, line_total, vendor_confirmed_price))

    union all

    -- products ----------------------------------------------------------------
    select 'catalog_pim/products.csv', product_id, column_name, our_value
    from (
        select
            product_id,
            barcode,
            product_name,
            brand,
            category,
            pack_size,
            to_varchar(base_price_egp)                                      as base_price_egp,
            to_varchar(is_perishable)                                       as is_perishable,
            to_varchar(is_active)                                           as is_active
        from {{ ref('stg_catalog_pim__products') }}
    ) unpivot include nulls (our_value for column_name in
        (barcode, product_name, brand, category, pack_size, base_price_egp, is_perishable, is_active))

    union all

    -- menu items --------------------------------------------------------------
    select 'restaurant_portal/menu_items.csv', menu_item_id, column_name, our_value
    from (
        select
            menu_item_id,
            item_name,
            menu_section,
            to_varchar(price_egp)                                           as price_egp,
            to_varchar(is_vegetarian)                                       as is_vegetarian
        from {{ ref('stg_restaurant_portal__menu_items') }}
    ) unpivot include nulls (our_value for column_name in
        (item_name, menu_section, price_egp, is_vegetarian))

    union all

    -- vendors (answer-key column names are Title_Case) -------------------------
    select 'partners_system/vendors.csv', vendor_id, column_name, our_value
    from (
        select
            vendor_id,
            vendor_type                                                     as "Vendor_Type",
            cuisine                                                         as "Cuisine",
            integration_type                                                as "Integration_Type",
            city                                                            as "City",
            area                                                            as "Area",
            to_varchar(commission_pct)                                      as "Commission_Pct",
            to_varchar(rating)                                              as "Rating",
            to_char(onboarded_on, 'DD/MM/YYYY')                             as "Onboarded_On",
            status                                                          as "Status"
        from {{ ref('stg_partners_system__vendors') }}
    ) unpivot include nulls (our_value for column_name in
        ("Vendor_Type", "Cuisine", "Integration_Type", "City", "Area", "Commission_Pct",
         "Rating", "Onboarded_On", "Status"))

    union all

    -- inventory movements (times in local Cairo time, like the answer key) ------
    select 'wms/inventory_movements.csv', movement_id, column_name, our_value
    from (
        select
            movement_id,
            warehouse_code,
            product_barcode,
            movement_type,
            to_varchar(quantity)                                            as quantity,
            to_varchar(unit_cost_egp)                                       as unit_cost_egp,
            to_char(expiry_date, 'YYYY-MM-DD')                              as expiry_date,
            to_char(convert_timezone('UTC', 'Africa/Cairo', movement_ts), 'DD/MM/YYYY HH24:MI:SS') as movement_ts
        from {{ ref('fact_inventory_movement') }}
    ) unpivot include nulls (our_value for column_name in
        (warehouse_code, product_barcode, movement_type, quantity, unit_cost_egp, expiry_date, movement_ts))

    union all

    -- vendor-item events (record key = lsn=<number>; price in the unit the event reported) ---
    select 'ops_db_cdc/vendor_items_events.csv', record_key, column_name, our_value
    from (
        select
            'lsn=' || to_varchar(lsn)                                       as record_key,
            currency,
            to_varchar(is_available)                                        as is_available,
            to_varchar(item_price_reported)                                 as item_price,
            to_varchar(qty_on_hand)                                         as qty_on_hand,
            to_char(convert_timezone('UTC', 'Africa/Cairo', event_time_utc), 'YYYY-MM-DD HH24:MI:SS') as updated_at
        from {{ ref('int_vendor_items__events') }}
    ) unpivot include nulls (our_value for column_name in
        (currency, is_available, item_price, qty_on_hand, updated_at))

),

compared as (

    select
        m.*,
        o.our_value,
        o.record_key is not null                                            as record_found,
        case
            when m.clean_value is null and o.our_value is null              then true
            when try_to_double(m.clean_value) is not null
             and try_to_double(o.our_value) is not null
                then abs(try_to_double(m.clean_value) - try_to_double(o.our_value)) < 0.005
            else equal_null(m.clean_value, o.our_value)
        end                                                                 as is_match
    from manifest as m
    left join ours as o
        on  o.source_file = m.source_file
        and o.record_key = m.record_key
        and lower(o.column_name) = lower(m.column_name)

)

select
    source_file,
    column_name,
    issue_category,
    issue_detail,
    count(*)                                                                as issues,
    count_if(is_match)                                                      as matched,
    count_if(not is_match and our_value is null and record_found)           as ours_is_null,
    count_if(not is_match and our_value is not null)                        as different,
    count_if(not record_found)                                              as record_not_found,
    round(100 * count_if(is_match) / count(*), 1)                           as match_pct,
    max(iff(not is_match and our_value is not null, record_key, null))      as sample_record,
    max(iff(not is_match and our_value is not null, clean_value, null))     as sample_expected,
    max(iff(not is_match and our_value is not null, our_value, null))       as sample_ours
from compared
group by all
order by source_file, match_pct, issues desc
