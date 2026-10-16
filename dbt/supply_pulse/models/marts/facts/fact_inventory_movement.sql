-- Fact: one row per stock movement. Grain: movement_id.
-- ALL movements are kept (project style); rows with repaired or missing values carry
-- dq_flag / dq_issues so analysts can include or exclude them.
--
-- item_key         : looked up from dim_item by BARCODE (the WMS key). A barcode that Excel turned
--                    into scientific notation (6.22882E+12) is recovered when exactly ONE catalog
--                    barcode lies within its rounding range; otherwise the item is Unknown ('-1').
-- vendor_key       : the dark store that owns the warehouse (WH-0012 -> V0012); '-1' when not found
-- movement_date_key: UTC date, same convention as the other facts
-- expiry_date_key  : NULL when there is no (valid) expiry date. Not tested against dim_date
--                    because expiry dates can fall after the dim_date range.

{{ config(materialized='table') }}

with movements as (

    select * from {{ ref('stg_wms__inventory_movements') }}

),

items as (

    select barcode, try_to_number(barcode) as barcode_number, item_key
    from {{ ref('dim_item') }}
    where barcode is not null

),

-- scientific-notation barcodes: find the catalog barcode inside the rounding range
recovered_barcodes as (

    select
        m.movement_id,
        i.barcode,
        count(*) over (partition by m.movement_id) as candidates
    from movements as m
    join items as i
        on abs(i.barcode_number - m.barcode_approx_value) <= m.barcode_approx_tolerance
    where m.is_barcode_scientific

),

resolved as (

    select
        m.*,
        coalesce(m.product_barcode, rb.barcode)                    as product_barcode_resolved
    from movements as m
    left join recovered_barcodes as rb
        on  rb.movement_id = m.movement_id
        and rb.candidates = 1

),

vendors as (

    select vendor_id, vendor_key from {{ ref('dim_vendor') }}

)

select
    -- keys
    r.movement_id,
    coalesce(i.item_key, '-1')                                  as item_key,
    coalesce(v.vendor_key, '-1')                                as vendor_key,
    {{ to_date_key('r.movement_ts') }}                          as movement_date_key,
    to_number(to_char(r.expiry_date, 'YYYYMMDD'))               as expiry_date_key,

    -- measures (quantity is signed: SALE / WASTE negative, RESTOCK / OPENING_BALANCE positive)
    r.quantity,
    r.unit_cost_egp,
    r.total_cost_egp,

    -- context
    r.movement_ts,
    r.expiry_date,
    r.warehouse_code,
    r.vendor_id,
    r.movement_type,
    r.reference_id,
    r.product_barcode_resolved                                  as product_barcode,
    (r.is_barcode_scientific and r.product_barcode_resolved is not null) as is_barcode_recovered,

    -- quality
    r.dq_flag,
    r.dq_issues
from resolved as r
left join items as i
    on i.barcode = r.product_barcode_resolved
left join vendors as v
    on v.vendor_id = r.vendor_id
