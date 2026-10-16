{{ config(severity='warn') }}

-- Warns when more than 5% of listing changes could not be matched to a vendor.
-- A high share usually means the store_id -> vendor_id rule (store 12 = V0012) is wrong.
select unknown_share
from (
    select count_if(vendor_key = '-1') / nullif(count(*), 0) as unknown_share
    from {{ ref('fact_listing_change') }}
)
where unknown_share > 0.05
