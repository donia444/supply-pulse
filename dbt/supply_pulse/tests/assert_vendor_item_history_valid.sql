-- SCD Type 2 integrity checks on dim_vendor_item_history. Returns failing rows.
-- valid_to is NULL for the current version, so it is read as "far future" here.

-- 1) A vendor item must not have more than one current version
select vendor_id, item_ref, 'more_than_one_current_version' as problem
from {{ ref('dim_vendor_item_history') }}
where is_current
group by vendor_id, item_ref
having count(*) > 1

union all

-- 2) A version must not end before it starts
select vendor_id, item_ref, 'valid_to_before_valid_from' as problem
from {{ ref('dim_vendor_item_history') }}
where valid_to < valid_from

union all

-- 3) Two versions of the same vendor item must not overlap in time
select a.vendor_id, a.item_ref, 'overlapping_versions' as problem
from {{ ref('dim_vendor_item_history') }} as a
join {{ ref('dim_vendor_item_history') }} as b
  on a.vendor_id = b.vendor_id
 and a.item_ref = b.item_ref
 and a.vendor_item_version_key <> b.vendor_item_version_key
 and a.valid_from < coalesce(b.valid_to, '9999-12-31'::timestamp_ntz)
 and b.valid_from < coalesce(a.valid_to, '9999-12-31'::timestamp_ntz)
