-- Compares dim_vendor_item_history with the answer key (expected_scd2_vendor_items.csv).
-- Run:  dbt compile --select audit_01_scd2_vs_answer_key
--       then run the SQL from target/compiled/supply_pulse/analyses/ in Snowsight.
-- Perfect result: every *_match column equals expected_versions, and only_in_* are 0.

with expected as (

    select
        vendor_id,
        item_ref,
        try_to_number(version)                                              as version_number,
        try_to_decimal(price_egp, 12, 2)                                    as price_egp,
        try_to_boolean(is_available)                                        as is_available,
        try_to_timestamp_ntz(replace(replace(valid_from_utc, 'T', ' '), 'Z', ''))          as valid_from,
        try_to_timestamp_ntz(replace(replace(nullif(valid_to_utc, ''), 'T', ' '), 'Z', '')) as valid_to,
        try_to_boolean(is_current)                                          as is_current
    from {{ source('answer_key', 'expected_scd2') }}

),

ours as (

    select vendor_id, item_ref, version_number, price_egp, is_available,
           valid_from, valid_to, is_current
    from {{ ref('dim_vendor_item_history') }}

),

compared as (

    select
        e.vendor_id is not null                                             as in_expected,
        o.vendor_id is not null                                             as in_ours,
        e.price_egp = o.price_egp                                           as price_ok,
        e.is_available = o.is_available                                     as available_ok,
        e.valid_from = o.valid_from                                         as from_ok,
        equal_null(e.valid_to, o.valid_to)                                  as to_ok,
        e.is_current = o.is_current                                         as current_ok
    from expected as e
    full outer join ours as o
        on  e.vendor_id = o.vendor_id
        and e.item_ref = o.item_ref
        and e.version_number = o.version_number

)

select
    count_if(in_expected)                                                   as expected_versions,
    count_if(in_ours)                                                       as our_versions,
    count_if(in_expected and not in_ours)                                   as only_in_expected,
    count_if(in_ours and not in_expected)                                   as only_in_ours,
    count_if(price_ok)                                                      as price_match,
    count_if(available_ok)                                                  as availability_match,
    count_if(from_ok)                                                       as valid_from_match,
    count_if(to_ok)                                                         as valid_to_match,
    count_if(current_ok)                                                    as is_current_match,
    count_if(price_ok and available_ok and from_ok and to_ok and current_ok) as fully_matching_versions
from compared
