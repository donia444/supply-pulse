-- Staging model: partners_system.vendors
-- Grain: one row per vendor_id.
-- Project style: rows are never dropped for bad values; a value that cannot be repaired
-- becomes NULL, and dq_flag / dq_issues say what was repaired or nulled.
--
-- Repair rules (all taken from the data-quality answer key):
--   vendor_id        12 / v-0012 / V0012 -> V0012 (same macro as orders)
--   labels           vendor_type, integration_type, status, cuisine, city, area -> mapping seeds
--   commission_pct   '21.6%' -> 21.6 ; fraction 0.2 -> 20 ; above 100 (190) -> 19 (one digit too many)
--   rating           0-50 scale (42) -> 4.2 ; any other value outside 1-5 -> NULL
--   onboarded_on     DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, YYYYMMDD or 'Jun 05, 2024' ; after the cutoff -> NULL
--   area             missing -> taken from the vendor name when it names a known zone
--                    ('Fresh Hub Mokattam DS-184' -> Mokattam)
--   duplicates       exact copies collapse to one row; a duplicate id with conflicting
--                    status keeps the 'active' row

with source as (

    -- SCD Type 1 on the shared account: latest CDC event per vendorid (deletes dropped).
    -- On dev the macro returns the RAW table unchanged.
    select * from {{ latest_cdc_row(source('partners_system', 'vendors'), ['vendorid']) }}

),

-- NULL-like text -> real NULL once, so every rule below receives clean text
cleaned_text as (

    select distinct                                    -- exact duplicate rows collapse here
        {{ clean_vendor_id(clean_text('vendorid')) }}  as vendor_id,
        {{ clean_text('vendor_name') }}                as vendor_name,
        lower({{ clean_text('vendor_type') }})         as vendor_type_raw,
        {{ clean_text('cuisine') }}                    as cuisine_raw,
        lower({{ clean_text('integration_type') }})    as integration_type_raw,
        lower({{ clean_text('city') }})                as city_raw,
        {{ clean_text('area') }}                       as area_raw,
        {{ clean_text('commission_pct') }}             as commission_raw,
        {{ clean_text('avg_prep_time_min') }}          as prep_time_raw,
        {{ clean_text('rating') }}                     as rating_raw,
        {{ clean_text('onboarded_on') }}               as onboarded_raw,
        lower({{ clean_text('status') }})              as status_raw
    from source

),

parsed as (

    select
        *,
        {{ parse_decimal('commission_raw') }}                           as commission_num,   -- '21.6%' -> 21.6
        {{ parse_int('prep_time_raw') }}                                as prep_time_num,
        {{ parse_decimal('rating_raw') }}                               as rating_num,
        coalesce(
            try_to_date(onboarded_raw, 'DD/MM/YYYY'),
            try_to_date(onboarded_raw, 'MM/DD/YYYY'),          -- 04/29/2026 (day > 12, so not day-first)
            try_to_date(onboarded_raw, 'YYYY-MM-DD'),
            try_to_date(onboarded_raw, 'YYYYMMDD'),            -- 20260506
            try_to_date(onboarded_raw, 'MON DD, YYYY')         -- Jun 05, 2024
        )                                                               as onboarded_parsed
    from cleaned_text

),

-- Zones known to the project (same list the orders use), to recover a missing area from the name
known_zones as (

    select distinct clean_value as zone
    from {{ ref('map_delivery_zone') }}

),

area_from_name as (

    select
        p.vendor_id,
        p.vendor_name,
        z.zone
    from parsed as p
    join known_zones as z
        on contains(lower(p.vendor_name), lower(z.zone))
    where p.area_raw is null
    -- the longest matching zone wins ('New Cairo' rather than a shorter name inside it)
    qualify row_number() over (partition by p.vendor_id, p.vendor_name order by length(z.zone) desc) = 1

),

repaired as (

    select
        p.vendor_id,
        p.vendor_name,
        vt.clean_value                                                  as vendor_type,
        coalesce(cu.clean_value, initcap(p.cuisine_raw))                as cuisine,
        it.clean_value                                                  as integration_type,
        split_part(it.clean_value, '_', 1)                              as integration_family,
        (it.clean_value = 'POS_LEGACY')                                 as prices_in_piasters,
        coalesce(ci.clean_value, initcap(p.city_raw))                   as city,
        coalesce(zn.clean_value, initcap(p.area_raw), an.zone)          as area,

        case
            when p.commission_num between 0 and 100 and p.commission_num >= 1   then p.commission_num
            when p.commission_num = 0                                           then 0
            when p.commission_num > 0 and p.commission_num < 1                  then p.commission_num * 100
            when p.commission_num > 100 and p.commission_num / 10 <= 100        then p.commission_num / 10
        end                                                             as commission_pct,

        case when p.prep_time_num between 1 and 240 then p.prep_time_num end as avg_prep_time_min,

        case
            when p.rating_num between 1 and 5   then p.rating_num
            when p.rating_num between 10 and 50 then p.rating_num / 10
        end                                                             as rating,

        case when p.onboarded_parsed <= {{ cutoff_ts() }}::date then p.onboarded_parsed end as onboarded_on,

        st.clean_value                                                  as status,

        -- data-quality reasons (NULL entries are dropped by array_construct_compact)
        nullif(array_to_string(array_construct_compact(
            iff(p.vendor_id is null,                                      'vendor_id_missing',          null),
            iff(p.vendor_type_raw is not null and vt.clean_value is null, 'vendor_type_unmapped',       null),
            iff(p.integration_type_raw is not null and it.clean_value is null, 'integration_type_unmapped', null),
            iff(p.status_raw is not null and st.clean_value is null,      'status_unmapped',            null),
            iff(contains(p.commission_raw, '%')
                or (p.commission_num > 0 and p.commission_num < 1)
                or p.commission_num > 100,                                'commission_repaired',        null),
            iff(p.commission_num is null,                                 'commission_missing',         null),
            iff(p.rating_num between 10 and 50,                           'rating_rescaled',            null),
            iff(p.rating_num is not null
                and not (p.rating_num between 1 and 5 or p.rating_num between 10 and 50),
                                                                          'rating_invalid_nulled',      null),
            iff(p.onboarded_raw is not null and p.onboarded_parsed is null, 'onboarded_on_unparseable', null),
            iff(p.onboarded_parsed > {{ cutoff_ts() }}::date,             'onboarded_in_future_nulled', null),
            iff(p.area_raw is null and an.zone is not null,               'area_from_vendor_name',      null),
            iff(p.area_raw is null and an.zone is null,                   'area_missing',               null),
            iff(p.prep_time_num is not null and not p.prep_time_num between 1 and 240,
                                                                          'prep_time_invalid_nulled',   null)
        ), ', '), '')                                                   as dq_issues

    from parsed as p
    left join {{ ref('map_vendor_type') }}      as vt on p.vendor_type_raw      = vt.raw_value
    left join {{ ref('map_integration_type') }} as it on p.integration_type_raw = it.raw_value
    left join {{ ref('map_vendor_status') }}    as st on p.status_raw           = st.raw_value
    left join {{ ref('map_cuisine') }}          as cu on lower(p.cuisine_raw)   = cu.raw_value
    left join {{ ref('map_city') }}             as ci on p.city_raw             = ci.raw_value
    left join {{ ref('map_delivery_zone') }}    as zn on lower(p.area_raw)      = zn.raw_value
    left join area_from_name                    as an
        on  p.vendor_id   = an.vendor_id
        and p.vendor_name = an.vendor_name

),

deduplicated as (

    -- one row per vendor_id: a duplicate id with conflicting status keeps the 'active' row
    -- (a row without vendor_id is kept on its own, keyed by its name)
    select *
    from repaired
    qualify row_number() over (
        partition by coalesce(vendor_id, vendor_name)
        order by iff(status = 'active', 0, 1), onboarded_on, vendor_name
    ) = 1

)

select
    *,
    dq_issues is not null as dq_flag
from deduplicated
