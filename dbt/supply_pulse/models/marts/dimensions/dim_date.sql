{{ config(materialized='table') }}

-- One row per calendar day. end_date is exclusive in date_spine,
-- so 2027-01-01 produces rows up to and including 2026-12-31.
with date_spine as (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2024-01-01' as date)",
        end_date   = "cast('2027-01-01' as date)"
    ) }}

),
-- One row per calendar day, 2024-01-01 .. 2026-12-31.
-- end_date is exclusive in date_spine, so 2027-01-01 yields rows up to 2026-12-31.


shaped as (

    select
        -- Smart integer key YYYYMMDD, as defined in the ERD
        cast(to_char(date_day, 'YYYYMMDD') as integer)  as date_key,
        cast(date_day as date)                          as full_date,
        year(date_day)                                  as year,
        quarter(date_day)                               as quarter,
        month(date_day)                                 as month,
        to_char(date_day, 'MMMM')                       as month_name,
        to_char(date_day, 'YYYY-MM')                    as year_month,
        day(date_day)                                   as day,

        -- ISO day of week: 1 = Monday ... 7 = Sunday
        dayofweekiso(date_day)                          as day_of_week,
        decode(dayofweekiso(date_day),
               1, 'Monday',   2, 'Tuesday',  3, 'Wednesday',
               4, 'Thursday', 5, 'Friday',   6, 'Saturday',
               7, 'Sunday')                             as day_name,

        -- Egyptian weekend = Friday (5) and Saturday (6). To confirm with the team.
        dayofweekiso(date_day) in (5, 6)                as is_weekend
    from date_spine

),

-- Unknown member so facts with a missing/invalid date can still join (ERD rule)
unknown_member as (

    select
        -1                          as date_key,
        cast(null as date)          as full_date,
        cast(null as integer)       as year,
        cast(null as integer)       as quarter,
        cast(null as integer)       as month,
        'Unknown'                   as month_name,
        'Unknown'                   as year_month,
        cast(null as integer)       as day,
        cast(null as integer)       as day_of_week,
        'Unknown'                   as day_name,
        cast(null as boolean)       as is_weekend

)

select * from shaped
union all
select * from unknown_member

