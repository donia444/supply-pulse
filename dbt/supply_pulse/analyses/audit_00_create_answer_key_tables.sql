-- Run ONCE in Snowsight (personal account, SUPPLY_CHAIN_DB) before the audit queries.
-- It creates empty tables; then load each CSV into its table with Snowsight:
--   Databases -> SUPPLY_CHAIN_DB -> ANSWER_KEY -> <table> -> "Load Data"
--   file format: CSV, skip header = 1, field optionally enclosed by = double quote
-- Every column is VARCHAR on purpose: the audit queries convert the values themselves.

create schema if not exists SUPPLY_CHAIN_DB.ANSWER_KEY;

create or replace table SUPPLY_CHAIN_DB.ANSWER_KEY.DQ_MANIFEST (          -- dq_manifest.csv
    issue_id        varchar,
    source_file     varchar,
    record_key      varchar,
    column_name     varchar,
    issue_category  varchar,
    issue_detail    varchar,
    clean_value     varchar,
    dirty_value     varchar
);

create or replace table SUPPLY_CHAIN_DB.ANSWER_KEY.SCENARIO_EVENTS (      -- scenario_events.csv
    lsn                   varchar,
    vendor_id             varchar,
    item_ref              varchar,
    op                    varchar,
    scenario              varchar,
    expected_gate_result  varchar,
    reject_reason         varchar,
    event_time_utc        varchar,
    delivered_late        varchar
);

create or replace table SUPPLY_CHAIN_DB.ANSWER_KEY.EXPECTED_SCD2 (        -- expected_scd2_vendor_items.csv
    vendor_id       varchar,
    item_ref        varchar,
    item_type       varchar,
    version         varchar,
    price_egp       varchar,
    is_available    varchar,
    valid_from_utc  varchar,
    valid_to_utc    varchar,
    is_current      varchar
);

-- After loading, these counts should be: 171844 / 295831 / 94014
select 'DQ_MANIFEST' as t, count(*) from SUPPLY_CHAIN_DB.ANSWER_KEY.DQ_MANIFEST
union all select 'SCENARIO_EVENTS', count(*) from SUPPLY_CHAIN_DB.ANSWER_KEY.SCENARIO_EVENTS
union all select 'EXPECTED_SCD2',   count(*) from SUPPLY_CHAIN_DB.ANSWER_KEY.EXPECTED_SCD2;
