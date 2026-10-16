-- Runs once, the first time the postgres container starts (empty data volume).
-- Postgres is the ONE source system: every table lives here, and Debezium streams
-- all of them (initial snapshot + changes) to Kafka. Raw files never go to the warehouse.

-- One schema per source system (same names as the folders in ./data)
CREATE SCHEMA IF NOT EXISTS partners_system;   -- vendors
CREATE SCHEMA IF NOT EXISTS catalog_pim;       -- products
CREATE SCHEMA IF NOT EXISTS restaurant_portal; -- menu_items
CREATE SCHEMA IF NOT EXISTS customer_app;      -- customers, orders, order_items
CREATE SCHEMA IF NOT EXISTS wms;               -- inventory_movements
-- (the live table vendor_items stays in schema public)

-- The live "price & availability" table: this one receives the change stream.
-- Dirty-prone columns are TEXT on purpose: the source accepts whatever managers type
-- ("104.5 EGP", "15,0", "Y", "OOS", ...). Cleaning and rejecting happens later,
-- in the QA gate (and dbt), not in the source database.
-- The other tables are created by `replayer seed` from the CSV files, also with raw TEXT columns.
CREATE TABLE IF NOT EXISTS public.vendor_items (
    store_id      integer NOT NULL,          -- vendor number (12 = V0012)
    item_ref      text    NOT NULL,          -- SKU-00045 or M0000123
    src_lsn       bigint,                    -- lsn from the generated file (to match the answer key)
    item_name     text,                      -- NULL/empty = manager typo -> QA gate rejects
    item_price    text,                      -- raw, may contain currency text or comma decimals
    currency      text,                      -- EGP / PT / LE / ... (variants)
    qty_on_hand   text,                      -- raw ("91" or "91.0"), NULL for menu items
    is_available  text,                      -- raw flag (true / Y / OOS / 0 ...)
    updated_by    text,
    updated_at    text,                      -- raw business time (local Cairo, many formats)
    PRIMARY KEY (store_id, item_ref)
);

-- Debezium sends the OLD row on UPDATE/DELETE only with REPLICA IDENTITY FULL.
ALTER TABLE public.vendor_items REPLICA IDENTITY FULL;

-- Dedicated CDC user (credentials match infra/debezium/connector.json).
-- Local development only: change these for anything real.
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'cdc_pass';
GRANT USAGE ON SCHEMA public, partners_system, catalog_pim, restaurant_portal, customer_app, wms TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
-- tables created later by `replayer seed` (run as the postgres user) become readable too
ALTER DEFAULT PRIVILEGES IN SCHEMA partners_system, catalog_pim, restaurant_portal, customer_app, wms
    GRANT SELECT ON TABLES TO cdc_user;

-- Logical replication publication that Debezium reads through (pgoutput).
-- "TABLES IN SCHEMA" also covers tables created after this line.
CREATE PUBLICATION qcommerce_pub FOR TABLES IN SCHEMA
    public, partners_system, catalog_pim, restaurant_portal, customer_app, wms;
