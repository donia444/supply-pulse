-- Serving schema for ACCEPTED vendor_items events only (rejected / quarantined events never reach it).
-- Own schema on purpose: the Debezium publication covers public, partners_system, catalog_pim,
-- restaurant_portal, customer_app and wms, so these tables are NOT streamed back into Kafka.
CREATE SCHEMA IF NOT EXISTS serving;

-- Latest accepted state per vendor / item (SKU or menu item).
CREATE TABLE IF NOT EXISTS serving.vendor_item_latest (
    vendor_id      text        NOT NULL,            -- V0012
    item_ref       text        NOT NULL,            -- SKU-00045 | M0000123
    item_type      text        NOT NULL,            -- grocery | menu
    item_name      text        NOT NULL,
    price_egp      numeric(12,2) NOT NULL,
    currency       text        NOT NULL DEFAULT 'EGP',
    is_available   boolean     NOT NULL,
    qty_on_hand    integer,                         -- NULL for menu items
    updated_by     text,
    event_time_utc timestamptz NOT NULL,            -- business time of the accepted version
    src_lsn        bigint      NOT NULL DEFAULT -1, -- tie-break inside the same instant
    is_deleted     boolean     NOT NULL DEFAULT false,
    deleted_at     timestamptz,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor_id, item_ref)
);

-- Accepted price / availability changes per item (late arrivals included). Idempotent on redelivery.
CREATE TABLE IF NOT EXISTS serving.vendor_item_price_history (
    vendor_id      text        NOT NULL,
    item_ref       text        NOT NULL,
    event_time_utc timestamptz NOT NULL,
    src_lsn        bigint      NOT NULL DEFAULT -1,
    price_egp      numeric(12,2) NOT NULL,
    is_available   boolean     NOT NULL,
    qty_on_hand    integer,
    late_arrival   boolean     NOT NULL DEFAULT false,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor_id, item_ref, event_time_utc, src_lsn)
);

CREATE INDEX IF NOT EXISTS ix_latest_item_ref ON serving.vendor_item_latest (item_ref) WHERE NOT is_deleted;
