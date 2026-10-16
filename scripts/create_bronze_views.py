"""
Creates the flat BRONZE views that dbt reads, on top of BRONZE.RAW_EVENTS.

Why: bronze_loader stores every Kafka message in ONE table (RAW_EVENTS, payload = VARIANT).
dbt expects one view per source table, with all columns as text plus the CDC columns:

    topic ops.customer_app.orders  ->  view BRONZE.CUSTOMER_APP_ORDERS
    topic ops.public.vendor_items  ->  view BRONZE.PUBLIC_VENDOR_ITEMS
    columns: <every source column as STRING>, CDC_OP, KAFKA_PARTITION, KAFKA_OFFSET, KAFKA_TS

Debezium envelope: payload = {before, after, source, op, ts_ms}. For a delete the row is in "before".
The loader is at-least-once, so each view keeps ONE copy per (topic, partition, offset).
Topics of the QA gate (vendor_items.valid / .dlq / .quarantine) are flat JSON and get a view too.

Run it AFTER events reached RAW_EVENTS (columns are discovered from the data), and again whenever a new
topic appears:   python scripts/create_bronze_views.py
"""
import os
import re
import sys

SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SAFE_TOPIC = re.compile(r"^[A-Za-z0-9_.\-]+$")
RESERVED = {"CDC_OP", "KAFKA_PARTITION", "KAFKA_OFFSET", "KAFKA_TS"}


def view_name(topic: str) -> str:
    """ops.customer_app.orders -> CUSTOMER_APP_ORDERS ; vendor_items.dlq -> VENDOR_ITEMS_DLQ"""
    parts = topic.split(".")
    if parts[0] == "ops" and len(parts) == 3:
        parts = parts[1:]
    return "_".join(parts).upper().replace("-", "_")


def build_view_sql(topic: str, keys, enveloped: bool) -> str:
    """Pure function (no database): the CREATE VIEW statement for one topic."""
    if not SAFE_TOPIC.match(topic):
        raise ValueError(f"unsafe topic name: {topic!r}")
    # row_obj = the row as JSON: Debezium keeps it in "after" (in "before" for a delete); gate topics are flat
    row_obj = "coalesce(payload:after, payload:before)" if enveloped else "payload"
    items = []
    for k in sorted(set(keys)):
        if not SAFE_NAME.match(k):
            print(f"  skipped column with unsafe name {k!r}", file=sys.stderr)
            continue
        if k.upper() in RESERVED:
            continue
        items.append(f'to_varchar(row_obj:"{k}") as {k.upper()}')
    items += [
        ("payload:op::string" if enveloped else "null::string") + " as CDC_OP",
        "kafka_partition::string as KAFKA_PARTITION",
        "kafka_offset::string as KAFKA_OFFSET",
        "to_timestamp_ntz(kafka_timestamp_ms, 3) as KAFKA_TS",
    ]
    select_list = ",\n    ".join(items)
    return (
        f"create or replace view {view_name(topic)} as\n"
        f"with ev as (\n"
        f"    select topic, kafka_partition, kafka_offset, kafka_timestamp_ms, ingested_at, payload,\n"
        f"           {row_obj} as row_obj\n"
        f"    from RAW_EVENTS\n"
        f"    where topic = '{topic}'\n"
        f"    qualify row_number() over (partition by topic, kafka_partition, kafka_offset order by ingested_at) = 1\n"
        f")\n"
        f"select\n    {select_list}\n"
        f"from ev"
    )


def main():
    import snowflake.connector

    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD") if not os.getenv(k)]
    if missing:
        sys.exit("Missing in the environment (.env): " + ", ".join(missing))
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"], role=os.getenv("SNOWFLAKE_ROLE") or None,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE") or "SUPPLY_PULSE_WH",
        database=os.getenv("SNOWFLAKE_DATABASE") or "SUPPLY_PULSE_DB",
        schema=os.getenv("SNOWFLAKE_SCHEMA") or "BRONZE")
    cur = conn.cursor()
    cur.execute("select distinct topic from RAW_EVENTS order by 1")
    topics = [r[0] for r in cur.fetchall()]
    if not topics:
        sys.exit("RAW_EVENTS is empty: start the bronze-loader and let it load events first.")
    for topic in topics:
        enveloped = topic.startswith("ops.")
        if not SAFE_TOPIC.match(topic):
            print(f"skip topic {topic!r}: unsafe characters"); continue
        sample_obj = "coalesce(s.payload:after, s.payload:before)" if enveloped else "s.payload"
        # discover column names from a sample of the topic (all rows of a table share the same columns)
        cur.execute(
            f"select distinct f.key from (select payload from RAW_EVENTS where topic = '{topic}' limit 2000) s, "
            f"lateral flatten(input => {sample_obj}) f")
        keys = [r[0] for r in cur.fetchall()]
        if not keys:
            print(f"skip {topic}: no columns found"); continue
        cur.execute(build_view_sql(topic, keys, enveloped))
        print(f"created view {view_name(topic)}  ({len(keys)} columns)  <- {topic}")
    conn.close()


if __name__ == "__main__":
    main()
