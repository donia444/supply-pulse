"""
Postgres sink for the QA gate output: vendor_items.valid -> serving.vendor_item_latest (+ price history).

Only accepted events are in vendor_items.valid; rejected ones stay in vendor_items.dlq and quarantined ones in
vendor_items.quarantine, so nothing invalid can be stored here. The latest-state upsert is also guarded in SQL:
a version only replaces the stored one if its (event_time_utc, src_lsn) is strictly newer, so a redelivered or
older event can never overwrite a newer valid state, even if the gate state was lost.

At-least-once: Kafka offsets are committed only after the Postgres transaction committed. Every write is
idempotent (upsert guarded by time, history ON CONFLICT DO NOTHING), so a replay changes nothing.
"""
import json
import os
import signal
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg
from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
VALID_TOPIC = os.environ.get("VALID_TOPIC", "vendor_items.valid")
GROUP_ID = os.environ.get("SINK_GROUP_ID", "pg-sink")
BATCH_SIZE = int(os.environ.get("SINK_BATCH_SIZE", "500"))
DSN = (f"host={os.environ.get('PGHOST', 'postgres')} port={os.environ.get('PGPORT', '5432')} "
       f"dbname={os.environ['PGDATABASE']} user={os.environ['PGUSER']} password={os.environ['PGPASSWORD']}")

UPSERT_LATEST = """
INSERT INTO serving.vendor_item_latest AS t
    (vendor_id, item_ref, item_type, item_name, price_egp, currency, is_available, qty_on_hand,
     updated_by, event_time_utc, src_lsn, is_deleted, deleted_at, loaded_at)
VALUES (%(vendor_id)s, %(item_ref)s, %(item_type)s, %(item_name)s, %(price_egp)s, %(currency)s, %(is_available)s,
        %(qty_on_hand)s, %(updated_by)s, %(event_time_utc)s, %(src_lsn)s, false, NULL, now())
ON CONFLICT (vendor_id, item_ref) DO UPDATE SET
    item_type = EXCLUDED.item_type, item_name = EXCLUDED.item_name, price_egp = EXCLUDED.price_egp,
    currency = EXCLUDED.currency, is_available = EXCLUDED.is_available, qty_on_hand = EXCLUDED.qty_on_hand,
    updated_by = EXCLUDED.updated_by, event_time_utc = EXCLUDED.event_time_utc, src_lsn = EXCLUDED.src_lsn,
    is_deleted = false, deleted_at = NULL, loaded_at = now()
WHERE (t.event_time_utc, t.src_lsn) < (EXCLUDED.event_time_utc, EXCLUDED.src_lsn)
"""

# History = price / availability changes only (like the SCD2 in the answer key): an event that repeats the
# price and availability of the version just before it (stock-only changes) adds no row. Explicit casts are
# needed because the parameters sit in a SELECT list.
INSERT_HISTORY = """
INSERT INTO serving.vendor_item_price_history
    (vendor_id, item_ref, event_time_utc, src_lsn, price_egp, is_available, qty_on_hand, late_arrival)
SELECT %(vendor_id)s::text, %(item_ref)s::text, %(event_time_utc)s::timestamptz, %(src_lsn)s::bigint,
       %(price_egp)s::numeric, %(is_available)s::boolean, %(qty_on_hand)s::integer, %(late_arrival)s::boolean
WHERE NOT EXISTS (
    SELECT 1 FROM (
        SELECT price_egp, is_available FROM serving.vendor_item_price_history h
        WHERE h.vendor_id = %(vendor_id)s::text AND h.item_ref = %(item_ref)s::text
          AND (h.event_time_utc, h.src_lsn) < (%(event_time_utc)s::timestamptz, %(src_lsn)s::bigint)
        ORDER BY h.event_time_utc DESC, h.src_lsn DESC LIMIT 1) prev
    WHERE prev.price_egp = %(price_egp)s::numeric AND prev.is_available = %(is_available)s::boolean)
ON CONFLICT DO NOTHING
"""

# a delete closes the item (history is kept); a later newer insert / update re-lists it
MARK_DELETED = """
UPDATE serving.vendor_item_latest SET is_deleted = true, deleted_at = now(), loaded_at = now()
WHERE vendor_id = %(vendor_id)s AND item_ref = %(item_ref)s AND NOT is_deleted
"""

running = True


def stop(*_):
    global running
    running = False


def connect():
    """Wait for Postgres, create the serving schema (idempotent) and return an open connection."""
    ddl = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
    while running:
        try:
            conn = psycopg.connect(DSN)
            with conn.transaction():
                conn.execute(ddl)
            return conn
        except psycopg.OperationalError as e:
            print("waiting for postgres:", str(e).strip().splitlines()[-1], flush=True)
            time.sleep(3)
    raise SystemExit(0)


def to_params(rec: dict) -> dict:
    """Message of vendor_items.valid -> SQL parameters. Raises ValueError/KeyError on a malformed message."""
    try:
        price = Decimal(str(rec["price_egp"]))
    except InvalidOperation as e:
        raise ValueError("bad price_egp") from e
    if price <= 0:
        raise ValueError("non-positive price")
    if not isinstance(rec["is_available"], bool) or not rec.get("event_time_utc"):
        raise ValueError("bad availability / event time")
    lsn = rec.get("src_lsn")
    return {"vendor_id": rec["vendor_id"], "item_ref": rec["item_ref"], "item_type": rec["item_type"],
            "item_name": rec["item_name"], "price_egp": price, "currency": rec.get("currency") or "EGP",
            "is_available": rec["is_available"], "qty_on_hand": rec.get("qty_on_hand"),
            "updated_by": rec.get("updated_by"), "event_time_utc": rec["event_time_utc"],
            "src_lsn": int(lsn) if lsn is not None else -1, "late_arrival": bool(rec.get("late_arrival"))}


def apply(conn, rec: dict, stats: Counter):
    if rec.get("event_type") == "delete" or rec.get("op") == "d":
        cur = conn.execute(MARK_DELETED, {"vendor_id": rec.get("vendor_id"), "item_ref": rec.get("item_ref")})
        stats["deleted" if cur.rowcount else "delete_ignored"] += 1
        return
    p = to_params(rec)
    if not rec.get("no_change"):                       # identical values again: nothing to record
        cur = conn.execute(INSERT_HISTORY, p)
        stats["history"] += cur.rowcount
    if rec.get("late_arrival"):
        stats["late_history_only"] += 1                # accepted late: history only, never the current state
        return
    cur = conn.execute(UPSERT_LATEST, p)
    stats["latest_written" if cur.rowcount else "older_ignored"] += 1


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    conn = connect()
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP_ID,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([VALID_TOPIC])
    stats, last_log, last_shown = Counter(), time.time(), None
    print(f"pg-sink started: {VALID_TOPIC} -> serving.vendor_item_latest / serving.vendor_item_price_history", flush=True)

    try:
        while running:
            msgs = [m for m in consumer.consume(BATCH_SIZE, 1.0) if m.error() is None]
            if msgs:
                try:
                    with conn.transaction():
                        for m in msgs:
                            if m.value() is None:
                                continue
                            try:
                                rec = json.loads(m.value())
                                with conn.transaction():          # savepoint: one bad message never blocks the batch
                                    apply(conn, rec, stats)
                            except (json.JSONDecodeError, KeyError, TypeError, ValueError, psycopg.DataError) as e:
                                stats["skipped_malformed"] += 1
                                print(f"skipped malformed valid event (offset {m.offset()}): {e}", flush=True)
                    consumer.commit(asynchronous=False)           # only after the Postgres commit
                except psycopg.OperationalError as e:
                    # offsets were not committed: exit and let Docker restart the service, which re-reads the
                    # batch from the last committed offset (writes are idempotent)
                    print("postgres connection lost, restarting:", e, flush=True)
                    raise SystemExit(1)
            if time.time() - last_log > 10:
                if dict(stats) != last_shown:
                    print(f"sink: {dict(stats)}", flush=True)
                    last_shown = dict(stats)
                last_log = time.time()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
