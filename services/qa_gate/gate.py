"""
QA gate for the vendor_items change stream. Reads Debezium change events, normalises and validates
them, and routes every event to exactly one topic:

    accepted    -> vendor_items.valid        clean, typed, EGP, UTC  (+ deletes)
    rejected    -> vendor_items.dlq          raw event + reject_reason (+ detail, and the clean item when it
                                             failed only the history checks, so it can be replayed)
    quarantined -> vendor_items.quarantine   valid row with a suspicious price jump, held for review

Two kinds of checks (see rules.py and state.py):
    * field checks     required fields, types, vendor id, SKU / menu item identity, price, currency,
                       stock, availability, timestamp
    * history checks   duplicates, stale / out-of-order events, price jumps, deletes

Only accepted events change the item state, so an invalid or older event can never overwrite a newer
valid one. At-least-once: offsets are committed only after the produced messages are flushed.
"""
import json
import os
import signal
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from config import DEFAULT_CONFIG, GateConfig
from reference import load_reference
from rules import Reject, parse_vendor_id, validate
from state import ACCEPT, QUARANTINE, REJECT, ItemStateStore

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SRC_PATTERN = os.environ.get("SRC_TOPIC_PATTERN", r"^ops\.public\.vendor_items$")
VALID_TOPIC = os.environ.get("VALID_TOPIC", "vendor_items.valid")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "vendor_items.dlq")
QUARANTINE_TOPIC = os.environ.get("QUARANTINE_TOPIC", "vendor_items.quarantine")
DATA_DIR = os.environ.get("DATA_DIR", "/data")

# Debezium op -> event type: r = initial snapshot, c = insert, u = update, d = delete
EVENT_TYPES = {"r": "snapshot", "c": "insert", "u": "update", "d": "delete"}


def _key(row):
    try:
        return f"V{parse_vendor_id(row['store_id']):04d}|{str(row['item_ref']).strip().upper()}"
    except (KeyError, TypeError, ValueError, Reject):
        return "unknown"


def process(envelope: dict, ref, store=None, cfg=None):
    """Debezium envelope -> (topic, key, value dict), or None if there is nothing to route.
    store: ItemStateStore for the history checks (None = field checks only). Accepted events are
    committed to the store here, so call it once per event, in order."""
    cfg = cfg or DEFAULT_CONFIG
    op = envelope.get("op")
    meta = {"op": op, "event_type": EVENT_TYPES.get(op), "cdc_ts_ms": envelope.get("ts_ms"),
            "pg_lsn": (envelope.get("source") or {}).get("lsn")}

    # ---- delete: valid when the key is readable and the item is not already deleted
    if op == "d":
        before = envelope.get("before") or {}
        key = _key(before)
        if key == "unknown":
            return DLQ_TOPIC, "unknown", {"reject_reason": "MISSING_KEY", "raw": before, **meta}
        decision = store.evaluate_delete(key) if store else None
        if decision is not None and decision.action == REJECT:
            return DLQ_TOPIC, key, {"reject_reason": decision.reason, "detail": decision.detail, "raw": before, **meta}
        if store:
            store.commit_delete(key)
        vendor_id, item_ref = key.split("|")
        return VALID_TOPIC, key, {"vendor_id": vendor_id, "item_ref": item_ref,
                                  **(decision.flags if decision else {}), **meta}

    after = envelope.get("after")
    if op not in EVENT_TYPES:
        if after is None:
            return None                                  # e.g. truncate / message events: nothing to route
        return DLQ_TOPIC, _key(after), {"reject_reason": "UNSUPPORTED_OP", "raw": after, **meta}
    if after is None:
        return DLQ_TOPIC, "unknown", {"reject_reason": "MISSING_AFTER_IMAGE", "raw": envelope.get("before"), **meta}

    # ---- snapshot / insert / update: field checks
    try:
        item = validate(after, ref, cfg)
    except Reject as r:
        return DLQ_TOPIC, _key(after), {"reject_reason": r.reason, "raw": after, **meta}
    except (KeyError, TypeError, ValueError) as e:       # unreadable event: never crash, send to the DLQ
        return DLQ_TOPIC, _key(after), {"reject_reason": "MALFORMED_EVENT", "detail": str(e), "raw": after, **meta}

    key = f"{item.vendor_id}|{item.item_ref}"
    clean = json.loads(item.model_dump_json())
    if store is None:
        return VALID_TOPIC, key, {**clean, **meta}

    # ---- history checks: duplicate, stale / out of order, suspicious price
    d = store.evaluate(key, item)
    if d.action == REJECT:
        return DLQ_TOPIC, key, {"reject_reason": d.reason, "detail": d.detail, "item": clean, "raw": after, **meta}
    if d.action == QUARANTINE:
        return QUARANTINE_TOPIC, key, {"quarantine_reason": d.reason, "detail": d.detail, "item": clean,
                                       "raw": after, **meta}
    store.commit(key, item, d)
    return VALID_TOPIC, key, {**clean, **d.flags, **meta}


def rebuild_state(store: ItemStateStore, topic: str) -> int:
    """Restore the item state from vendor_items.valid (everything the gate accepted so far), so a restart
    still knows the newest accepted version of every item. Returns the number of messages read."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "qa-gate-state-rebuild",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    read = 0
    try:
        meta = consumer.list_topics(topic, timeout=10).topics.get(topic)
        if meta is None or meta.error is not None or not meta.partitions:
            return 0                                     # first run: nothing accepted yet
        ends, assign = {}, []
        for p in meta.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high > low:
                ends[p] = high
                assign.append(TopicPartition(topic, p, low))
        if not assign:
            return 0
        consumer.assign(assign)
        done, idle = set(), 0
        while len(done) < len(ends) and idle < 15:       # 15 empty polls in a row: give up, never block the gate
            msg = consumer.poll(2.0)
            if msg is None:
                idle += 1
                continue
            idle = 0
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("state rebuild consumer error:", msg.error(), flush=True)
                continue
            if msg.value() is not None:
                try:
                    store.restore(json.loads(msg.value()))
                    read += 1
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
            if msg.offset() >= ends[msg.partition()] - 1:
                done.add(msg.partition())
        if len(done) < len(ends):
            print("WARNING: state rebuild stopped early; duplicate / stale checks may miss older events", flush=True)
    finally:
        consumer.close()
    return read


def main():
    cfg = GateConfig.from_env()
    ref = load_reference(DATA_DIR)
    print(f"reference loaded: {len(ref.integration)} vendors, {len(ref.list_price)} list prices, "
          f"{len(ref.products)} products, {len(ref.menu_vendor)} menu items", flush=True)
    print(f"config: {cfg}", flush=True)

    store = ItemStateStore(cfg) if cfg.stateful else None
    if store is not None and cfg.rebuild_state:
        n = rebuild_state(store, VALID_TOPIC)
        print(f"state rebuilt from {VALID_TOPIC}: {n} messages, {len(store)} items", flush=True)

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "qa-gate",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
    consumer.subscribe([SRC_PATTERN])      # regex: works even before Debezium has created the topic

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    stats, reasons, qreasons, pending, last_log, last_shown = Counter(), Counter(), Counter(), 0, time.time(), None

    def commit():
        nonlocal pending
        if pending:
            producer.flush()
            consumer.commit(asynchronous=False)
            pending = 0

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                commit()
            elif msg.error():
                print("consumer error:", msg.error(), flush=True)
            elif msg.value() is not None:
                try:
                    out = process(json.loads(msg.value()), ref, store, cfg)
                except json.JSONDecodeError:
                    out = (DLQ_TOPIC, "unknown", {"reject_reason": "NOT_JSON", "raw": msg.value().decode("utf-8", "replace")})
                if out:
                    topic, key, value = out
                    producer.produce(topic, key=key, value=json.dumps(value, ensure_ascii=False))
                    producer.poll(0)
                    stats[topic] += 1
                    if topic == DLQ_TOPIC:
                        reasons[value["reject_reason"]] += 1
                    elif topic == QUARANTINE_TOPIC:
                        qreasons[value["quarantine_reason"]] += 1
                pending += 1
                if pending >= 500:
                    commit()
            if time.time() - last_log > 10:
                shown = (dict(stats), dict(reasons), dict(qreasons))
                if shown != last_shown:                      # only print when something changed
                    print(f"routed: {dict(stats)}  dlq reasons: {dict(reasons)}  quarantine: {dict(qreasons)}", flush=True)
                    last_shown = shown
                last_log = time.time()
    finally:
        commit()
        consumer.close()


if __name__ == "__main__":
    main()
