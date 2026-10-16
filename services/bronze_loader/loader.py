"""
Bronze loader: Kafka -> Snowflake (BRONZE.RAW_EVENTS).

Every Kafka message becomes one row, kept exactly as it arrived (append-only):
    topic, kafka_partition, kafka_offset, kafka_key, payload (VARIANT), kafka_timestamp_ms, ingested_at

Why files + COPY INTO instead of INSERT: the platform produces more than a million events.
Messages are collected into a batch, written to a gzip JSON-lines file, uploaded to an internal
stage (PUT) and loaded with one COPY INTO. Offsets are committed only after COPY succeeded
(at-least-once). If the loader stops mid-batch the batch is read again, so dbt must de-duplicate
on (topic, kafka_partition, kafka_offset).
"""
import gzip
import json
import os
import signal
import sys
import tempfile
import time
import uuid
from collections import Counter

import snowflake.connector
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP") or "kafka:9092"
# comma separated regexes (each one must start with ^)
PATTERNS = [p.strip() for p in (os.getenv("TOPIC_PATTERNS") or r"^ops\..*,^vendor_items\..*").split(",") if p.strip()]
BATCH_SIZE = int(os.getenv("BATCH_SIZE") or "10000")
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS") or "10")

DDL = [
    """
    CREATE TABLE IF NOT EXISTS RAW_EVENTS (
        topic              STRING,
        kafka_partition    INT,
        kafka_offset       NUMBER,
        kafka_key          STRING,
        payload            VARIANT,
        kafka_timestamp_ms NUMBER,
        ingested_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
    "CREATE STAGE IF NOT EXISTS RAW_STAGE",
]

COPY_SQL = """
COPY INTO RAW_EVENTS (topic, kafka_partition, kafka_offset, kafka_key, payload, kafka_timestamp_ms)
FROM (
    SELECT $1:topic::STRING, $1:partition::INT, $1:offset::NUMBER, $1:key::STRING, $1:payload, $1:ts::NUMBER
    FROM @RAW_STAGE/{name}
)
FILE_FORMAT = (TYPE = JSON)
PURGE = TRUE
"""

running = True


def stop(*_):
    global running
    running = False




def connect():
    missing = [
        key
        for key in (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
        )
        if not os.getenv(key)
    ]

    if missing:
        sys.exit("Missing in .env: " + ", ".join(missing))

    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE") or "SUPPLY_PULSE_WH",
        "database": os.getenv("SNOWFLAKE_DATABASE") or "SUPPLY_PULSE_DB",
        "schema": os.getenv("SNOWFLAKE_SCHEMA") or "BRONZE",
    }

    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        params["role"] = role

    return snowflake.connector.connect(**params)

def to_record(msg):
    raw = msg.value()
    text = raw.decode("utf-8", "replace") if raw is not None else None
    try:
        payload = json.loads(text) if text is not None else None
    except json.JSONDecodeError:
        payload = {"_unparsable": text}
    key = msg.key().decode("utf-8", "replace") if msg.key() is not None else None
    return {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
            "key": key, "ts": msg.timestamp()[1], "payload": payload}


def write_batch(records):
    """Write the batch as gzip JSON lines in the temp folder and return the file path."""
    name = f"events_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}.json.gz"
    path = os.path.join(tempfile.gettempdir(), name)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False))
            f.write("\n")
    return path


def flush(conn, consumer, records):
    if not records:
        return
    started = time.time()
    path = write_batch(records)
    name = os.path.basename(path)
    cur = conn.cursor()
    try:
        cur.execute(f"PUT 'file://{path}' @RAW_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        cur.execute(COPY_SQL.format(name=name))
    finally:
        cur.close()
        os.remove(path)
    consumer.commit(asynchronous=False)          # only after Snowflake accepted the file
    per_topic = Counter(r["topic"] for r in records)
    print(f"Loaded {len(records)} events in {time.time() - started:.1f}s  {dict(per_topic)}", flush=True)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    print("Connecting to Snowflake ...", flush=True)
    try:
        conn = connect()
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        cur.close()
    except Exception as e:
        print("Snowflake connection failed:", e, flush=True)
        sys.exit(1)
    print("Connected. BRONZE.RAW_EVENTS and RAW_STAGE are ready.", flush=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "bronze-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 10000,   # pick up topics created later
        "max.poll.interval.ms": 900000,
    })
    consumer.subscribe(PATTERNS)
    print(f"Listening on topics matching {PATTERNS}", flush=True)

    records, last_flush = [], time.time()
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print("Kafka error:", msg.error(), flush=True)
                else:
                    records.append(to_record(msg))
            due = time.time() - last_flush >= FLUSH_SECONDS
            if len(records) >= BATCH_SIZE or (records and due):
                flush(conn, consumer, records)
                records, last_flush = [], time.time()
            elif due:
                last_flush = time.time()
    finally:
        if records:
            try:
                flush(conn, consumer, records)
            except Exception as e:
                print("Final flush failed (events will be read again):", e, flush=True)
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
