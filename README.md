# Supply Pulse

**A real-time + batch data platform that keeps prices and stock of q-commerce vendors fresh and trustworthy.**

Every change in an operational Postgres database reaches a live dashboard in seconds, and the same events feed a Snowflake warehouse (bronze, silver, gold) that analysts can trust. Bad data is stopped at the door, and a failed nightly build is rolled back automatically.

> Team project (5 engineers, one week): migration of a legacy supply-chain setup to a modern streaming + warehouse stack.
> Original team repository: [AhlammAhmedd/Supply-Pulse-](https://github.com/AhlammAhmedd/Supply-Pulse-)

## The problem

In a delivery app, vendors change prices and availability all day. A stale price or an item that is already out of stock disappoints the customer and costs the vendor money. The raw vendor data is also dirty: prices stored as text (`'157.5 EGP'`, `'44,75'`), five spellings of the pound, timestamps in many formats, duplicates, out-of-order events, and typos such as a price of `9999`.

## Architecture

<!-- TODO: add the diagram: ![Supply Pulse architecture](docs/architecture.png) -->

One change stream splits into a **real-time path** for the dashboard and a **batch path** for Snowflake. Only events that pass the QA gate reach the dashboard.

### Real-time path
1. A vendor (or the replayer script) changes a row in `public.vendor_items`.
2. Debezium reads the Postgres write-ahead log and publishes the change to the Kafka topic `ops.public.vendor_items`.
3. The **QA gate** routes each event to `vendor_items.valid`, `vendor_items.dlq` (rejected, with a reason) or `vendor_items.quarantine` (suspicious price, held back).
4. `pg-sink` writes valid events to `serving.vendor_item_latest` and `serving.vendor_item_price_history`; the Streamlit dashboard reads those tables.

### Batch / warehouse path (medallion)
1. `bronze-loader` copies every topic message into Snowflake `BRONZE.RAW_EVENTS` in micro-batches (10,000 messages or 10 seconds, one `COPY INTO` per batch). It runs all day.
2. A **rule engine inside Snowflake** (`QA.RUN_QA('BRONZE')`) checks the raw layer before anything is built. An ERROR rule above its failure threshold blocks the run; WARN rules never block.
3. **dbt** builds `SILVER` (staging: rename, cast, clean; intermediate: business rules and a PASS/REJECT gate) and `GOLD` (6 dimensions and 4 facts).
4. `dim_vendor_item_history` is an **SCD Type 2** table: every change of price, availability or deletion creates a new version with `valid_from`, `valid_to` and `is_current`.

### Orchestration and recovery (Airflow)
- `supply_pulse_batch` (nightly 03:00 Cairo): bronze check, QA gate, **backup of GOLD** (zero-copy clone), dbt deps, seed, then run + test for every layer.
- If any dbt step fails, `restore_gold` swaps the backup back (`ALTER SCHEMA GOLD SWAP WITH GOLD_BACKUP`), so GOLD is always the last good version. The run stays red on purpose and can notify Slack.
- `streaming_health` (every 15 minutes) checks the connector, consumer groups, freshness and stage flow. `cdc_bootstrap` registers the Debezium connector.

## Tech stack

| Layer | Tools |
| --- | --- |
| Source and serving | PostgreSQL 16 |
| CDC and streaming | Debezium 2.7, Apache Kafka 3.8 (KRaft) |
| Validation | Python QA gate (DLQ + quarantine) |
| Warehouse | Snowflake |
| Transformation and tests | dbt Core 1.12 |
| Orchestration | Apache Airflow 2.10.2 |
| Dashboard | Streamlit |
| Runtime | Docker Compose (profiles) |

## Repository map

| Path | What it is |
| --- | --- |
| `docker-compose.yml` | All services; optional parts switch on with profiles (`tools`, `gate`, `dashboard`, `snowflake`, `airflow`) |
| `infra/` | Postgres init, Debezium connector, Snowflake setup |
| `services/qa_gate/` | Streaming QA gate (field rules, history rules, Kafka loop) |
| `services/pg_sink/` | Valid events to Postgres serving tables |
| `services/bronze_loader/` | Kafka to Snowflake BRONZE |
| `services/replayer/` | Replays change events into Postgres (modes: seed, snapshot, stream, verify) |
| `dashboard/` | Streamlit app |
| `dbt/supply_pulse/` | dbt project: staging, intermediate, marts, seeds, macros, tests, analyses |
| `airflow/` | Airflow image and DAGs |
| `scripts/` | Helper scripts (for example the BRONZE flat views) |
| `tests/` | Python tests: QA gate, integration, reconciliation |

## Quick start

**Prerequisites:** Docker Desktop and a Snowflake account (for the batch path).

```powershell
# 1. Configure secrets (never commit .env)
Copy-Item .env.example .env
# edit .env and fill in the values

# 2. Core platform: postgres, kafka, connect, kafka-ui
docker compose up -d

# 3. QA gate + pg-sink, dashboard, Snowflake loader, Airflow
docker compose --profile gate up -d --build
docker compose --profile dashboard up -d
docker compose --profile snowflake up -d
docker compose --profile airflow up -d

# 4. Everything should be running or healthy
docker compose ps

# 5. Register the Debezium connector (the cdc_bootstrap DAG can also do it)
curl.exe -X POST -H "Content-Type: application/json" --data "@infra/debezium/connector.json" http://localhost:8083/connectors

# 6. Stream data
docker compose --profile tools run --rm replayer snapshot
docker compose --profile tools run --rm replayer stream --limit 100 --rate 20

# 7. Create the BRONZE flat views in Snowflake
python scripts/create_bronze_views.py
```

| Service | URL |
| --- | --- |
| Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8088 |
| Airflow | http://localhost:8080 |
| Kafka Connect REST | http://localhost:8083 |

Airflow admin user is `admin`; the password is printed by `docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`.

The replayer starts from the first change event of its file, so events that were already applied come back in the DLQ as `DUPLICATE_EVENT` or `STALE_EVENT`. That is expected.

<!-- TODO: say where the replayer input data comes from (data/ is not committed) -->

### Run dbt manually

```powershell
cd dbt/supply_pulse
dbt deps
dbt seed
dbt build --indirect-selection buildable
dbt docs generate
dbt docs serve --port 8081
```

## Live demo: change a row and watch it travel

Every manual change must also raise `src_lsn`, otherwise the QA gate rejects it as `DUPLICATE_EVENT`.

```powershell
docker compose exec postgres psql -U qcommerce_user -d qcommerce -c "update public.vendor_items set item_price = '45.00', src_lsn = (select max(src_lsn) + 1 from public.vendor_items), updated_at = to_char(now() at time zone 'Africa/Cairo', 'YYYY-MM-DD HH24:MI:SS'), updated_by = 'demo' where store_id = 13 and item_ref = 'SKU-00091' returning store_id, item_ref, item_price, src_lsn;"
```

In Kafka UI open `ops.public.vendor_items` (Newest First) to see the `"op":"u"` event with `before` and `after`, then `vendor_items.valid`, then refresh the dashboard.

| Bad change | Expected result |
| --- | --- |
| `qty_on_hand = '-5'` | DLQ, `NEGATIVE_STOCK` |
| `item_price = '9999'` (more than 3x the list price) | DLQ, `PRICE_OUTLIER` |
| Price between 1.6x and 3x the last accepted price | Quarantine, `SUSPICIOUS_PRICE_CHANGE` |
| Update without a larger `src_lsn` | DLQ, `DUPLICATE_EVENT` |

## Data quality at three levels

1. **Streaming QA gate**: field and history rules, DLQ and quarantine, reasons kept.
2. **Snowflake rule engine** (`QA.QA_RULES`, `QA.RUN_QA`): checks BRONZE before any model builds; ERROR rules block, WARN rules only log.
3. **dbt tests**: generic (unique, not_null, relationships, accepted_values) and singular tests after every layer. A failing ERROR test stops the chain.

## Notes

- Credentials such as `cdc_pass` in `infra/` are **demo credentials for the local Docker setup only**. Snowflake and Postgres secrets come from `.env`, which is git-ignored.
- Airflow runs in standalone mode (SQLite, SequentialExecutor) to keep the demo light. Its admin password and connections live inside the container, so `docker compose down` removes them.
- GOLD is rebuilt table by table (no blue/green swap); the backup and restore tasks are the safety net.

## Team

<!-- TODO: replace with the five real names and roles -->
| Name | Role |
| --- | --- |
| Donia Mostafa | Batch pipeline: Snowflake BRONZE, dbt (SILVER/GOLD), Airflow orchestration and recovery |
| [Name] | [Role] |
| [Name] | [Role] |
| [Name] | [Role] |
| [Name] | [Role] |

Thank you to my teammates for a great week of building and learning together.