# DAG 2 - STREAMING (supervision). The streaming services (Debezium, Kafka, QA gate, pg-sink,
# bronze-loader) run all the time in Docker; Airflow watches them every 15 minutes.
#
#   check_connector -> restart_if_failed (only if the check failed) -> check_consumer_groups  (is each service alive?)
#                                                                   -> check_freshness       (is data arriving?)
#                                                                   -> check_stage_flow      (did it pass the QA gate?)
#
# DESIGN RULE: a quiet source is NOT a failure. Every check compares the pipeline with ITSELF
# (Kafka lag, stage against stage), never with the clock alone, so it stays green when nobody writes
# and turns red only when something is really broken:
#   - a service is alive when its Kafka consumer group has members (Kafka UI API), not when data moved
#   - "no new data" is fine when the loader's lag is 0 (nothing is waiting); it fails only when
#     messages are waiting and nothing is written
#   - the QA gate is judged against the events it RECEIVED; the DLQ share needs a minimum sample size
#
# Every run prints a detailed report in the task logs and appends one row per check to
# BRONZE.STREAMING_HEALTH_LOG. A failed task sends a Slack message when SLACK_WEBHOOK_URL is set.
import os
import time

import pendulum
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
KAFKA_UI_URL = os.environ.get("KAFKA_UI_URL", "http://kafka-ui:8080")   # Kafka UI REST API (cluster name "local")
CONNECTOR_NAME = "qcommerce-cdc"
VENV = "/home/airflow/dbt_venv/bin"   # has snowflake-connector-python and requests (installed with dbt-snowflake)
# consumer group -> label. The group names are the ones set in the services' code.
GROUPS = {"qa-gate": "QA gate", "pg-sink": "pg-sink (Postgres serving)", "bronze-loader": "bronze-loader (Snowflake)"}
# data counts as stale only when messages are WAITING (lag > 0) and nothing was written for this long
MAX_AGE_MIN = int(os.environ.get("BRONZE_MAX_AGE_MIN", "30"))
# the QA gate is stuck when the newest ops event is this many minutes newer than the newest routed event
GATE_LAG_MIN = int(os.environ.get("GATE_LAG_MIN", "10"))
# the DLQ share is judged only with at least this many events in the last hour (2 events cannot prove anything)
DLQ_MIN_SAMPLE = int(os.environ.get("DLQ_MIN_SAMPLE", "20"))
DLQ_MAX_RATIO = float(os.environ.get("DLQ_MAX_RATIO", "0.20"))


def notify_slack(context):
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return
    ti = context["task_instance"]
    text = f"Airflow: task {ti.task_id} failed in DAG {ti.dag_id} (run {context['run_id']})"
    try:
        requests.post(url, json={"text": text}, timeout=10)
    except requests.RequestException:
        pass  # a failed alert must never hide the original failure


def _status():
    resp = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=15)
    resp.raise_for_status()
    return resp.json()


def check_connector():
    status = _status()
    connector = status["connector"]
    print(f"connector {CONNECTOR_NAME}: state={connector['state']} worker={connector.get('worker_id')}")
    states = [connector["state"]]
    for task in status.get("tasks", []):
        print(f"  task {task['id']}: state={task['state']} worker={task.get('worker_id')}")
        if task.get("trace"):
            print("  error trace (first 500 chars):", task["trace"][:500])
        states.append(task["state"])
    # which Kafka topics Debezium wrote to (proof that data really flows out of Postgres)
    topics = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/topics", timeout=15).json()
    names = topics.get(CONNECTOR_NAME, {}).get("topics", [])
    print(f"  active topics ({len(names)}):", ", ".join(sorted(names)))
    if len(states) < 2 or any(s != "RUNNING" for s in states):
        raise RuntimeError(f"Connector not healthy: {states}")


def restart_connector():
    # PAUSED needs a resume (a restart does not un-pause it); FAILED needs a restart
    state = _status()["connector"]["state"]
    if state == "PAUSED":
        resp = requests.put(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/resume", timeout=30)
        print("connector was PAUSED: resume requested, HTTP", resp.status_code)
    else:
        resp = requests.post(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/restart",
                             params={"includeTasks": "true", "onlyFailed": "true"}, timeout=30)
        print(f"connector was {state}: restart requested, HTTP", resp.status_code)
    resp.raise_for_status()
    time.sleep(20)


def group_details(group_id):
    # Kafka UI REST API: state, number of members and how many messages are waiting (lag) for one group
    resp = requests.get(f"{KAFKA_UI_URL}/api/clusters/local/consumer-groups/{group_id}", timeout=15)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def check_consumer_groups():
    # A service is ALIVE when its consumer group is connected. This holds even when nobody writes:
    # a dead service has no members, a live one waits for messages with lag 0.
    problems = []
    for group_id, label in GROUPS.items():
        details = group_details(group_id)
        if details is None:
            problems.append(f"{label}: consumer group '{group_id}' does not exist (the service never started)")
            print(f"  {label}: group '{group_id}' NOT FOUND")
            continue
        state = details.get("state")
        members = details.get("members")
        members = len(members) if isinstance(members, (list, dict)) else (members or 0)
        lag = details.get("messagesBehind")
        print(f"  {label}: state={state} members={members} messages waiting (lag)={lag}")
        if state in ("EMPTY", "DEAD", "UNKNOWN") or members == 0:
            problems.append(f"{label}: consumer group is {state} with {members} members, the service is down")
    if problems:
        raise RuntimeError("; ".join(problems) + "  (check: docker compose ps)")


# Python snippet that runs inside the dbt virtualenv (it has the Snowflake connector)
SF_CONNECT = """
import os, sys
import requests
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.environ["SHARED_SNOWFLAKE_ACCOUNT"], user=os.environ["SHARED_SNOWFLAKE_USER"],
    password=os.environ["SHARED_SNOWFLAKE_PASSWORD"], role=os.environ.get("SHARED_SNOWFLAKE_ROLE"),
    warehouse=os.environ.get("SHARED_SNOWFLAKE_WAREHOUSE"),
    database=os.environ.get("SHARED_SNOWFLAKE_DATABASE", "SUPPLY_PULSE_DB"), schema="BRONZE")
cur = conn.cursor()
cur.execute("create table if not exists STREAMING_HEALTH_LOG (checked_at timestamp_ltz default current_timestamp(), "
            "check_name string, status string, detail string)")


def log(check_name, status, detail):
    # one history row per check; a failed insert must never hide the check result
    try:
        cur.execute("insert into STREAMING_HEALTH_LOG (check_name, status, detail) values (%s, %s, %s)",
                    (check_name, status, detail))
    except Exception as e:
        print("could not write STREAMING_HEALTH_LOG:", e)


def show(columns, rows):
    # tiny text table for the Airflow log
    widths = [max(len(str(x)) for x in [c] + [r[i] for r in rows]) for i, c in enumerate(columns)]
    print("  ".join(str(c).ljust(w) for c, w in zip(columns, widths)))
    for r in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
"""


def sf_bash(body):
    return f"{VENV}/python - <<'PYEOF'\n{SF_CONNECT}{body}\nconn.close()\nPYEOF\n"


# FRESHNESS in detail, per topic. Two things are measured:
#   age      = minutes since the bronze-loader last wrote this topic
#   latency  = Kafka message time -> time written to Snowflake (last hour): how slow is the path?
# Verdict: age alone proves nothing (the source may be quiet), so it is combined with the loader's lag.
FRESHNESS = sf_bash("""
TOPICS = ("ops.public.vendor_items", "vendor_items.valid", "vendor_items.dlq", "vendor_items.quarantine")
cur.execute('''
    select topic,
           count(*)                                                                    as total_events,
           count_if(ingested_at > dateadd('minute', -15, current_timestamp()))         as last_15_min,
           to_varchar(max(ingested_at), 'YYYY-MM-DD HH24:MI:SS TZH')                   as last_loaded,
           datediff('minute', max(ingested_at), current_timestamp())                   as age_min,
           round(avg(iff(ingested_at > dateadd('hour', -1, current_timestamp()),
                 datediff('millisecond', to_timestamp_ltz(kafka_timestamp_ms, 3), ingested_at), null)) / 1000, 1)
                                                                                       as latency_sec_1h
    from RAW_EVENTS
    where topic in (%s, %s, %s, %s)
    group by topic order by topic''', TOPICS)
show(["topic", "total", "last_15min", "last_loaded", "age_min", "latency_sec_1h"], cur.fetchall())

cur.execute("select count(*), datediff('minute', max(ingested_at), current_timestamp()) from RAW_EVENTS")
total, age = cur.fetchone()
print("RAW_EVENTS total:", total, "| minutes since the last write (any topic):", age)

# messages waiting in Kafka for the bronze-loader (0 = it is caught up)
lag = None
try:
    resp = requests.get("__KAFKA_UI__/api/clusters/local/consumer-groups/bronze-loader", timeout=15)
    resp.raise_for_status()
    lag = resp.json().get("messagesBehind")
except Exception as e:
    print("could not read the loader lag from Kafka UI:", e)
print("bronze-loader lag (messages waiting in Kafka):", lag)

if not total:
    log("freshness", "FAIL", "RAW_EVENTS is empty")
    sys.exit("RAW_EVENTS is empty")
if age <= __MAX_AGE__:
    log("freshness", "OK", "last write %s min ago, lag %s" % (age, lag))
elif lag == 0:
    # nothing is waiting in Kafka, so the loader has everything the source sent: the source is just quiet
    print("IDLE: no new events for %s min, and the loader is caught up (lag 0). This is healthy." % age)
    log("freshness", "OK", "idle %s min, lag 0" % age)
elif lag is not None and lag > 0:
    detail = "%s messages wait in Kafka and nothing was written for %s min: the bronze-loader is stuck" % (lag, age)
    log("freshness", "FAIL", detail)
    print("HINT: docker compose logs --tail 20 bronze-loader")
    sys.exit(detail)
else:
    detail = "nothing written for %s min and the lag is unknown (Kafka UI unreachable): cannot tell quiet from stuck" % age
    log("freshness", "FAIL", detail)
    sys.exit(detail)
""").replace("__MAX_AGE__", str(MAX_AGE_MIN)).replace("__KAFKA_UI__", KAFKA_UI_URL)

# STAGE FLOW: did the events pass the QA gate? Compared with ITSELF, not with the clock:
#   ops_newest   = newest event that came from Debezium
#   routed_newest = newest event the QA gate wrote to valid / dlq / quarantine
# If the gate is alive, routed_newest follows ops_newest within seconds. A quiet source keeps both still.
STAGE_FLOW = sf_bash("""
cur.execute('''
    select max(iff(topic = 'ops.public.vendor_items', ingested_at, null))                    as ops_newest,
           max(iff(topic in ('vendor_items.valid', 'vendor_items.dlq', 'vendor_items.quarantine'), ingested_at, null))
                                                                                             as routed_newest,
           count_if(topic = 'ops.public.vendor_items'  and ingested_at > dateadd('hour', -1, current_timestamp())) as ops_1h,
           count_if(topic in ('vendor_items.valid', 'vendor_items.quarantine')
                                                       and ingested_at > dateadd('hour', -1, current_timestamp())) as ok_1h,
           count_if(topic = 'vendor_items.dlq'          and ingested_at > dateadd('hour', -1, current_timestamp())) as dlq_1h
    from RAW_EVENTS
    where topic in ('ops.public.vendor_items', 'vendor_items.valid', 'vendor_items.dlq', 'vendor_items.quarantine')''')
ops_newest, routed_newest, ops_1h, ok_1h, dlq_1h = cur.fetchone()
routed_1h = ok_1h + dlq_1h
# how far the QA gate is behind Debezium, in minutes (computed here, both values are timestamps)
gate_behind = None
if ops_newest is not None and routed_newest is not None:
    gate_behind = round((ops_newest - routed_newest).total_seconds() / 60, 1)
print("STAGE FLOW (events written to Snowflake, last hour)")
show(["stage", "events_last_hour", "newest"], [
    ("Debezium -> Kafka (ops.public.vendor_items)", ops_1h, ops_newest),
    ("QA gate -> valid + quarantine", ok_1h, "-"),
    ("QA gate -> dlq (rejected)", dlq_1h, routed_newest)])
print("the QA gate is %s min behind the newest Debezium event (limit __GATE_LAG__)" % gate_behind)

if ops_newest is None:
    log("stage_flow", "OK", "no vendor_items events yet")
    print("No vendor_items events in Snowflake yet: nothing to judge.")
elif routed_newest is None or (gate_behind is not None and gate_behind > __GATE_LAG__):
    detail = "the QA gate has not routed events that arrived %s min ago: qa-gate is stuck" % (gate_behind,)
    log("stage_flow", "FAIL", detail)
    print("HINT: docker compose logs --tail 20 qa-gate")
    sys.exit(detail)
else:
    ratio = (dlq_1h / routed_1h) if routed_1h else 0.0
    if routed_1h >= __DLQ_MIN__ and ratio > __DLQ_MAX__:
        detail = "DLQ share %.1f%% (%d of %d) is above %.0f%%: the source is sending bad data" % (
            ratio * 100, dlq_1h, routed_1h, __DLQ_MAX__ * 100)
        log("stage_flow", "FAIL", detail)
        sys.exit(detail)
    note = "judged" if routed_1h >= __DLQ_MIN__ else "too few events (%d < %d) to judge the DLQ share" % (routed_1h, __DLQ_MIN__)
    print("DLQ share: %.1f%% (%s)" % (ratio * 100, note))
    log("stage_flow", "OK", "ops=%d routed=%d dlq=%d, %s" % (ops_1h, routed_1h, dlq_1h, note))
""").replace("__GATE_LAG__", str(GATE_LAG_MIN)).replace("__DLQ_MIN__", str(DLQ_MIN_SAMPLE)).replace(
    "__DLQ_MAX__", str(DLQ_MAX_RATIO))

with DAG(
    dag_id="streaming_health",
    description="Supervises the streaming side: connector, service liveness (consumer groups), freshness, QA gate flow",
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Cairo"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": notify_slack},
    tags=["streaming", "monitoring"],
) as dag:

    t_check = PythonOperator(task_id="check_connector", python_callable=check_connector)
    t_restart = PythonOperator(task_id="restart_if_failed", python_callable=restart_connector,
                               trigger_rule="one_failed")   # runs only when check_connector failed
    # three independent leaves: each shows as its own box, and the run is red when any of them fails
    t_groups = PythonOperator(task_id="check_consumer_groups", python_callable=check_consumer_groups,
                              trigger_rule="none_failed")   # runs after a skipped or a successful restart
    t_fresh = BashOperator(task_id="check_freshness", bash_command=FRESHNESS, trigger_rule="none_failed")
    t_flow = BashOperator(task_id="check_stage_flow", bash_command=STAGE_FLOW, trigger_rule="none_failed")

    t_check >> t_restart >> [t_groups, t_fresh, t_flow]