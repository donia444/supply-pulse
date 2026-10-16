"""Standalone manual smoke-test DAG for Supply Pulse Snowflake QA.

Requires apache-airflow-providers-snowflake and an Airflow Snowflake
connection with conn_id='supply_pulse_snowflake'. No dbt dependency.
"""
import re
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# The location of fetch_one_handler depends on the installed common.sql provider version
try:
    from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
except ImportError:
    from airflow.providers.common.sql.hooks.sql import fetch_one_handler

CONN_ID = "supply_pulse_snowflake"
DB = "SUPPLY_PULSE_DB"
WH = "SUPPLY_PULSE_WH"


def hook():
    return SnowflakeHook(snowflake_conn_id=CONN_ID)


def test_connection():
    result = hook().get_first(
        f"SELECT CURRENT_DATABASE(), CURRENT_WAREHOUSE(), CURRENT_ROLE()"
    )
    if not result:
        raise AirflowException("Snowflake returned no connection details")
    print(f"Connected to Snowflake: database={result[0]}, warehouse={result[1]}, role={result[2]}")
    # Fully qualified QA objects avoid dependence on default database/schema.
    found = hook().get_first(
        f"SELECT COUNT(*) FROM {DB}.INFORMATION_SCHEMA.PROCEDURES "
        "WHERE PROCEDURE_SCHEMA='QA' AND PROCEDURE_NAME='RUN_QA'"
    )
    if not found or found[0] == 0:
        raise AirflowException("QA.RUN_QA procedure not found in " + DB)


def run_qa(**context):
    

    output = hook().run(
    sql=f"CALL {DB}.QA.RUN_QA('BRONZE')",
    autocommit=True,
    handler=fetch_one_handler
)
    message = str(output[0]) if output else ""
    print("QA procedure returned:", message)
    match = re.search(r"RUN_ID=([a-fA-F0-9-]{36})", message)
    if not match:
        raise AirflowException("Cannot extract RUN_ID from procedure output: " + message)
    run_id = match.group(1)
    context["ti"].xcom_push(key="qa_run_id", value=run_id)
    return run_id


def check_qa_result(**context):
    run_id = context["ti"].xcom_pull(task_ids="run_qa", key="qa_run_id")
    if not run_id or not re.fullmatch(r"[a-fA-F0-9-]{36}", run_id):
        raise AirflowException("Missing or invalid QA RUN_ID from XCom")

    row = hook().get_first(f"""
        SELECT COUNT(*) AS rules_executed,
               COUNT_IF(STATUS='RULE_ERROR') AS rules_broken,
               COUNT_IF(STATUS='ERROR') AS error_rules_failed,
               COUNT_IF(STATUS='WARNING') AS warning_rules_failed,
               COUNT_IF(STATUS='PASSED') AS rules_passed,
               COUNT_IF(STATUS='ERROR' AND FAILURE_RATE > 0.10) AS threshold_breaches
          FROM {DB}.QA.QA_EXECUTION_RESULTS
         WHERE RUN_ID = '{run_id}' AND LAYER = 'BRONZE'
    """)
    if not row:
        raise AirflowException("No results returned for RUN_ID=" + run_id)
    total, broken, errors, warnings, passed, breaches = [int(v or 0) for v in row]
    print(f"RUN_ID={run_id}; executed={total}; broken={broken}; errors={errors}; warnings={warnings}; passed={passed}; error_rules_over_10pct={breaches}")
    # Known baseline is 99 enabled rules. If someone changes enabled flags,
    # use the current catalog to detect partial execution rather than hardcoding 99.
    expected_row = hook().get_first(f"SELECT COUNT(*) FROM {DB}.QA.QA_RULES WHERE ENABLED=TRUE")
    expected = int(expected_row[0])
    if total != expected:
        raise AirflowException(f"Incomplete QA execution: {total} results, {expected} enabled rules")
    if broken or breaches:
        raise AirflowException(f"QA BLOCK: broken_rules={broken}, threshold_breaches={breaches}")
    print("QA PASS â€” current QA gate policy met. This does NOT mean data is error-free.")


with DAG(
    dag_id="supply_pulse_qa_smoke_test",
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Cairo"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["supply_pulse", "qa", "smoke_test"],
    description="Manual QA-only Snowflake connectivity/run/gate smoke test",
) as dag:
    connection = PythonOperator(task_id="test_snowflake_connection", python_callable=test_connection)
    execute = PythonOperator(task_id="run_qa", python_callable=run_qa)
    check = PythonOperator(task_id="check_qa_result", python_callable=check_qa_result)
    connection >> execute >> check