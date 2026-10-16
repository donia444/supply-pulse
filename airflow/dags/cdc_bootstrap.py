"""
Keeps the Debezium connector registered and healthy.
Streaming itself runs all the time in Kafka Connect; Airflow only supervises it.
  1) register_connector : idempotent PUT of the connector config
  2) check_connector    : fails the run if the connector or any task is not RUNNING
"""
import json
from datetime import datetime

import requests
from airflow.decorators import dag, task

CONNECT = "http://connect:8083"
NAME = "qcommerce-cdc"
CONFIG_FILE = "/opt/airflow/infra/debezium/connector.json"


@dag(dag_id="cdc_bootstrap", schedule="*/15 * * * *", start_date=datetime(2026, 9, 1),
     catchup=False, tags=["cdc", "debezium"])
def cdc_bootstrap():
    @task
    def register_connector():
        with open(CONFIG_FILE) as f:
            config = json.load(f)["config"]
        r = requests.put(f"{CONNECT}/connectors/{NAME}/config", json=config, timeout=30)
        r.raise_for_status()

    @task
    def check_connector():
        r = requests.get(f"{CONNECT}/connectors/{NAME}/status", timeout=30)
        r.raise_for_status()
        status = r.json()
        states = [status["connector"]["state"]] + [t["state"] for t in status["tasks"]]
        if not states or any(s != "RUNNING" for s in states):
            raise RuntimeError(f"Connector not healthy: {status}")

    register_connector() >> check_connector()


cdc_bootstrap()
