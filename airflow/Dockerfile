# Airflow image with dbt installed in its own virtualenv (so it cannot clash with Airflow's own packages)
FROM apache/airflow:2.10.2

USER airflow

# versions match the ones used on the development machine
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
      dbt-core==1.12.5 dbt-snowflake==1.12.1