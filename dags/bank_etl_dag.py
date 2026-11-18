import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger("bank_etl")


def _extract():
    from src.extract import run
    return run()


def _transform(**ctx):
    from src.transform import run
    return run(ctx["ti"].xcom_pull(task_ids="extract")["path"])


def _quality(**ctx):
    import pandas as pd
    from src.quality import validate
    tr = ctx["ti"].xcom_pull(task_ids="transform")
    if tr.get("fact"):
        validate(pd.read_csv(tr["fact"], parse_dates=["transaction_date"]))


def _load(**ctx):
    from src.load import run
    run(ctx["ti"].xcom_pull(task_ids="transform"), ctx["ds"])


def _alert(context):
    ti = context.get("ti")
    log.error("ETL ошибка: задача=%s, запуск=%s", ti.task_id if ti else "?", context["run_id"])


def _success(context):
    log.info("ETL bank_etl успешно завершён (run=%s)", context["run_id"])


with DAG(
    dag_id="bank_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "on_failure_callback": _alert},
    on_success_callback=_success,
    tags=["bank", "etl"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    quality = PythonOperator(task_id="quality", python_callable=_quality, retries=0)
    load = PythonOperator(task_id="load", python_callable=_load)

    extract >> transform >> quality >> load