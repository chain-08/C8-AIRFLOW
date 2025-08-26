from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def always_fail():
    raise Exception("Atlas test: failing on purpose")

with DAG(
    dag_id="atlas_fail_test",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@once",
    catchup=False,
    default_args={"owner": "atlas", "retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["atlas","test"],
) as dag:
    PythonOperator(task_id="fail_now", python_callable=always_fail)
