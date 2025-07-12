from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.eod_fetcher import fetch_and_store_eod_data
import pendulum

DAG_ID = "euro_currency_eod_pipeline"
SCHEDULE_CRON = "0 21 * * 1-5"  # 10:00 PM Paris = 21:00 UTC (adjust if needed)
CET = pendulum.timezone("Europe/Paris")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def create_dag():
    with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        start_date=datetime(2024, 1, 1, tzinfo=CET),
        schedule_interval=SCHEDULE_CRON,
        catchup=False,
        tags=["eur", "eod", "clickhouse"],
    ) as dag:

        fetch_task = PythonOperator(
            task_id="fetch_and_store_eod",
            python_callable=fetch_and_store_eod_data,
            op_kwargs={"retry_failed": False, "currency": "EUR"},
        )

        return dag

globals()[DAG_ID] = create_dag()
