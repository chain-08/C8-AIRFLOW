from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.eod_fetcher import fetch_and_store_eod_data
import pendulum

DAG_ID = "usd_currency_eod_retry"
SCHEDULE_CRON = "0 8 * * 1-5"  # 3:00 AM New York = 08:00 UTC
TZ = pendulum.timezone("America/New_York")

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
        start_date=datetime(2024, 1, 1, tzinfo=TZ),
        schedule_interval=SCHEDULE_CRON,
        catchup=False,
        tags=["usd", "eod", "clickhouse", "retry"],
    ) as dag:

        retry_task = PythonOperator(
            task_id="retry_failed_tickers",
            python_callable=fetch_and_store_eod_data,
            op_kwargs={"retry_failed": True, "currency": "USD"},
        )

        return dag

globals()[DAG_ID] = create_dag()
