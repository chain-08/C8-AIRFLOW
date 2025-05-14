# dags/btc_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/media/inglorious/ds/chain8/c8-airflow/btc_pipeline')
from fetch_ohlc_data import fetch_and_insert_data

# === 1️⃣ Default args ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7),   # Start date of data
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
    'depends_on_past': False
}

# === 2️⃣ DAG Definition ===
with DAG(
    'btc_ohlc_daily_ingestion',
    default_args=default_args,
    description='DAG to fetch and insert BTC/USDT OHLC data into ClickHouse',
    schedule_interval='15 0 * * *',  # Runs every day at 00:15 UTC
    catchup=True,
    max_active_runs=1
) as dag:

    fetch_and_insert_task = PythonOperator(
        task_id='fetch_and_insert_ohlc',
        python_callable=fetch_and_insert_data,
        op_args=['{{ ds }}'],   # Passes the execution date (e.g. 2025-05-07)
    )
    
    fetch_and_insert_task
