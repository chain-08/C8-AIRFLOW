from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect
import requests
import pandas as pd

# ------------------------
# Config
# ------------------------
SYMBOL = "SHOP.TO"
COMPANY_NAME = "Shopify Inc."
API_KEY = "67ad1bea200726.95055451"
BASE_URL = "https://eodhd.com/api"
TABLE_NAME = "tsx_eod"

CLICKHOUSE_CONFIG = {
    "host": "54.234.38.203",
    "port": 8123,
    "username": "chain8",
    "password": "c8_2025",
    "database": "default"
}

# ------------------------
# ETL Function
# ------------------------
def fetch_and_store_tsx(**context):
    from datetime import datetime, timedelta

    # Compute the previous day (EOD data)
    target_date = (datetime.strptime(context["ds"], "%Y-%m-%d") - timedelta(days=1)).strftime('%Y-%m-%d')
    url = f"{BASE_URL}/eod/{SYMBOL}?api_token={API_KEY}&fmt=json&from={target_date}&to={target_date}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list) or not data:
        raise ValueError(f"No EOD data found for {target_date}")

    df = pd.DataFrame(data)
    df["company_name"] = COMPANY_NAME
    df["date"] = pd.to_datetime(df["date"]).dt.date

    df = df[[
        "company_name", "date", "open", "high", "low",
        "close", "adjusted_close", "volume"
    ]]

    records = df.values.tolist()

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

    client.command(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        company_name LowCardinality(String),
        date Date,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        adjusted_close Float64,
        volume UInt64,
        created_on DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (company_name, date)
    """)

    client.insert(
        table=TABLE_NAME,
        data=records,
        column_names=[
            "company_name", "date", "open", "high", "low",
            "close", "adjusted_close", "volume"
        ]
    )

    print(f"✅ Inserted EOD data for {target_date} into {TABLE_NAME}")

# ------------------------
# DAG Definition
# ------------------------
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='tsx_to_clickhouse_daily',
    default_args=default_args,
    schedule_interval='0 21 * * *',  # 9:00 PM UTC
    start_date=datetime(2025, 6, 1),
    catchup=True,
    tags=['tsx', 'clickhouse', 'eodhd']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_tsx_eod_and_store',
        python_callable=fetch_and_store_tsx,
        provide_context=True
    )
