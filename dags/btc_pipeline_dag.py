from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import clickhouse_connect


def date_to_millis(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def fetch_and_store(**context):
    import socket, requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    symbol = "BTCUSDT"
    start_date = context["ds"]
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    ip = socket.getaddrinfo("api.binance.com", 443, socket.AF_INET)[0][4][0]

    url = f"https://{ip}/api/v3/uiKlines"
    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": date_to_millis(start_date),
        "endTime": date_to_millis(end_date),
        "limit": 1
    }

    response = requests.get(
        url,
        headers={"Host": "api.binance.com"},
        params=params,
        timeout=10,
        verify=False  # bypass SSL hostname mismatch
    )
    response.raise_for_status()
    data = response.json()

    if not data:
        raise ValueError(f"No data returned for date: {start_date}")

    record = data[0]
    row = [(
        datetime.fromtimestamp(record[0] / 1000, timezone.utc),
        float(record[1]),
        float(record[2]),
        float(record[3]),
        float(record[4]),
        float(record[5]),
        datetime.fromtimestamp(record[6] / 1000, timezone.utc),
        float(record[7]),
        int(record[8]),
        float(record[9]),
        float(record[10])
    )]

    client = clickhouse_connect.get_client(
        host='54.234.38.203',
        port=8123,
        username='chain8',
        password='c8_2025',
    database='default'
    )

    client.command("""
    CREATE TABLE IF NOT EXISTS btc_usdt_test (
        open_time DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        close_time DateTime,
        quote_asset_volume Float64,
        number_of_trades UInt32,
        taker_buy_base_vol Float64,
        taker_buy_quote_vol Float64
    ) ENGINE = MergeTree()
    ORDER BY open_time
    """)

    client.insert('btc_usdt_test', row, column_names=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_vol", "taker_buy_quote_vol"
    ])

    print(f"Inserted data for {start_date}")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='btc_to_clickhouse_daily',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 1),
    catchup=True,
    tags=["binance", "clickhouse"],
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_btc_and_store',
        python_callable=fetch_and_store,
        provide_context=True,
    )
