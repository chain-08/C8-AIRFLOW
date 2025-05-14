# btc_pipeline/fetch_ohlc_data.py

import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
import clickhouse_connect

# === 1️⃣ Load the environment variables from the .env file ===
load_dotenv()
API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'default')

# === 2️⃣ Connect to ClickHouse ===
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    port=8123,
    interface='http'
)

# === 3️⃣ Binance API endpoint and parameters ===
url = "https://api.binance.com/api/v3/klines"

def fetch_and_insert_data(execution_date):
    """
    Fetches OHLC data from Binance API and inserts it into ClickHouse.
    """
    print(f"Fetching data for {execution_date}...")
    
    start_date = datetime.strptime(execution_date, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    
    params = {
        'symbol': 'BTCUSDT',
        'interval': '1d',
        'startTime': int(start_date.timestamp() * 1000),
        'endTime': int(end_date.timestamp() * 1000),
        'limit': 1000
    }

    headers = {
        'X-MBX-APIKEY': API_KEY
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        
        batch_data = []
        for candle in data:
            batch_data.append((
                datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                float(candle[1]),
                float(candle[2]),
                float(candle[3]),
                float(candle[4]),
                float(candle[5]),
                float(candle[7]),
                int(candle[8]),
                float(candle[9]),
                float(candle[10]),
                int(candle[11]),
                datetime.fromtimestamp(candle[6] / 1000, tz=timezone.utc)
            ))

        client.insert('btc_usdt_daily_ohlc', batch_data, 
            column_names=[
                'open_time',
                'open_price',
                'high_price',
                'low_price',
                'close_price',
                'volume',
                'quote_asset_volume',
                'number_of_trades',
                'taker_buy_base_volume',
                'taker_buy_quote_volume',
                'ignored_field',
                'close_time'
            ]
        )

        print(f"✅ Data inserted successfully for {execution_date}")
    else:
        print(f"❌ Failed to retrieve data: {response.status_code}")
        print(f"Error: {response.text}")
