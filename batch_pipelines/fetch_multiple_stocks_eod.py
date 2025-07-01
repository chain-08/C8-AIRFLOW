import clickhouse_connect
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps


# ------------------------
# Configuration
# ------------------------
CURRENCY = "INR"  # 🔥 Change to 'USD', 'CAD', 'EUR' as needed
API_KEY = "67ad1bea200726.95055451"
BASE_URL = "https://eodhd.com/api"
CLICKHOUSE_HOST = '54.234.38.203'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'chain8'
CLICKHOUSE_PASS = 'c8_2025'
CLICKHOUSE_DB = 'default'
TABLE_NAME = 'eod'
N_THREADS = 10


# ------------------------
# Threaded Decorator
# ------------------------
def threaded_executor(max_workers=N_THREADS):
    def decorator(func):
        @wraps(func)
        def wrapper(args_list):
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_args = {
                    executor.submit(func, *args): args for args in args_list
                }
                for future in as_completed(future_to_args):
                    args = future_to_args[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results.append(result)
                    except Exception as e:
                        print(f"❌ Error processing {args}: {e}")
            return results
        return wrapper
    return decorator


# ------------------------
# ClickHouse Connection
# ------------------------
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASS,
    database=CLICKHOUSE_DB
)


# ------------------------
# Fetch Symbol List (Unique Code with First Exchange and Type)
# ------------------------
metadata_df = client.query_df(f"""
    SELECT Code, Exchange, Currency, Type
    FROM tickers_metadata
    WHERE Currency = '{CURRENCY}'
""")

metadata_df = metadata_df.dropna()

# Keep first Exchange and Type for each symbol Code
metadata_df = metadata_df.sort_values(by=["Code", "Exchange"]).drop_duplicates(subset=["Code"], keep="first")

symbol_list = metadata_df[["Code", "Exchange", "Currency", "Type"]].values.tolist()

print(f"✅ Found {len(symbol_list)} {CURRENCY} currency tickers.")


# ------------------------
# Dynamic Date Range Setup
# ------------------------
min_date_result = client.query(f"SELECT min(date) FROM {TABLE_NAME}")
min_date = min_date_result.result_rows[0][0]

if min_date is None:
    print("⚠️ No data in table yet. Using today's date as end date.")
    min_date = datetime.today().date()
else:
    min_date = pd.to_datetime(min_date).date() - timedelta(days=1)

from_date = '2023-01-01'
to_date = min_date.strftime('%Y-%m-%d')

print(f"\n🚀 Fetching {CURRENCY} EOD data from {from_date} to {to_date}\n")


# ------------------------
# Fetch function
# ------------------------
def fetch_eod_for_symbol(symbol_code, exchange, currency, ticker_type):
    symbol = f"{symbol_code}.{exchange}"
    url = (
        f"{BASE_URL}/eod/{symbol}"
        f"?api_token={API_KEY}&fmt=json&from={from_date}&to={to_date}"
    )

    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed {symbol}: HTTP {response.status_code}")
            return None

        data = response.json()
        if not isinstance(data, list) or not data:
            print(f"⚠️ No EOD data for {symbol}")
            return None

        df = pd.DataFrame(data)
        df["code"] = symbol_code
        df["exchange"] = exchange
        df["currency"] = currency
        df["type"] = ticker_type
        df["date"] = pd.to_datetime(df["date"]).dt.date

        df = df[[
            "code", "exchange", "currency", "type", "date", "open", "high", "low",
            "close", "adjusted_close", "volume"
        ]]

        print(f"✅ Success {symbol}")
        return df

    except Exception as e:
        print(f"⚠️ Error fetching {symbol}: {e}")
        return None


# ------------------------
# Apply Multithreading
# ------------------------
@threaded_executor(max_workers=N_THREADS)
def fetch_worker(symbol_code, exchange, currency, ticker_type):
    return fetch_eod_for_symbol(symbol_code, exchange, currency, ticker_type)


args_list = [(code, exch, curr, typ) for code, exch, curr, typ in symbol_list]
result_dfs = fetch_worker(args_list)


# ------------------------
# Insert into ClickHouse
# ------------------------
if result_dfs:
    final_df = pd.concat(result_dfs, ignore_index=True)

    client.insert(
        table=TABLE_NAME,
        data=final_df.values.tolist(),
        column_names=[
            "code", "exchange", "currency", "type", "date", "open", "high", "low",
            "close", "adjusted_close", "volume"
        ]
    )
    print(f"\n✅ Inserted {len(final_df)} rows into {TABLE_NAME}")
else:
    print("\n❌ No EOD data fetched for any symbol.")
