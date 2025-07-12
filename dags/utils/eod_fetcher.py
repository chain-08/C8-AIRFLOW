import os
import logging
import requests
from datetime import datetime
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
import clickhouse_connect
import pytz

# ------------------------
# Config per currency/region
# ------------------------
CURRENCY_CONFIG = {
    "INR": {
        "threads": 10,
        "timezone": "Asia/Kolkata"
    },
    "CAD": {
        "threads": 10,
        "timezone": "America/Toronto"
    },
    "USD": {
        "threads": 10,
        "timezone": "America/New_York"
    },
    "EUR": {
        "threads": 10,
        "timezone": "Europe/Paris"
    }
}

# ------------------------
# Environment config
# ------------------------
API_KEY = os.getenv("EODHD_API_KEY")
BASE_URL = os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

LOG_TABLE = 'eod_fetch_logs'
TABLE_NAME = 'eod'

# ------------------------
# Threading Decorator
# ------------------------
def threaded_executor(max_workers):
    def decorator(func):
        @wraps(func)
        def wrapper(args_list):
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_args = {executor.submit(func, *args): args for args in args_list}
                for future in as_completed(future_to_args):
                    args = future_to_args[future]
                    try:
                        result = future.result()
                        if result is not None:
                            results.append(result)
                    except Exception as e:
                        logging.error(f"❌ Error processing {args}: {e}")
            return results
        return wrapper
    return decorator

# ------------------------
# Main Fetch Function
# ------------------------
def fetch_and_store_eod_data(retry_failed=False, currency="INR", **context):
    config = CURRENCY_CONFIG.get(currency.upper(), {"threads": 10, "timezone": "UTC"})
    n_threads = config["threads"]
    tz = pytz.timezone(config["timezone"])

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASS,
        database=CLICKHOUSE_DB,
    )

    execution_dt = context["execution_date"]
    tz = pytz.timezone(CURRENCY_CONFIG[currency.upper()]["timezone"])
    fetch_date = execution_dt.astimezone(tz).strftime("%Y-%m-%d")
    
    logging.info(f"\n🚀 Fetching EOD data for {currency} on date: {fetch_date}")

    current_run_id = f"{currency}_{fetch_date.replace('-', '')}_{'retry' if retry_failed else 'main'}"

    if retry_failed:
        failed_query = f"""
            SELECT arrayJoin(failed_tickers) AS ticker
            FROM {LOG_TABLE}
            WHERE dag_id = '{currency.lower()}_currency_eod_pipeline'
            AND fetch_date = '{fetch_date}' AND run_id LIKE '%main%'
        """
        failed_df = client.query_df(failed_query)
        if failed_df.empty:
            logging.info("✅ No failed tickers from previous run. Skipping retry.")
            return
        symbols = [(t.split('.')[0], t.split('.')[1], 'Stock') for t in failed_df['ticker'] if '.' in t]
    else:
        metadata_df = client.query_df(f"""
            SELECT Code, Exchange, Type
            FROM tickers_metadata
            WHERE Currency = '{currency.upper()}'
        """)
        metadata_pl = pl.from_pandas(metadata_df).drop_nulls().unique(subset=["Code"], keep="first")
        symbols = metadata_pl.select(["Code", "Exchange", "Type"]).to_numpy().tolist()

    logging.info(f"✅ Tickers to fetch: {len(symbols)}")

    def fetch_eod(symbol_code, exchange_code, type_):
        symbol = f"{symbol_code}.{exchange_code}"
        url = f"{BASE_URL}/eod/{symbol}?api_token={API_KEY}&fmt=json&from={fetch_date}&to={fetch_date}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logging.warning(f"❌ Failed {symbol}: HTTP {resp.status_code}")
                return None
            data = resp.json()
            if not isinstance(data, list) or not data:
                logging.warning(f"⚠️ No data for {symbol}")
                return None
            df = pl.DataFrame(data).select([
                pl.lit(symbol_code).alias('code'),
                pl.col('date').str.strptime(pl.Date, "%Y-%m-%d").alias('date'),
                pl.col('open').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('adjusted_close').cast(pl.Float64),
                pl.col('volume').cast(pl.UInt64),
            ]).with_columns([
                pl.lit(currency.upper()).alias('currency'),
                pl.lit(exchange_code).alias('exchange'),
                pl.lit(type_).alias('type'),
            ])
            logging.info(f"✅ Success {symbol}")
            return df
        except Exception as e:
            logging.error(f"⚠️ Error fetching {symbol}: {e}")
            return None

    @threaded_executor(max_workers=n_threads)
    def fetch_worker(symbol_code, exchange_code, type_):
        return fetch_eod(symbol_code, exchange_code, type_)

    results = fetch_worker(symbols)
    successes = [r for r in results if r is not None]
    succeeded_keys = {(r[0, 'code'], r[0, 'exchange']) for r in successes}
    failed = [(code, exch, typ) for code, exch, typ in symbols if (code, exch) not in succeeded_keys]

    if successes:
        final_df = pl.concat(successes)
        client.insert(
            table=TABLE_NAME,
            data=final_df.to_numpy().tolist(),
            column_names=["code", "date", "open", "high", "low", "close",
                          "adjusted_close", "volume", "currency", "exchange", "type"],
        )
        logging.info(f"\n✅ Inserted {final_df.shape[0]} rows into {TABLE_NAME}")
    else:
        logging.warning("\n❌ No data inserted.")

    failed_symbols = [f"{code}.{exch}" for code, exch, _ in failed]

    client.insert(
        table=LOG_TABLE,
        data=[[
            datetime.today().date(),
            f"{currency.lower()}_currency_eod_pipeline",
            current_run_id,
            'retry_failed_tickers' if retry_failed else 'fetch_and_store_eod',
            datetime.strptime(fetch_date, "%Y-%m-%d").date(),
            len(symbols),
            len(successes),
            len(failed_symbols),
            failed_symbols,
            datetime.now(),
        ]],
        column_names=[
            "log_date", "dag_id", "run_id", "task_id", "fetch_date",
            "total", "success", "failed", "failed_tickers", "run_time"
        ]
    )

    logging.info("\n📊 Fetch Summary:")
    logging.info(f"➡️ Total Tickers: {len(symbols)}")
    logging.info(f"✅ Success: {len(successes)}")
    logging.info(f"❌ Failed: {len(failed_symbols)}")
    if failed_symbols:
        logging.warning(f"⚠️ Failed tickers: {failed_symbols}")
