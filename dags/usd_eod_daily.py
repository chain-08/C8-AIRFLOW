from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import clickhouse_connect
import requests
import polars as pl
import logging
import os


# ------------------------
# Configurable Variables
# ------------------------
CURRENCY = 'USD'
DAG_ID = 'usd_currency_eod_pipeline'
SCHEDULE_CRON = '30 0 * * 2-6'  # ✅ 8:30 PM New York = 00:30 AM UTC (next day)

TABLE_NAME = 'eod'
LOG_TABLE = 'eod_fetch_logs'
N_THREADS = 10
TIME_BUFFER = -4  # ✅ UTC -4 for New York (safe buffer post-market)

# ------------------------
# Environment Variables
# ------------------------
API_KEY = os.getenv("EODHD_API_KEY")
BASE_URL = os.getenv("EODHD_BASE_URL", "https://eodhd.com/api")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")


# ------------------------
# DAG Configuration
# ------------------------
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE_CRON,
    catchup=False,
    tags=['usa', 'usd', 'eod', 'clickhouse'],
) as dag:

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
                            logging.error(f"❌ Error processing {args}: {e}")
                return results
            return wrapper
        return decorator


    def fetch_and_store_eod_data(retry_failed=False):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )

        fetch_date = (datetime.now(timezone.utc) + timedelta(hours=TIME_BUFFER)).strftime('%Y-%m-%d')

        logging.info(f"\n🚀 Fetching EOD data for date: {fetch_date}")

        current_run_id = f"{DAG_ID}_{fetch_date.replace('-', '')}_{'retry' if retry_failed else 'main'}"

        if retry_failed:
            failed_query = f"""
                SELECT arrayJoin(failed_tickers) AS ticker
                FROM {LOG_TABLE}
                WHERE dag_id = '{DAG_ID}' AND fetch_date = '{fetch_date}' AND run_id LIKE '%main%'
            """
            failed_df = client.query_df(failed_query)

            if failed_df.empty:
                logging.info("✅ No failed tickers from previous run. Skipping retry.")
                return

            symbols = []
            for t in failed_df['ticker']:
                parts = t.split('.')
                if len(parts) == 2:
                    symbols.append((parts[0], parts[1], 'Stock'))
        else:
            metadata_df = client.query_df(f"""
                SELECT Code, Exchange, Type
                FROM tickers_metadata
                WHERE Currency = '{CURRENCY}'
            """)
            metadata_pl = (
                pl.from_pandas(metadata_df)
                .drop_nulls()
                .unique(subset=["Code"], keep="first")
            )
            symbols = metadata_pl.select(["Code", "Exchange", "Type"]).to_numpy().tolist()

        total_tickers = len(symbols)
        logging.info(f"✅ Tickers to fetch: {total_tickers}")

        def fetch_eod_for_symbol(symbol_code, exchange_code, type_):
            symbol = f"{symbol_code}.{exchange_code}"
            url = (
                f"{BASE_URL}/eod/{symbol}"
                f"?api_token={API_KEY}&fmt=json&from={fetch_date}&to={fetch_date}"
            )
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    logging.warning(f"❌ Failed {symbol}: HTTP {resp.status_code}")
                    return None

                data = resp.json()
                if not isinstance(data, list) or not data:
                    logging.warning(f"⚠️ No data for {symbol}")
                    return None

                df = pl.DataFrame(data)
                df = df.select([
                    pl.lit(symbol_code).alias('code'),
                    pl.col('date').str.strptime(pl.Date, "%Y-%m-%d").alias('date'),
                    pl.col('open').cast(pl.Float64),
                    pl.col('high').cast(pl.Float64),
                    pl.col('low').cast(pl.Float64),
                    pl.col('close').cast(pl.Float64),
                    pl.col('adjusted_close').cast(pl.Float64),
                    pl.col('volume').cast(pl.UInt64),
                ]).with_columns([
                    pl.lit(CURRENCY).alias('currency'),
                    pl.lit(exchange_code).alias('exchange'),
                    pl.lit(type_).alias('type'),
                ])

                logging.info(f"✅ Success {symbol}")
                return df

            except Exception as e:
                logging.error(f"⚠️ Error fetching {symbol}: {e}")
                return None


        @threaded_executor(max_workers=N_THREADS)
        def fetch_worker(symbol_code, exchange_code, type_):
            return fetch_eod_for_symbol(symbol_code, exchange_code, type_)


        args_list = [(code, exch, typ) for code, exch, typ in symbols]

        success_results = []
        failed = args_list.copy()

        current_results = fetch_worker(failed) if failed else []

        successes = [r for r in current_results if r is not None]
        success_results.extend(successes)

        succeeded_symbols = {
            (r[0, 'code'], r[0, 'exchange']) for r in successes
        }

        failed = [
            (code, exch, typ) for code, exch, typ in failed
            if (code, exch) not in succeeded_symbols
        ]


        # ✅ Insert data
        if success_results:
            combined_df = pl.concat(success_results)

            client.insert(
                table=TABLE_NAME,
                data=combined_df.to_numpy().tolist(),
                column_names=[
                    "code", "date", "open", "high", "low",
                    "close", "adjusted_close", "volume",
                    "currency", "exchange", "type"
                ],
            )
            logging.info(f"\n✅ Inserted {combined_df.shape[0]} rows into {TABLE_NAME}")
        else:
            logging.warning("\n❌ No data inserted.")

        # ✅ Log run info
        failed_symbols = [f"{code}.{exch}" for code, exch, _ in failed]

        client.insert(
            table=LOG_TABLE,
            data=[[
                datetime.today().date(),
                DAG_ID,
                current_run_id,
                'retry_failed_tickers' if retry_failed else 'fetch_and_store_eod',
                datetime.strptime(fetch_date, "%Y-%m-%d").date(),
                total_tickers,
                len(success_results),
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
        logging.info(f"➡️ Total Tickers: {total_tickers}")
        logging.info(f"✅ Success: {len(success_results)}")
        logging.info(f"❌ Failed: {len(failed_symbols)}")
        if failed_symbols:
            logging.warning(f"⚠️ Failed tickers: {failed_symbols}")


    # ✅ Main Fetch at 8:30 PM New York Time
    fetch_full_task = PythonOperator(
        task_id='fetch_and_store_eod',
        python_callable=lambda: fetch_and_store_eod_data(retry_failed=False),
    )

    # ✅ Retry Task at 12:30 AM New York (4 hours later)
    fetch_retry_task = PythonOperator(
        task_id='retry_failed_tickers',
        python_callable=lambda: fetch_and_store_eod_data(retry_failed=True),
    )

    fetch_full_task >> fetch_retry_task
