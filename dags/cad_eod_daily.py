from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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
CURRENCY = 'CAD'
DAG_ID = 'cad_currency_eod_pipeline'
SCHEDULE_CRON = '0 2 * * 2-6'  # 10 PM Toronto (Eastern) = 2 AM UTC next day

TABLE_NAME = 'eod'
LOG_TABLE = 'eod_fetch_logs'
N_THREADS = 10
RETRY_LIMIT = 1

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
    'retries': RETRY_LIMIT,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE_CRON,
    catchup=False,
    tags=['canada', 'cad', 'eod', 'clickhouse'],
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


    def fetch_and_store_eod_data():
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )

        # ✅ Create target table if not exists
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                code LowCardinality(String),
                date Date,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                adjusted_close Float64,
                volume UInt64,
                currency LowCardinality(String),
                exchange LowCardinality(String),
                type LowCardinality(String),
                created_on DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (currency, code, date)
        """)

        # ✅ Create logs table if not exists
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                log_date Date DEFAULT today(),
                dag_id String,
                task_id String,
                fetch_date Date,
                total UInt32,
                success UInt32,
                failed UInt32,
                failed_tickers Array(String),
                run_time DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (log_date, dag_id, task_id, fetch_date)
        """)

        # ✅ Fetch ticker metadata for CAD
        metadata_df = client.query_df(f"""
            SELECT Code, Exchange, Type
            FROM tickers_metadata
            WHERE Currency = '{CURRENCY}'
        """)

        metadata_pl = (
            pl.from_pandas(metadata_df)
            .drop_nulls()
            .unique(subset=["Code"], keep="first")  # ✅ Unique symbols with first Exchange and Type
        )

        symbols = metadata_pl.select(["Code", "Exchange", "Type"]).to_numpy().tolist()

        total_tickers = len(symbols)
        logging.info(f"✅ Found {total_tickers} tickers for {CURRENCY} currency.")

        fetch_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        logging.info(f"\n🚀 Fetching EOD data for date: {fetch_date}\n")

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

        for attempt in range(1, RETRY_LIMIT + 2):
            logging.info(f"\n🚀 Attempt {attempt} - Fetching {len(failed)} tickers...\n")
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

            if not failed:
                break


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
                dag.dag_id,
                'fetch_and_store_eod',
                datetime.strptime(fetch_date, "%Y-%m-%d").date(),
                total_tickers,
                len(success_results),
                len(failed_symbols),
                failed_symbols,
                datetime.now(),
            ]],
            column_names=[
                "log_date", "dag_id", "task_id", "fetch_date",
                "total", "success", "failed", "failed_tickers", "run_time"
            ]
        )

        logging.info("\n📊 Fetch Summary:")
        logging.info(f"➡️ Total Tickers: {total_tickers}")
        logging.info(f"✅ Success: {len(success_results)}")
        logging.info(f"❌ Failed: {len(failed_symbols)}")
        if failed_symbols:
            logging.warning(f"⚠️ Failed tickers: {failed_symbols}")

    fetch_and_store_task = PythonOperator(
        task_id='fetch_and_store_eod',
        python_callable=fetch_and_store_eod_data,
    )

    fetch_and_store_task
