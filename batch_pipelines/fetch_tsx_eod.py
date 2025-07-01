import clickhouse_connect
import requests
import pandas as pd
from datetime import datetime, timedelta

# ------------------------
# Configuration
# ------------------------
API_KEY = "67ad1bea200726.95055451"
BASE_URL = "https://eodhd.com/api"
SYMBOL = "SHOP.TO"
COMPANY_NAME = "Shopify Inc."

CLICKHOUSE_HOST = '54.234.38.203'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'chain8'
CLICKHOUSE_PASS = 'c8_2025'
CLICKHOUSE_DB = 'default'
TABLE_NAME = 'tsx_eod'

# ------------------------
# ClickHouse: Connect
# ------------------------
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASS,
    database=CLICKHOUSE_DB
)

# ------------------------
# Step 1: Create Table (if not exists)
# ------------------------
create_table_query = f"""
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
ORDER BY (company_name, date);
"""

client.command(create_table_query)
print(f"✅ Table '{TABLE_NAME}' ready.")

# ------------------------
# Step 2: Extract EOD Data for Yesterday
# ------------------------
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
url = f"{BASE_URL}/eod/{SYMBOL}?api_token={API_KEY}&fmt=json&from={yesterday}&to={yesterday}"
response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"❌ API error: {response.status_code} - {response.text}")

data = response.json()

if not isinstance(data, list) or not data:
    print(f"⚠️ No data available for {yesterday}. Market might have been closed.")
    exit(0)

# ------------------------
# Step 3: Transform
# ------------------------
df = pd.DataFrame(data)
df["company_name"] = COMPANY_NAME
df["date"] = pd.to_datetime(df["date"]).dt.date  # Convert to datetime.date

# Reorder to match ClickHouse schema (excluding created_on)
df = df[[
    "company_name", "date", "open", "high", "low",
    "close", "adjusted_close", "volume"
]]

# ------------------------
# Step 4: Insert into ClickHouse
# ------------------------
records = df.values.tolist()

client.insert(
    table=TABLE_NAME,
    data=records,
    column_names=[
        "company_name", "date", "open", "high", "low",
        "close", "adjusted_close", "volume"
    ]
)

print(f"✅ Inserted EOD data for {yesterday} for '{COMPANY_NAME}' into '{TABLE_NAME}'.")
