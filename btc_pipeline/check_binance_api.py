import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timezone

# Load the environment variables from the .env file
load_dotenv()

# Retrieve the API key and secret from the environment
API_KEY = os.getenv('BINANCE_API_KEY')
SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

# Binance API endpoint to fetch OHLC data
url = "https://api.binance.com/api/v3/klines"

# Parameters for the request (24-hour interval for BTC/USDT)
params = {
    'symbol': 'BTCUSDT',
    'interval': '1d',  # 1d for 24-hour interval
    'limit': 1         # Limit to 1 data point (the most recent)
}

# Set up headers with the API key (if needed, for authenticated requests)
headers = {
    'X-MBX-APIKEY': API_KEY
}

# Send the GET request to the Binance API
response = requests.get(url, params=params, headers=headers)

# Check if the API call was successful
if response.status_code == 200:
    data = response.json()
    # Extracting all relevant data
    open_time = data[0][0]  # in milliseconds
    open_price = data[0][1]
    high_price = data[0][2]
    low_price = data[0][3]
    close_price = data[0][4]
    volume = data[0][5]
    close_time = data[0][6]  # in milliseconds
    quote_asset_volume = data[0][7]
    number_of_trades = data[0][8]
    taker_buy_base_asset_volume = data[0][9]
    taker_buy_quote_asset_volume = data[0][10]
    ignored_field = data[0][11]  # Unused field

    # Convert timestamps to timezone-aware datetime format
    open_time_str = datetime.fromtimestamp(open_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    close_time_str = datetime.fromtimestamp(close_time / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # Print all the data
    print(f"BTC/USDT 24-Hour OHLC Data:")
    print(f"Open Time: {open_time_str}")
    print(f"Open: {open_price}")
    print(f"High: {high_price}")
    print(f"Low: {low_price}")
    print(f"Close: {close_price}")
    print(f"Volume: {volume}")
    print(f"Quote Asset Volume: {quote_asset_volume}")
    print(f"Number of Trades: {number_of_trades}")
    print(f"Taker Buy Base Asset Volume: {taker_buy_base_asset_volume}")
    print(f"Taker Buy Quote Asset Volume: {taker_buy_quote_asset_volume}")
    print(f"Ignored Field: {ignored_field}")
    print(f"Close Time: {close_time_str}")
else:
    print(f"Failed to retrieve data: {response.status_code}")
    print(f"Error: {response.text}")
