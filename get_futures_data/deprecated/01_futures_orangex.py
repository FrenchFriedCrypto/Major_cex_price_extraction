import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl
import json

from http_retry import request_json

class Timer():
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")


def fetch_klines(symbol, resolution, start_time, end_time, base_url):
    url = f'{base_url}/public/get_tradingview_chart_data'
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "/public/get_tradingview_chart_data",
        "params": {
            "instrument_name": symbol,
            "start_timestamp": str(start_time),
            "end_timestamp": str(end_time),
            "resolution": resolution
        }
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response_json = request_json(
        "POST",
        url,
        headers=headers,
        data=json.dumps(payload),
        max_retries=5,
        retry_sleep_seconds=2,
    )
    return response_json or {"result": []}

def process_data_for_symbol(symbol, resolution, output_folder, base_url):
    # Define start and end dates
    start_date = datetime.strptime("01/01/23", "%d/%m/%y")
    end_date = datetime.now()

    current_date = start_date

    # Convert the datetime object to server time (timestamp in seconds)
    def convert_to_timestamp(dt):
        return int(dt.replace(tzinfo=timezone.utc).timestamp())

    last_dt = datetime.strptime('2022-12-31 16:00:00', '%Y-%m-%d %H:%M:%S')

    while current_date < end_date:
        next_date = current_date + timedelta(days=150)
        # Adjust next_date if it exceeds end_date
        if next_date > end_date:
            next_date = end_date

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        max_retries = 5  # Reduced retries for efficiency
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = fetch_klines(symbol, resolution, start_time, end_time, base_url)
                if 'result' in response:
                    success = True
                elif 'error' in response:
                    error_code = response['error'].get('code', 'Unknown')
                    error_message = response['error'].get('message', 'No message provided')
                    print(f"API Error for symbol {symbol}: {error_code} - {error_message}")
                    # Skip retrying if the error is due to invalid instrument
                    if error_code == 5001:  # Instrument does not exist
                        return
                    else:
                        retry_count += 1
                        print(f"Retrying {retry_count}/{max_retries}...")
                        time.sleep(2)
                else:
                    raise ValueError(f"Unexpected response format: {response}")
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except (requests.exceptions.HTTPError, ValueError) as e:
                retry_count += 1
                print(f"Error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            print(f"Failed to fetch data for {symbol} after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        # Writing to CSV file (appending without duplicates)
        csv_file_path = os.path.join(output_folder, f'{symbol}.csv')

        data = response['result']  # 'result' contains the data directly
        # Assuming 'result' is a list of candle objects
        times = [entry['tick'] for entry in data]
        opens = [entry['open'] for entry in data]
        highs = [entry['high'] for entry in data]
        lows = [entry['low'] for entry in data]
        closes = [entry['close'] for entry in data]
        vols = [entry['volume'] for entry in data]
        amounts = [entry['cost'] for entry in data]  # Assuming 'cost' corresponds to 'amount'

        if times:
            if not (os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0):
                with open(csv_file_path, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(
                        ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume"])

            final_arr = []
            for i in range(len(times)):
                try:
                    open_time = datetime.fromtimestamp(int(times[i]), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    close_time = open_time  # Since there's no separate close time in the response
                    cur_dt = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S')
                    if cur_dt > last_dt:
                        final_arr.append(
                            [open_time, opens[i], highs[i], lows[i], closes[i], vols[i], close_time, amounts[i]])
                        last_dt = cur_dt
                except ValueError:
                    print(f"Error converting timestamp in row: {times[i]}")

            if final_arr:
                with open(csv_file_path, 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(final_arr)

        current_date = next_date
        time.sleep(0.2)


# Updated base URL
base_url = "https://api.orangex.com/api/v1"

# Map your intervals to the new API's resolution values
interval_mapping = {
    'Min1': '1',
    'Min3': '3',
    'Min5': '5',
    'Min10': '10',
    'Min15': '15',
    'Min30': '30',
    'Min60': '60',
    'Min120': '120',
    'Min180': '180',
    'Min240': '240',
    'Min360': '360',
    'Min720': '720',
    'D': 'D'
}

# Select the desired intervals
# For example, using '240' for 'Hour4'
intervals = ['Min240']  # Change to desired intervals based on interval_mapping keys

for interval in intervals:
    resolution = interval_mapping.get(interval)
    if not resolution:
        print(f"Unsupported interval: {interval}. Skipping.")
        continue

    output_folder = f'{interval}_orangex'
    os.makedirs(output_folder, exist_ok=True)
    symbols_csv_path = '../Symbols/futures/orangex_symbols.csv'

    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        # Convert symbols to the format expected by the new API, e.g., 'BTC-USDT-SPOT'
        # Adjust the suffix based on your specific instrument type
        symbols = []
        for row in symbols_reader:
            symbol = row[0].strip()
            symbols.append(symbol)

    timer = Timer()
    timer.start()

    print(f"Starting for interval: {interval} (Resolution: {resolution})")

    for symbol in symbols:
        print(f"Processing symbol: {symbol}")
        process_data_for_symbol(symbol, resolution, output_folder, base_url)

    timer.end()
