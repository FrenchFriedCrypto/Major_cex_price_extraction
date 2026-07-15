import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl

from http_retry import request_json

class Timer:
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")

def fetch_klines(symbol, interval, start_time, end_time, limit, host):
    """
    Fetch kline data from XT.com API.

    Parameters:
    - symbol (str): Trading pair.
    - interval (str): Time interval (e.g., '1m', '5m', '1h', etc.).
    - start_time (int): Start time in milliseconds.
    - end_time (int): End time in milliseconds.
    - limit (int): Number of data points to fetch.
    - host (str): Base URL for the API.

    Returns:
    - dict: JSON response from the API.
    """
    url = host
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time,
        'limit': limit
    }
    response = request_json("GET", url, params=params, max_retries=999, retry_sleep_seconds=2)
    return response or {}

def get_last_date_from_csv(csv_file_path):
    """
    Get the last date (open time) from the existing CSV file.

    Parameters:
    - csv_file_path (str): Path to the CSV file.

    Returns:
    - datetime or None: The last date in the CSV or None if the file doesn't exist or is empty.
    """
    last_date = None
    if os.path.exists(csv_file_path):
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            next(reader, None)  # Skip the header
            rows = list(reader)
            if rows:
                last_row = rows[-1]
                last_date_str = last_row[0]  # Assuming the first column is the open time
                last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
                last_date = last_date.replace(tzinfo=timezone.utc)  # Make it timezone-aware
    return last_date

def process_data_for_symbol(symbol, interval, output_folder, host, gap, limit):
    """
    Process and fetch kline data for a specific symbol and interval.

    Parameters:
    - symbol (str): Trading pair.
    - interval (str): Time interval.
    - output_folder (str): Directory to save CSV files.
    - host (str): API endpoint URL.
    - gap (int): Time gap in appropriate units based on interval.
    - limit (int): Number of data points per API request.
    """
    # Define CSV file path
    csv_file_path = os.path.join(output_folder, f'{symbol}.csv')
    last_date = get_last_date_from_csv(csv_file_path)

    if last_date:
        # Start from the next millisecond after the last date
        start_date = last_date + timedelta(milliseconds=1)
        print(f"Resuming from last date for {symbol}: {start_date}")
    else:
        # Start from a default date if CSV doesn't exist
        start_date = datetime.strptime("31/12/22", "%d/%m/%y")
        start_date = start_date.replace(tzinfo=timezone.utc)  # Make it timezone-aware

    end_date = datetime.now(timezone.utc)

    current_date = start_date

    def convert_to_timestamp_ms(dt):
        """Convert datetime to timestamp in milliseconds."""
        return int(dt.timestamp() * 1000)

    last_dt = start_date

    while current_date < end_date:
        # Determine the next date based on the interval
        if interval.endswith('m'):
            minutes = int(interval[:-1])
            next_date = current_date + timedelta(minutes=gap)
        elif interval.endswith('h'):
            hours = int(interval[:-1])
            next_date = current_date + timedelta(hours=gap)
        elif interval.endswith('d'):
            days = int(interval[:-1])
            next_date = current_date + timedelta(days=gap)
        elif interval.endswith('w'):
            weeks = int(interval[:-1])
            next_date = current_date + timedelta(weeks=gap)
        else:
            print(f"Unsupported interval format: {interval}")
            return

        start_time = convert_to_timestamp_ms(current_date)
        end_time = convert_to_timestamp_ms(next_date)

        max_retries = 999
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = fetch_klines(symbol, interval, start_time, end_time, limit, host)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # Wait before retrying
            except requests.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {e}. Skipping this time period.")
                break
            except requests.exceptions.RequestException as e:
                retry_count += 1
                print(f"Request exception: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            print(f"Failed to fetch data after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        # Writing to CSV file (appending without duplicates)
        if 'returnCode' in response and response['returnCode'] == 0 and 'result' in response and response['result']:
            data = response['result']
            # Each kline is a dict with keys: 's', 'p', 't', 'o', 'c', 'h', 'l', 'a', 'v'

            final_arr = []

            # **New Addition:** Sort klines in ascending order based on timestamp
            data_sorted = sorted(data, key=lambda x: x['t'])

            for kline in data_sorted:
                try:
                    open_time_ms = kline['t']
                    open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    open_price = kline['o']
                    high_price = kline['h']
                    low_price = kline['l']
                    close_price = kline['c']
                    volume = kline['v']
                    # Assuming 'a' is Quote asset volume
                    quote_asset_volume = kline['a']

                    # For close_time, since the API doesn't provide a separate close time,
                    # we'll assume it's the same as open_time or calculate based on interval
                    close_time = open_time  # Modify if the API provides a different close time

                    cur_dt = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

                    if cur_dt > last_dt:
                        final_arr.append([
                            open_time,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            volume,
                            close_time,
                            quote_asset_volume
                        ])
                        last_dt = cur_dt
                except (ValueError, KeyError) as e:
                    print(f"Error processing kline data: {kline} - {e}")

            if final_arr:
                # Write headers if CSV is new or empty
                if not (os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0):
                    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow([
                            "Open time", "Open", "High", "Low", "Close",
                            "Volume", "Close time", "Quote asset volume"
                        ])

                # Append data to CSV
                with open(csv_file_path, 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(final_arr)

                print(f"Appended {len(final_arr)} records to '{csv_file_path}'")
            else:
                print(f"No new data to append for {symbol} at interval {interval}")
        else:
            print(f"No data returned for {symbol} at interval {interval}")

        current_date = next_date
        time.sleep(0.2)  # Brief pause to respect API rate limits

# Hosts URL for the XT.com API
host = "https://fapi.xt.com/future/market/v1/public/q/kline"

# Supported intervals based on the new API
# intervals = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
intervals = ["4h"]

# Mapping intervals to gap values
# The gap determines how much time to fetch in each API request
# Adjust these values based on your requirements and API rate limits
interval_gap_mapping = {
    '1m': 1000,  # Fetch 1000 minutes per request
    '5m': 5000,  # Fetch 5000 minutes (approx. 347 hours) per request
    '15m': 3000, # Fetch 3000 intervals of 15 minutes
    '30m': 2000, # Adjust as needed
    '1h': 1000,  # Fetch 1000 hours per request
    '4h': 4000,  # Fetch 4000 hours per request
    '1d': 1000,  # Fetch 1000 days per request
    '1w': 1000   # Fetch 1000 weeks per request
}

# Define the limit parameter for the API
# The API documentation should specify the maximum allowed limit per request
# Adjust accordingly. Here, we'll set it to 1000 as an example
default_limit = 1000

# Path to the CSV file containing symbols
symbols_csv_path = 'Symbols/futures/xt_symbols.csv'

# Loop through each interval and process symbols
for interval in intervals:
    output_folder = f'{interval}_xt'
    os.makedirs(output_folder, exist_ok=True)

    # Read symbols from CSV
    if not os.path.exists(symbols_csv_path):
        print(f"Symbols CSV file '{symbols_csv_path}' does not exist. Skipping interval {interval}.")
        continue

    with open(symbols_csv_path, 'r', newline='', encoding='utf-8') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0] for row in symbols_reader if row]

    timer = Timer()
    timer.start()

    print(f"Starting for interval: {interval}")

    # Get the gap value for the current interval
    gap = interval_gap_mapping.get(interval, 1000)  # Default to 1000 if not found

    for symbol in symbols:
        process_data_for_symbol(symbol, interval, output_folder, host, gap, default_limit)

    timer.end()
