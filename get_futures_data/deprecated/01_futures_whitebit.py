import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl
import re

from http_retry import request_json


class Timer():
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")


def fetch_klines(symbol, interval, start_time, end_time, limit, url):
    params = {
        'market': symbol,
        'interval': interval,
        'start': start_time,
        'end': end_time,
        'limit': limit
    }
    json_response = request_json("GET", url, params=params, max_retries=5, retry_sleep_seconds=2)
    if json_response is None:
        return []
    if json_response.get('success'):
        return json_response['result']
    else:
        raise ValueError(f"API error: {json_response.get('message')}")


def get_timedelta_for_interval(interval, num_candles):
    units = {
        'm': 'minutes',
        'h': 'hours',
        'd': 'days',
        'w': 'weeks',
        'M': 'months'  # Note: months are variable length, handle separately
    }
    match = re.match(r'(\d+)([mhdwM])', interval)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        if unit == 'M':
            # Months are variable in length, assume 30 days per month
            return timedelta(days=value * 30 * num_candles)
        else:
            kwargs = {units[unit]: value * num_candles}
            return timedelta(**kwargs)
    else:
        raise ValueError(f"Invalid interval format: {interval}")


def process_data_for_symbol(symbol, interval, output_folder, url):
    # Define start and end dates (timezone-aware in UTC)
    start_date = datetime.strptime("01/01/23", "%d/%m/%y").replace(tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)

    current_date = start_date

    # Convert the datetime object to timestamp in seconds
    def convert_to_timestamp(dt):
        return int(dt.timestamp())

    last_dt = datetime.strptime('2022-12-31 16:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

    # Calculate maximum time range per request based on interval and max data points (1440)
    max_candles_per_request = 1440
    max_time_per_request = get_timedelta_for_interval(interval, max_candles_per_request)

    while current_date < end_date:
        next_date = current_date + max_time_per_request
        if next_date > end_date:
            next_date = end_date

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        # Ensure end_time does not exceed current UTC timestamp
        current_timestamp = int(datetime.now(timezone.utc).timestamp())
        if end_time > current_timestamp:
            end_time = current_timestamp

        if start_time >= end_time:
            # No time range to fetch, break out of the loop
            break

        max_retries = 5
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                data = fetch_klines(symbol, interval, start_time, end_time, max_candles_per_request, url)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except requests.exceptions.HTTPError as e:
                retry_count += 1
                print(f"HTTP error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)
            except ValueError as e:
                retry_count += 1
                print(f"API error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            print(f"Failed to fetch data after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        # Writing to CSV file (appending without duplicates)
        csv_file_path = os.path.join(output_folder, f'{symbol}.csv')

        # Process the response data
        if data:
            # Prepare CSV header if the file doesn't exist
            if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
                with open(csv_file_path, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(
                        ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"])

            final_arr = []
            for entry in data:
                try:
                    open_time_dt = datetime.fromtimestamp(entry[0], tz=timezone.utc)
                    open_time_str = open_time_dt.strftime('%Y-%m-%d %H:%M:%S')
                    open_price = entry[1]
                    close_price = entry[2]
                    high_price = entry[3]
                    low_price = entry[4]
                    volume = entry[5]
                    close_time_str = open_time_str  # No separate close time in response

                    cur_dt = open_time_dt  # Already a datetime object with tzinfo

                    if cur_dt > last_dt:
                        final_arr.append(
                            [open_time_str, open_price, high_price, low_price, close_price, volume, close_time_str])
                        last_dt = cur_dt
                except ValueError as e:
                    print(f"Error processing entry {entry}: {e}")

            with open(csv_file_path, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(final_arr)
        else:
            print(f"No data returned for symbol {symbol} from {current_date} to {next_date}")

        current_date = next_date
        time.sleep(0.1)  # Adjust sleep to comply with rate limits


url = "https://whitebit.com/api/v1/public/kline"

# intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
intervals = ['4h']

for interval in intervals:

    output_folder = f'{interval}_whitebit'
    os.makedirs(output_folder, exist_ok=True)
    symbols_csv_path = 'Symbols/futures/whitebit_symbols.csv'

    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0] for row in symbols_reader]

    timer = Timer()
    timer.start()

    print(f"Starting for interval: {interval}")

    for symbol in symbols:
        print(f"Processing symbol: {symbol}")
        process_data_for_symbol(symbol, interval, output_folder, url)

    timer.end()
