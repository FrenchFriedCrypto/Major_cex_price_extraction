import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl

from http_retry import request_json


class Timer():
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")


def fetch_klines(symbol, granularity, start_time, end_time, url):
    params = {
        'symbol': symbol,
        'granularity': granularity,
        'to': end_time,
    }
    if start_time is not None:
        params['from'] = start_time
    response = request_json("GET", url, params=params, max_retries=5, retry_sleep_seconds=2)
    print(response)
    return response or {"code": "", "data": []}


def process_data_for_symbol(symbol, granularity, output_folder, url):
    # Define start and end dates
    start_date = datetime.strptime("01/01/23", "%d/%m/%y")
    end_date = datetime.now()

    current_date = start_date

    # Convert the datetime object to timestamp in milliseconds
    def convert_to_timestamp(dt):
        return int(dt.timestamp() * 1000) if dt is not None else None

    last_dt = datetime.strptime('2022-12-31 16:00:00', '%Y-%m-%d %H:%M:%S')

    while current_date < end_date:

        next_date = current_date + timedelta(days=28)
        print(f"{next_date} before change")

        if next_date > end_date:
            next_date = end_date


        print(f"{next_date} after change")
        print(f"{current_date} is current_date")

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        max_retries = 5
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = fetch_klines(symbol, granularity, start_time, end_time, url)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except requests.exceptions.HTTPError as e:
                retry_count += 1
                print(f"HTTP error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            print(f"Failed to fetch data after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        # Writing to CSV file (appending without duplicates)
        csv_file_path = os.path.join(output_folder, f'{symbol}.csv')

        if response['code'] == "200000":
            data = response['data']

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
                        open_time_ms = entry[0]
                        open_price = entry[1]
                        high_price = entry[2]
                        low_price = entry[3]
                        close_price = entry[4]
                        volume = entry[5]

                        open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).strftime(
                            '%Y-%m-%d %H:%M:%S')
                        close_time = open_time  # Since there's no separate close time in the response

                        cur_dt = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S')
                        if cur_dt > last_dt:
                            final_arr.append(
                                [open_time, open_price, high_price, low_price, close_price, volume, close_time])
                            last_dt = cur_dt
                    except ValueError as e:
                        print(f"Error processing entry {entry}: {e}")

                with open(csv_file_path, 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(final_arr)
        else:
            print(f"Unexpected response code: {response['code']}")

        current_date = next_date if next_date is not None else end_date
        time.sleep(0.2)


url = "https://api-futures.kucoin.com/api/v1/kline/query"

# Define intervals as tuples of (interval_name, granularity_in_minutes)
intervals = [('Hour4', 240)]

for interval, granularity in intervals:

    output_folder = f'{interval}_kucoin'
    os.makedirs(output_folder, exist_ok=True)

    symbols_csv_path = 'Symbols/futures/kucoin_symbols.csv'

    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0] for row in symbols_reader]

    timer = Timer()
    timer.start()

    print(f"Starting data fetching for interval: {interval}")

    for symbol in symbols:
        print(f"Processing symbol: {symbol}")
        process_data_for_symbol(symbol, granularity, output_folder, url)

    timer.end()
