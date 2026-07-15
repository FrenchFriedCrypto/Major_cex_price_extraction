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


def fetch_klines(symbol, interval, start_time, end_time, hosts):
    url = f'{hosts}'
    params = {
        'contract_code': symbol,
        'period': interval,
        'from': start_time,
        'to': end_time
    }
    response = request_json("GET", url, params=params, max_retries=999, retry_sleep_seconds=2)
    return response or {}


def get_last_date_from_csv(csv_file_path):
    """
    Get the last date (open time) from the existing CSV file.
    """
    last_date = None
    if os.path.exists(csv_file_path):
        with open(csv_file_path, 'r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)  # Skip the header
            rows = list(reader)
            if rows:
                last_row = rows[-1]
                last_date = last_row[0]  # Assuming the first column is the open time
                last_date = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
    return last_date


def process_data_for_symbol(symbol, interval, output_folder, hosts, days_gap):
    # Define start date as either from the CSV file or from "31/12/22"
    csv_file_path = os.path.join(output_folder, f'{symbol}.csv')
    last_date = get_last_date_from_csv(csv_file_path)

    if last_date:
        start_date = last_date + timedelta(seconds=1)  # Start from the next second after the last date
        print(f"Resuming from last date for {symbol}: {start_date}")
    else:
        start_date = datetime.strptime("31/12/22", "%d/%m/%y")

    end_date = datetime.now()

    current_date = start_date

    # Convert the datetime object to server time (timestamp in seconds)
    def convert_to_timestamp(dt):
        return int(time.mktime(dt.timetuple()))

    last_dt = start_date

    while current_date < end_date:
        next_date = current_date + timedelta(days=days_gap)
        if interval == '1min':
            next_date = current_date + timedelta(minutes=days_gap)

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        max_retries = 999
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = fetch_klines(symbol, interval, start_time, end_time, hosts)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying

        if not success:
            print(f"Failed to fetch data after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        # Writing to CSV file (appending without duplicates)
        if response.get('status') == 'ok':
            data = response['data']

            # Ensure there is data returned
            if data and len(data) > 1:
                # Exclude the last bar by slicing the data up to the second last element
                data = data[:-1]

                # Prepare to write the CSV header if the file doesn't exist
                csv_file_path = os.path.join(output_folder, f'{symbol}.csv')
                if not (os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0):
                    with open(csv_file_path, 'w', newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(
                            ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume"])

                final_arr = []
                for item in data:
                    try:
                        open_time = datetime.fromtimestamp(float(item['id']), tz=timezone.utc).strftime(
                            '%Y-%m-%d %H:%M:%S')
                        close_time = open_time  # Since there's no separate close time in the response
                        cur_dt = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S')
                        if cur_dt > last_dt:
                            final_arr.append(
                                [open_time, item['open'], item['high'], item['low'], item['close'], item['vol'],
                                 close_time, item['amount']])
                            last_dt = cur_dt
                    except ValueError:
                        print(f"Error converting timestamp in row: {item['id']}")

                with open(csv_file_path, 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(final_arr)
            else:
                print(f"Not enough data points returned for {symbol} at interval {interval}")
        else:
            print(f"Error fetching data: {response.get('err_msg', 'Unknown error')}")

        current_date = next_date
        time.sleep(0.2)


# Hosts URL for the API
hosts = "https://api.hbdm.com/linear-swap-ex/market/history/kline"

# intervals = ["1min", "5min", "15min", "30min", "60min", "1hour", "4hour", "1day", "1mon"]
intervals = ["4hour"]

# Mapping intervals to days_gap values
days_gap_mapping = {
    '1min': 1000,
    '5min': 3,
    '15min': 10,
    '30min': 20,
    '60min': 40,
    '1hour': 40,
    '4hour': 160,
    'Hour8': 320,
    '1day': 1000,
    '1mon': 3000
}

# Loop through intervals and process symbols
for interval in intervals:
    output_folder = f'{interval}_htx'
    os.makedirs(output_folder, exist_ok=True)

    symbols_csv_path = 'Symbols/futures/htx_symbols.csv'
    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0] for row in symbols_reader]

    timer = Timer()
    timer.start()

    print("Starting for interval: " + interval)

    # Get the days_gap value for the current interval
    days_gap = days_gap_mapping[interval]

    for symbol in symbols:
        # Pass the days_gap to your processing function
        process_data_for_symbol(symbol, interval, output_folder, hosts, days_gap)

    timer.end()
