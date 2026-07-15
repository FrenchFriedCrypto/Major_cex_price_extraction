import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl
import logging

from http_retry import request_json

# Configure logging
# logging.basicConfig(
#     filename='coincatcher.log',
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )


class Timer():
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")
        logging.info(f"Timer started at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    def end(self):
        self.end_time = time.time()
        elapsed = round(self.end_time - self.start_time, 4)
        print("")
        print(f"Total runtime: {elapsed:,.2f} seconds")
        logging.info(f"Timer ended. Total runtime: {elapsed:,.2f} seconds")


def fetch_klines(symbol, interval, start_time, end_time, base_url, headers=None):
    """
    Fetch candlestick data from Coincatch API.
    """
    url = base_url
    params = {
        'symbol': symbol,
        'granularity': interval,
        'startTime': start_time,
        'endTime': end_time
    }
    try:
        json_response = request_json("GET", url, params=params, headers=headers, max_retries=5, retry_sleep_seconds=2)
        if json_response is None:
            return []

        # Debugging: Print the response (can be commented out in production)
        logging.debug(f"API Response for {symbol} [{interval}]: {json_response}")

        return json_response
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for {symbol} [{interval}]: {e}")
        raise


def get_last_date_from_csv(csv_file_path):
    """
    Get the last date (open time) from the existing CSV file.
    """
    last_date = None
    if os.path.exists(csv_file_path):
        with open(csv_file_path, 'r', newline='') as csv_file:
            reader = csv.reader(csv_file)
            next(reader, None)  # Skip the header
            rows = list(reader)
            if rows:
                last_row = rows[-1]
                last_date_str = last_row[0]  # Assuming the first column is the open time
                try:
                    # Parse the date and make it timezone-aware (UTC)
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    logging.info(f"Last date from CSV ({csv_file_path}): {last_date}")
                except ValueError:
                    logging.error(f"Error parsing date from CSV: {last_date_str}")
    else:
        logging.info(f"CSV file does not exist: {csv_file_path}")
    return last_date


def process_data_for_symbol(symbol, interval, output_folder, base_url, days_gap):
    """
    Fetch and process candlestick data for a given symbol and interval.
    """
    # Define CSV file path
    csv_file_path = os.path.join(output_folder, f'{symbol}.csv')
    last_date = get_last_date_from_csv(csv_file_path)

    if last_date:
        # Start from the next second after the last date
        start_date = last_date + timedelta(seconds=1)
        logging.info(f"Resuming from last date for {symbol}: {start_date}")
        print(f"Resuming from last date for {symbol}: {start_date}")
    else:
        # Start from a default date if no CSV exists
        start_date = datetime.strptime("31/12/22", "%d/%m/%y").replace(tzinfo=timezone.utc)
        logging.info(f"No existing CSV. Starting from {start_date} for {symbol}")
        print(f"No existing CSV. Starting from {start_date} for {symbol}")

    # Set end_date as current UTC time
    end_date = datetime.now(timezone.utc)
    current_date = start_date

    # Convert the datetime object to server time (UNIX timestamp in milliseconds)
    def convert_to_timestamp(dt):
        return int(dt.timestamp() * 1000)  # Ensure dt is timezone-aware

    last_dt = last_date if last_date else start_date

    while current_date < end_date:
        # Calculate the next date based on days_gap
        next_date = current_date + timedelta(days=days_gap)

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        max_retries = 5  # Reduced retries for practicality
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = fetch_klines(symbol, interval, start_time, end_time, base_url)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                logging.warning(f"SSL error for {symbol} [{interval}]: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except requests.exceptions.HTTPError as http_err:
                retry_count += 1
                logging.warning(
                    f"HTTP error for {symbol} [{interval}]: {http_err}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)
            except Exception as e:
                retry_count += 1
                logging.warning(
                    f"General error for {symbol} [{interval}]: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            logging.error(
                f"Failed to fetch data after {max_retries} attempts for {symbol} [{interval}]. Skipping this time period.")
            current_date = next_date
            continue

        # Writing to CSV file (appending without duplicates)
        data = response  # Assuming the response is a list

        if isinstance(data, list):
            if data:
                final_arr = []
                for entry in data:
                    try:
                        # Each entry is a list: [timestamp, open, high, low, close, volume, amount]
                        if not isinstance(entry, list) or len(entry) < 7:
                            logging.warning(f"Unexpected entry format: {entry}. Skipping.")
                            continue

                        timestamp = int(entry[0])
                        open_price = entry[1]
                        high_price = entry[2]
                        low_price = entry[3]
                        close_price = entry[4]
                        volume = entry[5]
                        amount = entry[6]

                        # Convert timestamp to datetime string (UTC)
                        open_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).strftime(
                            '%Y-%m-%d %H:%M:%S')
                        close_time = open_time  # Modify if there's a separate close time

                        # Parse open_time into a timezone-aware datetime object
                        cur_dt = datetime.strptime(open_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                        if cur_dt > last_dt:
                            final_arr.append([
                                open_time, open_price, high_price, low_price, close_price, volume, close_time, amount
                            ])
                            last_dt = cur_dt
                    except (ValueError, TypeError) as e:
                        logging.error(f"Error processing entry: {entry}. Error: {e}")

                if final_arr:
                    # Write header if file doesn't exist
                    if not (os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0):
                        with open(csv_file_path, 'w', newline='') as csv_file:
                            writer = csv.writer(csv_file)
                            writer.writerow([
                                "Open time", "Open", "High", "Low", "Close", "Volume", "Close time",
                                "Quote asset volume"
                            ])

                    with open(csv_file_path, 'a', newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerows(final_arr)
                    logging.info(f"Appended {len(final_arr)} records for {symbol} [{interval}].")
                else:
                    logging.info(f"No new data to append for {symbol} [{interval}].")
        else:
            logging.warning(f"Unexpected response format for {symbol} [{interval}]: {data}")

        # Move to the next date regardless of whether data was returned
        current_date = next_date
        time.sleep(0.2)  # Respect API rate limits


# Hosts URL for the new Coincatch API
base_url = "https://api.coincatch.com/api/mix/v1/market/candles"

# Define your intervals (ensure they match Coincatch's supported intervals)
# For testing, we're using only "4H". Uncomment and adjust as needed.
intervals = ["4H"]
# intervals = [
#     "1m", "3m", "5m", "15m", "30m",
#     "1H", "2H", "4H", "6H", "12H",
#     "1D", "3D", "1W", "1M",
#     "6Hutc", "12Hutc", "1Dutc", "3Dutc", "1Wutc", "1Mutc"
# ]

# Mapping intervals to gap values based on their time units
days_gap_mapping = {
    '1m': 1,  # 1 minute
    '3m': 3,  # 3 minutes
    '5m': 5,  # 5 minutes
    '15m': 15,  # 15 minutes
    '30m': 30,  # 30 minutes
    '1H': 60,  # 1 hour = 60 minutes
    '2H': 120,  # 2 hours = 120 minutes
    '4H': 240,  # 4 hours = 240 minutes
    '6H': 360,  # 6 hours = 360 minutes
    '12H': 720,  # 12 hours = 720 minutes
    '1D': 1440,  # 1 day = 1440 minutes
    '3D': 4320,  # 3 days = 4320 minutes
    '1W': 10080,  # 1 week = 10080 minutes
    '1M': 43200,  # 1 month ≈ 30 days = 43200 minutes
    '6Hutc': 360,  # UTC0 6 hours = 360 minutes
    '12Hutc': 720,  # UTC0 12 hours = 720 minutes
    '1Dutc': 1440,  # UTC0 1 day = 1440 minutes
    '3Dutc': 4320,  # UTC0 3 days = 4320 minutes
    '1Wutc': 10080,  # UTC0 1 week = 10080 minutes
    '1Mutc': 43200  # UTC0 1 month ≈ 30 days = 43200 minutes
}

# Loop through intervals and process symbols
for interval in intervals:
    output_folder = f'{interval}_coincatch'
    os.makedirs(output_folder, exist_ok=True)

    symbols_csv_path = 'Symbols/futures/coincatch_usdt_umcbl_symbols.csv'
    if not os.path.exists(symbols_csv_path):
        logging.error(f"Symbols file not found: {symbols_csv_path}")
        print(f"Symbols file not found: {symbols_csv_path}")
        continue

    with open(symbols_csv_path, 'r', newline='') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0].strip() for row in symbols_reader if row]

    if not symbols:
        logging.warning(f"No symbols found in {symbols_csv_path}")
        print(f"No symbols found in {symbols_csv_path}")
        continue

    timer = Timer()
    timer.start()

    logging.info(f"Starting data fetch for interval: {interval}")
    print(f"Starting data fetch for interval: {interval}")

    # Get the days_gap value for the current interval
    days_gap = days_gap_mapping.get(interval, 1)  # Default to 1 if not found

    for symbol in symbols:
        logging.info(f"Processing symbol: {symbol}")
        print(f"Processing symbol: {symbol}")
        try:
            process_data_for_symbol(symbol, interval, output_folder, base_url, days_gap)
        except Exception as e:
            logging.error(f"Unhandled exception for {symbol} [{interval}]: {e}")
            print(f"Unhandled exception for {symbol} [{interval}]: {e}")

    timer.end()
