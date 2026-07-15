import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl
import pandas as pd  # Import pandas for data handling

from http_retry import request_json


class Timer:
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")


def fetch_klines(market_pair, period, start_time, end_time, base_url):
    """
    Fetches OHLCV data from BYDFI API.

    :param market_pair: Market pair (e.g., 'LTC-PERPBTC')
    :param period: Candle period (e.g., '1min', '5min', '15min', etc.)
    :param start_time: Start time in milliseconds since epoch
    :param end_time: End time in milliseconds since epoch
    :param base_url: Base URL for the BYDFI API
    :return: List of OHLCV data entries
    """
    url = f"{base_url}"

    params = {
        'market_pair': market_pair,
        'period': period,
        'start_time': start_time,
        'end_time': end_time,
    }

    try:
        response_json = request_json("GET", url, params=params, max_retries=5, retry_sleep_seconds=2)
        if response_json is None:
            return []
        # print(response_json)

        if response_json.get('code') == 200:
            data = response_json.get('data', [])
            return data
        else:
            error_code = response_json.get('code', 'Unknown')
            error_message = response_json.get('message', 'No message provided')
            raise ValueError(f"API Error {error_code}: {error_message}")

    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Error fetching klines for {market_pair}: {e}")
        raise  # Re-raise exception to handle retries in the calling function


def process_data_for_symbol(market_pair, period, output_folder, base_url):
    """
    Processes and saves OHLCV data for a single market pair.

    :param market_pair: Market pair (e.g., 'LTC-PERPBTC')
    :param period: Candle period (e.g., '1min', '5min', '15min', etc.)
    :param output_folder: Directory to save CSV files
    :param base_url: Base URL for the BYDFI API
    """
    # Define start and end dates
    start_date = datetime.strptime("01/01/23", "%d/%m/%y")
    end_date = datetime.now()

    current_date = start_date

    # Convert the datetime object to server time (timestamp in milliseconds)
    def convert_to_timestamp(dt):
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    # CSV file path
    csv_file_path = os.path.join(output_folder, f'{market_pair}.csv')

    all_data = []

    while current_date < end_date:
        next_date = current_date + timedelta(days=1000)
        if next_date > end_date:
            next_date = end_date

        print(f"Fetching data for {market_pair} from {current_date} to {next_date}")
        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        max_retries = 5  # Reduced retries for efficiency
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                data = fetch_klines(market_pair, period, start_time, end_time, base_url)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred for {market_pair}: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except ValueError as e:
                # If it's an API error indicating the instrument doesn't exist, skip
                print(f"Error for {market_pair}: {e}")
                if "Instrument does not exist" in str(e):
                    return  # Skip further processing for this symbol
                retry_count += 1
                print(f"Retrying {retry_count}/{max_retries}...")
                time.sleep(2)
            except Exception as e:
                # Catch-all for any other exceptions
                retry_count += 1
                print(f"Unexpected error for {market_pair}: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            print(f"Failed to fetch data for {market_pair} after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        if data:
            # Sort data by 'openTime' in ascending order
            data.sort(key=lambda x: int(x['openTime']))

            for entry in data:
                try:
                    # BYDFI 'openTime' and 'closeTime' are in milliseconds as strings
                    open_time_ms = int(entry['openTime'])
                    close_time_ms = int(entry['closeTime'])

                    open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
                    close_time = datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc)

                    all_data.append([
                        open_time.strftime('%Y-%m-%d %H:%M:%S'),
                        float(entry['open']),
                        float(entry['high']),
                        float(entry['low']),
                        float(entry['close']),
                        float(entry['vol']),
                        close_time.strftime('%Y-%m-%d %H:%M:%S')
                    ])
                except (ValueError, KeyError) as e:
                    print(f"Error processing entry for {market_pair}: {e}")
        else:
            print(f"No data returned for {market_pair} in the time range {current_date} to {next_date}.")

        current_date = next_date
        time.sleep(0.2)  # To respect API rate limits

    # After collecting all data, write to CSV
    if all_data:
        columns = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]
        new_data_df = pd.DataFrame(all_data, columns=columns)

        # Convert 'Open time' to datetime for proper sorting and deduplication
        new_data_df['Open time'] = pd.to_datetime(new_data_df['Open time'], format='%Y-%m-%d %H:%M:%S')

        if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
            existing_data_df = pd.read_csv(csv_file_path)
            existing_data_df['Open time'] = pd.to_datetime(existing_data_df['Open time'], format='%Y-%m-%d %H:%M:%S')

            combined_data = pd.concat([existing_data_df, new_data_df], ignore_index=True)
            combined_data.drop_duplicates(subset=['Open time'], inplace=True)
            combined_data.sort_values(by='Open time', inplace=True)
            combined_data.to_csv(csv_file_path, index=False)
            print(f"Data for {market_pair} updated in {csv_file_path}")
        else:
            new_data_df.sort_values(by='Open time', inplace=True)
            new_data_df.to_csv(csv_file_path, index=False)
            print(f"Data for {market_pair} saved to {csv_file_path}")
    else:
        print(f"No new data to write for {market_pair}")


# Updated base URL for BYDFI API
base_url = "https://www.bydfi.com/b2b/rank/market/kline"

# Define the periods directly as per BYDFI's request description
# periods = ['1min', '3min', '5min', '15min', '30min', '60min', '1day']
periods = ['15min']  # You can add more periods as needed

# Select the desired periods based on your needs
# Ensure that the periods you choose are supported by BYDFI API
for interval in periods:
    output_folder = f'{interval}_bydfi'
    os.makedirs(output_folder, exist_ok=True)

    symbols_csv_path = 'Symbols/futures/bydfi_symbols.csv'  # Ensure this CSV contains valid 'market_pair' entries
    if not os.path.exists(symbols_csv_path):
        print(f"Symbols CSV file '{symbols_csv_path}' does not exist. Please ensure the file is present.")
        continue

    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0].strip() for row in symbols_reader if row]  # Ensure non-empty rows

    timer = Timer()
    timer.start()

    print(f"Starting for period: {interval}")

    for market_pair in symbols:
        if not market_pair:
            print("Encountered empty market_pair. Skipping.")
            continue
        print(f"Processing market_pair: {market_pair}")
        process_data_for_symbol(market_pair, interval, output_folder, base_url)

    timer.end()
