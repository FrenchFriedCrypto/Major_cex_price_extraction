import os
import csv
import time
from datetime import datetime, timezone, timedelta
import requests
import ssl
import pandas as pd  # Import pandas for data manipulation

from http_retry import request_json


class Timer:
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")


def fetch_klines(symbol, resolution, start_time, end_time, base_url):
    """
    Fetches OHLCV data from BTSE API.

    :param symbol: Market symbol (e.g., 'BTCUSDT')
    :param resolution: Resolution as per BTSE API (e.g., '1', '5', '15', etc.)
    :param start_time: Start time in seconds since epoch
    :param end_time: End time in seconds since epoch
    :param base_url: Base URL for the BTSE API
    :return: JSON response from the API
    """
    endpoint = "/api/v2.2/ohlcv"
    url = f"{base_url}{endpoint}"

    params = {
        'symbol': symbol,
        'resolution': resolution,
        'start': start_time,
        'end': end_time
    }

    try:
        response_json = request_json("GET", url, params=params, max_retries=5, retry_sleep_seconds=2)
        if response_json is None:
            return []

        if isinstance(response_json, dict):
            if 'result' in response_json:
                return response_json['result']
            elif 'error' in response_json:
                error_code = response_json['error'].get('code', 'Unknown')
                error_message = response_json['error'].get('message', 'No message provided')
                raise ValueError(f"API Error {error_code}: {error_message}")
            else:
                raise ValueError(f"Unexpected response format: {response_json}")
        elif isinstance(response_json, list):
            # Handle list response
            return response_json
        else:
            raise ValueError(f"Unexpected response format: {response_json}")

    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Error fetching klines for {symbol}: {e}")
        raise  # Re-raise exception to handle retries in the calling function


def process_data_for_symbol(symbol, resolution, output_folder, base_url):
    """
    Processes and saves OHLCV data for a single symbol.

    :param symbol: Market symbol (e.g., 'BTCUSDT')
    :param resolution: Resolution as per BTSE API (e.g., '1', '5', '15', etc.)
    :param output_folder: Directory to save CSV files
    :param base_url: Base URL for the BTSE API
    """
    # Define start and end dates
    start_date = datetime.strptime("01/01/23", "%d/%m/%y").replace(tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)

    current_date = start_date

    # Convert the datetime object to server time (timestamp in seconds)
    def convert_to_timestamp(dt):
        return int(dt.timestamp())

    # Initialize last_dt from existing CSV data if available
    csv_file_path = os.path.join(output_folder, f'{symbol}.csv')
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        existing_data_df = pd.read_csv(csv_file_path)
        if 'Open time' in existing_data_df.columns:
            existing_data_df['Open time'] = pd.to_datetime(
                existing_data_df['Open time'],
                format='%Y-%m-%d %H:%M:%S',
                utc=True
            )
            last_dt = existing_data_df['Open time'].max()
        else:
            last_dt = start_date
    else:
        last_dt = start_date

    all_data = []

    while current_date < end_date:
        next_date = current_date + timedelta(days=10)
        if next_date > end_date:
            next_date = end_date

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        max_retries = 5  # Reduced retries for efficiency
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                data = fetch_klines(symbol, resolution, start_time, end_time, base_url)
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred for {symbol}: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except ValueError as e:
                # If it's an API error indicating the instrument doesn't exist, skip
                print(f"Error for {symbol}: {e}")
                if "Instrument does not exist" in str(e):
                    return  # Skip further processing for this symbol
                retry_count += 1
                print(f"Retrying {retry_count}/{max_retries}...")
                time.sleep(2)
            except Exception as e:
                # Catch-all for any other exceptions
                retry_count += 1
                print(f"Unexpected error for {symbol}: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)

        if not success:
            print(f"Failed to fetch data for {symbol} after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        if data:
            # Sort data in chronological order
            if isinstance(data, list) and data:
                if isinstance(data[0], dict):
                    data.sort(key=lambda x: x['tick'])
                elif isinstance(data[0], list):
                    data.sort(key=lambda x: x[0])

            for entry in data:
                try:
                    if isinstance(entry, dict):
                        # For dictionary entries
                        open_time_ts = entry['tick']
                        open_time = datetime.fromtimestamp(open_time_ts, tz=timezone.utc)

                        close_time = open_time  # Assuming close_time is same as open_time

                        if open_time > last_dt:
                            all_data.append([
                                open_time.strftime('%Y-%m-%d %H:%M:%S'),
                                entry['open'],
                                entry['high'],
                                entry['low'],
                                entry['close'],
                                entry['volume'],
                                close_time.strftime('%Y-%m-%d %H:%M:%S'),
                                entry.get('cost', 0)
                            ])
                    elif isinstance(entry, list):
                        # For list entries
                        timestamp = entry[0]
                        open_price = entry[1]
                        high_price = entry[2]
                        low_price = entry[3]
                        close_price = entry[4]
                        volume = entry[5]
                        cost = 0  # 'cost' field may not be available

                        open_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        close_time = open_time

                        if open_time > last_dt:
                            all_data.append([
                                open_time.strftime('%Y-%m-%d %H:%M:%S'),
                                open_price,
                                high_price,
                                low_price,
                                close_price,
                                volume,
                                close_time.strftime('%Y-%m-%d %H:%M:%S'),
                                cost
                            ])
                    else:
                        print(f"Unknown entry format for {symbol}: {entry}")
                except (ValueError, KeyError, IndexError) as e:
                    print(f"Error processing entry for {symbol}: {e}")
        else:
            print(f"No data returned for {symbol} in the time range {current_date} to {next_date}.")

        current_date = next_date
        time.sleep(0.2)  # To respect API rate limits

    # After collecting all data, write to CSV
    if all_data:
        columns = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume"]
        new_data_df = pd.DataFrame(all_data, columns=columns)
        new_data_df['Open time'] = pd.to_datetime(
            new_data_df['Open time'],
            format='%Y-%m-%d %H:%M:%S',
            utc=True
        )

        if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
            existing_data_df = pd.read_csv(csv_file_path)
            existing_data_df['Open time'] = pd.to_datetime(
                existing_data_df['Open time'],
                format='%Y-%m-%d %H:%M:%S',
                utc=True
            )

            combined_data = pd.concat([existing_data_df, new_data_df], ignore_index=True)
            combined_data.drop_duplicates(subset=['Open time'], inplace=True)
            combined_data.sort_values(by='Open time', inplace=True)
            combined_data.to_csv(csv_file_path, index=False)
            print(f"Data for {symbol} updated in {csv_file_path}")
        else:
            new_data_df.sort_values(by='Open time', inplace=True)
            new_data_df.to_csv(csv_file_path, index=False)
            print(f"Data for {symbol} saved to {csv_file_path}")

        # Update last_dt to the latest 'Open time' in the data
        last_dt = new_data_df['Open time'].max()
    else:
        print(f"No new data to write for {symbol}")


# Updated base URL for BTSE API
base_url = "https://api.btse.com/futures"

# Mapping of your existing intervals to BTSE's resolution values

# intervals = ['1', '5', '15', '30', '60', '240', '360', '1440', '10080', '43200']
intervals = ['240']


for interval in intervals:
    resolution = interval
    if not resolution:
        print(f"Unsupported interval: {interval}. Skipping.")
        continue

    output_folder = f'{interval}_btse'
    os.makedirs(output_folder, exist_ok=True)

    symbols_csv_path = 'Symbols/futures/btse_symbols.csv'
    if not os.path.exists(symbols_csv_path):
        print(f"Symbols CSV file '{symbols_csv_path}' does not exist. Please ensure the file is present.")
        continue

    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0].strip() for row in symbols_reader if row]  # Ensure non-empty rows

    timer = Timer()
    timer.start()

    print(f"Starting for interval: {interval} (Resolution: {resolution})")

    for symbol in symbols:
        if not symbol:
            print("Encountered empty symbol. Skipping.")
            continue
        print(f"Processing symbol: {symbol}")
        process_data_for_symbol(symbol, resolution, output_folder, base_url)

    timer.end()
