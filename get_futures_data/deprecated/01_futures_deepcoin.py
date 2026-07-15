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


def fetch_candles(inst_id, bar, after, before, limit, hosts):
    url = f'{hosts}/deepcoin/market/candles'
    params = {
        'instId': inst_id,
        'bar': bar,
        'after': after,
        'before': before,
        'limit': limit
    }
    response = request_json("GET", url, params=params, max_retries=5, retry_sleep_seconds=2)
    time.sleep(0.2)
    return response or {}


def process_data_for_symbol(inst_id, bar, output_folder, hosts):
    # Define the time range for data fetching
    start_date = datetime.strptime("01/01/18", "%d/%m/%y").replace(tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)

    # Convert datetime to timestamp in milliseconds
    def to_timestamp_ms(dt):
        return int(dt.timestamp() * 1000)

    # CSV file path
    csv_file_path = os.path.join(output_folder, f'{inst_id}.csv')

    # Check for existing data to resume
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        # Read the CSV file and find the latest 'Timestamp'
        existing_data = pd.read_csv(csv_file_path)
        existing_data['Timestamp'] = pd.to_datetime(
            existing_data['Timestamp'],
            format='%Y-%m-%d %H:%M:%S UTC',
            utc=True,
            errors='coerce'  # This will convert invalid parsing to NaT
        )
        latest_dt = existing_data['Timestamp'].max()
        if pd.isnull(latest_dt):
            current_date = start_date
            print(f"No valid 'Timestamp' found in existing CSV. Starting data retrieval for {inst_id} from {current_date}")
        else:
            current_date = latest_dt
            print(f"Resuming data retrieval for {inst_id} from {current_date}")
    else:
        # If no existing data, start from start_date
        current_date = start_date
        print(f"Starting data retrieval for {inst_id} from {current_date}")

    # Mapping bars to timedelta increments
    bar_mapping = {
        '1m': timedelta(minutes=300),
        '5m': timedelta(minutes=1500),
        '15m': timedelta(minutes=4500),
        '30m': timedelta(minutes=9000),
        '1H': timedelta(hours=300),
        '4H': timedelta(hours=1200),
        '12H': timedelta(hours=3600),
        '1D': timedelta(days=300),
        '1W': timedelta(weeks=300),
        '1M': timedelta(days=9000),  # Approximation
        '1Y': timedelta(days=360*1)  # Approximate one year
    }

    increment = bar_mapping.get(bar, timedelta(days=1))  # Default increment if bar not found

    total_fetched = 0
    max_retries = 5
    retry_delay = 2  # seconds

    while current_date < end_date:
        next_date = current_date + increment
        after = to_timestamp_ms(current_date)
        before = to_timestamp_ms(next_date)

        limit = 300  # Adjust as per API limits

        retries = 0
        success = False

        while retries < max_retries and not success:
            try:
                response = fetch_candles(inst_id, bar, after, before, limit, hosts)
                time.sleep(0.2)
                success = True
            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"Request error for {inst_id}: {e}. Retrying {retries}/{max_retries} after {retry_delay} seconds...")
                time.sleep(retry_delay)

        if not success:
            print(f"Failed to fetch data for {inst_id} after {max_retries} retries. Moving to next time window.")
            current_date = next_date
            continue

        # Check if the response code indicates success
        if response.get('code') == '0':
            data = response.get('data', [])
            if not data:
                print(f"No data returned for {inst_id} between {current_date} and {next_date}")
                current_date = next_date
                continue

            # Process data in chronological order
            data.sort(key=lambda x: int(x[0]))

            # Prepare data to append
            data_rows = []
            for entry in data:
                try:
                    # Extract and convert data
                    timestamp_ms = int(entry[0])
                    timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    timestamp = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S UTC')

                    open_price = entry[1]
                    high = entry[2]
                    low = entry[3]
                    close = entry[4]
                    volume = entry[5]
                    quote_volume = entry[6]

                    data_rows.append([timestamp, open_price, high, low, close, volume, quote_volume])
                    total_fetched += 1

                except (IndexError, ValueError) as e:
                    print(f"Error processing entry {entry}: {e}")

            # Write data to CSV
            if data_rows:
                # If CSV does not exist, write headers
                if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
                    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(["Timestamp", "Open", "High", "Low", "Close", "Volume", "Quote Asset Volume"])
                        writer.writerows(data_rows)
                else:
                    # Append data and remove duplicates
                    existing_data = pd.read_csv(csv_file_path)
                    new_data_df = pd.DataFrame(data_rows, columns=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Quote Asset Volume"])
                    combined_data = pd.concat([existing_data, new_data_df], ignore_index=True)
                    combined_data.drop_duplicates(subset=['Timestamp'], inplace=True)
                    combined_data.sort_values(by='Timestamp', inplace=True)
                    combined_data.to_csv(csv_file_path, index=False)

            # Respect rate limit
            time.sleep(0.2)  # 0.2 seconds between requests

        else:
            print(f"API response unsuccessful for {inst_id}: {response}")
            current_date = next_date
            continue

        current_date = next_date

    print(f"Total candles fetched for {inst_id}: {total_fetched}")


def main():
    hosts = "https://api.deepcoin.com"

    # Define the bar sizes you want to fetch
    bars = ['1m', '5m', '15m', '30m', '1H', '4H', '12H', '1D', '1W', '1M', '1Y']

    # Read symbols that end with 'USDT-SWAP' from the CSV
    symbols_csv_path = '../Symbols/futures/deepcoin_symbols.csv'
    if not os.path.exists(symbols_csv_path):
        print(f"Symbols CSV file '{symbols_csv_path}' not found.")
        return

    with open(symbols_csv_path, 'r', encoding='utf-8') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        next(symbols_reader, None)  # Skip header
        symbols = [row[0] for row in symbols_reader if row]

    if not symbols:
        print("No symbols found to process.")
        return

    # Choose the bar size you want to process, e.g., '1H'
    interval = '4H'  # Adjust as needed

    output_folder = f'{interval}_deepcoin'
    os.makedirs(output_folder, exist_ok=True)

    timer = Timer()
    timer.start()

    print(f"Starting data fetch for bar interval: {interval}")

    for idx, symbol in enumerate(symbols, 1):
        print(f"Processing {idx}/{len(symbols)}: {symbol}")
        process_data_for_symbol(symbol, interval, output_folder, hosts)

    timer.end()


if __name__ == "__main__":
    main()
