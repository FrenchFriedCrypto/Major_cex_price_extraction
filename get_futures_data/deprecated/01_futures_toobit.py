import requests
import csv
import time
from datetime import datetime, timezone, timedelta
import ssl
import os
import pandas as pd

from http_retry import request_json

def process_data_for_symbol(host_url, symbol, interval, output_folder):
    global data
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    # Set end_date as timezone-aware datetime object
    end_date = datetime.now(timezone.utc)

    # Convert the datetime object to server time (timestamp in milliseconds)
    def convert_to_timestamp(dt):
        return int(dt.timestamp() * 1000)  # datetime.timestamp() returns seconds since epoch

    # CSV file path
    csv_file_path = os.path.join(output_folder, f'{symbol}.csv')

    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        # Read the CSV file and find the latest 'Open time'
        existing_data = pd.read_csv(csv_file_path)

        # Parse 'Open time' as timezone-aware datetime objects
        existing_data['Open time'] = pd.to_datetime(
            existing_data['Open time'],
            format='%Y-%m-%d %H:%M:%S',
            utc=True
        )

        last_dt = existing_data['Open time'].max()
        # last_dt is now timezone-aware

        # Set current_date to last_dt
        current_date = last_dt
        print(f"Resuming {symbol} from {current_date}")
    else:
        # If no existing data, start from initial date
        current_date = datetime.strptime("01/01/18", "%d/%m/%y").replace(tzinfo=timezone.utc)
        last_dt = datetime.strptime('2018-01-01 16:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        print(f"Starting {symbol} from {current_date}")

    while current_date < end_date:
        if interval == '1m':
            next_date = current_date + timedelta(minutes=days_gap)
            # print(f"minutes_gap activated {days_gap}")
        else:
            next_date = current_date + timedelta(days=days_gap)
            # print(f"days_gap activated {days_gap}")

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        params = f'symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit=1000'

        # **Added Print Statement to Display the Request URL**
        request_url = f"{host_url}?{params}"
        # print(f"Requesting URL: {request_url}")

        max_retries = 10
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                data = request_json(
                    "GET",
                    host_url + "?" + params,
                    headers=headers,
                    max_retries=max_retries,
                    retry_sleep_seconds=1,
                )
                if data is None:
                    break


                # **Modification Starts Here**
                # Exclude the last data point
                if len(data) > 1:
                    data = data[:-1]
                else:
                    # If only one data point is returned, skip this batch
                    # print(f"Not enough data returned for {symbol} from {current_date} to {next_date}")
                    break
                # **Modification Ends Here**
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error occurred: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)  # wait a bit before retrying
            except Exception as e:
                retry_count += 1
                print(f"Error fetching data for {symbol}: {e}. Retrying {retry_count}/{max_retries}...")
                time.sleep(2)
        # Why is this still failing
        if not success:
            print(f"Failed to fetch data after {max_retries} attempts. Skipping this time period.")
            current_date = next_date
            continue

        # Check if data is returned
        if len(data) > 0:
            # If CSV file doesn't exist, write headers
            if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
                with open(csv_file_path, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(["Open time", "Open", "High", "Low", "Close", "Volume", "Close time",
                                     "Quote asset volume", "# trades", "Taker Buy Base", "Taker Buy Quote"])

            final_arr = list()
            for row in data:
                try:
                    # Convert Unix timestamp (index 0)
                    open_time = datetime.fromtimestamp(float(row[0]/1000), tz=timezone.utc)
                    row[0] = open_time.strftime('%Y-%m-%d %H:%M:%S')
                    close_time = datetime.fromtimestamp(float(row[6]/1000), tz=timezone.utc)
                    row[6] = close_time.strftime('%Y-%m-%d %H:%M:%S')

                    # Ensure open_time is timezone-aware (it should be already)
                    if open_time.tzinfo is None:
                        open_time = open_time.replace(tzinfo=timezone.utc)

                    if open_time > last_dt:
                        final_arr.append(row)
                    else:
                        # Skip data that is already in CSV
                        continue
                except ValueError:
                    print(f"Error converting timestamp in row: {row}")

            if final_arr:
                with open(csv_file_path, 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(final_arr)

                # Update last_dt to the last 'Open time' in final_arr
                last_open_time_str = final_arr[-1][0]  # get the last 'Open time' as string
                last_dt = datetime.strptime(last_open_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            else:
                print(f"No new data to append for {symbol} from {current_date} to {next_date}")
        else:
            print(f"No data returned for {symbol} from {current_date} to {next_date}")

        current_date = next_date
        time.sleep(0.2)


def read_and_clean_csv(filepath):
    # Read the CSV file without setting any column as index
    data = pd.read_csv(filepath, index_col=False)

    # Drop unnamed columns
    data = data.loc[:, ~data.columns.str.contains('^Unnamed')]

    return data


def process_csv_files(folder_path):
    # Iterate through all files in the specified folder
    for filename in os.listdir(folder_path):
        # Check if the file is a CSV file
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {file_path}")

            # Read and clean the CSV file
            data = read_and_clean_csv(file_path)

            # Save the modified DataFrame back to CSV
            data.to_csv(file_path, index=False)
            print(f"Updated file: {file_path}")


# prefix = "/fapi/v1/exchangeInfo"
host = "https://api.toobit.com"
# host = "https://data-api.binance.vision"
prefix = "/quote/v1/klines"
host_url = host + prefix

# intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
# intervals = ['1M', '1w', '3d', '1d', '12h', '8h', '6h', '4h', '2h', '1h', '30m', '15m', '5m', '3m', '1m']
intervals = ['4h']
# Mapping intervals to days_gap values
days_gap_mapping = {
    '1m': 1000,
    '3m': 2,
    '5m': 3,
    '15m': 10,
    '30m': 20,
    '1h': 40,
    '2h': 80,
    '4h': 160,
    '6h': 240,
    '8h': 330,
    '12h': 450,
    '1d': 1000,
    '3d': 3000,
    '1w': 3000,
    '1M': 3000
}

for interval in intervals:
    output_folder = f'{interval}_toobit'
    if interval == '1m':
        output_folder = 'b_USDT' + interval + 'minute_04'

    os.makedirs(output_folder, exist_ok=True)

    # Set the days_gap according to the interval
    days_gap = days_gap_mapping.get(interval)  # Default to 1000 if interval is not in the dictionary
    symbols_csv_path = 'Symbols/futures/toobit_symbols.csv'

    with open(symbols_csv_path, 'r') as symbols_csv:
        symbols_reader = csv.reader(symbols_csv)
        symbols = [row[0] for row in symbols_reader]

    print("starting for interval: " + interval)

    for symbol in symbols:
        try:
            process_data_for_symbol(host_url, symbol, interval, output_folder)
        except Exception as e:
            print(f"Error processing symbol {symbol} for interval {interval}: {e}")
            continue  # This will skip to the next symbol in case of error

    # try:
    #     process_csv_files(output_folder)
    # except Exception as e:
    #     print(f"Error processing CSV files in folder {output_folder}: {e}")
    #     # You could add additional handling here if necessary
