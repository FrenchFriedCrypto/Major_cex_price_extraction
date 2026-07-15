import os
import csv
import time
from datetime import datetime, timezone, timedelta
import mexc_spot_v3
import ssl
import requests

class Timer():
    def start(self):
        self.start_time = time.time()
        print(f"Starting now: {time.strftime('%H:%M')}")

    def end(self):
        self.end_time = time.time()
        print("")
        print(f"Total runtime: {round(self.end_time - self.start_time, 4):,.2f} seconds")

def process_data_for_symbol(data, symbol, interval, output_folder, symbols_csv_path):
    # Define start and end dates
    start_date = datetime.strptime("01/01/23", "%d/%m/%y")
    end_date = datetime.now()

    current_date = start_date

    # Convert the datetime object to server time (timestamp in milliseconds)
    def convert_to_timestamp(dt):
        return int(time.mktime(dt.timetuple()) * 1000)

    last_dt = datetime.strptime('2022-12-31 16:00:00', '%Y-%m-%d %H:%M:%S')

    while current_date < end_date:
        next_date = current_date + timedelta(days=3)

        start_time = convert_to_timestamp(current_date)
        end_time = convert_to_timestamp(next_date)

        # print(f"Fetching data from {current_date} to {next_date}")

        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': 1000,
            'startTime': start_time,
            'endTime': end_time
        }

        max_retries = 999
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = data.get_kline(params)
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
        csv_file_path = os.path.join(output_folder, f'{symbol}.csv')
        existing_timestamps = set()

        if len(response) > 0:
            if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
                pass
            else:
                with open(csv_file_path, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume"])

            final_arr = list()
            for row in response:
                try:
                    # Convert "Open time" (index 0) and "Close time" (index 6) timestamps
                    row[0] = datetime.fromtimestamp(float(row[0]) / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    row[6] = datetime.fromtimestamp(float(row[6]) / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    cur_dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                    if cur_dt > last_dt:
                        final_arr.append(row)
                        last_dt = cur_dt
                except ValueError:
                    print(f"Error converting timestamp in row: {row}")

            with open(csv_file_path, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(final_arr)

            # print(f"Data appended to {csv_file_path}")

        # else:
        #     print(f"No data available from {current_date} to {next_date}")

        current_date = next_date
        time.sleep(0.2)


hosts = "https://api.mexc.com"
# mexc_key = "YOUR_MEXC_API_KEY"
# mexc_secret = "YOUR_MEXC_API_SECRET"

interval = '60m'
output_folder = 'm_USDT' + interval + '_04' #01 to compare with 02, 03 to optimise the code, 04 further optimise
os.makedirs(output_folder, exist_ok=True)

data = mexc_spot_v3.mexc_market(mexc_hosts=hosts)
#
# Read symbols from CSV file
# symbols_csv_path = 'symbol1m.csv'
symbols_csv_path = 'symbol2m.csv'
# symbols_csv_path = 'symbol3m.csv'
# symbols_csv_path = 'symbol4m.csv'
# symbols_csv_path = 'symbol5m.csv'
# symbols_csv_path = 'symbol6m.csv'
# symbols_csv_path = 'symbol7m.csv'
# symbols_csv_path = 'symbol8m.csv'
# symbols_csv_path = 'symbol9m.csv'
# symbols_csv_path = 'symbol10m.csv'
with open(symbols_csv_path, 'r') as symbols_csv:
    symbols_reader = csv.reader(symbols_csv)
    symbols = [row[0] for row in symbols_reader]

# symbol = "BTCUSDT"

timer = Timer()
timer.start()

for symbol in symbols:
    process_data_for_symbol(data, symbol, interval, output_folder, symbol)

timer.end()
