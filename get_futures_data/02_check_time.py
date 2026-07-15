import pandas as pd
import os
from datetime import datetime, timedelta

from futures_common import get_output_folder

print("Now running check_time_get futures data script", flush=True)

# Define the folder path
folder_path = get_output_folder("4h", "binance", create=False)

# Define the expected time interval (e.g., 4 hours)
expected_interval = timedelta(hours=4)

# Define a tolerance for floating-point discrepancies (optional)
tolerance = timedelta(seconds=1)  # Allow a 1-second difference

# Loop over each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        # print(f'Processing file: {file_path}')
        try:
            # Load the CSV file
            df = pd.read_csv(file_path)

            # Ensure that 'Open time' is parsed as datetime
            df['Open time'] = pd.to_datetime(df['Open time'])

            # Sort the dataframe by 'Open time' to ensure correct order
            df.sort_values(by='Open time', inplace=True)

            # Reset index after sorting
            df.reset_index(drop=True, inplace=True)

            # Check intervals
            for i in range(len(df) - 1):
                current_time = df['Open time'].iloc[i]
                next_time = df['Open time'].iloc[i + 1]

                # Calculate the difference
                time_diff = next_time - current_time

                # Check if the difference is not equal to the expected interval within tolerance
                if abs(time_diff - expected_interval) > tolerance:
                    print(f"In file {filename}, dates not {expected_interval} apart:")
                    print(f"  {current_time} and {next_time}")
                    print(f"  Duration apart: {time_diff}\n")
        except Exception as e:
            print(f"Error processing file {filename}: {e}\n")
