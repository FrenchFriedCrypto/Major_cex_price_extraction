import requests
import csv
import os
from Master_data_extract.global_variables import output_folder  # Optional: Use if you have an output folder defined

from http_retry import request_with_retries

def get_ticker_ids(suffix='USDT'):
    """
    Fetches ticker IDs from BYDFI API, filters them based on the specified suffix,
    and writes the filtered ticker IDs to a CSV file.

    :param suffix: The suffix to filter ticker IDs (default is 'USDT')
    """
    # API endpoint URL for BYDFI
    url = "https://www.bydfi.com/b2b/rank/contracts"

    try:
        # Make the API request
        response = request_with_retries("GET", url)
        if response is None:
            return
        print(f"Request URL: {response.url}")
        print(f"Status Code: {response.status_code}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            try:
                # Parse the JSON response
                data = response.json()

                # Extract 'ticker_id's based on the response structure
                ticker_ids = []

                if isinstance(data, list):
                    # Assuming each item in the list is a contract with a 'ticker_id'
                    ticker_ids = [item.get('ticker_id') for item in data if 'ticker_id' in item]
                elif isinstance(data, dict):
                    # If data is a dictionary, adjust according to actual structure
                    # For example, data might have a key like 'contracts' which is a list
                    for key, value in data.items():
                        if isinstance(value, list):
                            for item in value:
                                if 'ticker_id' in item:
                                    ticker_ids.append(item['ticker_id'])
                else:
                    print("Unexpected response structure.")
                    return

                if not ticker_ids:
                    print("No ticker_id values found in the response.")
                    return

                # Filter 'ticker_id's that end with the specified suffix
                filtered_ticker_ids = [tid for tid in ticker_ids if tid.endswith(suffix)]

                if not filtered_ticker_ids:
                    print(f"No ticker_ids ending with '{suffix}' found.")
                    return

                # Ensure the output directory exists (if using output_folder)
                if 'output_folder' in globals() and output_folder:
                    if not os.path.exists(output_folder):
                        os.makedirs(output_folder)
                        print(f"Created output directory: {output_folder}")

                    # Define the path for the filtered CSV file
                    filtered_csv_file_path = os.path.join(output_folder, 'bydfi_usdt_symbols.csv')
                else:
                    # If no output_folder is defined, save in the current directory
                    filtered_csv_file_path = 'bydfi_symbols.csv'

                # Write filtered ticker_ids to the CSV file
                with open(filtered_csv_file_path, 'w', newline='') as filtered_csv_file:
                    writer = csv.writer(filtered_csv_file)
                    writer.writerows([[ticker_id] for ticker_id in filtered_ticker_ids])

                print(f"Filtered ticker_ids ending with '{suffix}' written to {filtered_csv_file_path}")

            except ValueError as e:
                print(f"Error parsing JSON response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")

if __name__ == "__main__":
    get_ticker_ids()
