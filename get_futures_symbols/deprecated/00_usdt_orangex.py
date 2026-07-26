import requests
import csv
import os
from Master_data_extract.global_variables import output_folder

from http_retry import request_with_retries

def get_symbols():
    # API endpoint URL
    url = "https://api.orangex.com/api/v1/public/get_instruments"

    # Set currency to 'PERPETUAL' as per your request
    currencies = ['PERPETUAL']  # The trading areas as per API documentation

    all_symbols = []

    try:
        for currency in currencies:
            # Parameters for the API request
            params = {'currency': currency}

            # Make the API request
            response = request_with_retries("GET", url, params=params)
            if response is None:
                continue
            print(f"Request URL: {response.url}")
            print(f"API Response Status for {currency}: {response.status_code}")

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                try:
                    # Parse the JSON response
                    data = response.json()

                    # Check if 'result' key is present and is a list
                    if 'result' in data and isinstance(data['result'], list):
                        instruments = data['result']

                        # Extract symbols from instruments
                        symbols = [
                            instrument['instrument_name']
                            for instrument in instruments
                            if 'instrument_name' in instrument
                        ]

                        all_symbols.extend(symbols)
                        print(f"Retrieved {len(symbols)} symbols for currency {currency}.")
                    else:
                        print(f"No data found for currency {currency}.")
                except ValueError as e:
                    print(f"Error parsing JSON response for currency {currency}: {e}")
            else:
                print(f"Failed to retrieve data for currency {currency}. Status code: {response.status_code}")

        if all_symbols:
            # Remove duplicates
            unique_symbols = list(set(all_symbols))
            print(f"Total unique symbols retrieved: {len(unique_symbols)}")

            # Filter symbols that end with 'PERPETUAL'
            filtered_symbols = [symbol for symbol in unique_symbols if symbol.endswith('PERPETUAL')]

            if not filtered_symbols:
                print("No symbols ending with 'PERPETUAL' were found.")
                return

            # Define the final CSV file path
            filtered_csv_file_path = os.path.join(output_folder, 'orangex_symbols.csv')

            # Ensure the output directory exists
            os.makedirs(output_folder, exist_ok=True)

            # Write the filtered symbols directly to the final CSV file
            with open(filtered_csv_file_path, 'w', newline='', encoding='utf-8') as filtered_csv_file:
                writer = csv.writer(filtered_csv_file)
                for symbol in filtered_symbols:
                    writer.writerow([symbol])

            print(f"Filtered data written to {filtered_csv_file_path}")

        else:
            print("No symbols retrieved.")

    except requests.RequestException as req_err:
        print(f"Request error: {req_err}")
    except OSError as os_err:
        print(f"File system error: {os_err}")

if __name__ == "__main__":
    get_symbols()
