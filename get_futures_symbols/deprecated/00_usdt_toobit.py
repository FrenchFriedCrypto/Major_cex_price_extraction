import requests
import csv
import os
from Master_data_extract.global_variables import output_folder

from http_retry import request_with_retries

def get_symbols():
    # Updated API base URL and endpoint
    base_url = "https://api.toobit.com"
    endpoint = "/api/v1/exchangeInfo"
    url = f"{base_url}{endpoint}"

    try:
        # Make the API request
        response = request_with_retries("GET", url)
        if response is None:
            return
        print(f"Response Status Code: {response.status_code}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            try:
                # Parse the JSON response
                data = response.json()

                # Debug: Print the response structure (optional)
                # print(data)

                # Adjust the parsing logic based on the API's response structure
                # Assuming the response has a 'symbols' key containing a list of symbol info
                if isinstance(data, dict) and 'symbols' in data and isinstance(data['symbols'], list):
                    symbols_info = data['symbols']

                    # Extract symbol names from the symbols_info
                    symbols = [
                        symbol_info['symbol']
                        for symbol_info in symbols_info
                        if 'symbol' in symbol_info
                    ]

                    if not symbols:
                        print("No symbols found in the response.")
                        return

                    # Filter entries that end with 'USDT'
                    filtered_symbols = [symbol for symbol in symbols if symbol.endswith('USDT')]

                    if not filtered_symbols:
                        print("No symbols ending with 'USDT' found.")
                        return

                    # Define the final CSV file path
                    filtered_csv_file_path = os.path.join(output_folder, 'toobit_symbols.csv')

                    # Ensure the output directory exists
                    os.makedirs(output_folder, exist_ok=True)

                    # Write the filtered symbols directly to the final CSV file
                    with open(filtered_csv_file_path, 'w', newline='', encoding='utf-8') as filtered_csv_file:
                        writer = csv.writer(filtered_csv_file)
                        for symbol in filtered_symbols:
                            writer.writerow([symbol])

                    print(f"Filtered symbols written to {filtered_csv_file_path}")

                else:
                    print("Unexpected response format. Please check the API response structure.")
            except ValueError as e:
                print(f"Error parsing JSON response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"An error occurred while making the API request: {e}")

if __name__ == "__main__":
    get_symbols()
