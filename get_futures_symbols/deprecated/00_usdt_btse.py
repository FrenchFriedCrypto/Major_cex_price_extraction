import requests
import csv
import os
from Master_data_extract.global_variables import output_folder

from http_retry import request_with_retries

def get_symbols():
    # BTSE API endpoint URL
    url = "https://api.btse.com/futures/api/v2.2/market_summary"

    try:
        # Make the API request
        response = request_with_retries("GET", url)
        if response is None:
            return
        print(f"Request URL: {url}")
        print(f"Status Code: {response.status_code}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            try:
                # Parse the JSON response
                data = response.json()

                # Check if data is a list (assuming BTSE returns a list of market summaries)
                if isinstance(data, list) and data:
                    symbols = []

                    # Extract symbols from each market summary
                    for market in data:
                        # Assuming each market summary has a 'symbol' key
                        symbol = market.get('symbol')
                        if symbol:
                            symbols.append(symbol)

                    # Check if any symbols were extracted
                    if not symbols:
                        print("No symbols found in the response.")
                        return

                    # Filter entries that end in 'PERP'
                    filtered_symbols = [symbol for symbol in symbols if symbol.endswith('PERP')]

                    if not filtered_symbols:
                        print("No symbols ending with 'PERP' found.")
                        return

                    # Writing filtered data to a new CSV file
                    filtered_csv_file_path = os.path.join(output_folder, 'btse_symbols.csv')
                    with open(filtered_csv_file_path, 'w', newline='') as filtered_csv_file:
                        writer = csv.writer(filtered_csv_file)
                        for symbol in filtered_symbols:
                            writer.writerow([symbol])

                    print(f"Filtered symbols ending with 'PERP' written to {filtered_csv_file_path}")

                else:
                    print("Unexpected response format: Expected a list of market summaries.")
            except ValueError as e:
                print(f"Error parsing JSON response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")


if __name__ == "__main__":
    get_symbols()
