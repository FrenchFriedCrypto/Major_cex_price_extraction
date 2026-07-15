import requests
import csv
import os
from datetime import datetime
from Master_data_extract.global_variables import output_folder

from http_retry import request_with_retries


def get_symbols(suffix='USDTM'):
    """
    Fetches symbols from KuCoin Futures API, filters them based on the specified suffix,
    and writes the filtered symbols to a CSV file.

    :param suffix: The suffix to filter symbols (default is 'USDTM')
    """
    # API endpoint URL
    url = "https://api-futures.kucoin.com/api/v1/contracts/active"

    try:
        # Make the API request with a timeout for better control
        response = request_with_retries("GET", url, timeout=10)
        if response is None:
            return
        print(f"Successfully fetched data from {url}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return

    # Parse the JSON response
    try:
        data = response.json()
    except ValueError as e:
        print(f"Error parsing JSON response: {e}")
        return

    # Check if the response contains the expected data
    if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list) and data['data']:
        contracts = data['data']
        print(f"Number of contracts fetched: {len(contracts)}")
    else:
        print("Unexpected response format.")
        print(f"Response Content: {data}")
        return

    # Extract 'symbol' from each contract
    symbols = [contract.get('symbol') for contract in contracts if 'symbol' in contract]

    if not symbols:
        print("No 'symbol' found in the response data.")
        return

    print(f"Number of symbols extracted: {len(symbols)}")

    # Filter symbols that end with the specified suffix
    filtered_symbols = [symbol for symbol in symbols if symbol.endswith(suffix)]

    if not filtered_symbols:
        print(f"No symbols ending with '{suffix}' found.")
        return

    print(f"Number of symbols after filtering with suffix '{suffix}': {len(filtered_symbols)}")

    # Ensure the output directory exists
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
            print(f"Created output directory: {output_folder}")
        except OSError as e:
            print(f"Error creating output directory '{output_folder}': {e}")
            return

    # Define the path for the filtered CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file_name = f'kucoin_symbols.csv'  # Adding timestamp to prevent overwriting
    csv_file_path = os.path.join(output_folder, csv_file_name)

    # Write filtered symbols to the CSV file
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as filtered_csv_file:
            writer = csv.writer(filtered_csv_file)

            # Writing data
            for symbol in filtered_symbols:
                writer.writerow([symbol])

        print(f"Filtered symbols written to '{csv_file_path}'.")
    except IOError as e:
        print(f"Error writing to CSV file '{csv_file_path}': {e}")
        return

    # Optional: Further processing or notifications can be added here


if __name__ == "__main__":
    # Example usage:
    # To fetch all symbols without filtering:
    # get_symbols()

    # To fetch only symbols ending with 'USDTM':
    get_symbols(suffix='USDTM')
