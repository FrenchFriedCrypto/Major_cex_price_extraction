import requests
import csv
import os
from Master_data_extract.global_variables import output_folder  # Ensure this import exists

from http_retry import request_with_retries


def get_symbols(suffix='USDT-SWAP'):
    """
    Fetches symbols from Deepcoin API, filters them based on the specified suffix,
    and writes the filtered symbols to a CSV file.

    :param suffix: The suffix to filter symbols (default is 'USDT-SWAP')
    """
    # API endpoint URL
    url = "https://api.deepcoin.com/deepcoin/market/instruments"

    # Query parameters
    params = {
        'instType': 'SWAP'  # Setting instType to 'SWAP' as per your requirement
        # If additional parameters are required, include them here
        # Example: 'bail': 'SWAP'
    }

    # Headers for the API request (if required)
    headers = {
        # "Authorization": "Bearer YOUR_API_KEY",  # Uncomment and replace with your API key if needed
        "Content-Type": "application/json"
    }

    try:
        # Make the API request with parameters and headers
        response = request_with_retries("GET", url, headers=headers, params=params, timeout=10)
        if response is None:
            return
        print(f"Successfully fetched data from {response.url}")
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
    if isinstance(data, dict):
        # Adjust the key based on the actual response structure
        symbols = data.get('data') or data.get('instruments') or data.get('symbols')

        if not symbols:
            print("No symbols found in the response.")
            return
    else:
        print("Unexpected response format.")
        return

    # Extract 'instId' from each symbol entry
    symbol_list = [
        symbol.get('instId') for symbol in symbols
        if isinstance(symbol, dict) and symbol.get('instId')
    ]

    if not symbol_list:
        print("No 'instId' values found in the response data.")
        return

    # Filter symbols that end with the specified suffix
    filtered_symbols = [inst_id for inst_id in symbol_list if inst_id.endswith(suffix)]

    if not filtered_symbols:
        print(f"No symbols ending with '{suffix}' found.")
        return

    # Ensure the output directory exists
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
            print(f"Created output directory: {output_folder}")
        except OSError as e:
            print(f"Error creating output directory '{output_folder}': {e}")
            return

    # Define the path for the filtered CSV file
    filtered_csv_file_path = os.path.join(output_folder, 'deepcoin_symbols.csv')

    # Write filtered symbols to the CSV file
    try:
        with open(filtered_csv_file_path, 'w', newline='', encoding='utf-8') as filtered_csv_file:
            writer = csv.writer(filtered_csv_file)

            # Writing data
            for inst_id in filtered_symbols:
                writer.writerow([inst_id])

        print(f"Filtered symbols ending with '{suffix}' written to {filtered_csv_file_path}")
    except IOError as e:
        print(f"Error writing to CSV file '{filtered_csv_file_path}': {e}")
        return

    # Optional: Further processing or notifications can be added here


if __name__ == "__main__":
    get_symbols()
