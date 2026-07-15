import requests
import csv
import os
from datetime import datetime
from Master_data_extract.global_variables import output_folder  # Ensure this import exists

from http_retry import request_with_retries


def get_contract_codes(filter_suffix=None):
    """
    Fetches contract codes from HTX API, optionally filters them based on a suffix,
    and writes the filtered codes to a CSV file.

    :param filter_suffix: (Optional) Suffix to filter contract codes. If None, no filtering is applied.
    """
    # API endpoint URL
    url = "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info"

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

    # Check if the response status is 'ok' and contains 'data'
    if data.get('status') == 'ok' and 'data' in data and isinstance(data['data'], list):
        contracts = data['data']
        print(f"Number of contracts fetched: {len(contracts)}")
    else:
        print("Unexpected response format or status not 'ok'.")
        print(f"Response Content: {data}")
        return

    # Extract 'contract_code' from each contract
    contract_codes = [contract.get('contract_code') for contract in contracts if 'contract_code' in contract]

    if not contract_codes:
        print("No 'contract_code' found in the response data.")
        return

    print(f"Number of contract codes extracted: {len(contract_codes)}")

    # Optional: Filter contract codes based on the specified suffix
    if filter_suffix:
        filtered_contract_codes = [code for code in contract_codes if code.endswith(filter_suffix)]
        print(f"Number of contract codes after filtering with suffix '{filter_suffix}': {len(filtered_contract_codes)}")
        if not filtered_contract_codes:
            print(f"No contract codes ending with '{filter_suffix}' found.")
            return
    else:
        filtered_contract_codes = contract_codes
        print("No filtering applied to contract codes.")

    # Ensure the output directory exists
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
            print(f"Created output directory: {output_folder}")
        except OSError as e:
            print(f"Error creating output directory '{output_folder}': {e}")
            return

    # Define the CSV file path with timestamp to prevent overwriting
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file_name = f'htx_symbols.csv'
    csv_file_path = os.path.join(output_folder, csv_file_name)

    # Write the filtered contract codes to the CSV file
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)

            # Writing data
            for code in filtered_contract_codes:
                writer.writerow([code])

        print(f"Filtered contract codes written to '{csv_file_path}'.")
    except IOError as e:
        print(f"Error writing to CSV file '{csv_file_path}': {e}")
        return

    # Optional: Further processing or notifications can be added here


if __name__ == "__main__":
    # Example usage:
    # To fetch all contract codes without filtering:
    get_contract_codes()

    # To fetch only contract codes ending with 'USDT':
    # get_contract_codes(filter_suffix='USDT')
