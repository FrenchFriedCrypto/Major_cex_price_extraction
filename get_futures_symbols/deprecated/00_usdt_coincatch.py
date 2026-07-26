import requests
import csv
import os
from Master_data_extract.global_variables import output_folder  # Ensure this import exists

from http_retry import request_with_retries


def get_symbols(suffix='USDT_UMCBL'):
    """
    Fetches symbols from Coincatch API, filters them based on the specified suffix,
    and writes the filtered symbols to a CSV file.

    :param suffix: The suffix to filter symbols (default is 'USDT_UMCBL')
    """
    # API endpoint URL
    url = "https://api.coincatch.com/api/mix/v1/market/contracts"

    # Query parameters
    params = {
        "productType": "umcbl"  # Adding the productType parameter
    }

    # Headers for the API request
    headers = {
        # "Authorization": "Bearer YOUR_API_KEY",  # Uncomment and replace with your API key if needed
        "Content-Type": "application/json"
    }

    try:
        # Make the API request with parameters and headers
        response = request_with_retries("GET", url, headers=headers, params=params)
        if response is None:
            return
        print(f"Request URL: {response.url}")
        print(f"Response Status Code: {response.status_code}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            try:
                # Parse the JSON response
                data = response.json()

                # Optional: Pretty-print the JSON response for debugging
                # import json
                # print(json.dumps(data, indent=4))

                # Check if the response indicates success
                if data.get("code") != "00000":
                    print(f"API returned error: {data.get('msg', 'No message provided')}")
                    return

                # Extract the 'data' list from the response
                contracts = data.get("data", [])

                if not isinstance(contracts, list) or not contracts:
                    print("No contract data found in the response.")
                    return

                # Extract 'symbol' from each contract
                symbol_list = [contract.get("symbol") for contract in contracts if "symbol" in contract]

                if not symbol_list:
                    print("No symbols found in the response data.")
                    return

                # Filter symbols that end with the specified suffix
                filtered_symbols = [symbol for symbol in symbol_list if symbol.endswith(suffix)]

                if not filtered_symbols:
                    print(f"No symbols ending with '{suffix}' found.")
                    return

                # Ensure the output directory exists
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)
                    print(f"Created output directory: {output_folder}")

                # Define the path for the filtered CSV file
                filtered_csv_file_path = os.path.join(output_folder, 'coincatch_usdt_umcbl_symbols.csv')

                # Write filtered symbols to the CSV file
                with open(filtered_csv_file_path, 'w', newline='', encoding='utf-8') as filtered_csv_file:
                    writer = csv.writer(filtered_csv_file)

                    # Writing data
                    for symbol in filtered_symbols:
                        writer.writerow([symbol])

                print(f"Filtered symbols ending with '{suffix}' written to {filtered_csv_file_path}")

            except ValueError as e:
                print(f"Error parsing JSON response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            # Optional: Print response content for debugging
            try:
                print("Response Content:", response.text)
            except Exception:
                pass
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the API request: {e}")


if __name__ == "__main__":
    get_symbols()
