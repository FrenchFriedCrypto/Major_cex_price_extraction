import requests
import csv
import os
from Master_data_extract.global_variables import output_folder  # Ensure this import is correct

from http_retry import request_with_retries

def get_symbols():
    """
    Fetches futures symbols from the XT API, filters those ending with 'USDT' (case-insensitive),
    and writes them directly to a CSV file.
    """
    # New API endpoint URL
    base_url = "https://fapi.xt.com"
    endpoint = "/future/market/v3/public/symbol/list"
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

                # Optional: Print the entire JSON response for debugging
                # print("JSON Response:", data)

                # Validate the response structure
                if (
                    isinstance(data, dict) and
                    data.get('returnCode') == 0 and
                    'result' in data and
                    'symbols' in data['result'] and
                    isinstance(data['result']['symbols'], list)
                ):
                    symbols = data['result']['symbols']
                else:
                    print("Unexpected response format. Please check the JSON structure.")
                    return

                # Extract symbol names as is
                extracted_symbols = []
                for item in symbols:
                    if isinstance(item, dict) and 'symbol' in item:
                        symbol = item['symbol']  # Keep the original symbol format
                        extracted_symbols.append(symbol)
                    elif isinstance(item, str):
                        extracted_symbols.append(item)
                    else:
                        print(f"Unexpected symbol format: {item}")

                if not extracted_symbols:
                    print("No symbols found in the response.")
                    return

                # Filter entries that end with 'usdt' (case-insensitive)
                filtered_symbols = [symbol for symbol in extracted_symbols if symbol.lower().endswith('usdt')]

                if not filtered_symbols:
                    print("No USDT symbols found.")
                    return

                # Define the final CSV file path
                filtered_csv_file_path = os.path.join(output_folder, 'xt_symbols.csv')

                # Ensure the output directory exists
                os.makedirs(output_folder, exist_ok=True)

                # Write the filtered symbols directly to the final CSV file
                with open(filtered_csv_file_path, 'w', newline='', encoding='utf-8') as filtered_csv_file:
                    writer = csv.writer(filtered_csv_file)
                    for symbol in filtered_symbols:
                        writer.writerow([symbol])

                print(f"Filtered USDT symbols written to '{filtered_csv_file_path}'")

            except ValueError as e:
                print(f"Error parsing JSON response: {e}")
            except KeyError as e:
                print(f"Missing expected data in response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
    except OSError as e:
        print(f"File system error: {e}")

if __name__ == "__main__":
    get_symbols()
