import requests
import csv
import os
from Master_data_extract.global_variables import output_folder  # Ensure this import is correct

from http_retry import request_with_retries

def get_symbols():
    """
    Fetches futures symbols from the WhiteBIT API, filters those with money currency 'USDT',
    and writes them directly to a CSV file.
    """
    # API endpoint URL
    base_url = "https://whitebit.com"
    endpoint = "/api/v4/public/futures"
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

                # Validate the response structure
                if isinstance(data, dict) and data.get('success') and 'result' in data and isinstance(data['result'], list):
                    markets = data['result']

                    # Extract symbols and their money currencies
                    symbols = [market['ticker_id'] for market in markets if 'ticker_id' in market]
                    money_currencies = [market.get('money_currency', '') for market in markets]

                    if not symbols:
                        print("No symbols found in the response.")
                        return

                    # Filter symbols where money currency is 'USDT'
                    filtered_symbols = [
                        symbol for symbol, money_currency in zip(symbols, money_currencies)
                        if money_currency == 'USDT'
                    ]

                    if not filtered_symbols:
                        print("No symbols with 'USDT' as money currency were found.")
                        return

                    # Define the final CSV file path
                    filtered_csv_file_path = os.path.join(output_folder, 'whitebit_symbols.csv')

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
            except KeyError as e:
                print(f"Missing expected data in response: {e}")
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"An error occurred while making the API request: {e}")
    except OSError as e:
        print(f"File system error: {e}")

if __name__ == "__main__":
    get_symbols()
