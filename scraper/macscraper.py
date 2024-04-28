import socket
import json
from datetime import datetime, timedelta
import requests 

class CryptoScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.historical_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/historical'

    def fetch_data(self, symbol, start_date, end_date):
        parameters = {
            'symbol': symbol,
            'convert': 'USD',
            'time_start': start_date,
            'time_end': end_date
        }
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }

        response = requests.get(self.historical_url, headers=headers, params=parameters)

        # It's good practice to check the status code before processing the JSON
        if response.status_code != 200:
            # Better error handling: provide more context about the failure
            error_message = response.json().get('status', {}).get('error_message', 'Unknown error')
            raise Exception(f"API Error: {error_message} - Status Code: {response.status_code}")

        # Safely access the 'data' key
        try:
            data = response.json()['data']
        except KeyError:
            # Handle the case where 'data' key is missing
            raise KeyError("The 'data' key is missing from the JSON response.")

        return data


def create_test_data():
    # Creating a simpler JSON object with minimal data for testing
    test_data = {
        "quotes": [
            {
                "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "quote": {
                    "USD": {
                        "percent_change_1h": 0.1,
                        "price": 50000
                    }
                }
            }
        ]
    }
    return test_data

def scrape_data():
    api_key = '9254faed-95a3-4cf0-801f-4a45c497d1b4'
    scraper = CryptoScraper(api_key)

    symbol = 'BTC'  # Example: Bitcoin
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=.01)  

    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    data = scraper.fetch_data(symbol, start_date_str, end_date_str)

    with open('crypto_data.json', 'w') as f:
        json.dump(data, f, indent=4)  

    return data

def connect_to_server(server_ip, port=65433):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((server_ip, port))
            print("Connected to server.")

            # Use the test data instead of scraping
            data = scrape_data()
            serialized_data = json.dumps(data).encode('utf-8')
            sock.sendall(serialized_data)
            print(data, "Data sent to server.")

            response = sock.recv(1024)
            print("Received response:", response.decode())
    except socket.error as e:
        print(f"Could not connect to server {server_ip} on port {port}: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    SERVER_IP = '10.0.0.224' 
    connect_to_server(SERVER_IP)

# def connect_to_server(server_ip, port=65433):
#     try:
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
#             sock.connect((server_ip, port))
#             print("Connected to server.")

#             data = scrape_data()
#             serialized_data = json.dumps(data).encode('utf-8')
#             sock.sendall(serialized_data)
#             print(data, "Data sent to server.")

#             response = sock.recv(1024)
#             print("Received response:", response.decode())
#     except socket.error as e:
#         print(f"Could not connect to server {server_ip} on port {port}: {e}")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     SERVER_IP = '10.0.0.224' 
#     connect_to_server(SERVER_IP)
