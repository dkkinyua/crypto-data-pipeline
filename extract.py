import os 
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('CRYPTO_API_KEY')
crypto_data = []

# Get's crypto data from the API
def get_data():
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin&names=Bitcoin&symbols=btc&x_cg_demo_api_key={API_KEY}"

    headers = {
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)

    """
    I need the symbol, name, current_price, high_24, low_24, price_change_24h, last_updated
    We'll retrieve this data through data[0] as the dictionary holding the info is the first item in the list (index 0)

    """
    if response.status_code == 200:
        data = response.json()
        name = data[0]['name']
        symbol = data[0]['symbol']
        current_price = data[0]['current_price']
        high_24h = data[0]['high_24h']
        low_24h = data[0]['low_24h']
        price_change_24h = data[0]['price_change_24h']
        last_updated = data[0]['last_updated']

        crypto_dict = {
            'name': name,
            'symbol': symbol,
            'current_price': current_price,
            'high_24h': high_24h,
            'low_24h': low_24h,
            'price_change_24h': price_change_24h,
            'last_updated': last_updated
        }

        crypto_data.append(crypto_dict)
        print(crypto_data)
    else:
        print('Error loading data')

def load_data(data):
    with open('data.json', 'w') as f:
        json.dump(data, f, indent=4)

    print('Data dumped successfully.')


if __name__ == '__main__':
    get_data()
    load_data(crypto_data)