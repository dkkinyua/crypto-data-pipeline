import os 
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('CRYPTO_API_KEY')

url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin&names=Bitcoin&symbols=btc&x_cg_demo_api_key={API_KEY}"

headers = {
    "accept": "application/json"
}

response = requests.get(url, headers=headers)
if response.status_code == 200:
    print(response.json())
else:
    print('Error loading data')