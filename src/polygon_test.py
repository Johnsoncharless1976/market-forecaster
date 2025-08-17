import requests
from datetime import date, timedelta

API_KEY = "jYeR6QVhnmhFe7V0aQm1_ZuGM6QawAEO"
base_url = "https://api.polygon.io/v2/aggs/ticker"

# Symbols to test
symbols = ["SPY", "VIX", "VVIX"]

# Yesterday’s date
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

for symbol in symbols:
    url = f"{base_url}/{symbol}/range/1/day/{yesterday}/{yesterday}?adjusted=true&sort=asc&limit=1&apiKey={API_KEY}"
    print(f"Requesting {symbol}...")
    resp = requests.get(url)
    print(f"Status {resp.status_code}")
    try:
        data = resp.json()
        print(data)
    except Exception as e:
        print("Error parsing JSON:", e)
    print("-" * 50)
