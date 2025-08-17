import requests

API_KEY = "jYeR6QVhnmhFe7V0aQm1_ZuGM6QawAEO"
url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?apiKey={API_KEY}"

resp = requests.get(url)
print("Status:", resp.status_code)
print(resp.json())
