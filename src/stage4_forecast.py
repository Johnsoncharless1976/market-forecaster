import os
import sys
import json
from datetime import datetime

# --- Ensure repo root on sys.path ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import zen_rules




# ensure out/ folder exists
os.makedirs("out", exist_ok=True)

# --- Fetch live prices using Yahoo Finance ---
def get_price(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d", interval="1m")
        return round(float(data["Close"].iloc[-1]), 2)
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return 0.0

# --- RSI Calculation ---
def compute_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi.iloc[-1], 2)

# --- Auto-update VVIX from CBOE ---
import os

def get_vvix():
    try:
        url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv"
        r = requests.get(url, timeout=10)
        r.raise_for_status()

        # ensure data/ folder exists
        os.makedirs("data", exist_ok=True)

        with open("data/VVIX_History.csv", "wb") as f:
            f.write(r.content)

        df = pd.read_csv("data/VVIX_History.csv")
        df.columns = [c.strip().upper() for c in df.columns]  # normalize headers

        if "VVIX" not in df.columns:
            raise ValueError(f"Unexpected VVIX CSV schema: {df.columns.tolist()}")

        df["DATE"] = pd.to_datetime(df["DATE"])
        df = df.sort_values("DATE")
        return float(df["VVIX"].iloc[-1])
    except Exception as e:
        print(f"Error fetching VVIX: {e}")
        return 0.0



# --- Pull SPY history for RSI ---
spy_ticker = yf.Ticker("SPY")
spy_hist = spy_ticker.history(period="5d", interval="2m")  # 2-min candles
spy_price = round(float(spy_hist["Close"].iloc[-1]), 2)

es_price = get_price("ES=F")
vix_value = get_price("^VIX")
vvix_value = get_vvix()

# --- RSI on SPY ---
rsi_value = compute_rsi(spy_hist["Close"], period=14)

# --- Placeholder headline until API wired ---
headline = {
    "title": "Markets steady ahead of Powell speech",
    "link": "https://www.reuters.com/markets/"
}

# --- Fail fast if critical values missing ---
if spy_price == 0.0 or es_price == 0.0 or vix_value == 0.0 or vvix_value == 0.0:
    raise ValueError("Live data fetch failed (SPY/ES/VIX/VVIX returned 0.0)")

# --- Build forecast dict ---
forecast_data = {
    "spy": spy_price,
    "es": es_price,
    "vix": vix_value,
    "vvix": vvix_value,
    "rsi": rsi_value,
    "headline": headline,
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

# --- Save forecast to JSON + TXT ---
with open("out/forecast.json", "w", encoding="utf-8") as f:
    json.dump(forecast_data, f, indent=2)

with open("out/forecast.txt", "w", encoding="utf-8") as f:
    f.write(f"SPY: {spy_price}\n")
    f.write(f"ES: {es_price}\n")
    f.write(f"VIX: {vix_value}\n")
    f.write(f"VVIX: {vvix_value}\n")
    f.write(f"RSI: {rsi_value}\n")
    f.write(f"Bias: Neutral\n")  # placeholder bias until Zen rules integrate
    f.write(f"Headline: {headline['title']}\n")
