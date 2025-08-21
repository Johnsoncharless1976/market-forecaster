# src/stage4_forecast.py

import datetime
from zen_rules import generate_forecast
from send_email import send_email

# 🔹 Data sources
import yfinance as yf
import requests


def fetch_spx():
    try:
        spx = yf.Ticker("^GSPC").history(period="1d")["Close"].iloc[-1]
        return float(spx)
    except Exception as e:
        print(f"[ERROR] Failed to fetch SPX: {e}")
        return None


def fetch_es():
    try:
        es = yf.Ticker("ES=F").history(period="1d")["Close"].iloc[-1]
        return float(es)
    except Exception as e:
        print(f"[ERROR] Failed to fetch ES: {e}")
        return None


def fetch_vix():
    try:
        vix = yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1]
        return float(vix)
    except Exception as e:
        print(f"[ERROR] Failed to fetch VIX: {e}")
        return None


def fetch_vvix():
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/^VVIX"
        r = requests.get(url, timeout=10)
        data = r.json()
        return float(data["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception as e:
        print(f"[WARN] VVIX fetch failed: {e}")
        return None


def main():
    # 🔹 Pull market data
    spx = fetch_spx()
    es = fetch_es()
    vix = fetch_vix()
    vvix = fetch_vvix()

    if spx is None or es is None or vix is None:
        print("[FATAL] Missing core market data. No forecast generated.")
        return

    # 🔹 Generate Zen forecast
    zen_text = generate_forecast(spx=spx, es=es, vix=vix, vvix=vvix)

    # 🔹 Timestamp
    now = datetime.datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")
    subject = f"📌 ZeroDay Zen Forecast – {now}"

    # 🔹 Send email
    send_email(subject, zen_text)
    print("[SUCCESS] Forecast email sent.")


if __name__ == "__main__":
    main()
