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
        r.raise_for_status()  # Raise exception for bad HTTP status codes
        data = r.json()
        
        # Safely navigate the JSON structure
        if "chart" in data and data["chart"]["result"]:
            result = data["chart"]["result"][0]
            if "meta" in result and "regularMarketPrice" in result["meta"]:
                price = result["meta"]["regularMarketPrice"]
                if price is not None:
                    return float(price)
        
        print("[WARN] VVIX data structure unexpected - using fallback")
        return None
        
    except requests.exceptions.Timeout:
        print("[WARN] VVIX fetch timed out after 10 seconds")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[WARN] VVIX network error: {e}")
        return None
    except (ValueError, TypeError, KeyError) as e:
        print(f"[WARN] VVIX data parsing failed: {e}")
        return None
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
        print("[FATAL] Missing core market data (SPX, ES, VIX). No forecast generated.")
        return
    
    if vvix is None:
        print("[INFO] VVIX unavailable - forecast will proceed without VVIX data")

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
