# src/stage4_forecast.py
import os
import json
import yfinance as yf
import pandas as pd
import requests

# Ensure output folder exists
os.makedirs("out", exist_ok=True)

def fetch_spy():
    spy = yf.Ticker("SPY")
    data = spy.history(period="1d")
    return float(data["Close"].iloc[-1]) if not data.empty else None

def fetch_es():
    es = yf.download("ES=F", period="1d", interval="1d")
    return float(es["Close"].iloc[-1]) if not es.empty else None

def fetch_vix():
    vix = yf.Ticker("^VIX")
    data = vix.history(period="1d")
    return float(data["Close"].iloc[-1]) if not data.empty else None

def fetch_vvix():
    try:
        vvix_df = pd.read_csv("data/VVIX_History.csv")
        return float(vvix_df["VVIX"].iloc[-1])
    except Exception as e:
        print(f"⚠️ Warning: VVIX fetch failed ({e}), using None.")
        return None

if __name__ == "__main__":
    spy_val = fetch_spy()
    es_val = fetch_es()
    vix_val = fetch_vix()
    vvix_val = fetch_vvix()

    forecast = {
        "SPX": spy_val,
        "ES": es_val,
        "VIX": vix_val,
        "VVIX": vvix_val
    }

    with open("out/forecast.json", "w", encoding="utf-8") as f:
        json.dump(forecast, f, indent=2)

    print("✅ Forecast written to out/forecast.json")
