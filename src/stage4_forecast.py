# src/stage4_forecast.py
import os
import sys
import json
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime

# Ensure repo root on sys.path (so `from src import zen_rules` works in CI/CD)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import zen_rules


# ---------------------------
# Fetch helpers
# ---------------------------

def get_spy_close():
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1d", interval="1d")
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"Error fetching SPY: {e}")
        return 0.0


def get_es_close():
    try:
        es = yf.download("ES=F", period="1d", interval="1d")
        return float(es["Close"].iloc[-1])
    except Exception as e:
        print(f"Error fetching ES: {e}")
        return 0.0


def get_vix_close():
    try:
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1d", interval="1d")
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"Error fetching VIX: {e}")
        return 0.0


def get_vvix_close():
    try:
        vvix_df = pd.read_csv("data/VVIX_History.csv")
        if "CLOSE" in vvix_df.columns:
            return float(vvix_df["CLOSE"].iloc[-1])
        elif "VVIX" in vvix_df.columns:
            return float(vvix_df["VVIX"].iloc[-1])
        else:
            print(f"⚠️ Unexpected VVIX schema: {vvix_df.columns.tolist()}")
            return 0.0
    except Exception as e:
        print(f"Error fetching VVIX: {e}")
        return 0.0


# ---------------------------
# Main Forecast Build
# ---------------------------

spy_price = get_spy_close()
es_price = get_es_close()
vix_value = get_vix_close()
vvix_value = get_vvix_close()

# Fail fast only if SPY or VIX are missing (critical inputs)
if spy_price == 0.0 or vix_value == 0.0:
    raise ValueError("Critical fetch failed (SPY or VIX missing). Aborting.")

if es_price == 0.0:
    print("⚠️ Warning: ES fetch failed, continuing with SPY only.")
if vvix_value == 0.0:
    print("⚠️ Warning: VVIX fetch failed, continuing without VVIX.")

# Placeholder RSI + candles until full pipeline connected
rsi_value = 55.0
last_candles = []

# Zen Rules analysis
straddle_status = zen_rules.straddle_zone(spy_price, spy_price)
rsi_status = zen_rules.rsi_check(rsi_value)
candle_status = zen_rules.candle_structure(last_candles)
vol_status = zen_rules.volatility_overlay(vix_value, vvix_value, 0.0, 0.0)
event_status = zen_rules.event_filter([])
headline_status = zen_rules.headline_overlay("No headline")

zen_bias = zen_rules.combine_bias(
    straddle_status, rsi_status, candle_status,
    vol_status, event_status, headline_status
)

# Key Levels
support_level = round(spy_price - 15, 2)
resistance_level = round(spy_price + 15, 2)

# Probable Path
if "Bullish" in zen_bias:
    base_case = f"Hold above {support_level}, targeting {resistance_level}."
    bull_case = f"Break >{resistance_level} opens {resistance_level+20}."
    bear_case = f"Only if <{support_level}, risk toward {support_level-20}."
elif "Bearish" in zen_bias:
    base_case = f"Struggle below {resistance_level}, leaning lower."
    bear_case = f"Break <{support_level} opens {support_level-20}."
    bull_case = f"Only if >{resistance_level}, relief toward {resistance_level+20}."
else:
    base_case = f"Chop between {support_level}-{resistance_level}."
    bear_case = f"If <{support_level}, watch {support_level-20}."
    bull_case = f"If >{resistance_level}, opens {resistance_level+20}."

# Trade Implications (mock)
if "Bullish" in zen_bias:
    spread_text = f"Bull Put Credit Spread: Sell {support_level} / Buy {support_level-20} (0DTE)"
elif "Bearish" in zen_bias:
    spread_text = f"Bear Call Credit Spread: Sell {resistance_level} / Buy {resistance_level+20} (0DTE)"
else:
    spread_text = "Neutral Zone – consider Iron Condor."

# ---------------------------
# Write forecast.json
# ---------------------------

forecast = {
    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "spy": spy_price,
    "es": es_price,
    "vix": vix_value,
    "vvix": vvix_value,
    "rsi": rsi_value,
    "bias": zen_bias,
    "support": support_level,
    "resistance": resistance_level,
    "base_case": base_case,
    "bull_case": bull_case,
    "bear_case": bear_case,
    "spread": spread_text,
    "headline": {"title": "No headline available", "link": ""}
}

os.makedirs("out", exist_ok=True)
with open("out/forecast.json", "w", encoding="utf-8") as f:
    json.dump(forecast, f, indent=2)

print("✅ Forecast written to out/forecast.json")
