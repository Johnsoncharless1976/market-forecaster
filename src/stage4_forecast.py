# src/stage4_forecast.py
import os
import json
import yfinance as yf
import pandas as pd
from datetime import datetime
from zen_rules import generate_forecast   # Zen forecast logic

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
    except Exception:
        return None

if __name__ == "__main__":
    spy_val = fetch_spy()
    es_val = fetch_es()
    vix_val = fetch_vix()
    vvix_val = fetch_vvix()

    forecast_data = {
        "SPX": spy_val,
        "ES": es_val,
        "VIX": vix_val,
        "VVIX": vvix_val
    }

    # Save raw JSON
    with open("out/forecast.json", "w", encoding="utf-8") as f:
        json.dump(forecast_data, f, indent=2)

    # Build Zen Forecast body
    zen_text = generate_forecast(forecast_data)

    # Timestamp
    now = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")

    # Assemble final email
    email_body = (
        "📌 ZeroDay Zen Forecast \n"  # 📌 thumbtack header
        f"📈 ZeroDay Zen Forecast – {now}\n"
        "Sent automatically by Zen Market AI\n\n"
        f"{zen_text}\n\n"
        "✅ Summary\n"
        f"Bias: {zen_text.split('🧠 Bias')[1].splitlines()[1].strip()}. "
        f"Watch {zen_text.split('Support: ')[1].splitlines()[0]}-"
        f"{zen_text.split('Resistance: ')[1].splitlines()[0]} zone and volatility cues.\n\n"
        "End of forecast"
    )

    with open("forecast_output.txt", "w", encoding="utf-8") as f:
        f.write(email_body)

    print("✅ Forecast written to out/forecast.json and forecast_output.txt")

