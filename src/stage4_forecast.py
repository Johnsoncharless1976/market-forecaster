# stage4_forecast.py

import snowflake.connector
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os

# --- Snowflake connection ---
conn = snowflake.connector.connect(
    user="JOHNSONCHARLESS",
    password="s7AfXRG7krgnh3H",
    account="GFXGPHR-HXC94041",
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)

# --- Queries ---
query_changes = "SELECT * FROM MARKET_CHANGES ORDER BY date DESC LIMIT 1;"
query_hist = """
SELECT date, spy_close, es_close, vix_close, vvix_close
FROM MARKET_MASTER
ORDER BY date DESC
LIMIT 60
"""

# --- Load Data ---
df_changes = pd.read_sql(query_changes, conn)
df_hist = pd.read_sql(query_hist, conn)
conn.close()

df_changes.columns = df_changes.columns.str.lower()
df_hist.columns = df_hist.columns.str.lower()
df_hist["date"] = pd.to_datetime(df_hist["date"])
df_hist.set_index("date", inplace=True)
df_hist = df_hist.sort_index()  # oldest → newest

# --- Rolling correlations (last 30 days) ---
corr_df = pd.DataFrame({
    "spy_es_corr": df_hist["spy_close"].rolling(30).corr(df_hist["es_close"]),
    "spy_vix_corr": df_hist["spy_close"].rolling(30).corr(df_hist["vix_close"]),
    "vix_vvix_corr": df_hist["vix_close"].rolling(30).corr(df_hist["vvix_close"])
})
latest_corr = corr_df.dropna().iloc[-1]

# --- Daily pct moves ---
spy_pct = float(df_changes["spy_pct"].iloc[0])
es_pct  = float(df_changes["es_pct"].iloc[0])
vix_pct = float(df_changes["vix_pct"].iloc[0])
vvix_pct = float(df_changes["vvix_pct"].iloc[0])

# --- Technicals: RSI (14) ---
def compute_rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.rolling(period).mean()
    ma_down = down.rolling(period).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

df_hist["rsi"] = compute_rsi(df_hist["spy_close"])
latest_rsi = df_hist["rsi"].iloc[-1]

# --- Support/Resistance (last 20 days) ---
lookback = df_hist.tail(20)
support = round(lookback["spy_close"].min(), 2)
resistance = round(lookback["spy_close"].max(), 2)

# ATR bands
df_hist["hl"] = df_hist["spy_close"].diff().abs()  # crude since no OHLC
atr = df_hist["hl"].rolling(14).mean().iloc[-1]
last_close = df_hist["spy_close"].iloc[-1]
atr_support = round(last_close - atr, 2)
atr_resistance = round(last_close + atr, 2)

# --- Forecast Bias Logic ---
if spy_pct < -0.5 and latest_corr.spy_vix_corr < -0.6 and latest_rsi < 40:
    bias = "Bearish"
elif spy_pct > 0.5 and latest_corr.spy_vix_corr > -0.3 and latest_rsi > 60:
    bias = "Bullish"
else:
    bias = "Neutral"

# --- Volatility Context ---
vix_level = df_hist["vix_close"].iloc[-1]
vvix_level = df_hist["vvix_close"].iloc[-1]
vol_comment = []
if vix_level > 20:
    vol_comment.append("Risk-off regime (VIX > 20)")
elif vix_level < 15:
    vol_comment.append("Calm regime (VIX < 15)")
if vvix_pct > vix_pct:
    vol_comment.append("VVIX rising faster → vol pressure building")

vol_context = "; ".join(vol_comment) if vol_comment else "Stable vol conditions"

# --- Headline (RSS stub) ---
def get_headline():
    try:
        rss_url = "https://www.reuters.com/rssFeed/markets"
        resp = requests.get(rss_url, timeout=5)
        root = ET.fromstring(resp.content)
        item = root.find(".//item")
        if item is not None:
            return item.find("title").text, item.find("link").text
    except Exception:
        pass
    return ("U.S. CPI hotter than expected; rate cut odds repriced lower",
            "https://www.reuters.com/markets/us-cpi-aug2025")

headline, headline_link = get_headline()

# --- Forecast Output ---
today = datetime.now().strftime("%A, %b %d, %Y @ %I:%M %p ET")

summary_text = f"""
=== ZeroDay Zen SPY Forecast Update – {today} ===

📰 Headline of the Day:
{headline}
Link: {headline_link}

🧠 Bias: {bias} 
(SPY {spy_pct:+.2f}%, ES {es_pct:+.2f}%, VIX {vix_pct:+.2f}%, VVIX {vvix_pct:+.2f}%, RSI {latest_rsi:.1f})

Technical Structure Overview (SPY | ES | VIX | VVIX)
SPY: {last_close:.2f} | ES: {df_hist['es_close'].iloc[-1]:.2f} | VIX: {vix_level:.2f} | VVIX: {vvix_level:.2f}

Key Support / Resistance Zones (SPY)
- Resistance: {resistance} / ATR-band {atr_resistance}
- Support: {support} / ATR-band {atr_support}

Volatility Outlook
SPY–ES Corr: {latest_corr.spy_es_corr:.3f} | SPY–VIX Corr: {latest_corr.spy_vix_corr:.3f} | VIX–VVIX Corr: {latest_corr.vix_vvix_corr:.3f}
Interpretation: {vol_context}

Macro & Event Context
Econ reports, FedWatch, headlines, and whispers not yet integrated (Stage 7)

Probable Paths (next 3–5 hours)
Base Case – Placeholder
Upside – Placeholder
Downside – Placeholder

Potential 0DTE Spread Context (Educational Only)
Placeholder spreads until Zen Grid levels are wired in

📌 Forecast Summary
Bias = {bias}, SPY daily % = {spy_pct:+.2f}, VIX daily % = {vix_pct:+.2f}, RSI = {latest_rsi:.1f}
"""

print(summary_text)

# --- Save Outputs ---
import os, json

os.makedirs("out", exist_ok=True)

forecast_json = {
    "date": str(datetime.now().date()),
    "bias": bias,
    "spy_close": float(last_close),
    "support": [support, atr_support],
    "resistance": [resistance, atr_resistance],
    "rsi": round(float(latest_rsi), 2),
    "volatility": {
        "vix": float(vix_level),
        "vvix": float(vvix_level),
        "context": vol_context
    },
    "headline": {"title": headline, "link": headline_link}
}

with open("out/forecast.json", "w", encoding="utf-8") as jf:
    json.dump(forecast_json, jf, indent=2)

with open("out/forecast.txt", "w", encoding="utf-8") as tf:
    tf.write(summary_text)
