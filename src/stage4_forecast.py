# stage4_forecast.py

import snowflake.connector
import pandas as pd
from datetime import datetime

# --- Snowflake connection ---
conn = snowflake.connector.connect(
    user="JOHNSONCHARLESS",
    password="s7AfXRG7krgnh3H",
    account="GFXGPHR-HXC94041",
    warehouse="COMPUTE_WH",
    database="ZEN_MARKET",
    schema="FORECASTING"
)

# --- Pull most recent changes and correlations ---
query_changes = "SELECT * FROM MARKET_CHANGES ORDER BY date DESC LIMIT 1;"
query_corr = """
SELECT * FROM (
    SELECT date, spy_close, es_close, vix_close, vvix_close
    FROM MARKET_MASTER
    ORDER BY date DESC
    LIMIT 60
) sub
"""
df_changes = pd.read_sql(query_changes, conn)
df_corr_base = pd.read_sql(query_corr, conn)
conn.close()

df_changes.columns = df_changes.columns.str.lower()
df_corr_base.columns = df_corr_base.columns.str.lower()

# --- Rolling correlations (last 30 days) ---
df_corr_base["date"] = pd.to_datetime(df_corr_base["date"])
df_corr_base.set_index("date", inplace=True)

corr_df = pd.DataFrame({
    "spy_es_corr": df_corr_base["spy_close"].rolling(30).corr(df_corr_base["es_close"]),
    "spy_vix_corr": df_corr_base["spy_close"].rolling(30).corr(df_corr_base["vix_close"]),
    "vix_vvix_corr": df_corr_base["vix_close"].rolling(30).corr(df_corr_base["vvix_close"])
})
latest_corr = corr_df.dropna().iloc[-1]

# --- Forecast Bias Logic ---
spy_pct = float(df_changes["spy_pct"].iloc[0])
es_pct  = float(df_changes["es_pct"].iloc[0])
vix_pct = float(df_changes["vix_pct"].iloc[0])
vvix_pct = float(df_changes["vvix_pct"].iloc[0])

if spy_pct < -0.5 and latest_corr.spy_vix_corr < -0.6:
    bias = "Bearish"
elif spy_pct > 0.5 and latest_corr.spy_vix_corr > -0.3:
    bias = "Bullish"
else:
    bias = "Neutral"

# --- Forecast Output ---
today = datetime.now().strftime("%A, %b %d, %Y @ %I:%M %p ET")

print(f"\n=== ZeroDay Zen SPX Forecast Update – {today} ===")

# --- Headline placeholder (to be replaced by NLP in Stage 7) ---
headline = "U.S. CPI hotter than expected; rate cut odds repriced lower"
headline_link = "https://www.reuters.com/markets/us-cpi-aug2025"
print("\n📰 Headline of the Day:")
print(f"{headline}\nLink: {headline_link}\n")

print(f"🧠 Bias: {bias} (SPY {spy_pct:+.2f}%, ES {es_pct:+.2f}%, VIX {vix_pct:+.2f}%, VVIX {vvix_pct:+.2f}%)\n")

print("Technical Structure Overview (SPY | ES | VIX)")
print(f"SPY: {df_corr_base['spy_close'].iloc[-1]:.2f} | ES: {df_corr_base['es_close'].iloc[-1]:.2f} | VIX: {df_corr_base['vix_close'].iloc[-1]:.2f} | VVIX: {df_corr_base['vvix_close'].iloc[-1]:.2f}\n")

print("Key Support / Resistance Zones (SPY)")
print("- Resistance: Placeholder (will come from Zen Grid)")
print("- Support: Placeholder (will come from Zen Grid)\n")

print("Volatility Outlook")
print(f"SPY–ES Corr: {latest_corr.spy_es_corr:.3f} | SPY–VIX Corr: {latest_corr.spy_vix_corr:.3f} | VIX–VVIX Corr: {latest_corr.vix_vvix_corr:.3f}")
print("Interpretation: Placeholder text (will refine with thresholds)\n")

print("Macro & Event Context")
print("Econ reports, FedWatch, headlines, and whispers not yet integrated (Stage 7)\n")

print("Probable Paths (next 3–5 hours)")
print("Base Case – Placeholder\nUpside – Placeholder\nDownside – Placeholder\n")

print("Potential 0DTE Spread Context (Educational Only)")
print("Placeholder spreads until Zen Grid levels are wired in\n")

print("📌 Forecast Summary")
print(f"Bias = {bias}, SPY daily % = {spy_pct:+.2f}, VIX daily % = {vix_pct:+.2f}, Corr(SPY–VIX) = {latest_corr.spy_vix_corr:.3f}")
