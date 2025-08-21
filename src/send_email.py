# src/send_email.py
import os
import smtplib
import json
from email.mime.text import MIMEText
from datetime import datetime
from src import zen_rules   # << Zen engine

# --- Load forecast JSON ---
with open("out/forecast.json", "r", encoding="utf-8") as f:
    forecast_data = json.load(f)

# --- Extract values from JSON ---
spy_price   = float(forecast_data.get("spy", 0.0))
es_price    = float(forecast_data.get("es", 0.0))
vix_value   = float(forecast_data.get("vix", 0.0))
vvix_value  = float(forecast_data.get("vvix", 0.0))
rsi_value   = float(forecast_data.get("rsi", 50.0))

# Handle headline dict vs. string
headline_data = forecast_data.get("headline", {})
if isinstance(headline_data, dict):
    headline_text = headline_data.get("title", "No headline available")
    headline_link = headline_data.get("link", "")
else:
    headline_text = str(headline_data)
    headline_link = ""

# --- Current timestamp ---
today = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")

# Placeholder values until Stage 6/7 data sources wired in
last_candles  = []
vix_change    = 0.0
vvix_change   = 0.0
events_today  = []

# --- Run Zen Rules ---
straddle_status = zen_rules.straddle_zone(spy_price, spy_price)
rsi_status      = zen_rules.rsi_check(rsi_value)
candle_status   = zen_rules.candle_structure(last_candles)
vol_status      = zen_rules.volatility_overlay(vix_value, vvix_value, vix_change, vvix_change)
event_status    = zen_rules.event_filter(events_today)
headline_status = zen_rules.headline_overlay(headline_text)

zen_bias = zen_rules.combine_bias(
    straddle_status, rsi_status, candle_status,
    vol_status, event_status, headline_status
)

# --- Bias color mapping ---
bias_color = "#808080"
if "Bullish" in zen_bias:
    bias_color = "#2e8b57"
elif "Bearish" in zen_bias:
    bias_color = "#b22222"

# --- Key Levels ---
support_level    = round(spy_price - 15, 2)   # placeholder until Zen Grid
resistance_level = round(spy_price + 15, 2)

# --- Probable Path ---
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

# --- Trade Implications (mock until live options feed) ---
if "Bullish" in zen_bias:
    spread_text = (
        f"Bull Put Credit Spread: Sell {support_level} / Buy {support_level-20} (0DTE)<br>"
        f"Cost: ~2.10 | Max Return: ~18%"
    )
elif "Bearish" in zen_bias:
    spread_text = (
        f"Bear Call Credit Spread: Sell {resistance_level} / Buy {resistance_level+20} (0DTE)<br>"
        f"Cost: ~2.25 | Max Return: ~20%"
    )
else:
    spread_text = "Neutral Zone – consider Iron Condor around straddle range."

# --- Headline interpretation ---
if headline_link:
    headline_interp = (
        f"📰 {headline_text}<br>"
        f"<a href='{headline_link}'>{headline_link}</a><br>"
        f"<i>Zen read: {headline_status}</i>"
    )
else:
    headline_interp = f"📰 {headline_text}<br><i>Zen read: {headline_status}</i>"

# --- Build HTML email ---
html_body = f"""
<html>
  <body style="font-family: Arial, sans-serif; background-color:#fafafa; color:#222; padding:20px;">
    <h2 style="margin:0;">📈 ZeroDay Zen Forecast – {today}</h2>
    <p style="font-size:12px; color:#666; margin-top:2px;">Sent automatically by Zen Market AI</p>
    <hr style="border:none; border-top:1px solid #ddd; margin:15px 0;">

    <p style="font-size:14px; line-height:1.5;">
       <b>SPX Spot:</b> {spy_price}<br>
       <b>/ES:</b> {es_price}<br>
       <b>VIX:</b> {vix_value}<br>
       <b>VVIX:</b> {vvix_value}
    </p>

    <h3>🧠 Bias</h3>
    <p style="color:{bias_color}; font-weight:bold; font-size:15px;">{zen_bias}</p>

    <h3>🔑 Key Levels</h3>
    <p style="font-size:14px; line-height:1.5;">
       <span style="color:#b22222;"><b>Resistance:</b> {resistance_level}</span><br>
       <span style="color:#1e90ff;"><b>Support:</b> {support_level}</span>
    </p>

    <h3>📊 Probable Path</h3>
    <p style="font-size:14px; line-height:1.5;">
       Base Case: {base_case}<br>
       Bear Case: {bear_case}<br>
       Bull Case: {bull_case}
    </p>

    <h3>⚖️ Trade Implications</h3>
    <p style="font-size:14px; line-height:1.5;">{spread_text}</p>

    <h3>🌍 Context / News Check</h3>
    <p style="font-size:14px; line-height:1.5;">{headline_interp}</p>

    <h3>✅ Summary</h3>
    <p style="font-size:14px; line-height:1.5;">
      Bias: {zen_bias}. Watch {support_level}-{resistance_level} zone and volatility cues.
    </p>

    <hr style="border:none; border-top:1px solid #ddd; margin:20px 0;">
    <p style="font-size:11px; color:#888;">End of forecast</p>
  </body>
</html>
"""

# --- Send email ---
msg = MIMEText(html_body, "html", "utf-8")
msg["Subject"] = "📌 ZeroDay Zen Forecast"
msg["From"] = os.environ["EMAIL_USER"]
msg["To"] = os.environ["EMAIL_TO"]

server = smtplib.SMTP(os.environ["SMTP_SERVER"], int(os.environ["SMTP_PORT"]))
server.starttls()
server.login(os.environ["EMAIL_USER"], os.environ["EMAIL_PASS"])
server.sendmail(os.environ["EMAIL_USER"], [os.environ["EMAIL_TO"]], msg.as_string())
server.quit()
