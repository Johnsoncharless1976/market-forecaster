# src/send_email.py
import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# --- Load forecast text ---
with open("out/forecast.txt", "r", encoding="utf-8") as f:
    raw_forecast = f.read()

# --- Extract values (fallback defaults if not present) ---
def extract_line(keyword, default="N/A"):
    for line in raw_forecast.splitlines():
        if keyword in line:
            return line
    return default

bias_line   = extract_line("Bias")
spy_line    = extract_line("SPY:")
es_line     = extract_line("ES:")
vix_line    = extract_line("VIX:")
vvix_line   = extract_line("VVIX:")
headline    = extract_line("Headline", "No headline available")

# --- Current timestamp ---
today = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")

# --- Bias color ---
bias_color = "#808080"  # default gray
if "Bullish" in bias_line:
    bias_color = "#2e8b57"  # green
elif "Bearish" in bias_line:
    bias_color = "#b22222"  # red

# --- Build HTML email ---
html_body = f"""
<html>
  <body style="font-family: Arial, sans-serif; background-color:#fafafa; color:#222; padding:20px;">
    <h2 style="margin:0;">📈 ZeroDay Zen Forecast – {today}</h2>
    <p style="font-size:12px; color:#666; margin-top:2px;">
      Sent automatically by Zen Market AI
    </p>
    <hr style="border:none; border-top:1px solid #ddd; margin:15px 0;">

    <!-- Bump info font size -->
    <p style="font-size:14px; line-height:1.5;">
      <b>SPX Spot:</b> {spy_line.replace("SPY:", "").strip()}<br>
      <b>/MES:</b> {es_line.replace("ES:", "").strip()}<br>
      <b>VIX:</b> {vix_line.replace("VIX:", "").strip()}<br>
      <b>VVIX:</b> {vvix_line.replace("VVIX:", "").strip()}
    </p>

    <h3>🔍 Technical & Volatility Structure</h3>
    <p style="font-size:14px; line-height:1.5;">
      {spy_line}<br>
      {es_line}<br>
      {vix_line} / {vvix_line}
    </p>

    <h3>🧠 Bias</h3>
    <p style="color:{bias_color}; font-weight:bold; font-size:14px;">
      {bias_line.replace("Bias", "").strip()}
    </p>

    <h3>🔑 Key Levels</h3>
    <p style="font-size:14px; line-height:1.5;">
      Resistance: (auto-filled soon)<br>
      Support: (auto-filled soon)
    </p>

    <h3>📊 Probable Path</h3>
    <p style="font-size:14px; line-height:1.5;">
      Base Case: (auto-filled soon)<br>
      Bear Case: (auto-filled soon)<br>
      Bull Case: (auto-filled soon)
    </p>

    <h3>⚖️ Trade Implications</h3>
    <p style="font-size:14px; line-height:1.5;">
      (contextual notes will be populated from Stage 6+)
    </p>

    <h3>🌍 Context / News Check</h3>
    <p style="font-size:14px; line-height:1.5;">{headline}</p>

    <h3>✅ Summary</h3>
    <p style="font-size:14px; line-height:1.5;">
      {bias_line}. Watch SPX key levels and VIX for confirmation.
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
