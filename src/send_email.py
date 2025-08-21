# src/send_email.py
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

with open("out/forecast.json", "r", encoding="utf-8") as f:
    forecast = json.load(f)

# Relaxed validation: only abort if *all* feeds missing
if all(v in (0.0, None) for v in forecast.values()):
    raise ValueError("Forecast data invalid – all prices missing. Email aborted.")

# Build email content
body = f"""
📈 ZeroDay Zen Forecast

SPX: {forecast.get("SPX")}
ES: {forecast.get("ES")}
VIX: {forecast.get("VIX")}
VVIX: {forecast.get("VVIX")}

(Generated automatically by CI/CD)
"""

msg = MIMEMultipart()
msg["From"] = EMAIL_USER
msg["To"] = EMAIL_TO
msg["Subject"] = "ZeroDay Zen Forecast"
msg.attach(MIMEText(body, "plain"))

# Send email
try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())
    print("✅ Email sent successfully")
except Exception as e:
    print(f"❌ Email send failed: {e}")
