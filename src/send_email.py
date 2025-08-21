# src/send_email.py
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
    raise ValueError("❌ Missing email configuration variables")

# Format subject line with timestamp
now = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")
subject = f"📈 ZeroDay Zen Forecast – {now}"

msg = MIMEMultipart()
msg["From"] = EMAIL_USER
msg["To"] = EMAIL_TO
msg["Subject"] = subject

body = ""
forecast_file = "forecast_output.txt"
if os.path.exists(forecast_file):
    with open(forecast_file, "r") as f:
        body = f.read()
else:
    body = "(⚠️ No forecast generated)"

msg.attach(MIMEText(body, "plain"))

try:
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, EMAIL_TO.split(","), msg.as_string())
    print("✅ Email sent successfully")
except Exception as e:
    print("❌ Email send failed:", str(e))
    raise

