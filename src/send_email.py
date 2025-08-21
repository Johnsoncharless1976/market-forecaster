# src/send_email.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- Load from GitLab CI/CD variables ---
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# --- Safety checks ---
if not EMAIL_USER or not EMAIL_PASS:
    raise ValueError("❌ EMAIL_USER or EMAIL_PASS not set in environment")
if not EMAIL_TO:
    raise ValueError("❌ EMAIL_TO not set in environment")

# --- Build email ---
msg = MIMEMultipart()
msg["From"] = EMAIL_USER
msg["To"] = EMAIL_TO
msg["Subject"] = "ZeroDay Zen Forecast"

body = "📈 ZeroDay Zen Forecast\n\n"
forecast_file = "forecast_output.txt"
if os.path.exists(forecast_file):
    with open(forecast_file, "r") as f:
        body += f.read()
else:
    body += "(No forecast_output.txt found in artifacts)"

msg.attach(MIMEText(body, "plain"))

# --- Send ---
try:
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, EMAIL_TO.split(","), msg.as_string())
    print("✅ Email sent successfully")
except smtplib.SMTPAuthenticationError as e:
    print("❌ Authentication failed:", e.smtp_error.decode())
    raise
except Exception as e:
    print("❌ Email send failed:", str(e))
    raise
