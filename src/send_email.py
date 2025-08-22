# send_email.py
# ✉️ ZeroDay Zen Forecast Email Sender
# Purpose: Imports forecast_body from brain.py and emails it.

import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
from brain import forecast_body   # <--- import from brain.py

load_dotenv()

def send_email(body):
    msg = MIMEText(body, "plain")
    msg["Subject"] = "ZeroDay Zen Forecast"
    msg["From"] = os.getenv("EMAIL_USER")
    msg["To"] = os.getenv("EMAIL_TO")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"))
        server.send_message(msg)

    print(f"📨 Email sent to {msg['To']}")

if __name__ == "__main__":
    send_email(forecast_body)
