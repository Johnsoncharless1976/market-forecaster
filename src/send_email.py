# src/send_email.py

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(subject, body_html):
    sender = os.environ.get("EMAIL_USER")
    recipient = os.environ.get("EMAIL_TO")
    password = os.environ.get("EMAIL_PASS")
    smtp_server = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))

    if not all([sender, recipient, password]):
        raise RuntimeError("❌ Missing one or more required email environment variables.")

    # Create MIME message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    # Wrap plain text (fallback) + HTML
    plain_text = "This email requires HTML support. Please view in a modern email client."

    msg.attach(MIMEText(plain_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, [recipient], msg.as_string())
        print("[EMAIL] ✅ Forecast email sent successfully.")
    except Exception as e:
        print(f"[EMAIL] ❌ Failed to send email: {e}")
