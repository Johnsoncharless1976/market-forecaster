# src/send_email.py
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(subject: str, body: str):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    email_user = os.getenv("EMAIL_USER")
    email_pass = os.getenv("EMAIL_PASS")
    email_to = os.getenv("EMAIL_TO")

    # Build the email container
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_user
    msg["To"] = email_to

    # Wrap plain text fallback
    text_version = body

    # 🔥 HTML version for Gmail (exact look)
    html_version = f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6;">
      <h2>📌 ZeroDay Zen Forecast</h2>
      {body.replace("\n", "<br>")}
    </body>
    </html>
    """

    # Attach both
    msg.attach(MIMEText(text_version, "plain"))
    msg.attach(MIMEText(html_version, "html"))

    # Send
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(email_user, email_pass)
        server.sendmail(email_user, email_to, msg.as_string())

    print("✅ Email sent successfully!")
