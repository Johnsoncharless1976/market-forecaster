# src/send_email.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# --- Load from GitLab CI/CD variables ---
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
    raise ValueError("❌ Missing email configuration variables")

# --- Build the email ---
msg = MIMEMultipart()
msg["From"] = EMAIL_USER
msg["To"] = EMAIL_TO
msg["Subject"] = "ZeroDay Zen Forecast"

body = "📈 ZeroDay Zen Forecast\n\n"

forecast_file = "forecast_output.txt"
if os.path.exists(forecast_file):
    with open(forecast_file, "r") as f:
        forecast_content = f.read()
        body += forecast_content

        # Attach file too
        with open(forecast_file, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={forecast_file}")
        msg.attach(part)
else:
    body += "(No forecast_output.txt found)"

msg.attach(MIMEText(body, "plain"))

# --- Send the email ---
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
