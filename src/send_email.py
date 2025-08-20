# src/send_email.py
import os
import smtplib
from email.mime.text import MIMEText

with open("out/forecast.txt", "r", encoding="utf-8") as f:
    forecast = f.read()

bias_line = next((line for line in forecast.splitlines() if "Bias:" in line), "")
bias_color = "#888"
if "Bullish" in bias_line:
    bias_color = "#2e8b57"
elif "Bearish" in bias_line:
    bias_color = "#b22222"

html_body = f"""
<html>
  <body style="font-family:Arial, sans-serif; color:#222; background-color:#fafafa; padding:20px;">
    <h2 style="margin-bottom:5px;">📌 ZeroDay Zen SPY Forecast</h2>
    <p style="font-size:12px; color:#666; margin-top:0;">
      {os.environ["EMAIL_USER"]} · Sent automatically by GitLab CI
    </p>
    <hr style="border:none; border-top:1px solid #ddd; margin:15px 0;">
    <pre style="font-size:14px; line-height:1.4; white-space:pre-wrap;">{forecast}</pre>
  </body>
</html>
"""

msg = MIMEText(html_body, "html", "utf-8")
msg["Subject"] = "📌 ZeroDay Zen SPY Forecast"
msg["From"] = os.environ["EMAIL_USER"]
msg["To"] = os.environ["EMAIL_TO"]

server = smtplib.SMTP(os.environ["SMTP_SERVER"], int(os.environ["SMTP_PORT"]))
server.starttls()
server.login(os.environ["EMAIL_USER"], os.environ["EMAIL_PASS"])
server.sendmail(os.environ["EMAIL_USER"], [os.environ["EMAIL_TO"]], msg.as_string())
server.quit()
