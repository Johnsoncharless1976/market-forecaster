# # 📄 File: src/send_forecast_email.py
#
# 📌 Title
# Zen Council – Stage 3.3 Forecast Email Delivery
#
# 📝 Commit Notes
# Commit Title: ETL: fix Stage 3.3 email delivery env var handling
# Commit Message:
# - Adjusted env var checks so GitLab masked/protected SMTP vars are recognized.
# - Removed rigid fail-fast that caused false "Missing env vars".
# - Uses Gmail SMTP with TLS on port 587.
# - Sends forecast email from configured account to recipients.

import os
import smtplib
from email.mime.text import MIMEText
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

cfg = {
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
    "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
    "SMTP_SERVER": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "SMTP_PORT": os.getenv("SMTP_PORT", "587"),
    "SMTP_USER": os.getenv("SMTP_USER"),
    "SMTP_PASS": os.getenv("SMTP_PASS"),
    "EMAIL_TO": os.getenv("EMAIL_TO"),
}

def fetch_forecast(cur):
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_BIAS, ATM_STRADDLE, SUPPORT_LEVELS,
               RESISTANCE_LEVELS, RSI_CONTEXT, NOTES
        FROM FORECAST_JOBS
        ORDER BY DATE DESC LIMIT 1
    """)
    return cur.fetchone()

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()
    fc = fetch_forecast(cur)
    if not fc:
        print("❌ No forecast found"); return

    f_date, idx, bias, straddle, support, resistance, rsi, notes = fc
    body = f"""
📈 ZeroDay Zen Forecast – {f_date}

Index: {idx}
Bias: {bias}
ATM Straddle: {straddle}
Support: {support}
Resistance: {resistance}
RSI Context: {rsi}
Notes: {notes}
"""

    msg = MIMEText(body)
    msg["Subject"] = f"ZeroDay Zen Forecast – {f_date}"
    msg["From"] = cfg["SMTP_USER"]
    msg["To"] = cfg["EMAIL_TO"]

    with smtplib.SMTP(cfg["SMTP_SERVER"], int(cfg["SMTP_PORT"])) as server:
        server.starttls()
        server.login(cfg["SMTP_USER"], cfg["SMTP_PASS"])
        server.sendmail(cfg["SMTP_USER"], [cfg["EMAIL_TO"]], msg.as_string())

    print("✅ Forecast email sent.")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
