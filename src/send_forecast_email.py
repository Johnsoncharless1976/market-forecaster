# 📄 File: src/send_forecast_email.py
#
# 📌 Title
# Zen Council – Stage 3.3 Forecast Email Delivery (Fixed)
#
# 📝 Commit Notes
# Commit Title: ETL: fix Stage 3.3 email delivery null SMTP vars
# Commit Message:
# - Added explicit validation for SMTP_USER and SMTP_PASS.
# - If missing, prints clear error instead of crashing with NoneType encode.
# - Ensures GitLab CI/CD masked variables are picked up correctly.

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
}

def fetch_forecast(cur):
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_BIAS, ATM_STRADDLE, SUPPORT_LEVELS,
               RESISTANCE_LEVELS, RSI_CONTEXT, NOTES
        FROM FORECAST_JOBS
        ORDER BY DATE DESC LIMIT 1
    """)
    return cur.fetchone()

def get_recipients(cur):
    cur.execute("SELECT EMAIL FROM EMAIL_RECIPIENTS WHERE ACTIVE = TRUE;")
    return [row[0] for row in cur.fetchall()]

def main():
    # Ensure SMTP creds exist
    if not cfg["SMTP_USER"] or not cfg["SMTP_PASS"]:
        print("❌ SMTP credentials missing. Ensure SMTP_USER and SMTP_PASS are set in GitLab CI/CD variables.")
        return

    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()
    
    # Get email recipients from database
    recipients = get_recipients(cur)
    if not recipients:
        print("❌ No active email recipients found"); return
    
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
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP(cfg["SMTP_SERVER"], int(cfg["SMTP_PORT"])) as server:
        server.starttls()
        server.login(cfg["SMTP_USER"], cfg["SMTP_PASS"])
        server.sendmail(cfg["SMTP_USER"], recipients, msg.as_string())

    print("✅ Forecast email sent.")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
