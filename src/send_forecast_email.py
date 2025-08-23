# 📄 File: src/send_forecast_email.py
#
# 📌 Title
# Zen Council – Stage 3.3 Forecast Email Delivery
#
# 📝 Commit Notes
# Commit Title: ETL: implement Stage 3.3 forecast email delivery
# Commit Message:
# - Pulls latest forecast row from FORECAST_JOBS.
# - Formats into email body with SPX spot, straddle, support/resistance, RSI, bias.
# - Sends via SMTP (placeholder, extend with client distribution).
# - Fixed: corrected triple-quoted f-string formatting.

import os
import smtplib
from email.mime.text import MIMEText
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

REQ_VARS = [
    "SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA",
    "SMTP_SERVER","SMTP_PORT","SMTP_USER","SMTP_PASS","EMAIL_TO"
]
missing = [v for v in REQ_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing env vars: {', '.join(missing)}")

cfg = {k: os.getenv(k) for k in REQ_VARS}

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
        server.send_message(msg)

    print("✅ Forecast email sent.")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
