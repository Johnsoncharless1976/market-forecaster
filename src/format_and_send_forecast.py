# File: src/format_and_send_forecast.py
# Title: Stage 6 – Forecast Email Delivery (Gmail SMTP Fix)
# Commit Notes:
# - Fixed SMTP handshake for Gmail in CI.
# - Removed manual connect(); added proper EHLO before/after STARTTLS.

import os
from dotenv import load_dotenv
import snowflake.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()

cfg = {
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
    "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
    "SMTP_SERVER": os.getenv("SMTP_HOST"),   # Gmail = smtp.gmail.com
    "SMTP_PORT": int(os.getenv("SMTP_PORT", "587")),
    "SMTP_USER": os.getenv("SMTP_USER"),
    "SMTP_PASS": os.getenv("SMTP_PASS"),
    "EMAIL_SENDER": os.getenv("EMAIL_SENDER"),
    "EMAIL_TO": os.getenv("EMAIL_TO", os.getenv("SMTP_USER")),
}

def fetch_forecast(cur):
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_BIAS, ATM_STRADDLE, SUPPORT_LEVELS,
               RESISTANCE_LEVELS, RSI_CONTEXT, NOTES, FORECAST_TS
        FROM FORECAST_JOBS
        ORDER BY FORECAST_TS DESC
        LIMIT 1
    """)
    return cur.fetchone()

def fetch_market_levels(cur):
    def latest(table):
        cur.execute(f"SELECT CLOSE FROM {table} ORDER BY DATE DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else "n/a"
    return {
        "SPX": latest("SPX_HISTORICAL"),
        "ES": latest("ES_HISTORICAL"),
        "VIX": latest("VIX_HISTORICAL"),
        "VVIX": latest("VVIX_HISTORICAL"),
    }

def main():
    conn = snowflake.connector.connect(
        user=cfg["SNOWFLAKE_USER"], password=cfg["SNOWFLAKE_PASSWORD"],
        account=cfg["SNOWFLAKE_ACCOUNT"], warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"], schema=cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()

    forecast = fetch_forecast(cur)
    if not forecast:
        print("❌ No forecast found"); return
    f_date, idx, bias, straddle, support, resistance, rsi, notes, f_ts = forecast

    market = fetch_market_levels(cur)

    # Build email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📈 ZeroDay Zen Forecast – {f_ts}"
    msg["From"] = cfg["EMAIL_SENDER"]
    msg["To"] = cfg["EMAIL_TO"]

    html = f"""
    <html><body style="font-family:Arial, sans-serif; color:#333;">
    <h2>📈 ZeroDay Zen Forecast – {f_ts}</h2>
    <p><b>SPX:</b> {market['SPX']}<br>
       <b>/ES:</b> {market['ES']}<br>
       <b>VIX:</b> {market['VIX']}<br>
       <b>VVIX:</b> {market['VVIX']}</p>

    <h3>🧭 Bias</h3><p>{bias}</p>
    <h3>🔑 Support / Resistance</h3>
    <p>Support: {support}<br>Resistance: {resistance}</p>
    <h3>📊 ATM Straddle</h3><p>{straddle}</p>
    <h3>📊 RSI Context</h3><p>{rsi}</p>
    <h3>📝 Notes</h3><p>{notes}</p>
    </body></html>
    """

    plain = f"""
📈 ZeroDay Zen Forecast – {f_ts}

SPX: {market['SPX']} | /ES: {market['ES']} | VIX: {market['VIX']} | VVIX: {market['VVIX']}
Bias: {bias}
Support: {support}
Resistance: {resistance}
ATM Straddle: {straddle}
RSI Context: {rsi}
Notes: {notes}
"""

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(cfg["SMTP_SERVER"], cfg["SMTP_PORT"]) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(cfg["SMTP_USER"], cfg["SMTP_PASS"])
        server.sendmail(cfg["EMAIL_SENDER"], [cfg["EMAIL_TO"]], msg.as_string())

    print("✅ Forecast email sent.")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
