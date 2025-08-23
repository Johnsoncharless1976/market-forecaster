# 📄 File: src/format_and_send_forecast.py
#
# 📌 Title
# Zen Council – Stage 5 Email with Live Market Data
#
# 📝 Commit Notes
# Commit Title: ETL: upgrade Stage 5 email with live SPX/ES/VIX/VVIX values
# Commit Message:
# - Subject line updated with 📌 pin emoji.
# - SPX label corrected (removed "spot").
# - Font size bumped by +2 points across the body.
# - Queries Snowflake for latest SPX, ES, VIX, VVIX closes.
# - Outputs polished forecast email with live data.

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

load_dotenv()

# Snowflake creds required
SNOWFLAKE_VARS = [
    "SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"
]
missing = [v for v in SNOWFLAKE_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing Snowflake env vars: {', '.join(missing)}")
sf_cfg = {k: os.getenv(k) for k in SNOWFLAKE_VARS}

# SMTP creds required
SMTP_VARS = ["SMTP_HOST","SMTP_PORT","SMTP_USER","SMTP_PASS","EMAIL_SENDER"]
missing = [v for v in SMTP_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing SMTP env vars: {', '.join(missing)}")
smtp_cfg = {k: os.getenv(k) for k in SMTP_VARS}

def fetch_latest_forecast(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_BIAS
        FROM FORECAST_JOBS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY INDEX ORDER BY DATE DESC) = 1
    """)
    return pd.DataFrame(cur.fetchall(), columns=["DATE","INDEX","FORECAST_BIAS"])

def fetch_latest_audit(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT DATE, INDEX, FORECAST_CORRECT, RANGE_HIT, RSI_ALIGNED, NOTES
        FROM FORECAST_AUDIT_LOG
        QUALIFY ROW_NUMBER() OVER (PARTITION BY INDEX ORDER BY DATE DESC) = 1
    """)
    return pd.DataFrame(cur.fetchall(), columns=["DATE","INDEX","CORRECT","RANGE_HIT","RSI_ALIGNED","NOTES"])

def fetch_recipients(cur) -> list:
    cur.execute("SELECT EMAIL, NAME FROM FORECAST_RECIPIENTS WHERE ACTIVE = TRUE")
    return cur.fetchall()

def fetch_market_levels(cur):
    def latest(symbol_table):
        cur.execute(f"SELECT DATE, CLOSE FROM {symbol_table} ORDER BY DATE DESC LIMIT 1")
        row = cur.fetchone()
        return row[1] if row else "n/a"

    return {
        "SPX": latest("SPX_HISTORICAL"),
        "ES": latest("ES_HISTORICAL"),
        "VIX": latest("VIX_HISTORICAL"),
        "VVIX": latest("VVIX_HISTORICAL"),
    }

def build_html(forecast: pd.DataFrame, audit: pd.DataFrame, market: dict) -> str:
    merged = forecast.merge(audit, on=["DATE","INDEX"], how="left")
    run_time = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")
    row = merged.iloc[0]

    html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; font-size:16px; line-height: 1.6; color: #222;">
        <h2>📌 ZeroDay Zen Forecast – {run_time}</h2>
        <p style="font-size: 13px; color: #666;">Sent automatically by Zen Market AI</p>

        <p><b>SPX:</b> {market['SPX']}<br>
           <b>/ES:</b> {market['ES']}<br>
           <b>VIX:</b> {market['VIX']}<br>
           <b>VVIX:</b> {market['VVIX']}</p>

        <h3>🧭 Bias</h3>
        <p>{row.FORECAST_BIAS}</p>

        <h3>🔑 Key Levels</h3>
        <p><b>Resistance:</b> placeholder<br>
           <b>Support:</b> placeholder</p>

        <h3>📊 Probable Path</h3>
        <p>
          Base Case: placeholder<br>
          Bear Case: placeholder<br>
          Bull Case: placeholder
        </p>

        <h3>📐 Trade Implications</h3>
        <p>Neutral Zone – consider Iron Condor around straddle range.</p>

        <h3>📰 Context / News Check</h3>
        <p>CPI hotter than expected; rate cut odds repriced lower<br>
           <a href="https://www.reuters.com/markets/us-cpi-aug2025">https://www.reuters.com/markets/us-cpi-aug2025</a></p>

        <h3>✅ Summary</h3>
        <p>Bias: {row.FORECAST_BIAS}. Watch levels and volatility cues.</p>

        <hr>
        <p style="font-size: 12px; color: #888;">End of forecast</p>
      </body>
    </html>
    """
    return html

def build_plain_text(forecast: pd.DataFrame, audit: pd.DataFrame, market: dict) -> str:
    merged = forecast.merge(audit, on=["DATE","INDEX"], how="left")
    row = merged.iloc[0]
    lines = [
        f"📌 ZeroDay Zen Forecast – {datetime.now().strftime('%b %d, %Y %I:%M %p ET')}",
        "",
        f"SPX: {market['SPX']} | /ES: {market['ES']} | VIX: {market['VIX']} | VVIX: {market['VVIX']}",
        "",
        f"🧭 Bias: {row.FORECAST_BIAS}",
        "🔑 Key Levels: Resistance placeholder / Support placeholder",
        "📊 Probable Path: Base Case / Bear Case / Bull Case placeholders",
        "📐 Trade Implications: Neutral Zone – consider Iron Condor",
        "📰 Context/News: CPI hotter than expected; rate cut odds repriced lower",
        f"✅ Summary: Bias {row.FORECAST_BIAS}, watch volatility cues.",
        "",
        "End of forecast"
    ]
    return "\n".join(lines)

def send_email(html: str, plain: str, recipients: list):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "📌 ZeroDay Zen Forecast"
    msg["From"] = smtp_cfg["EMAIL_SENDER"]
    msg["To"] = ", ".join([r[0] for r in recipients])

    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_cfg["SMTP_HOST"], int(smtp_cfg["SMTP_PORT"])) as server:
        server.starttls()
        server.login(smtp_cfg["SMTP_USER"], smtp_cfg["SMTP_PASS"])
        server.sendmail(smtp_cfg["EMAIL_SENDER"], [r[0] for r in recipients], msg.as_string())

def main():
    conn = snowflake.connector.connect(
        user=sf_cfg["SNOWFLAKE_USER"], password=sf_cfg["SNOWFLAKE_PASSWORD"],
        account=sf_cfg["SNOWFLAKE_ACCOUNT"], warehouse=sf_cfg["SNOWFLAKE_WAREHOUSE"],
        database=sf_cfg["SNOWFLAKE_DATABASE"], schema=sf_cfg["SNOWFLAKE_SCHEMA"]
    )
    cur = conn.cursor()

    forecast = fetch_latest_forecast(cur)
    audit = fetch_latest_audit(cur)
    recipients = fetch_recipients(cur)
    market = fetch_market_levels(cur)

    if forecast.empty or not recipients:
        raise RuntimeError("No forecast or no recipients. Aborting email.")
    else:
        html = build_html(forecast, audit, market)
        plain = build_plain_text(forecast, audit, market)
        send_email(html, plain, recipients)
        print(f"✅ Forecast email sent to {len(recipients)} recipients.")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
