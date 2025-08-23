# 📄 File: src/format_and_send_forecast.py
#
# 📌 Title
# Zen Council – Stage 5 Formatting & Distribution
#
# 📝 Commit Notes
# Commit Title: ETL: finalize Stage 5 forecast email (sender-only SMTP, DB-driven recipients)
# Commit Message:
# - Validates only Snowflake creds as required.
# - SMTP creds (sender account) optional: if present, emails are sent; if missing, dry-run mode prints output.
# - Recipients always pulled from FORECAST_RECIPIENTS table (DB-driven, not env-driven).
# - Formats forecast into HTML table and plain-text fallback.
# - Clean separation of sender creds (.env) vs recipients (DB).

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()

# Required: Snowflake connection vars
SNOWFLAKE_VARS = [
    "SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"
]
missing = [v for v in SNOWFLAKE_VARS if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing Snowflake env vars: {', '.join(missing)}")

sf_cfg = {k: os.getenv(k) for k in SNOWFLAKE_VARS}

# Optional: SMTP (sender account only)
SMTP_VARS = ["SMTP_HOST","SMTP_PORT","SMTP_USER","SMTP_PASS","EMAIL_SENDER"]
smtp_cfg = {k: os.getenv(k) for k in SMTP_VARS}
smtp_enabled = all(smtp_cfg.values())

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

def build_html(forecast: pd.DataFrame, audit: pd.DataFrame) -> str:
    merged = forecast.merge(audit, on=["DATE","INDEX"], how="left")
    html = """
    <html>
      <body>
        <h2>📈 ZeroDay Zen Forecast</h2>
        <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
          <tr>
            <th>Date</th><th>Index</th><th>Bias</th>
            <th>Correct?</th><th>Range Hit?</th><th>RSI Aligned?</th><th>Notes</th>
          </tr>
    """
    for r in merged.itertuples():
        html += f"""
          <tr>
            <td>{r.DATE}</td><td>{r.INDEX}</td><td>{r.FORECAST_BIAS}</td>
            <td>{r.CORRECT}</td><td>{r.RANGE_HIT}</td><td>{r.RSI_ALIGNED}</td><td>{r.NOTES}</td>
          </tr>
        """
    html += "</table></body></html>"
    return html

def build_plain_text(forecast: pd.DataFrame, audit: pd.DataFrame) -> str:
    merged = forecast.merge(audit, on=["DATE","INDEX"], how="left")
    lines = ["📈 ZeroDay Zen Forecast"]
    for r in merged.itertuples():
        lines.append(
            f"Date: {r.DATE} | Index: {r.INDEX} | Bias: {r.FORECAST_BIAS} | "
            f"Correct: {r.CORRECT} | Range Hit: {r.RANGE_HIT} | RSI Aligned: {r.RSI_ALIGNED} | Notes: {r.NOTES}"
        )
    return "\n".join(lines)

def send_email(html: str, plain: str, recipients: list):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "ZeroDay Zen Forecast"
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

    if forecast.empty or not recipients:
        print("⚠ No forecast or no recipients. Skipping.")
    else:
        html = build_html(forecast, audit)
        plain = build_plain_text(forecast, audit)
        if smtp_enabled:
            send_email(html, plain, recipients)
            print(f"✅ Forecast email sent to {len(recipients)} recipients.")
        else:
            print("⚠ SMTP not configured. Dry-run mode:")
            print("\n---- Plain Text ----\n")
            print(plain)
            print("\n---- HTML ----\n")
            print(html)

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
