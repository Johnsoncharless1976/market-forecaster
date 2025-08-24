#!/usr/bin/env python3
# =========================================================
# Commit: Add Snowflake recipient query for forecast emails
# - Removed FORECAST_RECIPIENTS env fallback (kept for local test only)
# - Added Snowflake connector to query recipient table
# - Uses SENDGRID_API_KEY from .env (local) or GitLab CI/CD variables
# - Codex discipline: commit headers + inline notes preserved
# =========================================================

import os
import sys
import traceback
import snowflake.connector
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv  # Local .env support

# =========================================================
# Commit Note: Load .env for local development
# - In CI/CD, GitLab provides variables directly
# - Locally, python-dotenv ensures .env is loaded
# =========================================================
load_dotenv()

# =========================================================
# Commit Note: Configuration
# - FROM_EMAIL fixed to forecast@em7473.zenmarketai.com
# - TO_EMAILS dynamically queried from Snowflake recipients table
# =========================================================
FROM_EMAIL = "forecast@em7473.zenmarketai.com"
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

if not SENDGRID_API_KEY:
    print("ERROR: SENDGRID_API_KEY environment variable not set.")
    sys.exit(1)

# =========================================================
# Commit Note: Snowflake connection parameters
# - Values should be set in .env or GitLab CI/CD variables
# - Replace placeholders with your real Snowflake creds
# =========================================================
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# =========================================================
# Commit Note: Snowflake query for recipients
# - Replace RECIPIENTS_TABLE with actual table name
# - Must contain column: EMAIL
# =========================================================
RECIPIENTS_TABLE = "FORECAST_RECIPIENTS"


def get_recipients_from_snowflake() -> list[str]:
    """Query Snowflake table to retrieve forecast recipient emails."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        query = f"SELECT EMAIL FROM {RECIPIENTS_TABLE};"
        df = pd.read_sql(query, conn)
        conn.close()
        return df["EMAIL"].dropna().tolist()
    except Exception as e:
        print("ERROR: Failed to fetch recipients from Snowflake.")
        traceback.print_exc()
        # Fallback for dev/test only
        return os.getenv("FORECAST_RECIPIENTS", "your_email@example.com").split(",")


def build_email_content(forecast_data: dict) -> str:
    """
    Commit Note: Formats forecast data into HTML for email delivery.
    """
    html_body = f"""
    <html>
    <head>
      <style>
        body {{
          font-family: Arial, sans-serif;
          background-color: #0d1117;
          color: #e6edf3;
          padding: 20px;
        }}
        h1 {{
          color: #58a6ff;
        }}
        .section-title {{
          color: #f78166;
          margin-top: 20px;
        }}
        .data-block {{
          background-color: #161b22;
          padding: 15px;
          border-radius: 8px;
          margin-bottom: 15px;
        }}
      </style>
    </head>
    <body>
      <h1>📈 ZeroDay Zen Forecast – {forecast_data.get("date","")}</h1>
      <div class="data-block">
        <p><b>SPX Spot:</b> {forecast_data.get("spx","")}</p>
        <p><b>/MES:</b> {forecast_data.get("mes","")}</p>
        <p><b>VIX:</b> {forecast_data.get("vix","")}</p>
        <p><b>VVIX:</b> {forecast_data.get("vvix","")}</p>
      </div>
      <div class="data-block">
        <p><b>Bias:</b> {forecast_data.get("bias","")}</p>
        <p><b>Notes:</b> {forecast_data.get("notes","")}</p>
      </div>
      <p style="margin-top:30px; font-size:12px; color:#8b949e;">
        Generated automatically by ZenMarket AI – format_and_send_forecast.py
      </p>
    </body>
    </html>
    """
    return html_body


def send_forecast_email(forecast_data: dict) -> None:
    """
    Commit Note: Sends forecast email via SendGrid.
    - Recipients retrieved dynamically from Snowflake
    - Fallback env used only for dev/test
    """
    subject = f"ZeroDay Zen Forecast – {forecast_data.get('date','')}"
    body_html = build_email_content(forecast_data)
    recipients = get_recipients_from_snowflake()

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        for recipient in recipients:
            message = Mail(
                from_email=FROM_EMAIL,
                to_emails=recipient,
                subject=subject,
                html_content=body_html
            )
            response = sg.send(message)
            print(f"Sent forecast email to {recipient}: {response.status_code}")
    except Exception:
        print("ERROR: Failed to send forecast email.")
        traceback.print_exc()


if __name__ == "__main__":
    # =========================================================
    # Commit Note: Test payload (local run only)
    # - In CI/CD, Stage 4 provides real forecast_data
    # =========================================================
    test_data = {
        "date": "2025-08-24",
        "spx": "6411.37",
        "mes": "6426.75",
        "vix": "15.78 (+1.35%)",
        "vvix": "102.09",
        "bias": "Neutral Bias",
        "notes": "SPX holding above 6400 floor; RSI mid-range."
    }

    send_forecast_email(test_data)
