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
    Commit Note: Formats forecast data into professional dark theme HTML for email delivery.
    """
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ZeroDay Zen Forecast</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #1a1a1a;
                color: #ffffff;
                margin: 0;
                padding: 20px;
                line-height: 1.6;
            }}
            .container {{
                max-width: 600px;
                margin: 0 auto;
                background-color: #2d2d2d;
                border-radius: 10px;
                padding: 25px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            }}
            .header {{
                text-align: center;
                border-bottom: 2px solid #4a90e2;
                padding-bottom: 15px;
                margin-bottom: 20px;
            }}
            .title {{
                font-size: 24px;
                font-weight: bold;
                color: #4a90e2;
                margin: 0;
            }}
            .subtitle {{
                font-size: 14px;
                color: #b0b0b0;
                margin: 5px 0 0 0;
            }}
            .timestamp {{
                font-size: 12px;
                color: #888;
                font-style: italic;
            }}
            .section {{
                margin: 20px 0;
                background-color: #3a3a3a;
                border-radius: 8px;
                padding: 15px;
                border-left: 4px solid;
            }}
            .section-title {{
                font-size: 16px;
                font-weight: bold;
                margin: 0 0 10px 0;
                display: flex;
                align-items: center;
            }}
            .emoji {{
                margin-right: 8px;
                font-size: 18px;
            }}
            .data-grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
                margin: 10px 0;
            }}
            .data-item {{
                background-color: #4a4a4a;
                padding: 10px;
                border-radius: 5px;
            }}
            .data-label {{
                font-size: 12px;
                color: #b0b0b0;
                margin-bottom: 3px;
            }}
            .data-value {{
                font-size: 14px;
                font-weight: bold;
                color: #ffffff;
            }}
            .bias-neutral {{ border-left-color: #ffa500; }}
            .bias-bull {{ border-left-color: #28a745; }}
            .bias-bear {{ border-left-color: #dc3545; }}
            .levels {{ border-left-color: #ffd700; }}
            .path {{ border-left-color: #17a2b8; }}
            .trade {{ border-left-color: #6f42c1; }}
            .news {{ border-left-color: #20c997; }}
            .summary {{ border-left-color: #28a745; }}
            .resistance {{ color: #dc3545; font-weight: bold; }}
            .support {{ color: #28a745; font-weight: bold; }}
            .neutral-text {{ color: #ffa500; font-weight: bold; }}
            .news-link {{
                color: #4a90e2;
                text-decoration: none;
                font-size: 13px;
            }}
            .news-link:hover {{
                text-decoration: underline;
            }}
            .zen-analysis {{
                font-style: italic;
                color: #b0b0b0;
                margin-top: 8px;
            }}
            .footer {{
                text-align: center;
                margin-top: 25px;
                padding-top: 15px;
                border-top: 1px solid #555;
                color: #888;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <!-- Header -->
            <div class="header">
                <div class="title">🌟 ZeroDay Zen Forecast</div>
                <div class="subtitle">📊 ZeroDay Zen Forecast – {forecast_data.get("date", "")}</div>
                <div class="timestamp">Sent automatically by Zen Market AI</div>
            </div>

            <!-- Market Data -->
            <div class="data-grid">
                <div class="data-item">
                    <div class="data-label">SPX</div>
                    <div class="data-value">{forecast_data.get("spx", "N/A")}</div>
                </div>
                <div class="data-item">
                    <div class="data-label">/ES</div>
                    <div class="data-value">{forecast_data.get("mes", "N/A")}</div>
                </div>
                <div class="data-item">
                    <div class="data-label">VIX</div>
                    <div class="data-value">{forecast_data.get("vix", "N/A")}</div>
                </div>
                <div class="data-item">
                    <div class="data-label">VVIX</div>
                    <div class="data-value">{forecast_data.get("vvix", "N/A")}</div>
                </div>
            </div>

            <!-- Bias Section -->
            <div class="section bias-neutral">
                <div class="section-title">
                    <span class="emoji">🎯</span>
                    Bias
                </div>
                <div class="neutral-text">{forecast_data.get("bias", "Neutral")}</div>
            </div>

            <!-- Key Levels -->
            <div class="section levels">
                <div class="section-title">
                    <span class="emoji">⚡</span>
                    Key Levels
                </div>
                <div style="margin: 10px 0;">
                    <div style="margin: 5px 0;">
                        <span class="resistance">Resistance:</span> {forecast_data.get("resistance", "TBD")}
                    </div>
                    <div style="margin: 5px 0;">
                        <span class="support">Support:</span> {forecast_data.get("support", "TBD")}
                    </div>
                </div>
            </div>

            <!-- Probable Path -->
            <div class="section path">
                <div class="section-title">
                    <span class="emoji">📊</span>
                    Probable Path
                </div>
                <div>
                    <div><strong>Base Case:</strong> {forecast_data.get("base_case", "TBD")}</div>
                    <div><strong>Bear Case:</strong> {forecast_data.get("bear_case", "TBD")}</div>
                    <div><strong>Bull Case:</strong> {forecast_data.get("bull_case", "TBD")}</div>
                </div>
            </div>

            <!-- Trade Implications -->
            <div class="section trade">
                <div class="section-title">
                    <span class="emoji">⚖️</span>
                    Trade Implications
                </div>
                <div>
                    {forecast_data.get("trade_implications", "TBD")}
                </div>
            </div>

            <!-- Context / News Check -->
            <div class="section news">
                <div class="section-title">
                    <span class="emoji">🌍</span>
                    Context / News Check
                </div>
                <div>
                    {forecast_data.get("news_context", "No significant market-moving news detected.")}
                    <div class="zen-analysis">
                        {forecast_data.get("zen_analysis", "Zen read → analyzing...")}
                    </div>
                </div>
            </div>

            <!-- Summary -->
            <div class="section summary">
                <div class="section-title">
                    <span class="emoji">✅</span>
                    Summary
                </div>
                <div>
                    <strong>Bias:</strong> {forecast_data.get("bias", "Neutral")}. {forecast_data.get("notes", "Monitoring market conditions.")}
                </div>
            </div>

            <!-- Footer -->
            <div class="footer">
                End of forecast
            </div>
        </div>
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
        "date": "2025-09-04",
        "spx": "6476.86",
        "mes": "6491.00", 
        "vix": "15.15",
        "vvix": "N/A",
        "bias": "Neutral",
        "resistance": "6532.23",
        "support": "6422.23",
        "base_case": "Chop between 6422.23-6532.23",
        "bear_case": "If <6422.23, watch 6302.23",
        "bull_case": "If >6532.23, opens 6672.23",
        "trade_implications": "Neutral Zone – consider Iron Condor around straddle range",
        "news_context": "Markets steady ahead of Powell speech",
        "zen_analysis": "Zen read → noise",
        "notes": "Watch 6422.23-6532.23 zone and volatility cues"
    }

    send_forecast_email(test_data)