#!/usr/bin/env python3
"""
format_and_send_forecast.py

Responsible for formatting the ZeroDay Zen forecast output
and delivering it via SendGrid authenticated with
forecast@em7473.zenmarketai.com.

This is the canonical emailer script as of [DATE].
"""

import os
import sys
import traceback
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -------------------------
# Configuration
# -------------------------
FROM_EMAIL = "forecast@em7473.zenmarketai.com"
TO_EMAILS = os.getenv("FORECAST_RECIPIENTS", "your_email@example.com").split(",")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

if not SENDGRID_API_KEY:
    print("ERROR: SENDGRID_API_KEY environment variable not set.")
    sys.exit(1)


def build_email_content(forecast_data: dict) -> str:
    """
    Accepts forecast_data dictionary and returns HTML-formatted email body.
    Example of forecast_data expected keys:
    {
        "date": "2025-08-24",
        "spx": "6411.37",
        "mes": "6426.75",
        "vix": "15.78 (+1.35%)",
        "vvix": "102.09",
        "bias": "Neutral Bias",
        "notes": "SPX holding above 6400 floor..."
    }
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
    """Builds and sends forecast email via SendGrid."""
    subject = f"ZeroDay Zen Forecast – {forecast_data.get('date','')}"
    body_html = build_email_content(forecast_data)

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        for recipient in TO_EMAILS:
            message = Mail(
                from_email=FROM_EMAIL,
                to_emails=recipient,
                subject=subject,
                html_content=body_html
            )
            response = sg.send(message)
            print(f"Sent forecast email to {recipient}: {response.status_code}")
    except Exception as e:
        print("ERROR: Failed to send forecast email.")
        traceback.print_exc()


if __name__ == "__main__":
    # Example test payload for validation
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
