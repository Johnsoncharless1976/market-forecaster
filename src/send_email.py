# src/send_email.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# --- Load forecast text ---
with open("out/forecast.txt", "r", encoding="utf-8") as f:
    forecast = f.read()

# --- Extract key sections from the forecast text ---
lines = forecast.splitlines()

headline = next((l for l in lines if l.startswith("📰")), "📰 Headline: (not found)")
bias_line = next((l for l in lines if "Bias:" in l or "Bias =" in l), "")
tech_section = "\n".join(lines[lines.index("Technical Structure Overview (SPY | ES | VIX | VVIX)"):]) if "Technical Structure Overview" in forecast else ""
support_section = "\n".join(l for l in lines if "Support" in l or "Resistance" in l)
vol_section = "\n".join(lines[lines.index("Volatility Outlook"):]) if "Volatility Outlook" in forecast else ""
macro_section = "\n".join(lines[lines.index("Macro & Event Context"):]) if "Macro & Event Context" in forecast else ""
paths_section = "\n".join(lines[lines.index("Probable Paths (next 3–5 hours)"):]) if "Probable Paths" in forecast else ""
spreads_section = "\n".join(lines[lines.index("Potential 0DTE Spread Context (Educational Only)"):]) if "Potential 0DTE" in forecast else ""
summary_section = "\n".join(lines[lines.index("📌 Forecast Summary"):]) if "📌 Forecast Summary" in forecast else ""

# --- Bias color cue ---
bias_color = "#888"
if "Bullish" in bias_line:
    bias_color = "#2e8b57"  # green
elif "Bearish" in bias_line:
    bias_color = "#b22222"  # red
elif "Neutral" in bias_line:
    bias_color = "#808080"  # gray

# --- HTML body (report style) ---
html_body = f"""
<html>
  <body style="font-family:Arial, sans-serif; background-color:#f9f9f9; padding:20px; color:#222;">
    <table width="100%" style="max-width:700px; margin:auto; background:#fff; border-radius:10px; box-shadow:0 2px 8px rgba(0,0,0,0.1);">
      <tr>
        <td style="padding:20px;">
          <h2 style="margin:0 0 5px;">📌 ZeroDay Zen SPY Forecast</h2>
          <p style="font-size:12px; color:#666; margin:0 0 15px;">
            {os.environ.get("EMAIL_USER", "Zen Market AI")} · Sent automatically by GitLab CI
          </p>
          <hr style="border:none; border-top:1px solid #ddd; margin:15px 0;">

          <h3>📰 Headline of the Day</h3>
          <p style="margin:5px 0 15px; font-size:14px;">{headline}</p>

          <h3>🧠 Bias</h3>
          <p style="color:{bias_color}; font-size:16px; font-weight:bold; margin:5px 0 15px;">{bias_line}</p>

          <h3>📊 Technical Structure</h3>
          <pre style="background:#f6f6f6; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap;">{tech_section}</pre>

          <h3>🔔 Support / Resistance</h3>
          <pre style="background:#f6f6f6; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap;">{support_section}</pre>

          <h3>📉 Volatility Outlook</h3>
          <pre style="background:#f6f6f6; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap;">{vol_section}</pre>

          <h3>🌎 Macro & Events</h3>
          <pre style="background:#f6f6f6; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap;">{macro_section}</pre>

          <h3>📈 Probable Paths (3–5 hrs)</h3>
          <pre style="background:#f6f6f6; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap;">{paths_section}</pre>

          <h3>📝 0DTE Spread Context</h3>
          <pre style="background:#f6f6f6; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap;">{spreads_section}</pre>

          <h3>📌 Forecast Summary</h3>
          <pre style="background:#f0f8ff; padding:10px; border-radius:6px; font-size:13px; white-space:pre-wrap; font-weight:bold;">{summary_section}</pre>

          <hr style="border:none; border-top:1px solid #ddd; margin:20px 0;">
          <p style="font-size:11px; color:#888;">End of forecast</p>
        </td>
      </tr>
    </table>
  </body>
</html>
"""

# --- Build multipart email ---
msg = MIMEMultipart()
msg["Subject"] = "📌 ZeroDay Zen SPY Forecast"
msg["From"] = os.environ["EMAIL_USER"]
msg["To"] = os.environ["EMAIL_TO"]

# Attach HTML body
msg.attach(MIMEText(html_body, "html", "utf-8"))

# Attach forecast.json if it exists
json_path = "out/forecast.json"
if os.path.exists(json_path):
    with open(json_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(json_path)}"')
    msg.attach(part)

# --- Send email ---
server = smtplib.SMTP(os.environ["SMTP_SERVER"], int(os.environ["SMTP_PORT"]))
server.starttls()
server.login(os.environ["EMAIL_USER"], os.environ["EMAIL_PASS"])
server.sendmail(os.environ["EMAIL_USER"], [os.environ["EMAIL_TO"]], msg.as_string())
server.quit()
