# src/send_email.py
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# Example market data (replace with Snowflake or API fetch)
spx = 635.4600219726562
es = 6388.25
vix = 16.780000686645508
vvix = None

# Format numbers
spx_fmt = f"{spx:.2f}"
es_fmt = f"{es:.2f}"
vix_fmt = f"{vix:.2f}"
vvix_fmt = f"{vvix:.1f}" if vvix else "N/A"

# Timestamp
now = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")

# Email body (Zen style)
body = f"""📈 ZeroDay Zen Forecast – {now}
Sent automatically by Zen Market AI

SPX Spot: {spx_fmt}
/ES: {es_fmt}
VIX: {vix_fmt}
VVIX: {vvix_fmt}

🧠 Bias
Neutral

🔑 Key Levels
Resistance: {float(spx_fmt) + 15:.2f}
Support: {float(spx_fmt) - 15:.2f}

📊 Probable Path
Base Case: Chop between {float(spx_fmt) - 15:.2f}-{float(spx_fmt) + 15:.2f}.
Bear Case: If <{float(spx_fmt) - 15:.2f}, watch {float(spx_fmt) - 35:.2f}.
Bull Case: If >{float(spx_fmt) + 15:.2f}, opens {float(spx_fmt) + 35:.2f}.

⚖️ Trade Implications
Neutral Zone – consider Iron Condor around straddle range.

🌍 Context / News Check
📰 Markets steady ahead of Powell speech
https://www.reuters.com/markets/
Zen read → noise

✅ Summary
Bias: Neutral. Watch {float(spx_fmt) - 15:.2f}-{float(spx_fmt) + 15:.2f} zone and volatility cues.
"""

msg = MIMEText(body, "plain")
msg["Subject"] = "📈 ZeroDay Zen Forecast"
msg["From"] = "zenmarketai@gmail.com"
msg["To"] = "youremail@domain.com"

# Send (adjust SMTP settings for Gmail or SES)
with smtplib.SMTP("smtp.gmail.com", 587) as server:
    server.starttls()
    server.login("zenmarketai@gmail.com", "YOUR_APP_PASSWORD")
    server.sendmail(msg["From"], [msg["To"]], msg.as_string())

print("✅ Email sent successfully")
