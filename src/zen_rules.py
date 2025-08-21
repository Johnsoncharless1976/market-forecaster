# src/zen_rules.py
from datetime import datetime

def generate_forecast(spx, es, vix, vvix):
    now = datetime.datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")

    body_html = f"""
    <html>
      <head>
        <style>
          body {{
            font-family: Arial, Helvetica, sans-serif;
            font-size: 16px;   /* ⬅️ bumped up from 14px */
            line-height: 1.6;
            color: #111;
          }}
          h1, h2 {{
            font-size: 20px;   /* ⬅️ bumped up from 18px */
          }}
          .summary {{
            font-weight: bold;
            color: #2d862d;   /* green for summary */
          }}
        </style>
      </head>
      <body>
        <h2>📌 ZeroDay Zen Forecast – {now}</h2>
        <p><b>SPX Spot:</b> {spx}<br>
        <b>/ES:</b> {es}<br>
        <b>VIX:</b> {vix}<br>
        <b>VVIX:</b> {vvix if vvix else "N/A"}</p>

        <h3>🧠 Bias</h3>
        <p>Neutral</p>

        <h3>🔑 Key Levels</h3>
        <p>Resistance: {spx + 15:.2f}<br>
        Support: {spx - 15:.2f}</p>

        <h3>📊 Probable Path</h3>
        <p>Base Case: Chop between {spx - 15:.2f}–{spx + 15:.2f}.<br>
        Bear Case: If &lt;{spx - 15:.2f}, watch {spx - 35:.2f}.<br>
        Bull Case: If &gt;{spx + 15:.2f}, opens {spx + 35:.2f}.</p>

        <h3>⚖️ Trade Implications</h3>
        <p>Neutral Zone – consider Iron Condor around straddle range.</p>

        <h3>🌍 Context / News Check</h3>
        <p>📰 Markets steady ahead of Powell speech<br>
        <a href="https://www.reuters.com/markets/">https://www.reuters.com/markets/</a><br>
        Zen read → noise</p>

        <p class="summary">✅ Summary<br>
        Bias: Neutral. Watch {spx - 15:.2f}–{spx + 15:.2f} zone and volatility cues.</p>

        <p><i>End of forecast</i></p>
      </body>
    </html>
    """
    return body_html
