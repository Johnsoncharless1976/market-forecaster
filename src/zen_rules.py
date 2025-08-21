# src/zen_rules.py
from datetime import datetime

def generate_forecast(spx: float, es: float, vix: float, vvix: float = None) -> str:
    """
    Build a fully formatted HTML forecast email body.
    Matches the exact style (emojis, bold, spacing, colored text).
    """

    now = datetime.now().strftime("%b %d, %Y (%I:%M %p ET)")
    vvix_display = vvix if vvix is not None else "N/A"

    # Hardcoded example logic — replace with your real analysis
    bias = "Neutral"
    resistance = 652.23
    support = 622.23
    base_case = f"Chop between {support}-{resistance}."
    bear_case = f"If &lt;{support}, watch {support-20}."
    bull_case = f"If &gt;{resistance}, opens {resistance+20}."
    news_title = "Markets steady ahead of Powell speech"
    news_link = "https://www.reuters.com/markets/"
    news_comment = "Zen read → noise"
    summary = f"Bias: {bias}. Watch {support}-{resistance} zone and volatility cues."

    # HTML Body
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 16px; line-height: 1.6;">

    <h2>📌 ZeroDay Zen Forecast</h2>

    <h3>📈 ZeroDay Zen Forecast – {now}</h3>
    <p><em>Sent automatically by Zen Market AI</em></p>
    <hr>

    <p><b>SPX Spot:</b> {spx}<br>
    <b>/ES:</b> {es}<br>
    <b>VIX:</b> {vix}<br>
    <b>VVIX:</b> {vvix_display}</p>

    <h3>🧠 Bias</h3>
    <p>{bias}</p>

    <h3>🔑 Key Levels</h3>
    <p><b><span style="color:red;">Resistance:</span></b> {resistance}<br>
    <b><span style="color:blue;">Support:</span></b> {support}</p>

    <h3>📊 Probable Path</h3>
    <p>Base Case: {base_case}<br>
    Bear Case: {bear_case}<br>
    Bull Case: {bull_case}</p>

    <h3>⚖️ Trade Implications</h3>
    <p>Neutral Zone – consider Iron Condor around straddle range.</p>

    <h3>🌍 Context / News Check</h3>
    <p><b>📰 {news_title}</b><br>
    <a href="{news_link}">{news_link}</a><br>
    {news_comment}</p>

    <h3>✅ Summary</h3>
    <p>{summary}</p>

    <p style="color:gray; font-size:12px;">End of forecast</p>
    </body>
    </html>
    """

    return html
