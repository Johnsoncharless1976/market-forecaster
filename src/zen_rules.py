# src/zen_rules.py
import datetime

def bias_logic(spx, vix, vvix):
    if spx is None or vix is None:
        return "Neutral"

    if vix > 20:
        return "Cautious / Bearish"
    elif vix < 14:
        return "Bullish"
    return "Neutral"

def key_levels(spx):
    if spx is None:
        return {"Resistance": None, "Support": None}

    res = round(spx * 1.025, 2)
    sup = round(spx * 0.975, 2)
    return {"Resistance": res, "Support": sup}

def probable_path(spx, levels):
    if spx is None:
        return "Base Case: Data unavailable"

    return (
        f"Base Case: Chop between {levels['Support']}-{levels['Resistance']}.\n"
        f"Bear Case: If <{levels['Support']}, watch {round(levels['Support']*0.97,2)}.\n"
        f"Bull Case: If >{levels['Resistance']}, opens {round(levels['Resistance']*1.03,2)}."
    )

def trade_implications(bias):
    if "Bullish" in bias:
        return "Favor bull credit spreads or debit call spreads."
    elif "Bearish" in bias:
        return "Favor bear credit spreads or debit put spreads."
    else:
        return "Neutral Zone – consider Iron Condor around straddle range."

def news_context():
    # Placeholder for future scraper integration
    return "📰 Markets steady ahead of Powell speech\nhttps://www.reuters.com/markets/\nZen read → noise"

# 🔑 Main entry point
def generate_forecast(data: dict) -> str:
    spx = data.get("SPX")
    es = data.get("ES")
    vix = data.get("VIX")
    vvix = data.get("VVIX")

    bias = bias_logic(spx, vix, vvix)
    levels = key_levels(spx)
    path = probable_path(spx, levels)
    trades = trade_implications(bias)
    news = news_context()

    text = []
    text.append(f"SPX Spot: {spx}")
    text.append(f"/ES: {es}")
    text.append(f"VIX: {vix}")
    text.append(f"VVIX: {vvix}\n")
    text.append("🧠 Bias")
    text.append(bias + "\n")
    text.append("🔑 Key Levels")
    text.append(f"Resistance: {levels['Resistance']}")
    text.append(f"Support: {levels['Support']}\n")
    text.append("📊 Probable Path")
    text.append(path + "\n")
    text.append("⚖️ Trade Implications")
    text.append(trades + "\n")
    text.append("🌍 Context / News Check")
    text.append(news)

    return "\n".join(text)
