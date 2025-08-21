# src/zen_rules.py
"""
ZeroDay Zen Forecast Rules Engine
---------------------------------
Encapsulates the 6 Zen pillars into functions.
Each function outputs a classification, which the
combine_bias() function uses to determine daily bias.
"""

from datetime import datetime

# --- 1. ATM Straddle Zone ---
def straddle_zone(spy_price: float, straddle_mid: float, width: int = 25) -> str:
    """
    Compare SPY price to ATM straddle range.
    """
    lower = straddle_mid - width
    upper = straddle_mid + width
    if lower <= spy_price <= upper:
        return "inside zone"
    elif spy_price > upper:
        return "breakout up"
    else:
        return "breakout down"


# --- 2. RSI Check ---
def rsi_check(rsi: float) -> str:
    """
    Interpret RSI into bias context.
    """
    if rsi >= 60:
        return "bullish"
    elif rsi <= 40:
        return "bearish"
    else:
        return "neutral"


# --- 3. Candle Structure ---
def candle_structure(last_candles: list) -> str:
    """
    Accepts list of OHLC candle dicts: [{"o":, "h":, "l":, "c":}, ...]
    Uses last few bars to classify structure.
    """
    if not last_candles or len(last_candles) < 3:
        return "neutral chop"

    closes = [c["c"] for c in last_candles[-3:]]
    if closes[-1] > closes[0] and all(closes[i] <= closes[i+1] for i in range(2)):
        return "trend bullish"
    elif closes[-1] < closes[0] and all(closes[i] >= closes[i+1] for i in range(2)):
        return "trend bearish"
    else:
        return "neutral chop"


# --- 4. Volatility Overlay ---
def volatility_overlay(vix: float, vvix: float, vix_change: float = 0, vvix_change: float = 0) -> str:
    """
    Uses VIX + VVIX to determine risk/hedging tone.
    """
    if vvix > 100 and vix_change > 0:
        return "hedging demand"
    elif vix_change < 0 and vvix_change <= 0:
        return "vol compression"
    elif vix_change > 0 and vvix_change < 0:
        return "divergence caution"
    else:
        return "stable"


# --- 5. Event Filter ---
def event_filter(events: list) -> str:
    """
    Events: list of dicts [{"time":"08:30","event":"CPI"}...]
    Returns "risk event pending" if any are today.
    """
    now = datetime.now().strftime("%Y-%m-%d")
    todays_events = [e for e in events if now in e.get("date", "")]
    return "risk event pending" if todays_events else "clear"


# --- 6. Headline Overlay ---
def headline_overlay(headline: str) -> str:
    """
    Classify headline sentiment.
    Very basic placeholder until NLP model integrated.
    """
    text = headline.lower()
    if any(word in text for word in ["hawkish", "tightening", "yields spike", "inflation hot"]):
        return "hawkish"
    elif any(word in text for word in ["rally", "bullish", "optimism", "strong earnings"]):
        return "bullish"
    elif any(word in text for word in ["geopolitical", "war", "default", "risk-off", "fear"]):
        return "risk-off"
    else:
        return "noise"


# --- Combine All ---
def combine_bias(straddle: str, rsi: str, candles: str, vol: str, events: str, headline: str) -> str:
    """
    Combine all Zen signals into a single bias classification.
    Priority order: event > headline > structure.
    """
    if events == "risk event pending":
        return "Neutral (Event Risk)"
    if headline in ["hawkish", "risk-off"]:
        return "Bearish (Headline Override)"
    if headline == "bullish":
        return "Bullish (Headline Override)"

    # Otherwise combine core signals
    if straddle == "inside zone" and candles == "neutral chop":
        return "Neutral"
    if rsi == "bullish" and straddle == "breakout up" and vol in ["vol compression", "stable"]:
        return "Bullish"
    if rsi == "bearish" and straddle == "breakout down" and vol in ["hedging demand", "divergence caution"]:
        return "Bearish"

    return "Neutral"
