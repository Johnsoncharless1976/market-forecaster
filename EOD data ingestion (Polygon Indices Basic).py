
# src/data_ingestion.py
import os
import datetime as dt
import requests
import pandas as pd
from pathlib import Path
from config import POLYGON_API_KEY

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

BASE = "https://api.polygon.io/v2/aggs/ticker"

def fetch_eod(symbol: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch end-of-day aggregates (1 day bars) between start and end (YYYY-MM-DD).
    Works with Polygon Indices Basic (EOD only).
    """
    if not POLYGON_API_KEY:
        raise RuntimeError("Missing POLYGON_API_KEY. Set it in .env")

    url = f"{BASE}/{symbol}/range/1/day/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "results" not in j:
        raise RuntimeError(f"No results for {symbol}: {j}")
    df = pd.DataFrame(j["results"])
    # Convert epoch ms -> date
    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
    df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume","n":"trades","vw":"vwap"}, inplace=True)
    cols = ["date","open","high","low","close","volume","vwap","trades"]
    return df[cols]

def save_csv(df: pd.DataFrame, symbol: str):
    out = DATA_DIR / f"{symbol}_eod.csv"
    df.to_csv(out, index=False)
    print(f"Saved {out.resolve()} ({len(df)} rows)")

if __name__ == "__main__":
    # Examples (adjust as needed). For SPX index via Polygon, use ^GSPC equivalent in indices namespace if supported in your plan.
    # Polygon uses index tickers like: "I:SPX" for S&P 500 index
    # ES futures require a different subscription; for MVP we stick to indices EOD.
    start = "2020-01-01"
    end   = dt.date.today().isoformat()

    for symbol in ["I:SPX", "I:VIX"]:
        try:
            df = fetch_eod(symbol, start, end)
            save_csv(df, symbol.replace(":","_"))
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
