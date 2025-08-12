import datetime as dt
import pandas as pd
from src.config import POLYGON_API_KEY
from src.polygon_client import PolygonHTTP

# Example pipeline: fetch yesterday’s SPX (index) and SPY (ETF) daily bars
# You can change symbols or add endpoints as needed.

def _iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def fetch_previous_day_bars(symbols: list[str]) -> pd.DataFrame:
    client = PolygonHTTP(POLYGON_API_KEY)
    end = dt.date.today()
    start = end - dt.timedelta(days=7)  # fetch a small window, then slice last row

    rows = []
    for sym in symbols:
        # Aggregates: 1-day bars
        data = client.get(
            f"/v2/aggs/ticker/{sym}/range/1/day/{_iso_date(start)}/{_iso_date(end)}",
            params={"adjusted": "true", "limit": 50}
        )
        results = data.get("results", []) or []
        if results:
            last = results[-1]
            rows.append({
                "symbol": sym,
                "date": dt.datetime.utcfromtimestamp(last["t"]/1000).date().isoformat(),
                "open": last["o"],
                "high": last["h"],
                "low": last["l"],
                "close": last["c"],
                "volume": last.get("v"),
            })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    # Example run
    df = fetch_previous_day_bars(["SPY", "QQQ"])
    print(df.to_string(index=False))
