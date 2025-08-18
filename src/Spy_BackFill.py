import requests
import pandas as pd

POLYGON_API_KEY = "jYeR6QVhnmhFe7V0aQm1_ZuGM6QawAEO"

def fetch_spy_polygon(start="2010-01-01", end="2025-08-15"):
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start}/{end}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "apiKey": POLYGON_API_KEY
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json().get("results", [])
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError("No data returned for SPY from Polygon")
    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
    return df[["date", "c"]].rename(columns={"c": "close"})


if __name__ == "__main__":
    spy_df = fetch_spy_polygon()
    print(spy_df.head())
    print(f"\n✅ Retrieved {len(spy_df)} rows from Polygon for SPY")
spy_df.to_csv("spy.csv", index=False, header=False)
print("\n💾 Saved SPY history to spy.csv")

