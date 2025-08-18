import yfinance as yf

def fetch_es_yahoo(start="2010-01-01", end="2025-08-15"):
    """Fetch ES futures daily closes from Yahoo Finance"""
    es = yf.download("ES=F", start=start, end=end, interval="1d")
    if es.empty:
        raise ValueError("No data returned for ES=F from Yahoo Finance")
    es = es.reset_index()[["Date", "Close"]]
    es.columns = ["date", "close"]
    es["date"] = es["date"].dt.date
    return es
if __name__ == "__main__":
    es_df = fetch_es_yahoo()
    print(es_df.head())
    print(f"\n✅ Retrieved {len(es_df)} rows from Yahoo Finance for ES")
es_df.to_csv("es.csv", index=False, header=False)
print("\n💾 Saved ES history to es.csv")
