import os
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

# Load variables from .env (if it exists in project root)
load_dotenv()


class PolygonClient:
    """Thin wrapper for the Polygon.io REST API."""

    def __init__(self, api_key: str | None = None):
        self.base_url = "https://api.polygon.io"
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")

        if not self.api_key:
            raise ValueError(
                "Polygon API key not set. "
                "Please set POLYGON_API_KEY in a .env file or environment variable."
            )

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        """Perform a GET request to a Polygon endpoint."""
        if params is None:
            params = {}
        params["apiKey"] = self.api_key

        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, params=params)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {url}\nDetails: {response.text}"
            ) from e

        return response.json()


# Pre-configured singleton instance for convenience
client = PolygonClient()


# ---- Added CSV-save logic ----
def fetch_previous_day_bars(tickers):
    """Fetch previous day OHLC bars for given tickers.
    Skips tickers that are not available in current plan (403 errors)."""

    today = date.today()
    start = today - timedelta(days=7)
    end = today

    results = {}

    for ticker in tickers:
        try:
            data = client.get(
                f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
            )
            if "results" in data and data["results"]:
                results[ticker] = data["results"]
                print(f"[INFO] Retrieved {len(data['results'])} bars for {ticker}")
            else:
                print(f"[WARN] No results returned for {ticker}")
        except requests.HTTPError as e:
            if "403" in str(e):
                print(f"[WARN] Skipping {ticker}: 403 Forbidden (not in plan).")
                continue
            else:
                raise

    return results


def save_results_to_csv(results, out_dir="out"):
    """Save successful ticker data to CSVs in `out/` folder."""

    os.makedirs(out_dir, exist_ok=True)

    for ticker, rows in results.items():
        df = pd.DataFrame(rows)
        out_path = os.path.join(out_dir, f"{ticker}_bars.csv")
        df.to_csv(out_path, index=False)
        print(f"[INFO] Saved {ticker} data → {out_path}")


if __name__ == "__main__":
    tickers = ["SPY", "QQQ"]  # adjust as needed
    results = fetch_previous_day_bars(tickers)
    save_results_to_csv(results)
