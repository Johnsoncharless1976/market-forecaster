import os
import requests
from dotenv import load_dotenv
load_dotenv()



class PolygonClient:
    """Thin wrapper for the Polygon.io REST API."""

    def __init__(self, api_key: str | None = None):
        self.base_url = "https://api.polygon.io"
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("Polygon API key not set. Please set POLYGON_API_KEY env var.")

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
            # Wrap with more context for debugging in CI
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {url}\nDetails: {response.text}"
            ) from e

        return response.json()


# Pre-configured singleton instance for convenience
client = PolygonClient()
