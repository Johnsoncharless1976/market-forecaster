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
        """Perform a GET request to a Polygon endpoint with rate limiting."""
        if params is None:
            params = {}
        params["apiKey"] = self.api_key

        url = f"{self.base_url}{endpoint}"
        
        # Import rate limiter if available
        try:
            from stage7.rate_limiter import rate_limiter
            max_retries = 3
            
            for attempt in range(max_retries):
                # Apply rate limiting
                rate_limiter.wait_if_needed()
                
                response = requests.get(url, params=params)
                
                if response.status_code == 429:
                    # Handle rate limiting
                    retry_after = response.headers.get('Retry-After')
                    if attempt < max_retries - 1:
                        rate_limiter.handle_429(retry_after)
                        continue
                    else:
                        # Max retries exceeded
                        raise requests.HTTPError(
                            f"429 Too Many Requests after {max_retries} attempts for {url}\nDetails: {response.text}"
                        )
                        
                # Success or non-429 error
                rate_limiter.reset_retry_count()
                break
                
        except ImportError:
            # Rate limiter not available, use original logic
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
