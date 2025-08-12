import requests
from typing import Dict, Any

BASE = "https://api.polygon.io"
TIMEOUT = 30

class PolygonHTTP:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get(self, path: str, params: Dict[str, Any] | None = None):
        params = dict(params or {})
        params["apiKey"] = self.api_key
        url = f"{BASE}{path}"
        r = requests.get(url, params=params, timeout=TIMEOUT)
        if not r.ok:
            msg = ""
            try:
                msg = r.json().get("error", "")
            except Exception:
                msg = r.text[:300]
            raise requests.HTTPError(
                f"{r.status_code} {r.reason} for {url}\nDetails: {msg}"
            )
        return r.json()

