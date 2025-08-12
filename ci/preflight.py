import os, sys, requests
k = os.getenv("POLYGON_API_KEY")
print("CI var POLYGON_API_KEY present:", bool(k), "len:", (len(k) if k else None), "prefix:", ((k[:6] + "") if k else None))
if not k:
    sys.exit("FAIL: Missing POLYGON_API_KEY in Project  Settings  CI/CD  Variables. If the var is Protected, unprotect it or protect the branch.")
r = requests.get("https://api.polygon.io/v3/reference/tickers?limit=1&apiKey="+k, timeout=20)
print("Polygon /v3/reference/tickers status:", r.status_code)
if not r.ok:
    print("Body:", r.text[:200])
    sys.exit("FAIL: Polygon rejected the key (revoked/wrong). Update the CI variable value.")
print("Preflight OK")
