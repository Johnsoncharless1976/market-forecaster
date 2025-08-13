import os, sys, json, urllib.request
k = os.getenv('POLYGON_API_KEY')
print('POLYGON_API_KEY present:', bool(k), 'len:', (len(k) if k else None), 'prefix:', ((k[:6]+'') if k else None))
if not k:
    sys.exit('FAIL: Missing POLYGON_API_KEY in Project  Settings  CI/CD  Variables. If it is Protected, unprotect it or protect the branch.')

url = 'https://api.polygon.io/v3/reference/tickers?limit=1&apiKey=' + k
try:
    with urllib.request.urlopen(url, timeout=20) as r:
        code = r.getcode()
        body = r.read(200).decode('utf-8', 'ignore')
except Exception as e:
    sys.exit(f'FAIL: Network error hitting Polygon: {e!r}')

print('Polygon status:', code)
print('Body:', body)
if code != 200:
    sys.exit('FAIL: Polygon rejected the key (revoked/wrong). Update the CI variable value.')
print('Preflight OK')
