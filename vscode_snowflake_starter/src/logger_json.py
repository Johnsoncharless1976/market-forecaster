import json, sys, datetime, os

def log(**fields):
    payload = {
        "ts": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "stage": os.getenv("STAGE","stage1"),
        **fields
    }
    print(json.dumps(payload, default=str), flush=True)