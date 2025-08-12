from pathlib import Path
from dotenv import load_dotenv
import os, re

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path, override=True)

raw = os.getenv("POLYGON_API_KEY", "")
POLYGON_API_KEY = raw.strip().strip('"').strip("'")  # trim whitespace/quotes

if not POLYGON_API_KEY or not re.fullmatch(r"[A-Za-z0-9_\-]+", POLYGON_API_KEY):
    raise RuntimeError(
        "POLYGON_API_KEY looks missing or malformed. Check .env (no quotes/spaces) and copy your actual Polygon API key."
    )



