import pathlib, sys
# Ensure repo root is on sys.path (works even if PYTHONPATH isn't set)
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data_ingestion import fetch_previous_day_bars

if __name__ == '__main__':
    try:
        df = fetch_previous_day_bars(['SPY','QQQ'])
        print(df.to_string(index=False))
    except Exception:
        import traceback; traceback.print_exc(); raise SystemExit(1)
