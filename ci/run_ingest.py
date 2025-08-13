import pathlib, sys, datetime as dt
from zoneinfo import ZoneInfo

# Ensure repo root on import path
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data_ingestion import fetch_previous_day_bars

if __name__ == '__main__':
    try:
        df = fetch_previous_day_bars(['SPY','QQQ','I:SPX'])
        print(df.to_string(index=False))

        # Save artifact
        tz = ZoneInfo('America/New_York')
        d = dt.datetime.now(tz).date().isoformat()
        out_dir = ROOT / 'out'
        out_dir.mkdir(exist_ok=True)
        csv_path = out_dir / f'ingest_{d}.csv'
        df.to_csv(csv_path, index=False)
        print(f'[artifact] {csv_path}')
    except Exception:
        import traceback; traceback.print_exc(); raise SystemExit(1)
