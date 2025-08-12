from src.data_ingestion import fetch_previous_day_bars
try:
    df = fetch_previous_day_bars(["SPY","QQQ"])
    print(df.to_string(index=False))
except Exception as e:
    import traceback; traceback.print_exc(); raise SystemExit(1)
