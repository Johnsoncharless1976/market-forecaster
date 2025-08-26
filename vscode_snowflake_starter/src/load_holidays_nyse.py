# File: vscode_snowflake_starter/src/load_holidays_nyse.py
# Title: Stage 1 – Seed/refresh HOLIDAYS_NYSE from XNYS calendar
# Commit Notes:
# - Computes trading days via pandas_market_calendars; derives non-trading weekdays as holidays.
# - Idempotent by range: DELETE then INSERT for [start,end].
# - CLI: --start 2015-01-01 --end 2035-12-31

import argparse, pandas as pd, numpy as np
import pandas_market_calendars as mcal
from datetime import datetime, date, timedelta
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

# Local connector wrapper
from vscode_snowflake_starter.src.snowflake_conn import get_conn  # type: ignore

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end",   default="2035-12-31")
    args = ap.parse_args()

    cal = mcal.get_calendar('XNYS')
    trading = cal.valid_days(start_date=args.start, end_date=args.end)
    trading = pd.to_datetime(trading).tz_localize(None).normalize().date

    # All weekdays in range
    dr = pd.date_range(args.start, args.end, freq="D")
    weekdays = pd.to_datetime(dr).tz_localize(None)
    weekdays = weekdays[weekdays.weekday < 5].normalize().date

    trading_set = set(trading)
    holidays = sorted([d for d in weekdays if d not in trading_set])

    rows = [(pd.to_datetime(d).date(), "MARKET_CLOSED") for d in holidays]

    with get_conn() as conn:
        cur = conn.cursor()
        # idempotent for the window
        cur.execute("DELETE FROM HOLIDAYS_NYSE WHERE HOLIDAY BETWEEN %s AND %s", (args.start, args.end))
        if rows:
            cur.executemany("INSERT INTO HOLIDAYS_NYSE (HOLIDAY, NOTE) VALUES (%s,%s)", rows)
        # echo count
        cur.execute("SELECT COUNT(*) FROM HOLIDAYS_NYSE")
        print({"event":"holidays_loaded","total": cur.fetchone()[0], "start":args.start, "end":args.end})
        cur.close()

if __name__ == "__main__":
    main()