# File: vscode_snowflake_starter/src/exec_audit_summary.py
# Title: Stage 1  Executive Audit Summary (PASS/FAIL table)
# Commit Notes:
# - Queries Snowflake directly (no parsing of prior .out files).
# - Prints PASS/FAIL table and writes REPORT_EXEC.md + summary.csv
# - If Snowflake is unreachable, exits cleanly with a one-line error.

import os, sys, json, csv
from datetime import datetime, date, timedelta
import pandas as pd
import pandas_market_calendars as mcal

# ensure we can import our Snowflake connector helper
here = os.path.dirname(__file__)
sys.path.insert(0, here)
from snowflake_conn import get_conn

BASIC_CHECKS_SQL = """
WITH
dups AS (
  SELECT COUNT(*) AS violations
  FROM (
    SELECT SYMBOL, TRADE_DATE, COUNT(*) c
    FROM MARKET_OHLCV
    GROUP BY 1,2
    HAVING COUNT(*) > 1
  )
),
wknd AS (
  SELECT COUNT(*) AS violations
  FROM MARKET_OHLCV
  WHERE DAYOFWEEKISO(TRADE_DATE) IN (6,7)
),
nulls AS (
  SELECT COUNT(*) AS violations
  FROM MARKET_OHLCV
  WHERE OPEN IS NULL OR HIGH IS NULL OR LOW IS NULL OR CLOSE IS NULL OR ADJ_CLOSE IS NULL
),
ohlc AS (
  SELECT COUNT(*) AS violations
  FROM MARKET_OHLCV
  WHERE NOT (
    LOW  <= LEAST(OPEN, CLOSE, ADJ_CLOSE)
    AND HIGH >= GREATEST(OPEN, CLOSE, ADJ_CLOSE)
    AND HIGH >= LOW
  )
)
SELECT TO_JSON(OBJECT_CONSTRUCT('check','duplicates','violations',(SELECT violations FROM dups)))          AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','weekend_rows','violations',(SELECT violations FROM wknd)))        AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','null_prices','violations',(SELECT violations FROM nulls)))        AS JSON_ROW
UNION ALL
SELECT TO_JSON(OBJECT_CONSTRUCT('check','ohlc_sanity','violations',(SELECT violations FROM ohlc)))         AS JSON_ROW
;
"""

DATA_RANGE_SQL = """
SELECT SYMBOL, MIN(TRADE_DATE) AS MIN_DATE, MAX(TRADE_DATE) AS MAX_DATE
FROM MARKET_OHLCV 
GROUP BY SYMBOL
ORDER BY SYMBOL
"""

MISSING_DATES_SQL = """
WITH cal AS (
  SELECT '{symbol}' AS SYMBOL, '{date}' AS EXPECTED_DATE
  FROM (SELECT 1)  -- placeholder, will be replaced with actual trading dates
)
SELECT c.SYMBOL, c.EXPECTED_DATE, 
       CASE WHEN m.TRADE_DATE IS NOT NULL THEN 1 ELSE 0 END AS PRESENT
FROM cal c
LEFT JOIN MARKET_OHLCV m 
  ON m.SYMBOL = c.SYMBOL AND m.TRADE_DATE = c.EXPECTED_DATE
"""

def get_nyse_trading_calendar(start_date, end_date):
    """Generate NYSE trading calendar for date range."""
    try:
        nyse = mcal.get_calendar('NYSE')
        trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)
        
        # Create calendar snapshot
        calendar_data = []
        current_date = start_date
        while current_date <= end_date:
            is_trading_day = current_date in trading_days.date
            holiday_name = ''
            
            # Check if it's a holiday (weekday but not trading day)
            if current_date.weekday() < 5 and not is_trading_day:  # Mon-Fri but not trading
                # Try to identify holiday name from market calendar
                try:
                    holidays = nyse.holidays()
                    if current_date in holidays.date:
                        holiday_name = 'Market Holiday'  # Generic name since pandas_market_calendars doesn't provide names
                except:
                    holiday_name = 'Holiday'
            
            calendar_data.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'is_trading_day': 1 if is_trading_day else 0,
                'holiday_name': holiday_name
            })
            current_date = current_date + timedelta(days=1)
            
        return trading_days, calendar_data
    except Exception as e:
        print(f"Warning: Could not build NYSE calendar: {e}")
        return None, []

def analyze_weekday_gaps(conn, symbols_ranges, trading_days):
    """Analyze gaps and classify as Holiday vs TrueGap."""
    gap_details = []
    
    for symbol, min_date, max_date in symbols_ranges:
        print(f"Analyzing gaps for {symbol} ({min_date} to {max_date})")
        
        # Get expected trading days for this symbol's range
        symbol_start = pd.to_datetime(min_date).date()
        symbol_end = pd.to_datetime(max_date).date()
        
        if trading_days is not None:
            # Use NYSE calendar
            symbol_trading_days = [d for d in trading_days.date 
                                 if symbol_start <= d <= symbol_end]
        else:
            # Fallback: simple weekday calendar
            symbol_trading_days = []
            current = symbol_start
            while current <= symbol_end:
                if current.weekday() < 5:  # Mon-Fri
                    symbol_trading_days.append(current)
                current = current + timedelta(days=1)
        
        # Check each expected trading day
        for expected_date in symbol_trading_days:
            expected_date_str = expected_date.strftime('%Y-%m-%d')
            
            # Check if data exists for this symbol+date
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) as present
                FROM MARKET_OHLCV 
                WHERE SYMBOL = %s AND TRADE_DATE = %s
            """, (symbol, expected_date_str))
            
            result = cur.fetchone()
            present = 1 if result and result[0] > 0 else 0
            
            if present == 0:  # Missing data
                # Determine if it's a holiday or true gap
                is_weekday = expected_date.weekday() < 5
                is_trading_day = trading_days is None or expected_date in trading_days.date
                
                if is_weekday and not is_trading_day:
                    reason = 'Holiday'
                elif is_trading_day:
                    reason = 'TrueGap'
                else:
                    reason = 'Holiday'  # Weekend or other non-trading day
                
                gap_details.append({
                    'date': expected_date_str,
                    'symbol': symbol,
                    'expected_trading_day': 1 if is_trading_day else 0,
                    'present': 0,
                    'reason': reason
                })
    
    return gap_details

def main():
    # Pick/create an output folder
    root = os.path.abspath(os.path.join(os.path.dirname(here), "audit_exports"))
    os.makedirs(root, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = os.path.join(root, f"stage1_exec_{stamp}")
    os.makedirs(outdir, exist_ok=True)

    try:
        with get_conn() as conn:
            # 1. Run basic checks (duplicates, nulls, OHLC, weekends)
            cur = conn.cursor()
            cur.execute(BASIC_CHECKS_SQL)
            basic_rows = cur.fetchall()
            
            # 2. Get data range for each symbol
            cur.execute(DATA_RANGE_SQL)
            symbol_ranges = cur.fetchall()
            
            if not symbol_ranges:
                print("No data found in MARKET_OHLCV table")
                return
                
    except Exception as e:
        print("Snowflake unreachable  cannot produce counts. " + str(e))
        return

    # Parse basic checks
    basic_checks = []
    for (json_row,) in basic_rows:
        obj = json.loads(json_row) if isinstance(json_row, str) else json_row
        check = obj.get("check")
        viol = int(obj.get("violations", 0))
        status = "PASS" if viol == 0 else "FAIL"
        basic_checks.append((check, viol, status))

    # 3. Build NYSE trading calendar
    print(f"Building NYSE trading calendar for {len(symbol_ranges)} symbols...")
    overall_start = min(pd.to_datetime(r[1]) for r in symbol_ranges).date()
    overall_end = max(pd.to_datetime(r[2]) for r in symbol_ranges).date()
    
    trading_days, calendar_data = get_nyse_trading_calendar(overall_start, overall_end)
    
    # 4. Analyze weekday gaps with holiday classification
    print("Analyzing weekday gaps...")
    with get_conn() as conn:
        gap_details = analyze_weekday_gaps(conn, symbol_ranges, trading_days)
    
    # 5. Count holiday vs true gaps
    holiday_weekdays = sum(1 for gap in gap_details if gap['reason'] == 'Holiday')
    true_weekday_gaps = sum(1 for gap in gap_details if gap['reason'] == 'TrueGap')
    total_weekday_gaps = holiday_weekdays + true_weekday_gaps
    
    # Reconciliation check
    print(f"Gap analysis: {total_weekday_gaps} total weekday gaps = {holiday_weekdays} holidays + {true_weekday_gaps} true gaps")
    
    # 6. Determine overall status
    if true_weekday_gaps == 0:
        overall_status = "PASS" if holiday_weekdays == 0 else "PASS"  # Green even with holidays
        status_reason = f"No true gaps detected ({holiday_weekdays} holiday weekdays excluded)"
    else:
        overall_status = "FAIL"  # Red for any true gaps
        status_reason = f"{true_weekday_gaps} true gaps require attention"
    
    # 7. Build final check results
    all_checks = basic_checks + [
        ("weekday_gaps", total_weekday_gaps, "PASS" if total_weekday_gaps == 0 else "FAIL"),
        ("holiday_weekdays", holiday_weekdays, "PASS"),  # Always pass - informational
        ("true_weekday_gaps", true_weekday_gaps, "PASS" if true_weekday_gaps == 0 else "FAIL")
    ]

    # 8. Print executive table
    print(f"\n# Stage 1 Audit Executive Report ({stamp})")
    print(f"\n**EXECUTIVE STATUS: {overall_status}** - {status_reason}")
    print("\n| Check | Violations | Status |")
    print("|---|---:|:---:|")
    for check, viol, status in sorted(all_checks, key=lambda t: t[0]):
        print(f"| {check} | {viol} | {status} |")

    # 9. Write artifacts
    # summary.csv
    csv_path = os.path.join(outdir, "summary.csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        f.write("check,violations,status\n")
        for check, viol, status in sorted(all_checks):
            f.write(f"{check},{viol},{status}\n")
    
    # weekday_gaps_detail.csv
    detail_csv_path = os.path.join(outdir, "weekday_gaps_detail.csv")
    with open(detail_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'symbol', 'expected_trading_day', 'present', 'reason'])
        writer.writeheader()
        writer.writerows(gap_details)
    
    # calendar_snapshot.csv
    calendar_csv_path = os.path.join(outdir, "calendar_snapshot.csv")
    with open(calendar_csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'is_trading_day', 'holiday_name'])
        writer.writeheader()
        writer.writerows(calendar_data)
    
    # REPORT_EXEC.md
    md_path = os.path.join(outdir, "REPORT_EXEC.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Stage 1 Audit Executive Report ({stamp})\n\n")
        f.write(f"**EXECUTIVE STATUS: {overall_status}** - {status_reason}\n\n")
        f.write("## Summary\n\n")
        f.write("| Check | Violations | Status |\n|---|---:|:---:|\n")
        for check, viol, status in sorted(all_checks):
            f.write(f"| {check} | {viol} | {status} |\n")
        f.write(f"\n## Gap Analysis\n\n")
        f.write(f"- **Total weekday gaps**: {total_weekday_gaps}\n")
        f.write(f"- **Holiday weekdays**: {holiday_weekdays} (market closed)\n")
        f.write(f"- **True gaps**: {true_weekday_gaps} (missing trading day data)\n\n")
        f.write("_Scope_: Stage 1 (Ingestion Integrity & Controls with NYSE holiday calendar).\n")

    print(f"\nSaved:")
    print(f" - {md_path}")
    print(f" - {csv_path}")
    print(f" - {detail_csv_path} ({len(gap_details)} gap records)")
    print(f" - {calendar_csv_path} ({len(calendar_data)} calendar days)")

if __name__ == "__main__":
    main()
