# File: src/show_metrics.py
# Title: Stage 1  Metrics Snapshot (MARKET_OHLCV)
# Commit Notes:
# - Uses ROW_COUNT alias (avoids reserved word ROWS).
# - Weekend check via DAYOFWEEKISO IN (6,7).

from snowflake_conn import get_conn

SYMS = ["^VIX","^VVIX","^GSPC","ES=F"]

def main():
    with get_conn() as conn:
        cur = conn.cursor()
        # Summary per symbol
        cur.execute("""
            SELECT SYMBOL,
                   COUNT(*) AS ROW_COUNT,
                   TO_CHAR(MAX(TRADE_DATE),'YYYY-MM-DD') AS MAX_DATE
            FROM MARKET_OHLCV
            WHERE SYMBOL IN (%s,%s,%s,%s)
            GROUP BY SYMBOL
            ORDER BY SYMBOL
        """, SYMS)
        for sym, cnt, maxd in cur.fetchall():
            print({"symbol": sym, "row_count": int(cnt), "max_date": maxd})

        # Any weekend dates? (ISO: Sat=6, Sun=7)
        cur.execute("""
            SELECT SYMBOL, TO_CHAR(TRADE_DATE,'YYYY-MM-DD') AS D, DAYOFWEEKISO(TRADE_DATE) AS DOW
            FROM MARKET_OHLCV
            WHERE DAYOFWEEKISO(TRADE_DATE) IN (6,7)
              AND SYMBOL IN (%s,%s,%s,%s)
            LIMIT 5
        """, SYMS)
        odd = cur.fetchall()
        if odd:
            print({"warning":"found_weekend_dates","samples": odd})
        else:
            print({"check":"weekday_only_ok"})

if __name__ == "__main__":
    main()
