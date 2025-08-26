# File: src/show_forecast.py
# Title: Stage 2 – Forecast viewer (stub)
# Commit Notes:
# - Lists the latest rows from FORECAST_DAILY, if any.
# - Safe to run even when empty.

from snowflake_conn import get_conn

def main():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
          SELECT SYMBOL, TO_CHAR(TRADE_DATE,'YYYY-MM-DD') AS TRADE_DATE,
                 PREDICTION, PROB_UP, PROB_DOWN, CONFIDENCE, MODEL_VERSION
          FROM FORECAST_DAILY
          ORDER BY TRADE_DATE DESC, SYMBOL
          LIMIT 20
        """)
        rows = cur.fetchall()
        if not rows:
            print({"note":"FORECAST_DAILY is empty (expected until Stage 3)."})
            return
        for r in rows:
            print({"symbol": r[0], "trade_date": r[1], "prediction": r[2],
                   "prob_up": r[3], "prob_down": r[4], "confidence": r[5], "model": r[6]})

if __name__ == "__main__":
    main()
