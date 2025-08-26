import pandas as pd

REQUIRED = ["symbol","trade_date","open","high","low","close","adj_close","volume","source"]

def _reason_row(row):
    reasons=[]
    if any(pd.isna(row[c]) for c in ["open","high","low","close","adj_close"]):
        reasons.append("null_price")
    if pd.to_datetime(row["trade_date"]).weekday() > 4:
        reasons.append("weekend")
    lo = row["low"]; hi=row["high"]
    mn = min(row["open"], row["close"], row["adj_close"])
    mx = max(row["open"], row["close"], row["adj_close"])
    if not (lo <= mn and hi >= mx and hi >= lo):
        reasons.append("inconsistent_ohlc")
    return ",".join(reasons)

def split_valid_invalid(df: pd.DataFrame):
    if not set(REQUIRED).issubset(df.columns):
        missing = list(set(REQUIRED) - set(df.columns))
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None).dt.date
    reasons = df.apply(_reason_row, axis=1)
    bad_mask = reasons.str.len() > 0
    bad = df[bad_mask].copy()
    if not bad.empty:
        bad["reason"] = reasons[bad_mask].values
    good = df[~bad_mask].copy()
    return good, bad