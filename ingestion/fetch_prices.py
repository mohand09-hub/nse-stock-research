"""
fetch_prices.py
───────────────
Fetches daily price history (1-year, 1-day interval) and key fundamentals
for each ticker in the watchlist using the yfinance library.

Writes two types of JSON files to the landing volume:
  - Prices:    /Volumes/nse_stock_research/landing/raw_landing/prices/<TICKER>_<DATE>.json
  - Fundamentals: /Volumes/nse_stock_research/landing/raw_landing/fundamentals/<TICKER>_<DATE>.json

Each run produces one file per ticker per type, stamped with the run date.
Multiple runs on the same day overwrite each other (same filename).

The bronze layer (bronze.py) picks up these files via Auto Loader and
loads them into bronze.raw_prices and bronze.raw_fundamentals tables.

Run via: DABs job task `fetch_prices` (spark_python_task)
Dependencies: yfinance (installed via job environment spec)
"""
import json
import os
import time
from datetime import datetime, timezone

import yfinance as yf

# ─────────────────────────────────────────────
# Tickers must match the watchlist in seed_watchlist.py.
# ─────────────────────────────────────────────
TICKERS = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "BHARTIARTL.NS", "ITC.NS", "LT.NS", "SBIN.NS", "HINDUNILVR.NS",
    "MARUTI.NS", "SUNPHARMA.NS", "TMPV.NS", "ASIANPAINT.NS", "AXISBANK.NS",
]

# Landing volume subfolders for price and fundamental JSON files.
PRICES_PATH = "/Volumes/nse_stock_research/landing/raw_landing/prices"
FUNDAMENTALS_PATH = "/Volumes/nse_stock_research/landing/raw_landing/fundamentals"

# Fields extracted from yfinance Ticker.info for the fundamentals payload.
FUNDAMENTALS_FIELDS = [
    "shortName", "sector", "industry", "marketCap",
    "trailingPE", "forwardPE", "trailingEps", "dividendYield",
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "currency",
]


def fetch_ticker(ticker: str, run_date: str):
    """Fetch 1-year price history and fundamentals for a single ticker.

    Returns (price_records, fundamentals) where price_records is a list of
    dicts (one per trading day) and fundamentals is a single dict.
    """
    tk = yf.Ticker(ticker)

    # Download 1 year of daily OHLCV data.
    hist = tk.history(period="1y", interval="1d")
    price_records = [
        {
            "ticker": ticker,
            "date": idx.strftime("%Y-%m-%d"),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]),
        }
        for idx, row in hist.iterrows()
    ]

    # Extract selected fundamentals from the info dict.
    info = tk.info
    fundamentals = {"ticker": ticker, "as_of": run_date}
    for field in FUNDAMENTALS_FIELDS:
        fundamentals[field] = info.get(field)

    return price_records, fundamentals


def main():
    """Fetch prices and fundamentals for all tickers, write JSON to landing volume."""
    # Create landing subfolders if they don't exist.
    os.makedirs(PRICES_PATH, exist_ok=True)
    os.makedirs(FUNDAMENTALS_PATH, exist_ok=True)

    # Use UTC date so filenames are stable across timezone differences.
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for ticker in TICKERS:
        try:
            price_records, fundamentals = fetch_ticker(ticker, run_date)
        except Exception as e:
            # Log failure and continue to the next ticker — one bad ticker
            # should not abort the entire ingestion run.
            print(f"FAILED {ticker}: {e}")
            continue

        # Replace dots in ticker for filename safety (e.g. RELIANCE.NS -> RELIANCE_NS).
        safe_ticker = ticker.replace(".", "_")

        # Write price history as a JSON array (one element per trading day).
        price_path = f"{PRICES_PATH}/{safe_ticker}_{run_date}.json"
        with open(price_path, "w") as f:
            json.dump(price_records, f)
        print(f"wrote {len(price_records)} rows -> {price_path}")

        # Write fundamentals as a single JSON object.
        fund_path = f"{FUNDAMENTALS_PATH}/{safe_ticker}_{run_date}.json"
        with open(fund_path, "w") as f:
            json.dump(fundamentals, f)
        print(f"wrote fundamentals -> {fund_path}")

        # Be polite to Yahoo's unofficial API endpoint.
        time.sleep(1)


if __name__ == "__main__":
    main()
