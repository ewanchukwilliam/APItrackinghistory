#!/usr/bin/env python3
# documentation: libraries https://github.com/ranaroussi/yfinance
import argparse
import sys
from pathlib import Path

import yfinance as yf


def download_to_csv(ticker: str, start: str | None, end: str | None, output: str | None):
    # Default output filename: TICKER.csv (uppercased)
    if output is None:
        output_path = Path(f"{ticker.upper()}.csv")
    else:
        output_path = Path(output)

    # Build the yfinance call
    kwargs = {}
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end

    try:
        data = yf.download(ticker, **kwargs)
    except Exception as e:
        raise Exception(f"Error downloading data for {ticker}: {e}")

    if data.empty:
        raise Exception(f"No data returned for ticker '{ticker}'. Check the symbol or date range.")

    # Reset index so the Date is a normal column
    data.reset_index(inplace=True)

    # Save as CSV
    try:
        data.to_csv(output_path, index=False)
    except Exception as e:
        raise Exception(f"Error writing CSV to {output_path}: {e}")

    print(f"Saved {len(data)} rows to {output_path}")

def main():

    parser = argparse.ArgumentParser(
        description="Download historical price data from Yahoo Finance and save as CSV."
    )
    parser.add_argument("ticker", help="Ticker symbol, e.g. SPY, AAPL, TSLA")
    parser.add_argument(
        "--start",
        help="Start date in YYYY-MM-DD (optional, default: Yahoo max history)",
        default=None,
    )
    parser.add_argument(
        "--end",
        help="End date in YYYY-MM-DD (optional, default: today)",
        default=None,
    )
    parser.add_argument(
        "--output",
        help="Output CSV filename (optional, default: TICKER.csv)",
        default=None,
    )

    args = parser.parse_args()

    try:
        download_to_csv(args.ticker, args.start, args.end, args.output)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
