#!/usr/bin/env python3
# documentation: libraries https://github.com/ranaroussi/yfinance
import argparse
import sys
from pathlib import Path
from io import StringIO
import contextlib

import yfinance as yf
import pandas as pd


def download_to_csv(ticker: str, start: str | None, end: str | None, output: str | None):
    # Create csv directory if it doesn't exist
    csv_dir = Path("csv")
    csv_dir.mkdir(exist_ok=True)

    # Default output filename: csv/TICKER.csv (uppercased)
    if output is None:
        output_path = csv_dir / f"{ticker.upper()}.csv"
    else:
        output_path = Path(output)

    # Build the yfinance call
    kwargs = {}
    if start:
        kwargs["start"] = start
    if end:
        kwargs["end"] = end

    # Capture stderr to save HTML errors from yfinance
    stderr_capture = StringIO()

    try:
        with contextlib.redirect_stderr(stderr_capture):
            data = yf.download(ticker, **kwargs, progress=False)
    except Exception as e:
        # Check captured stderr for HTML before re-raising
        stderr_output = stderr_capture.getvalue()
        if stderr_output and ('<html' in stderr_output.lower() or '<!doctype' in stderr_output.lower()):
            error_dir = Path("errors")
            error_dir.mkdir(exist_ok=True)
            error_file = error_dir / f"{ticker.replace('/', '_')}_error.html"
            with open(error_file, 'w') as f:
                f.write(stderr_output)
            print(f"HTML error saved to {error_file}")
        raise Exception(f"Error downloading data for {ticker}: {e}")

    # Also check stderr for HTML even if no exception (some errors don't raise)
    stderr_output = stderr_capture.getvalue()
    if stderr_output and ('<html' in stderr_output.lower() or '<!doctype' in stderr_output.lower()):
        error_dir = Path("errors")
        error_dir.mkdir(exist_ok=True)
        error_file = error_dir / f"{ticker.replace('/', '_')}_error.html"
        with open(error_file, 'w') as f:
            f.write(stderr_output)
        print(f"HTML error saved to {error_file}")

    if data.empty:
        raise Exception(f"No data returned for ticker '{ticker}'. Check the symbol or date range.")

    # Reset index so the Date is a normal column
    data.reset_index(inplace=True)

    # Flatten multi-level column headers if they exist
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    # Save as CSV
    try:
        data.to_csv(output_path, index=False)
    except Exception as e:
        raise Exception(f"Error writing CSV to {output_path}: {e}")

    print(f"Saved {len(data)} rows to {output_path}")
    return output_path

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
