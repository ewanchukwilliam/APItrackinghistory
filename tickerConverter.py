#!/usr/bin/env python3
# documentation: libraries https://github.com/ranaroussi/yfinance
import argparse
import sys
from pathlib import Path
from io import StringIO
import contextlib

import yfinance as yf
import pandas as pd


class CSVDataManager:
    """Manager class for downloading and saving financial data to CSV files"""

    def __init__(self, base_dir: Path | str = "."):
        """
        Initialize the CSV Data Manager

        Args:
            base_dir: Base directory for all data folders (default: current directory)
        """
        self.base_dir = Path(base_dir)
        self.pricing_dir = self.base_dir / "pricing"
        self.options_dir = self.base_dir / "options"
        self.errors_dir = self.base_dir / "errors"
        self.graphs_dir = self.base_dir / "graphs"

    def _ensure_dir(self, directory: Path) -> Path:
        """Ensure a directory exists, create if it doesn't"""
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _sanitize_ticker(self, ticker: str) -> str:
        """Sanitize ticker symbol for use in filenames"""
        return ticker.upper().replace('/', '_')

    def _get_pricing_path(self, ticker: str, custom_output: str | None = None) -> Path:
        """Get the output path for pricing data"""
        if custom_output:
            return Path(custom_output)
        self._ensure_dir(self.pricing_dir)
        return self.pricing_dir / f"{self._sanitize_ticker(ticker)}.csv"

    def _get_options_path(self, ticker: str, custom_output: str | None = None) -> Path:
        """Get the output path for options data"""
        if custom_output:
            return Path(custom_output)
        self._ensure_dir(self.options_dir)
        return self.options_dir / f"{self._sanitize_ticker(ticker)}.csv"

    def _get_error_path(self, ticker: str) -> Path:
        """Get the output path for error HTML files"""
        self._ensure_dir(self.errors_dir)
        return self.errors_dir / f"{self._sanitize_ticker(ticker)}_error.html"

    def _save_html_error(self, ticker: str, html_content: str) -> Path:
        """Save HTML error content to a file"""
        error_file = self._get_error_path(ticker)
        with open(error_file, 'w') as f:
            f.write(html_content)
        print(f"HTML error saved to {error_file}")
        return error_file

    def download_price_data(self, ticker: str, start: str | None = None,
                           end: str | None = None, output: str | None = None) -> Path:
        """
        Download historical price data from Yahoo Finance and save to CSV

        Args:
            ticker: Stock ticker symbol
            start: Start date in YYYY-MM-DD format
            end: End date in YYYY-MM-DD format
            output: Optional custom output path

        Returns:
            Path to saved CSV file
        """
        output_path = self._get_pricing_path(ticker, output)

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
                self._save_html_error(ticker, stderr_output)
            raise Exception(f"Error downloading data for {ticker}: {e}")

        # Also check stderr for HTML even if no exception (some errors don't raise)
        stderr_output = stderr_capture.getvalue()
        if stderr_output and ('<html' in stderr_output.lower() or '<!doctype' in stderr_output.lower()):
            self._save_html_error(ticker, stderr_output)

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

    def save_options_data(self, ticker: str, options_data: dict, output: str | None = None) -> Path:
        """
        Save Market Data API options chain response to CSV

        Args:
            ticker: Stock ticker symbol
            options_data: JSON response from Market Data API
            output: Optional custom output path

        Returns:
            Path to saved CSV file
        """
        output_path = self._get_options_path(ticker, output)

        # Check if response has error
        if options_data.get('s') == 'error':
            raise Exception(f"API Error: {options_data.get('errmsg', 'Unknown error')}")

        # Check if no data available
        if options_data.get('s') == 'no_data':
            raise Exception(f"No options data available for ticker '{ticker}' in the requested date range")

        # Market Data API returns data in columnar format (each key is an array)
        # Remove metadata fields and keep only the data arrays
        metadata_keys = ['s', 'nextTime', 'prevTime']  # Status and pagination fields
        data_dict = {k: v for k, v in options_data.items() if k not in metadata_keys}

        # Ensure we have data to convert
        if not data_dict:
            raise Exception(f"No options data returned for ticker '{ticker}'")

        # Convert columnar data to DataFrame
        df = pd.DataFrame(data_dict)

        if df.empty:
            raise Exception(f"No options data returned for ticker '{ticker}'")

        # Save as CSV
        try:
            df.to_csv(output_path, index=False)
        except Exception as e:
            raise Exception(f"Error writing CSV to {output_path}: {e}")

        print(f"Saved {len(df)} option records to {output_path}")
        return output_path


# Backward compatibility: module-level functions that use a default instance
_default_manager = CSVDataManager()

def download_to_csv(ticker: str, start: str | None = None, end: str | None = None, output: str | None = None) -> Path:
    """Backward compatible wrapper for download_price_data"""
    return _default_manager.download_price_data(ticker, start, end, output)

def save_options_to_csv(ticker: str, options_data: dict, output: str | None = None) -> Path:
    """Backward compatible wrapper for save_options_data"""
    return _default_manager.save_options_data(ticker, options_data, output)

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
