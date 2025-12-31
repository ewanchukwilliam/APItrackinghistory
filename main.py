#!/usr/bin/env python3
"""
Main cron job script for insider trading data pipeline.
This script is designed to be run periodically by cron.
"""

import time
import uuid
from tickerCollections import tickerCollection
from tickerDB import Database


def main():
    """
    Main execution function for the insider trading data pipeline.
    Fetches insider trading data, enriches it with pricing/options data,
    and stores everything in PostgreSQL.
    """

    # TODO 1: EXIT CODES
    # Track whether any errors occurred during execution
    # At the end of the script, exit with code 0 (success) or 1 (failure)
    # Example: sys.exit(1 if errors_occurred else 0)


    # TODO 2: FILE-BASED LOGGING
    # Set up logging that writes to /app/errors/cron.log
    # Use Python's logging module with appropriate formatting
    # Include timestamps, log levels, and meaningful messages
    # Example: logging.basicConfig(filename='/app/errors/cron.log', ...)


    # TODO 3: HANDLE MISSING DB GRACEFULLY
    # Wrap database connection in try/except
    # If DB connection fails, log the error and exit with code 1
    # Don't let the script crash silently
    # Example: try/except around Database() context manager


    # TODO 4: TRACK EXECUTION METADATA
    # Log: start time, end time, total records processed, success/failure counts
    # Store this metadata somewhere (file, DB table, or just logs)
    # Helps with monitoring and debugging


    # TODO 5: IDEMPOTENT EXECUTION
    # Your unique constraints already handle this!
    # ON CONFLICT DO NOTHING means running twice won't duplicate data
    # Just document that this is safe to run multiple times


    print("Starting insider trading data pipeline...")
    batch_id = str(uuid.uuid4())

    # Initialize data collection
    collection = tickerCollection()
    listData = collection.tickerList

    print(f"Processing {len(listData)} tickers in batch {batch_id}")

    # Connect to database and process all tickers
    with Database() as db:
        print("Database connection established")

        total_time_outside_requests = 0

        for data in listData:
            print(f"Processing {data.symbol}")

            try:
                # Insert main insider trading record
                db.tickers.insert(data)

                # Fetch and insert pricing data
                data.getPriceData()
                interval1_start = time.time()
                db.pricing.insert(data.priceData, data)
                interval1_end = time.time()

                # Fetch and insert options data
                data.getOptionsData()
                interval2_start = time.time()
                db.options.insert(data.optionsData, data)
                interval2_end = time.time()

                total_time_outside_requests += (interval1_end - interval1_start) + (interval2_end - interval2_start)

            except Exception as e:
                print(f"Error processing {data.symbol}: {str(e)}")
                db.errors.log_error(
                    batch_id,
                    f"Error inserting data for {data.symbol}",
                    str(e),
                    {"symbol": data.symbol}
                )

        print(f"Total time spent on DB operations: {total_time_outside_requests:.2f}s")

    print("Pipeline execution completed")


if __name__ == "__main__":
    main()
