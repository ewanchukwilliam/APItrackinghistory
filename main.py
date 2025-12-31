#!/usr/bin/env python3
"""
Main cron job script for insider trading data pipeline.
This script is designed to be run periodically by cron.
"""

import logging
from pathlib import Path
import sys
import uuid
from tickerCollections import tickerCollection
from tickerDB import Database, BatchMetrics

LOG_DIR = Path("/app/logging")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "cron.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    """
    Main execution function for the insider trading data pipeline.
    Fetches insider trading data, enriches it with pricing/options data,
    and stores everything in PostgreSQL.
    """

    # TODO 2: FILE-BASED LOGGING
    # Set up logging that writes to /app/errors/cron.log
    # Use Python's logging module with appropriate formatting
    # Include timestamps, log levels, and meaningful messages
    # Example: logging.basicConfig(filename='/app/errors/cron.log', ...)

    try:
        logger.info("Starting insider trading data pipeline...")
        batch_id = str(uuid.uuid4())

        metrics = BatchMetrics(batch_id)
        metrics.start()

        # Initialize data collection
        with metrics.time_operation('time_to_fetch_trades'):
            collection = tickerCollection()
            listData = collection.tickerList

        logger.info(f"Processing {len(listData)} tickers in batch {batch_id}")
    except Exception as e:
        logger.error(f"Error in fetching ticker information: {str(e)}")
        sys.exit(1)

    try:
        with Database() as db:
            logger.info("Database connection established")

            for data in listData:
                logger.info(f"Processing {data.symbol}")
                try:
                    with metrics.time_operation('db_operation_time_seconds'):
                        is_new = db.tickers.insert(data, batch_id)

                    if not is_new:
                        metrics.add_duplicate_trade()
                        continue

                    with metrics.time_operation('time_to_fetch_price'):
                        data.getPriceData()

                    has_pricing = data.priceData is not None and not data.priceData.empty

                    if has_pricing:
                        with metrics.time_operation('db_operation_time_seconds'):
                            db.pricing.insert(data.priceData, data)
                        metrics.pricing_records_inserted += len(data.priceData)

                    with metrics.time_operation('time_to_fetch_options'):
                        data.getOptionsData()

                    has_options = data.optionsData is not None and not data.optionsData.empty

                    if has_options:
                        with metrics.time_operation('db_operation_time_seconds'):
                            db.options.insert(data.optionsData, data)
                        metrics.options_records_inserted += len(data.optionsData)

                    metrics.add_ticker_processed(
                        inserted=is_new,
                        had_pricing=has_pricing,
                        had_options=has_options
                    )
                    logger.info(f"Processed (pricing: {has_pricing}, options: {has_options})")

                except Exception as e:
                    logger.error(f"Error: {str(e)}")
                    metrics.increment_error()
                    db.errors.log_error(
                        batch_id,
                        f"Error processing {data.symbol}",
                        str(e),
                        {"symbol": data.symbol}
                    )
            success = metrics.error_count == 0
            metrics.complete(success=success)
            metrics.api_call_time_seconds = (
                metrics.time_to_fetch_trades +
                metrics.time_to_fetch_price +
                metrics.time_to_fetch_options
            )
            db.logging.log_batch(batch_id, metrics)
            print("\n" + "="*60)
            print(f"Batch Summary ({batch_id[:8]}):")
            print(f"  Total processed: {metrics.total_records_processed}")
            print(f"  New records: {metrics.records_inserted}")
            print(f"  Duplicates: {metrics.duplicated_trades}")
            print(f"  Errors: {metrics.error_count}")
            print(f"  Execution time: {metrics.execution_time_seconds:.2f}s")
            print(f"    - API: {metrics.api_call_time_seconds:.2f}s")
            print(f"    - DB: {metrics.db_operation_time_seconds:.2f}s")
            print(f"  Status: {metrics.status}")
            print("="*60)
        sys.exit(metrics.exit_code)
    except Exception as e:
        logger.error(f"Fatal DB error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
