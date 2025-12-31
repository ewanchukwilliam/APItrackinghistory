#!/usr/bin/env python3

import psycopg2
import os
import hashlib
import json
import uuid
import pandas as pd


from tickerCollections import tickerCollection
from tickerInfo import tickerInfo



class Database:
    """Manages database connection and provides access to repositories"""

    def __init__(self):
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.cursor = self.conn.cursor()
        # initialize tables
        self.tickers = InsiderTradingRecords(self.cursor, self.conn)
        self.errors = ErrorRecords(self.cursor, self.conn)  
        self.pricing = InsiderTradingPricingRecords(self.cursor, self.conn)
        self.tickers.createTable()
        self.errors.createTable()
        self.pricing.createTable()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()
        return False


class InsiderTradingRecords:
    """Handles all operations for the ticker table"""

    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self.table_name = "InsiderTradingRecords"

    def createTable(self):
        """Create ticker table if it doesn't exist"""
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                record_hash VARCHAR(64) UNIQUE NOT NULL,

                -- Core trade details (used for hashing)
                symbol VARCHAR(100) NOT NULL,
                transactionDate VARCHAR(100),
                firstName VARCHAR(100),
                lastName VARCHAR(100),
                type VARCHAR(100),
                amount VARCHAR(100),
                owner VARCHAR(100),
                assetType VARCHAR(100),

                -- Additional trade info (not hashed)
                disclosureDate VARCHAR(100),
                office VARCHAR(100),
                district VARCHAR(100),
                assetDescription TEXT,
                capitalGainsOver200USD VARCHAR(100),
                comment TEXT,
                link TEXT,

                -- Enriched data (added later by separate process)
                priceData JSONB,
                optionsData JSONB,

                -- Metadata
                batch_id UUID,
                first_seen_at TIMESTAMP DEFAULT NOW(),
                last_seen_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Indexes for common queries
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_symbol
            ON {self.table_name}(symbol)
        """)
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_transaction_date
            ON {self.table_name}(transactionDate)
        """)
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_batch_id
            ON {self.table_name}(batch_id)
        """)
        print(f"Table {self.table_name} created successfully")

    def insert(self, data, batch_id=None):
        """
        Insert a single ticker record with automatic deduplication.
        If duplicate (same hash), updates last_seen_at timestamp.
        """
        record_hash = data.record_hash
        self.cursor.execute(f"""
            INSERT INTO {self.table_name} (
                record_hash, symbol, transactionDate, firstName, lastName, type, amount,
                owner, assetType, disclosureDate, office, district, assetDescription,
                capitalGainsOver200USD, comment, link, priceData, optionsData, batch_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash) DO UPDATE
            SET last_seen_at = NOW(),
                batch_id = EXCLUDED.batch_id
        """,
            (record_hash, data.symbol, data.transactionDate, data.firstName, data.lastName,
             data.type, data.amount, data.owner, data.assetType, data.disclosureDate,
             data.office, data.district, data.assetDescription, data.capitalGainsOver200USD,
             data.comment, data.link, data.priceData, data.optionsData, batch_id)
        )
        print(f"Data processed for {data.symbol} (hash: {record_hash[:8]}...)")


class ErrorRecords:
    """Records All API errors and their raw responses"""
    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self.table_name = "ErrorRecords"

    def createTable(self):
        """Create error records table if it doesn't exist"""
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                batch_id UUID,
                occurred_at TIMESTAMP DEFAULT NOW(),
                error_type VARCHAR(100),
                error_message TEXT,
                raw_json JSONB,
                stack_trace TEXT
            )
        """)
        # Create index for querying by batch
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_error_batch_id
            ON {self.table_name}(batch_id)
        """)
        print(f"Table {self.table_name} created successfully")

    def log_error(self, batch_id, error_type, error_msg, raw_data, stack_trace=None):
        """Log an error to the error table"""
        self.cursor.execute(f"""
            INSERT INTO {self.table_name}
            (batch_id, error_type, error_message, raw_json, stack_trace)
            VALUES (%s, %s, %s, %s, %s)
        """, (batch_id, error_type, error_msg, json.dumps(raw_data), stack_trace))

    def show_all_errors(self):
        """Show all errors in the error table"""
        self.cursor.execute(f"""
            SELECT * FROM {self.table_name}
        """)
        for row in self.cursor:
            print(row)  # Prints each row as a tuple    

class InsiderTradingPricingRecords:
    """Handles all operations for the ticker table

            Close_FIS   High_FIS    Low_FIS   Open_FIS  Volume_FIS
Date                                                              
2025-10-31  62.140404  62.229857  60.808540  61.146475     3073800
2025-11-03  61.822346  62.060886  60.709147  61.762709     4051200
2025-11-04  62.766579  63.094573  61.176291  62.080766     5557700
2025-11-05  64.356865  64.605347  62.070830  63.333120     7694200
2025-11-06  64.287285  65.599270  63.064753  64.684858     3468800
2025-11-07  64.386681  64.883645  63.472268  63.621359     3601400
2025-11-10  65.042679  65.211645  63.661119  64.317108     2479400
2025-11-11  65.976959  66.116109  64.903522  65.161938     3175600
2025-11-12  65.827881  65.986905  64.824011  65.460125     3258500
2025-11-13  65.857697  66.046538  64.992976  65.639031     3739500
2025-11-14  63.849957  65.967029  63.581599  65.579397     3551300
2025-11-17  63.541840  64.486068  63.432507  63.929471     2822200
2025-11-18  62.766579  63.800265  62.627426  63.502084     3144500
    """


    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self.table_name = "InsiderTradingPricingRecords"

    def createTable(self):
        """Create ticker table if it doesn't exist"""
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(100),
                record_hash VARCHAR(64) REFERENCES InsiderTradingRecords(record_hash),
                date DATE,
                close_price DECIMAL(10,2),
                high_price DECIMAL(10,2),
                low_price DECIMAL(10,2),
                open_price DECIMAL(10,2),
                volume BIGINT,
                UNIQUE(record_hash, date)
            )
        """)
        # Index for querying by ticker
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ticker
            ON {self.table_name}(ticker)
        """)
        # Index for foreign key relationship
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_record_hash
            ON {self.table_name}(record_hash)
        """)
        # Composite index for time-series queries by ticker
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_ticker_date
            ON {self.table_name}(ticker, date)
        """)
        print(f"Table {self.table_name} created successfully")
        
    def insert(self, df: pd.DataFrame, tickerData: tickerInfo):
        """
        Insert pricing data for a specific trade (record_hash).
        df: pandas DataFrame from yfinance with date index and OHLCV columns
        tickerData: the specific trade this pricing data belongs to
        """
        # Prepare all rows as a list for bulk insert
        rows = []
        for date, row in df.iterrows():
            rows.append((
                tickerData.symbol,
                tickerData.record_hash,
                date.strftime('%Y-%m-%d'),
                float(row['Close']) if 'Close' in row and pd.notna(row['Close']) else None,
                float(row['High']) if 'High' in row and pd.notna(row['High']) else None,
                float(row['Low']) if 'Low' in row and pd.notna(row['Low']) else None,
                float(row['Open']) if 'Open' in row and pd.notna(row['Open']) else None,
                int(row['Volume']) if 'Volume' in row and pd.notna(row['Volume']) else None
            ))

        # Bulk insert with executemany
        self.cursor.executemany(f"""
            INSERT INTO {self.table_name}
            (ticker, record_hash, date, close_price, high_price, low_price, open_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash, date) DO NOTHING
        """, rows)

        print(f"Inserted {len(rows)} price records for {tickerData.symbol} (hash: {tickerData.record_hash[:8]}...)")

if __name__ == "__main__":
    print("Running main...")
    with Database() as db:
        print("Initialized DB")

        # Generate a proper UUID for batch_id
        test_batch_id = str(uuid.uuid4())
        # store a trade instance
        collection = tickerCollection()
        listData = collection.tickerList
        data1 = listData[0]
        db.tickers.insert(data1)
        data1.getPriceData()
        db.pricing.insert(data1.priceData, data1)

        # print(f"Storing error in the error table (batch_id: {test_batch_id})")
        # db.errors.log_error(test_batch_id, "TestError", "This is a test error", {"key": "value"})
        #
        # print("Showing all errors:")
        # db.errors.show_all_errors()
        #
