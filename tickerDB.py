#!/usr/bin/env python3

import time
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
        self.initialized = False

    def __enter__(self):
        self.conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.cursor = self.conn.cursor()
        TABLE_REGISTRY={
            'tickers': InsiderTradingRecords,
            'errors': ErrorRecords,
            'pricing': InsiderTradingPricingRecords,
            'options': InsiderTradingOptionsRecords,
        }
        # Auto-create from registry
        if not self.initialized:
            for attr_name, table_class in TABLE_REGISTRY.items():
                instance = table_class(self.cursor, self.conn)
                setattr(self, attr_name, instance)
                instance.createTable()
                self.initialized=True
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

    def _check_tables_initialized(self):
        """CHECK if all tables are initialized"""
        tables=self.TABLE_REGISTRY.keys()
        for table in tables:
            if not self._check_table_exists(table):
                return False
        return True
    
    def _check_table_exists(self, table_name):
        """CHECK if a table exists"""
        if self.cursor is None:
            raise Exception("Cursor is None")
        self.cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (table_name.lower(),))
        return self.cursor.fetchone()[0]


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
    """Handles all operations for the ticker table"""

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

class InsiderTradingOptionsRecords:
    """Handles all operations for the ticker table"""


    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self.table_name = "InsiderTradingOptionsRecords"

    def createTable(self):
        """Create options table if it doesn't exist"""
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(100),
                record_hash VARCHAR(64) REFERENCES InsiderTradingRecords(record_hash),
                s VARCHAR(50),
                option_symbol VARCHAR(100),
                underlying VARCHAR(50),
                expiration BIGINT,
                side VARCHAR(10),
                strike DECIMAL(10,2),
                first_traded BIGINT,
                dte INTEGER,
                updated BIGINT,
                bid DECIMAL(10,2),
                bid_size BIGINT,
                mid DECIMAL(10,2),
                ask DECIMAL(10,2),
                ask_size BIGINT,
                last DECIMAL(10,2),
                open_interest BIGINT,
                volume BIGINT,
                in_the_money BOOLEAN,
                intrinsic_value DECIMAL(10,2),
                extrinsic_value DECIMAL(10,2),
                underlying_price DECIMAL(10,2),
                iv DECIMAL(10,4),
                delta DECIMAL(10,4),
                gamma DECIMAL(10,4),
                theta DECIMAL(10,4),
                vega DECIMAL(10,4),
                UNIQUE(record_hash, option_symbol)
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
        # Index for expiration date queries
        self.cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_expiration
            ON {self.table_name}(expiration)
        """)
        print(f"Table {self.table_name} created successfully")

    def insert(self, df: pd.DataFrame, tickerData: tickerInfo):
        """
        Insert Options data for a specific trade (record_hash).
        df: pandas DataFrame from options API
        tickerData: the specific trade this Options data belongs to
        """
        # Prepare all rows as a list for bulk insert
        rows = []
        for _, row in df.iterrows():
            rows.append((
                tickerData.symbol,
                tickerData.record_hash,
                row.get('s'),
                row.get('optionSymbol'),
                row.get('underlying'),
                int(row['expiration']) if pd.notna(row.get('expiration')) else None,
                row.get('side'),
                float(row['strike']) if pd.notna(row.get('strike')) else None,
                int(row['firstTraded']) if pd.notna(row.get('firstTraded')) else None,
                int(row['dte']) if pd.notna(row.get('dte')) else None,
                int(row['updated']) if pd.notna(row.get('updated')) else None,
                float(row['bid']) if pd.notna(row.get('bid')) else None,
                int(row['bidSize']) if pd.notna(row.get('bidSize')) else None,
                float(row['mid']) if pd.notna(row.get('mid')) else None,
                float(row['ask']) if pd.notna(row.get('ask')) else None,
                int(row['askSize']) if pd.notna(row.get('askSize')) else None,
                float(row['last']) if pd.notna(row.get('last')) else None,
                int(row['openInterest']) if pd.notna(row.get('openInterest')) else None,
                int(row['volume']) if pd.notna(row.get('volume')) else None,
                bool(row['inTheMoney']) if pd.notna(row.get('inTheMoney')) else None,
                float(row['intrinsicValue']) if pd.notna(row.get('intrinsicValue')) else None,
                float(row['extrinsicValue']) if pd.notna(row.get('extrinsicValue')) else None,
                float(row['underlyingPrice']) if pd.notna(row.get('underlyingPrice')) else None,
                float(row['iv']) if pd.notna(row.get('iv')) else None,
                float(row['delta']) if pd.notna(row.get('delta')) else None,
                float(row['gamma']) if pd.notna(row.get('gamma')) else None,
                float(row['theta']) if pd.notna(row.get('theta')) else None,
                float(row['vega']) if pd.notna(row.get('vega')) else None,
            ))

        # Bulk insert with executemany
        self.cursor.executemany(f"""
            INSERT INTO {self.table_name}
            (ticker, record_hash, s, option_symbol, underlying, expiration, side, strike,
             first_traded, dte, updated, bid, bid_size, mid, ask, ask_size, last,
             open_interest, volume, in_the_money, intrinsic_value, extrinsic_value,
             underlying_price, iv, delta, gamma, theta, vega)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_hash, option_symbol) DO NOTHING
        """, rows)

        print(f"Inserted {len(rows)} options records for {tickerData.symbol} (hash: {tickerData.record_hash[:8]}...)")

if __name__ == "__main__":
    print("Running main...")
    with Database() as db:
        print("Initialized DB")
        collection = tickerCollection()
        listData = collection.tickerList
        total_time_outside_requests=0
        for data in listData:
            print(data.symbol)      
            try:
                db.tickers.insert(data)
                data.getPriceData()
                interval1_start= time.time()
                db.pricing.insert(data.priceData, data)
                interval1_end= time.time()
                data.getOptionsData()
                interval2_start= time.time()
                db.options.insert(data.optionsData, data)
                interval2_end= time.time()
                total_time_outside_requests=total_time_outside_requests+(interval1_end-interval1_start)+(interval2_end-interval2_start)

            except Exception as e:
                print("threw an error down here " + str(e)+ " for ticker symbol: "+ data.symbol)
        print(f"Total time spent outside requests: {total_time_outside_requests}")

        # Generate a proper UUID for batch_id
        # test_batch_id = str(uuid.uuid4())
        # store a trade instance
        # print(f"Storing error in the error table (batch_id: {test_batch_id})")
        # db.errors.log_error(test_batch_id, "TestError", "This is a test error", {"key": "value"})
        #
        # print("Showing all errors:")
        # db.errors.show_all_errors()
        #
