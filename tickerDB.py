#!/usr/bin/env python3

import psycopg2
import os
import hashlib
import json
import uuid



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
        self.tickers.createTable()
        self.errors.createTable()
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

    def _compute_hash(self, data):
        """
        Compute SHA256 hash of core trade fields.
        Only hash fields that identify a unique trade.
        """
        hash_dict = {
            'symbol': data.symbol,
            'transactionDate': data.transactionDate,
            'firstName': data.firstName,
            'lastName': data.lastName,
            'type': data.type,
            'amount': data.amount,
            'owner': data.owner,
            'assetType': data.assetType,
        }
        # Sort keys for consistent hashing
        hash_string = json.dumps(hash_dict, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()

    def insert(self, data, batch_id=None):
        """
        Insert a single ticker record with automatic deduplication.
        If duplicate (same hash), updates last_seen_at timestamp.
        """
        record_hash = self._compute_hash(data)

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

if __name__ == "__main__":
    print("Running main...")
    with Database() as db:
        print("Initialized DB")

        # Generate a proper UUID for batch_id
        test_batch_id = str(uuid.uuid4())
        print(f"Storing error in the error table (batch_id: {test_batch_id})")
        db.errors.log_error(test_batch_id, "TestError", "This is a test error", {"key": "value"})

        print("Showing all errors:")
        db.errors.show_all_errors()

