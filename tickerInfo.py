#!/usr/bin/env python3
# documentation: libraries https://github.com/ranaroussi/yfinance
import hashlib
import pandas as pd

import json
import os
import matplotlib.pyplot as plt
import requests
import yfinance as yf

from tickerConverter import CSVDataManager

class tickerInfo:
    # Shared CSV manager for all tickerInfo instances
    csv_manager = CSVDataManager()

    def __init__(self, data : dict):
        self.symbol=data.get('symbol')
        self.disclosureDate=data.get('disclosureDate')
        self.transactionDate=data.get('transactionDate')
        self.firstName=data.get('firstName')
        self.lastName=data.get('lastName')
        self.office=data.get('office')
        self.district=data.get('district')
        self.owner=data.get('owner')
        self.assetDescription=data.get('assetDescription')
        self.assetType=data.get('assetType')
        self.type=data.get('type')
        self.amount=data.get('amount')
        self.capitalGainsOver200USD=data.get('capitalGainsOver200USD')
        self.comment=data.get('comment')
        self.link=data.get('link')
        self.priceData=None
        self.optionsData=None
        self.isDataValid=None

    def getPriceData(self):
        """get price data for ticker"""
        if self.symbol is None:
            raise Exception("Symbol is None")
        if self.disclosureDate is None or self.transactionDate is None:
            raise Exception("Disclosure date or transaction date is None")

        start_dt = pd.to_datetime(self.disclosureDate) - pd.Timedelta(days=60)
        transaction_dt = pd.to_datetime(self.transactionDate)
        if start_dt > transaction_dt:
            if self.symbol is None:
                raise Exception("Symbol is None")
            start_dt = transaction_dt - pd.Timedelta(days=60)
            print("Extended the range for ticker symbol: "+ self.symbol)
        start = start_dt.strftime('%Y-%m-%d')
        end=None
        try:
            data = yf.download(self.symbol, start=start_dt, end=transaction_dt, progress=False)
            # Flatten MultiIndex columns if present (yfinance returns MultiIndex for single ticker)
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            self.priceData = data
            # self.priceData = self.csv_manager.download_price_data(self.symbol, start, end, None)
            print(self.priceData)
        except Exception as e:
            print(f"Error for {self.symbol}: {e}")
            self.isDataValid=False

    def generateGraphs(self, csv_data):
        """generate png graphs for all tickers""" 
        if self.symbol is None:
            raise Exception("Symbol is None")
        if self.disclosureDate is None or self.transactionDate is None:
            raise Exception("Disclosure date or transaction date is None")  
        if csv_data is not None:
            print(f"Generating graphs for {self.symbol}")
            # Ensure graphs directory exists using csv_manager
            self.csv_manager._ensure_dir(self.csv_manager.graphs_dir)

            df = pd.read_csv(csv_data)
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            # ax = df['Close'].plot(figsize=(20,10), label='Close Price')
            transaction_dt = pd.to_datetime(self.transactionDate)
            plt.axvline(x=transaction_dt, color='red', linestyle='--', linewidth=2,
                       label=f'Transaction Date ({self.transactionDate})')
            disclosure_dt = pd.to_datetime(self.disclosureDate)
            plt.axvline(x=disclosure_dt, color='green', linestyle='--', linewidth=2,
                       label=f'Disclosure Date ({self.disclosureDate})')
            plt.xlabel('Date')
            plt.ylabel('Close Price')
            politician_name = f'{self.firstName} {self.lastName}'
            plt.title(f'{self.symbol} - {politician_name} - {self.type}')
            plt.legend()

            graph_path = self.csv_manager.graphs_dir / f'{self.symbol}.png'
            plt.savefig(graph_path)
            plt.close() 


    def getOptionsData(self):
        """get options data for ticker"""
        url = "https://api.marketdata.app/v1/options/chain/"
        if self.symbol is None:
            raise Exception("Symbol is None")
        if self.disclosureDate is None or self.transactionDate is None:
            raise Exception("Disclosure date or transaction date is None")  
        full_url = url + self.symbol + "/"
        # disclosure_dt = pd.to_datetime(self.disclosureDate)
        transaction_dt = pd.to_datetime(self.transactionDate)
        headers = {
            'Authorization': f'Bearer {os.getenv("MARKETDATA_API_KEY")}'
        }
        # Get options available on transaction date, expiring within next 60 days
        params = {
            "date": transaction_dt.strftime('%Y-%m-%d'),           # Options chain snapshot from transaction date
            "from": transaction_dt.strftime('%Y-%m-%d'),           # Expiring from transaction date onwards
            "to": (transaction_dt + pd.Timedelta(days=60)).strftime('%Y-%m-%d')  # Up to 60 days after transaction
        }
        response = requests.get(full_url, headers=headers, params=params)
        response_data = response.json()
        try:
            # Save options data to CSV using csv_manager
            options_csv_path = self.csv_manager.save_options_data(self.symbol, response_data)
            print(f"Options data saved for {self.symbol} at {options_csv_path}")
            self.optionsData = options_csv_path
        except Exception as e:
            print(f"Error saving options data for {self.symbol}: {e}")
            self.isDataValid=False
            self.optionsData = None

    @property
    def record_hash(self):
        """
        Compute SHA256 hash of core trade fields.
        Only hash fields that identify a unique trade.
        """
        hash_dict = {
            'symbol': self.symbol,
            'transactionDate': self.transactionDate,
            'firstName': self.firstName,
            'lastName': self.lastName,
            'type': self.type,
            'amount': self.amount,
            'owner': self.owner,
            'assetType': self.assetType,
        }
        # Sort keys for consistent hashing
        hash_string = json.dumps(hash_dict, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
