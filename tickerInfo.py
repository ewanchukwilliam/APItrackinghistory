#!/usr/bin/env python3
# documentation: libraries https://github.com/ranaroussi/yfinance
from pathlib import Path
import pandas as pd

import matplotlib.pyplot as plt

from tickerConverter import download_to_csv

class tickerInfo:
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

    def getPriceData(self):
        start_dt = pd.to_datetime(self.disclosureDate) - pd.Timedelta(days=60)
        transaction_dt = pd.to_datetime(self.transactionDate)
        if start_dt > transaction_dt:
            start_dt = transaction_dt - pd.Timedelta(days=60)
            print("Extended the range for ticker symbol: "+ self.symbol)
        start = start_dt.strftime('%Y-%m-%d')
        end=None
        try:
            self.priceData = download_to_csv(self.symbol, start, end, None)
        except Exception as e:
            print(f"Error for {self.symbol}: {e}")

    def generateGraphs(self, csv_data):
        # generate png graphs for all tickers
        if csv_data is not None:
            print(f"Generating graphs for {self.symbol}")
            from pathlib import Path
            Path("graphs").mkdir(exist_ok=True)

            df = pd.read_csv(csv_data)
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            ax = df['Close'].plot(figsize=(20,10), label='Close Price')
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

            plt.savefig(f'graphs/{self.symbol}.png')
            plt.close() 


    def getOptionsData(self):
        pass

