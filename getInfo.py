#!/usr/bin/env python3
# documentation: libraries https://github.com/ranaroussi/yfinance
from typing import List
import requests
import os

import pandas as pd

from getcsvticker import download_to_csv

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
        start = (pd.to_datetime(self.disclosureDate) - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        end=None
        try:
            self.priceData = download_to_csv(self.symbol, start, end, None)
        except Exception as e:
            print(self.symbol + " threw an error here")

    def getOptionsData(self):
        pass

class tickerItems:
    def __init__(self, listData : List[tickerInfo]):
        self.listData = listData

def getTickerInfoList():
    # curl "https://financialmodelingprep.com/stable/house-latest?page=0&limit=10&apikey=$FMP_API_KEY" | jq > house.json
    url= "https://financialmodelingprep.com/stable/house-latest"
    params= {
        "page":0,
        "limit":3,
        "apikey":os.getenv('FMP_API_KEY')
    }
    response  = requests.get(url, params=params)
    jsonData = response.json()
    listData = []
    for data in jsonData:
        listData.append(tickerInfo(data))
    return listData

if __name__ == "__main__":
    listData = getTickerInfoList()
    for data in listData:
        try:
            data.getPriceData()
        except Exception as e:
            print(data.symbol + " threw an error here")
        # print(data.symbol)
        # print(data.disclosureDate)
        # print(data.transactionDate)
        # print(data.firstName)
        # print(data.lastName)
        # print(data.office)
        # print(data.district)
        # print(data.owner)
        # print(data.assetDescription)
        # print(data.assetType)
        # print(data.type)
        # print(data.amount)
        # print(data.capitalGainsOver200USD)
        # print(data.comment)
        # print(data.link)
        # print(data.priceData)
        # print(data.optionsData) 
        

