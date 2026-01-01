#!/usr/bin/env python3

import requests
import os
from tickerInfo import tickerInfo

class tickerCollection:
    def __init__(self):
        self.tickerList = self.getPoliticianTransactionData()
        self.politicianTransactionData = None
        self.optionsAPIResponse = None

    def getPoliticianTransactionData(self):
        # curl "https://financialmodelingprep.com/stable/house-latest?page=0&limit=10&apikey=$FMP_API_KEY" | jq > house.json
        url= "https://financialmodelingprep.com/stable/house-latest"
        params= {
            "page":0,
            "limit":10,
            "apikey":os.getenv('FMP_API_KEY')
        }
        try:
            self.politicianTransactionData = requests.get(url, params=params, timeout=15)
        except requests.exceptions.Timeout:
            raise Exception(f"Request to {url} timed out after 15 seconds")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request to {url} failed: {str(e)}")

        jsonData = self.politicianTransactionData.json()
        listData = []
        for data in jsonData:
            listData.append(tickerInfo(data))
        return listData
    

if __name__ == "__main__":
    collection = tickerCollection()
    listData = collection.tickerList
    data1 = listData[0]
    data1.getPriceData()
    # data1.getOptionsData()
    # for data in listData:
    #     try:
    #         data.getPriceData()
    #         # data.getOptionsData()
    #     except Exception as e:
    #         print("threw an error down here " + str(e)+ " for ticker symbol: "+ data.symbol)

    # generate png graphs for all tickers
    # for data in listData:
    #     data.generateGraphs(data.priceData)
