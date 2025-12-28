#!/bin/bash

echo "Using API Key: $FMP_API_KEY"
curl "https://financialmodelingprep.com/stable/house-latest?page=0&limit=10&apikey=$FMP_API_KEY" | jq > house.json
