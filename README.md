# Insider Trading Analysis

Track and analyze congressional stock trades with historical pricing and options data.

## API Documentation

### Political Trading Data
- **[FMP House Latest API](https://site.financialmodelingprep.com/developer/docs#house-latest)** - Congressional stock transaction data

### Stock Price Data
- **[yfinance](https://github.com/ranaroussi/yfinance)** - Historical stock price data from Yahoo Finance

### Options Data
- **[Market Data Options Chain API](https://www.marketdata.app/docs/api/options/chain#request-parameters)** - Historical options chain data
- **[Market Data Authentication](https://www.marketdata.app/docs/api/authentication)** - API authentication guide

## Setup

Set required API keys as environment variables:

```bash
export FMP_API_KEY="your_fmp_api_key"
export MARKETDATA_API_KEY="your_marketdata_api_key"
```

## Usage

```bash
./tickerCollections.py
```

## Output

- `pricing/` - Historical stock prices (CSV)
- `options/` - Historical options chains (CSV)
- `graphs/` - Price charts with transaction/disclosure dates (PNG)
- `errors/` - API error responses (HTML)
