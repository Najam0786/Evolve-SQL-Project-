# Data folder

## Goldman Sachs Stock Data (1999–2026)

This folder contains the raw CSV files for the Goldman Sachs (GS) stock dataset.  
Source: [Kaggle - Goldman Sachs Stock Data](https://www.kaggle.com/datasets/...)

### Files included:

| File | Description |
|------|-------------|
| `gs_barchart.csv` | Historical daily stock data from Barchart |
| `gs_investing_com.csv` | Historical daily stock data from Investing.com |
| `gs_marketwatch.csv` | Historical daily stock data from MarketWatch |
| `gs_master_dataset.csv` | Master file combining all sources with a `Source` column |
| `gs_nasdaq.csv` | Historical daily stock data from NASDAQ |
| `gs_yahoo_finance.csv` | Historical daily stock data from Yahoo Finance |

All files contain daily OHLC (Open, High, Low, Close) prices, trading volume, dividends, and stock splits.

**Important:** These files are the original raw data and should not be manually edited. All cleaning and transformations will be performed in the SQL scripts located in the `sql/` folder.
