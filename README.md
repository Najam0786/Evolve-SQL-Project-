# Goldman Sachs Stock Data Analysis Project

> A reproducible SQL pipeline analyzing 27 years of Goldman Sachs (GS) daily trading data (1999–2026) — covering price trends, volatility, source consistency, dividend impact, and data quality.

---

## Overview

This project builds a complete **staging → core → semantic** data pipeline in MySQL to analyze Goldman Sachs stock data sourced from six financial data providers. The dataset includes OHLC prices, volume, dividends, and stock splits.

---

## Dataset

| Attribute        | Details                                                                |
|------------------|------------------------------------------------------------------------|
| **Source**       | [Kaggle – Goldman Sachs Stock Data (1999–2026)](https://www.kaggle.com/datasets/...) |
| **Size**         | ~6,700 rows per file (~40,000 rows in staging, reduced to ~29,500 rows in fact table after cleaning and deduplication) |
| **Key Columns**  | Date, Open, High, Low, Close, Volume, Dividends, Stock Splits, Source  |

**CSV Files:**

| File                     | Provider       |
|--------------------------|----------------|
| `gs_barchart.csv`        | Barchart       |
| `gs_investing_com.csv`   | Investing.com  |
| `gs_marketwatch.csv`     | MarketWatch    |
| `gs_master_dataset.csv`  | Master (consolidated) |
| `gs_nasdaq.csv`          | NASDAQ         |
| `gs_yahoo_finance.csv`   | Yahoo Finance  |

---

## Business Questions

1. **Price trend and volatility** — How has the stock price evolved over time? What are the most volatile days/months?
2. **Trading activity** — Which days had the highest volume? Is there a seasonal pattern?
3. **Source reliability** — Do different data providers report consistent prices? Where are the largest discrepancies?
4. **Dividend impact** — How does the stock behave on ex-dividend dates?
5. **Data quality** — Are there missing dates, duplicate records, or invalid values across sources?

---

## SQL Engine

- **MySQL 8+** — window functions, stored procedures, triggers, and CTEs
- All scripts are written in MySQL syntax and tested with **MySQL 8.0**

---

## Project Structure

```
project_root/
├── data/                        # Raw CSV files (not modified)
│   ├── gs_barchart.csv
│   ├── gs_investing_com.csv
│   ├── gs_marketwatch.csv
│   ├── gs_master_dataset.csv
│   ├── gs_nasdaq.csv
│   ├── gs_yahoo_finance.csv
│   └── README.md                # Data folder description
├── sql/                         # All SQL scripts
│   ├── 01_schema.sql            # Create staging & core tables
│   ├── 02_load_staging.sql      # Validate CSV imports
│   ├── 03_transform_core.sql    # Clean and load core tables
│   ├── 04_semantic_views.sql    # Create business-friendly views
│   ├── 05_analysis_queries.sql  # 12 analytical queries
│   ├── 06_quality_checks.sql    # Data quality validations
│   └── 07_advanced_sql.sql      # Functions, procedures, triggers
├── PROJECT_BRIEF.md             # Project requirements & checklist
└── README.md                    # This file
```

---

## Reproduction Instructions

1. **Install MySQL 8+** (or use a cloud instance like AWS RDS, Google Cloud SQL, or a local Docker container).
2. **Clone/download** this repository.
3. **Place the CSV files** inside the `data/` folder (download from Kaggle if not already present).
4. **Run the scripts in order** using your MySQL client (e.g., MySQL Workbench, DBeaver, command line):

```sql
source sql/01_schema.sql;

-- Import each CSV into its corresponding staging table:
--   gs_barchart.csv       → stg_barchart_raw
--   gs_investing_com.csv  → stg_investing_raw
--   gs_marketwatch.csv    → stg_marketwatch_raw
--   gs_master_dataset.csv → stg_master_raw
--   gs_nasdaq.csv         → stg_nasdaq_raw
--   gs_yahoo_finance.csv  → stg_yahoo_raw
-- Use your tool's import wizard (ensure header row, map columns by name).

source sql/02_load_staging.sql;     -- Check imports
source sql/03_transform_core.sql;   -- Build core tables
source sql/04_semantic_views.sql;   -- Create views
source sql/05_analysis_queries.sql; -- Run analyses
source sql/06_quality_checks.sql;   -- Validate data quality
source sql/07_advanced_sql.sql;     -- Test advanced features
```

---

## Assumptions and Limitations

- **Trading days** — The dataset only includes days when the market was open. Weekends and holidays are absent. This is normal for stock data.
- **NASDAQ file** — Does not contain Dividends or Stock Splits columns; those fields are set to `0.0` in the fact table.
- **Date formats** — Most files use `YYYY-MM-DD` (with optional timezone); NASDAQ uses `MM/DD/YYYY`. The transformation handles both.
- **Source names** — Values may vary slightly (e.g., "Barchart (via proxy)", "Investing.com (via proxy)"). The dimension table stores them as-is.
- **No adjustment for splits/dividends** — Prices are raw historical values; for total return analysis, dividends should be reinvested — not covered here.
- **Outliers** — Extreme price movements are kept; they may correspond to real events (earnings, news). No automatic filtering is applied.

---

## Data Quality Checklist

| Check                  | Result                                                                 |
|------------------------|------------------------------------------------------------------------|
| **Row counts**         | Each staging table has ~6,700 rows (~40,000 total in staging, reduced to ~29,500 rows in fact table after cleaning and deduplication) |
| **Nulls**              | No critical nulls in core tables (checked in `06_quality_checks.sql`)  |
| **Orphan foreign keys**| Zero orphan records in fact table                                      |
| **Duplicates**         | Unique constraint (`date_id`, `source_id`) prevents duplicates; none found |
| **Date range**         | 1999-05-04 to 2026-03-11 (as expected)                                |
| **Price consistency**  | All prices > 0; high >= low for all rows                               |
| **Source consistency**  | Cross-source price discrepancies >2% flagged in analysis (Q5)         |
| **Dividends/Splits**   | Non-zero values present only on relevant dates; no unexpected values   |

---

## Advanced SQL Features Used

| Feature              | Details                                                                              |
|----------------------|--------------------------------------------------------------------------------------|
| **Function**         | `fn_safe_percent_change()` — calculates percentage change safely                     |
| **Procedure**        | `sp_refresh_core_from_staging()` — rebuilds core tables from staging in a transaction|
| **Triggers**         | `trg_fct_stock_bi_validate`, `trg_fct_stock_bu_validate` — enforce data quality on insert/update |
| **Window Functions** | Used in rolling averages, rankings, and YoY growth queries                           |
| **CTEs**             | Multiple queries use CTEs for readability and modularity                              |

---

## Results Summary (Key Insights)

- **Long-term trend** — GS stock has grown significantly since 1999, with notable dips during the 2008 financial crisis and 2020 pandemic.
- **Highest volume days** — Often coincide with earnings releases or major market events (e.g., 2008-09, 2020-03).
- **Source discrepancies** — Most sources agree within 1%, but occasional outliers (>2%) exist, often due to timing differences or data errors.
- **Dividend days** — Show modest price adjustments but no consistent pattern of abnormal returns.

---

## License

This project is for educational purposes. The dataset is provided under **CC0 Public Domain**. All SQL code is free to use and modify.

---

**Author:** Nazmul Farooquee
**Date:** March 2026
**Course:** Master in Data Science — SQL Project

