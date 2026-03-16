# Project Brief: Goldman Sachs Stock Data Analysis

> A comprehensive SQL-based analysis of 27 years of Goldman Sachs (GS) daily trading data, exploring price trends, volume patterns, dividend impacts, and cross-source data quality.

---

## Dataset Overview

| Attribute       | Details                                                                 |
|-----------------|-------------------------------------------------------------------------|
| **Name**        | Goldman Sachs (GS) Stock Data (1999–2026)                               |
| **Source**       | [Kaggle](https://www.kaggle.com/datasets/...)                          |
| **Files**        | 6 CSV files (identical structure, except NASDAQ lacks dividends/splits) |
| **Rows per File**| ~6,700+ records                                                        |

**Description:** 27 years of daily trading data for Goldman Sachs stock — including OHLC prices, volume, dividends, and stock splits — sourced from six financial data providers:

- Barchart
- Investing.com
- MarketWatch
- NASDAQ
- Yahoo Finance
- Master (consolidated file)

---

## Business Questions

This project aims to answer the following questions about Goldman Sachs stock performance and data quality:

| #  | Question                                                         | Focus Areas                                                  |
|----|------------------------------------------------------------------|--------------------------------------------------------------|
| 1  | How has the stock price trended over the last 27 years?          | Monthly/yearly avg close, YoY growth, volatility             |
| 2  | Which days saw the highest volume and largest price swings?      | Top volume days, largest daily gains/losses                   |
| 3  | How consistent are the different data sources?                   | Cross-source close comparison, discrepancy detection         |
| 4  | What is the impact of dividends on stock price?                  | Price changes on ex-dividend dates                           |
| 5  | Can we detect outliers or data quality issues across sources?    | Null checks, orphan keys, price consistency, source variance |

---

## SQL Engine

| Property     | Value                                                                                  |
|--------------|----------------------------------------------------------------------------------------|
| **Engine**   | MySQL 8+                                                                               |
| **Rationale**| Window functions, stored procedures, triggers, CTEs                                    |
| **Syntax**   | All scripts use MySQL syntax; triggers and procedures are included as advanced features |

---

## Data Model

### Core Tables (3 related)

| Table              | Type      | Description                                                                                   |
|--------------------|-----------|-----------------------------------------------------------------------------------------------|
| `dim_date`         | Dimension | Date attributes — year, month, day, quarter, weekday, weekend flag                            |
| `dim_source`       | Dimension | Data provider names                                                                           |
| `fct_daily_stock`  | Fact      | Daily OHLC prices, volume, dividends, splits (**grain:** one row per date per source)         |

### Semantic Views (3 views)

| View                       | Description                                                                            |
|----------------------------|----------------------------------------------------------------------------------------|
| `vw_stock_enriched`        | Fact + dimensions with calculated fields (daily change, % change, range, direction)    |
| `vw_monthly_source_stats`  | Monthly aggregates per source (avg close, total volume, avg daily range, etc.)         |
| `vw_source_comparison`     | Pivot-like view comparing closing prices across sources per date                       |

---

## Technical Requirements Checklist

- [x] **Staging Layer** — 6 raw tables:
  `stg_barchart_raw`, `stg_investing_raw`, `stg_marketwatch_raw`, `stg_master_raw`, `stg_nasdaq_raw`, `stg_yahoo_raw`

- [x] **Core Layer** — 3 tables (`dim_date`, `dim_source`, `fct_daily_stock`) with proper data types and foreign keys

- [x] **Semantic Layer** — 3 views (exceeds the minimum of 2)

- [x] **Transactions** — Explicit `START TRANSACTION ... COMMIT/ROLLBACK` in `sp_refresh_core_from_staging`

- [x] **Advanced SQL:**

  | Type        | Name                                                       |
  |-------------|------------------------------------------------------------|
  | Function    | `fn_safe_percent_change`                                   |
  | Procedure   | `sp_refresh_core_from_staging`                             |
  | Triggers    | `trg_fct_stock_bi_validate`, `trg_fct_stock_bu_validate`  |

- [x] **Analytical Queries** — 12 queries in `05_analysis_queries.sql`:

  | Requirement              | Covered By         |
  |--------------------------|--------------------|
  | 2+ temporal aggregations | Q1, Q3, Q7, Q9    |
  | 2+ CTEs                  | Q3, Q5, Q9        |
  | 1+ ranking               | Q10, Q12          |
  | Data quality detection   | Q5, `06_quality_checks.sql` |

---

## Deliverables

| #  | Deliverable                                | Details                                                        |
|----|--------------------------------------------|----------------------------------------------------------------|
| 1  | SQL Scripts                                | Organized by step: schema, staging, core, views, analysis, QA, advanced |
| 2  | README                                     | Reproduction instructions                                      |
| 3  | Dataset                                    | CSV files or download instructions                             |
| 4  | Demo                                       | 5-minute presentation (slides or live walkthrough)             |

---

## Notes

- **Fully reproducible:** run `01_schema.sql`, import CSVs, then execute remaining scripts in order.
- **Quality-first:** data quality checks are included to ensure consistency before analysis.
- **Tested:** all scripts verified against MySQL 8.0.
