/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 01_schema.sql
   DESCRIPTION: Create staging tables (matching CSV headers) and core tables
   ============================================================ */
SHOW DATABASES;
-- Create and use the project database
CREATE DATABASE IF NOT EXISTS gs_stock_project;
USE gs_stock_project;

/* ============================================================
   STAGING TABLES (raw, all VARCHAR, column names match CSV)
   ============================================================ */

-- Drop staging tables if they exist
DROP TABLE IF EXISTS stg_barchart_raw;
DROP TABLE IF EXISTS stg_investing_raw;
DROP TABLE IF EXISTS stg_marketwatch_raw;
DROP TABLE IF EXISTS stg_master_raw;
DROP TABLE IF EXISTS stg_nasdaq_raw;
DROP TABLE IF EXISTS stg_yahoo_raw;

-- 1. Barchart (columns: Date, Open, High, Low, Close, Volume, Dividends, Stock Splits, Source)
CREATE TABLE stg_barchart_raw (
    `Date`           VARCHAR(30),
    `Open`           VARCHAR(30),
    `High`           VARCHAR(30),
    `Low`            VARCHAR(30),
    `Close`          VARCHAR(30),
    `Volume`         VARCHAR(30),
    `Dividends`      VARCHAR(30),
    `Stock Splits`   VARCHAR(30),
    `Source`         VARCHAR(100),
    source_file      VARCHAR(120) DEFAULT 'gs_barchart.csv',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SELECT * FROM stg_barchart_raw sbr Limit 5;


-- 2. Investing.com (same columns)
CREATE TABLE stg_investing_raw (
    `Date`           VARCHAR(30),
    `Open`           VARCHAR(30),
    `High`           VARCHAR(30),
    `Low`            VARCHAR(30),
    `Close`          VARCHAR(30),
    `Volume`         VARCHAR(30),
    `Dividends`      VARCHAR(30),
    `Stock Splits`   VARCHAR(30),
    `Source`         VARCHAR(100),
    source_file      VARCHAR(120) DEFAULT 'gs_investing_com.csv',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM stg_investing_raw sir LIMIT 5;

-- 3. MarketWatch
CREATE TABLE stg_marketwatch_raw (
    `Date`           VARCHAR(30),
    `Open`           VARCHAR(30),
    `High`           VARCHAR(30),
    `Low`            VARCHAR(30),
    `Close`          VARCHAR(30),
    `Volume`         VARCHAR(30),
    `Dividends`      VARCHAR(30),
    `Stock Splits`   VARCHAR(30),
    `Source`         VARCHAR(100),
    source_file      VARCHAR(120) DEFAULT 'gs_marketwatch.csv',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM stg_marketwatch_raw smr LIMIT 5;

-- 4. Master dataset
CREATE TABLE stg_master_raw (
    `Date`           VARCHAR(30),
    `Open`           VARCHAR(30),
    `High`           VARCHAR(30),
    `Low`            VARCHAR(30),
    `Close`          VARCHAR(30),
    `Volume`         VARCHAR(30),
    `Dividends`      VARCHAR(30),
    `Stock Splits`   VARCHAR(30),
    `Source`         VARCHAR(100),
    source_file      VARCHAR(120) DEFAULT 'gs_master_dataset.csv',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM stg_master_raw smr LIMIT 5;

-- 5. NASDAQ (note: column order is Date, Close, Volume, Open, High, Low, Source)
-- But we keep same column names to allow "map by name"; missing columns will be NULL.
CREATE TABLE stg_nasdaq_raw (
    `Date`           VARCHAR(30),
    `Open`           VARCHAR(30),
    `High`           VARCHAR(30),
    `Low`            VARCHAR(30),
    `Close`          VARCHAR(30),
    `Volume`         VARCHAR(30),
    `Dividends`      VARCHAR(30),   -- will be NULL for NASDAQ
    `Stock Splits`   VARCHAR(30),   -- will be NULL for NASDAQ
    `Source`         VARCHAR(100),
    source_file      VARCHAR(120) DEFAULT 'gs_nasdaq.csv',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
SELECT * FROM stg_nasdaq_raw snr LIMIT 5;

-- 6. Yahoo Finance
CREATE TABLE stg_yahoo_raw (
    `Date`           VARCHAR(30),
    `Open`           VARCHAR(30),
    `High`           VARCHAR(30),
    `Low`            VARCHAR(30),
    `Close`          VARCHAR(30),
    `Volume`         VARCHAR(30),
    `Dividends`      VARCHAR(30),
    `Stock Splits`   VARCHAR(30),
    `Source`         VARCHAR(100),
    source_file      VARCHAR(120) DEFAULT 'gs_yahoo_finance.csv',
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM stg_yahoo_raw syr LIMIT 5;
/* ============================================================
   CORE TABLES (clean, typed, modeled)
   ============================================================ */

-- Dimension: Date
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id         INT PRIMARY KEY AUTO_INCREMENT,
    full_date       DATE NOT NULL UNIQUE,
    year            INT NOT NULL,
    month           INT NOT NULL,
    day             INT NOT NULL,
    quarter         INT NOT NULL,
    day_of_week     INT NOT NULL,   -- 1=Monday, 7=Sunday (adjust if needed)
    is_weekend      BOOLEAN NOT NULL,
    INDEX idx_year_month (year, month)
);
SELECT * FROM dim_date dd LIMIT 5;

-- Dimension: Data Source
DROP TABLE IF EXISTS dim_source;
CREATE TABLE dim_source (
    source_id       INT PRIMARY KEY AUTO_INCREMENT,
    source_name     VARCHAR(100) NOT NULL UNIQUE
);

SELECT * FROM dim_source ds  LIMIT 5;

-- Fact table: Daily Stock Data
DROP TABLE IF EXISTS fct_daily_stock;
CREATE TABLE fct_daily_stock (
    fact_id         BIGINT PRIMARY KEY AUTO_INCREMENT,
    date_id         INT NOT NULL,
    source_id       INT NOT NULL,
    open_price      DECIMAL(12,4),
    high_price      DECIMAL(12,4),
    low_price       DECIMAL(12,4),
    close_price     DECIMAL(12,4),
    volume          BIGINT,
    dividends       DECIMAL(12,4),
    stock_splits    DECIMAL(12,4),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (source_id) REFERENCES dim_source(source_id),
    UNIQUE KEY unique_date_source (date_id, source_id),
    INDEX idx_date (date_id),
    INDEX idx_source (source_id)
);

SELECT * FROM fct_daily_stock fds  LIMIT 5;

-- End of 01_schema.sql