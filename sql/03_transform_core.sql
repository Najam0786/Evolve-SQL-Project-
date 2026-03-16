/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 03_transform_core.sql (with INSERT IGNORE for safety)
   DESCRIPTION: Clean and load core tables from staging
   ============================================================ */

USE gs_stock_project;

-- Disable foreign key checks to allow truncation of tables with dependencies
SET FOREIGN_KEY_CHECKS = 0;

-- Truncate core tables (order matters: fact first, then dimensions)
TRUNCATE TABLE fct_daily_stock;
TRUNCATE TABLE dim_source;
TRUNCATE TABLE dim_date;

SET FOREIGN_KEY_CHECKS = 1;

/* -----------------------------------------------------------------
   1. DIMENSION: dim_source
   ----------------------------------------------------------------- */
INSERT INTO dim_source (source_name)
SELECT DISTINCT `Source` FROM (
    SELECT `Source` FROM stg_barchart_raw
    UNION
    SELECT `Source` FROM stg_investing_raw
    UNION
    SELECT `Source` FROM stg_marketwatch_raw
    UNION
    SELECT `Source` FROM stg_master_raw
    UNION
    SELECT `Source` FROM stg_nasdaq_raw
    UNION
    SELECT `Source` FROM stg_yahoo_raw
) all_sources
WHERE `Source` IS NOT NULL AND TRIM(`Source`) != ''
ORDER BY `Source`;

-- Quick check
SELECT 'dim_source' AS table_name, COUNT(*) AS row_count FROM dim_source;

/* -----------------------------------------------------------------
   2. DIMENSION: dim_date (corrected for timezone strings)
   ----------------------------------------------------------------- */
INSERT INTO dim_date (full_date, year, month, day, quarter, day_of_week, is_weekend)
WITH all_dates AS (
    -- Sources with YYYY-MM-DD (may have timezone) – extract date part before space
    SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1)) AS trade_date
    FROM stg_barchart_raw
    WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
    UNION
    SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1))
    FROM stg_investing_raw
    WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
    UNION
    SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1))
    FROM stg_marketwatch_raw
    WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
    UNION
    SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1))
    FROM stg_master_raw
    WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
    UNION
    SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1))
    FROM stg_yahoo_raw
    WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
    UNION
    -- NASDAQ: MM/DD/YYYY (no timezone) – parse with STR_TO_DATE
    SELECT DISTINCT STR_TO_DATE(TRIM(`Date`), '%m/%d/%Y') AS trade_date
    FROM stg_nasdaq_raw
    WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
)
SELECT
    trade_date,
    YEAR(trade_date) AS year,
    MONTH(trade_date) AS month,
    DAY(trade_date) AS day,
    QUARTER(trade_date) AS quarter,
    DAYOFWEEK(trade_date) AS day_of_week,
    CASE WHEN DAYOFWEEK(trade_date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM all_dates
WHERE trade_date IS NOT NULL
ORDER BY trade_date;

-- Quick check
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date;
SELECT MIN(full_date), MAX(full_date) FROM dim_date;

/* -----------------------------------------------------------------
   3. FACT TABLE: fct_daily_stock (with INSERT IGNORE for safety)
   ----------------------------------------------------------------- */

-- Barchart
INSERT IGNORE INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
SELECT
    d.date_id,
    s.source_id,
    CAST(TRIM(b.`Open`) AS DECIMAL(12,4)),
    CAST(TRIM(b.`High`) AS DECIMAL(12,4)),
    CAST(TRIM(b.`Low`) AS DECIMAL(12,4)),
    CAST(TRIM(b.`Close`) AS DECIMAL(12,4)),
    CAST(TRIM(b.`Volume`) AS UNSIGNED),
    CAST(TRIM(b.`Dividends`) AS DECIMAL(12,4)),
    CAST(TRIM(b.`Stock Splits`) AS DECIMAL(12,4))
FROM stg_barchart_raw b
INNER JOIN dim_date d ON d.full_date = DATE(SUBSTRING_INDEX(TRIM(b.`Date`), ' ', 1))
INNER JOIN dim_source s ON s.source_name = b.`Source`
WHERE b.`Open` IS NOT NULL AND TRIM(b.`Open`) != '';

-- Investing.com
INSERT IGNORE INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
SELECT
    d.date_id,
    s.source_id,
    CAST(TRIM(i.`Open`) AS DECIMAL(12,4)),
    CAST(TRIM(i.`High`) AS DECIMAL(12,4)),
    CAST(TRIM(i.`Low`) AS DECIMAL(12,4)),
    CAST(TRIM(i.`Close`) AS DECIMAL(12,4)),
    CAST(TRIM(i.`Volume`) AS UNSIGNED),
    CAST(TRIM(i.`Dividends`) AS DECIMAL(12,4)),
    CAST(TRIM(i.`Stock Splits`) AS DECIMAL(12,4))
FROM stg_investing_raw i
INNER JOIN dim_date d ON d.full_date = DATE(SUBSTRING_INDEX(TRIM(i.`Date`), ' ', 1))
INNER JOIN dim_source s ON s.source_name = i.`Source`
WHERE i.`Open` IS NOT NULL AND TRIM(i.`Open`) != '';

-- MarketWatch
INSERT IGNORE INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
SELECT
    d.date_id,
    s.source_id,
    CAST(TRIM(mw.`Open`) AS DECIMAL(12,4)),
    CAST(TRIM(mw.`High`) AS DECIMAL(12,4)),
    CAST(TRIM(mw.`Low`) AS DECIMAL(12,4)),
    CAST(TRIM(mw.`Close`) AS DECIMAL(12,4)),
    CAST(TRIM(mw.`Volume`) AS UNSIGNED),
    CAST(TRIM(mw.`Dividends`) AS DECIMAL(12,4)),
    CAST(TRIM(mw.`Stock Splits`) AS DECIMAL(12,4))
FROM stg_marketwatch_raw mw
INNER JOIN dim_date d ON d.full_date = DATE(SUBSTRING_INDEX(TRIM(mw.`Date`), ' ', 1))
INNER JOIN dim_source s ON s.source_name = mw.`Source`
WHERE mw.`Open` IS NOT NULL AND TRIM(mw.`Open`) != '';

-- Master dataset
INSERT IGNORE INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
SELECT
    d.date_id,
    s.source_id,
    CAST(TRIM(m.`Open`) AS DECIMAL(12,4)),
    CAST(TRIM(m.`High`) AS DECIMAL(12,4)),
    CAST(TRIM(m.`Low`) AS DECIMAL(12,4)),
    CAST(TRIM(m.`Close`) AS DECIMAL(12,4)),
    CAST(TRIM(m.`Volume`) AS UNSIGNED),
    CAST(TRIM(m.`Dividends`) AS DECIMAL(12,4)),
    CAST(TRIM(m.`Stock Splits`) AS DECIMAL(12,4))
FROM stg_master_raw m
INNER JOIN dim_date d ON d.full_date = DATE(SUBSTRING_INDEX(TRIM(m.`Date`), ' ', 1))
INNER JOIN dim_source s ON s.source_name = m.`Source`
WHERE m.`Open` IS NOT NULL AND TRIM(m.`Open`) != '';

-- NASDAQ (clean $ and commas)
INSERT IGNORE INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
SELECT
    d.date_id,
    s.source_id,
    CAST(REPLACE(REPLACE(TRIM(n.`Open`), '$', ''), ',', '') AS DECIMAL(12,4)),
    CAST(REPLACE(REPLACE(TRIM(n.`High`), '$', ''), ',', '') AS DECIMAL(12,4)),
    CAST(REPLACE(REPLACE(TRIM(n.`Low`), '$', ''), ',', '') AS DECIMAL(12,4)),
    CAST(REPLACE(REPLACE(TRIM(n.`Close`), '$', ''), ',', '') AS DECIMAL(12,4)),
    CAST(REPLACE(TRIM(n.`Volume`), ',', '') AS UNSIGNED),
    0.0,
    0.0
FROM stg_nasdaq_raw n
INNER JOIN dim_date d ON d.full_date = STR_TO_DATE(TRIM(n.`Date`), '%m/%d/%Y')
INNER JOIN dim_source s ON s.source_name = n.`Source`
WHERE n.`Open` IS NOT NULL AND TRIM(n.`Open`) != '';

-- Yahoo Finance
INSERT IGNORE INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
SELECT
    d.date_id,
    s.source_id,
    CAST(TRIM(y.`Open`) AS DECIMAL(12,4)),
    CAST(TRIM(y.`High`) AS DECIMAL(12,4)),
    CAST(TRIM(y.`Low`) AS DECIMAL(12,4)),
    CAST(TRIM(y.`Close`) AS DECIMAL(12,4)),
    CAST(TRIM(y.`Volume`) AS UNSIGNED),
    CAST(TRIM(y.`Dividends`) AS DECIMAL(12,4)),
    CAST(TRIM(y.`Stock Splits`) AS DECIMAL(12,4))
FROM stg_yahoo_raw y
INNER JOIN dim_date d ON d.full_date = DATE(SUBSTRING_INDEX(TRIM(y.`Date`), ' ', 1))
INNER JOIN dim_source s ON s.source_name = y.`Source`
WHERE y.`Open` IS NOT NULL AND TRIM(y.`Open`) != '';

-- Final count
SELECT 'fct_daily_stock' AS table_name, COUNT(*) AS row_count FROM fct_daily_stock;

SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM dim_source;
SELECT COUNT(*) FROM fct_daily_stock;
