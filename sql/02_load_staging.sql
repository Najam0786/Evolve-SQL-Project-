/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 02_load_staging.sql (CORRECTED COLUMN NAMES)
   DESCRIPTION: Validate staging tables after CSV import
   ============================================================ */

USE gs_stock_project;

/* -----------------------------------------------------------------
   1. ROW COUNTS PER TABLE
   Expected: each table should have ~6,700+ rows (1999-2026)
   ----------------------------------------------------------------- */
SELECT 'stg_barchart_raw' AS table_name, COUNT(*) AS row_count FROM stg_barchart_raw
UNION ALL SELECT 'stg_investing_raw', COUNT(*) FROM stg_investing_raw
UNION ALL SELECT 'stg_marketwatch_raw', COUNT(*) FROM stg_marketwatch_raw
UNION ALL SELECT 'stg_master_raw', COUNT(*) FROM stg_master_raw
UNION ALL SELECT 'stg_nasdaq_raw', COUNT(*) FROM stg_nasdaq_raw
UNION ALL SELECT 'stg_yahoo_raw', COUNT(*) FROM stg_yahoo_raw
ORDER BY table_name;

/* -----------------------------------------------------------------
   2. SAMPLE ROWS (first 5 from each table)
   ----------------------------------------------------------------- */
SELECT 'barchart' AS source, t.* FROM (SELECT * FROM stg_barchart_raw LIMIT 5) t;
SELECT 'investing' AS source, t.* FROM (SELECT * FROM stg_investing_raw LIMIT 5) t;
SELECT 'marketwatch' AS source, t.* FROM (SELECT * FROM stg_marketwatch_raw LIMIT 5) t;
SELECT 'master' AS source, t.* FROM (SELECT * FROM stg_master_raw LIMIT 5) t;
SELECT 'nasdaq' AS source, t.* FROM (SELECT * FROM stg_nasdaq_raw LIMIT 5) t;
SELECT 'yahoo' AS source, t.* FROM (SELECT * FROM stg_yahoo_raw LIMIT 5) t;

/* -----------------------------------------------------------------
   3. NULL / EMPTY CHECKS ON KEY COLUMNS
   ----------------------------------------------------------------- */
SELECT 'barchart' AS source,
       SUM(CASE WHEN `Date` IS NULL OR TRIM(`Date`) = '' THEN 1 ELSE 0 END) AS null_date,
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END) AS null_open,
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END) AS null_high,
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END) AS null_low,
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END) AS null_close,
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END) AS null_volume,
       SUM(CASE WHEN `Source` IS NULL OR TRIM(`Source`) = '' THEN 1 ELSE 0 END) AS null_source
FROM stg_barchart_raw
UNION ALL
SELECT 'investing',
       SUM(CASE WHEN `Date` IS NULL OR TRIM(`Date`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Source` IS NULL OR TRIM(`Source`) = '' THEN 1 ELSE 0 END)
FROM stg_investing_raw
UNION ALL
SELECT 'marketwatch',
       SUM(CASE WHEN `Date` IS NULL OR TRIM(`Date`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Source` IS NULL OR TRIM(`Source`) = '' THEN 1 ELSE 0 END)
FROM stg_marketwatch_raw
UNION ALL
SELECT 'master',
       SUM(CASE WHEN `Date` IS NULL OR TRIM(`Date`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Source` IS NULL OR TRIM(`Source`) = '' THEN 1 ELSE 0 END)
FROM stg_master_raw
UNION ALL
SELECT 'nasdaq',
       SUM(CASE WHEN `Date` IS NULL OR TRIM(`Date`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Source` IS NULL OR TRIM(`Source`) = '' THEN 1 ELSE 0 END)
FROM stg_nasdaq_raw
UNION ALL
SELECT 'yahoo',
       SUM(CASE WHEN `Date` IS NULL OR TRIM(`Date`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Source` IS NULL OR TRIM(`Source`) = '' THEN 1 ELSE 0 END)
FROM stg_yahoo_raw;

/* -----------------------------------------------------------------
   4. DATE FORMAT VALIDATION
   ----------------------------------------------------------------- */
-- NASDAQ: expects MM/DD/YYYY
SELECT 'nasdaq' AS source,
       COUNT(*) AS total_rows,
       SUM(CASE WHEN `Date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}' THEN 1 ELSE 0 END) AS valid_format,
       SUM(CASE WHEN `Date` NOT REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}' THEN 1 ELSE 0 END) AS invalid_format
FROM stg_nasdaq_raw;

-- Other sources: expects YYYY-MM-DD at start (may have timezone)
SELECT 'barchart' AS source,
       COUNT(*) AS total_rows,
       SUM(CASE WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END) AS valid_format,
       SUM(CASE WHEN `Date` NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END) AS invalid_format
FROM stg_barchart_raw
UNION ALL
SELECT 'investing',
       COUNT(*),
       SUM(CASE WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Date` NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END)
FROM stg_investing_raw
UNION ALL
SELECT 'marketwatch',
       COUNT(*),
       SUM(CASE WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Date` NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END)
FROM stg_marketwatch_raw
UNION ALL
SELECT 'master',
       COUNT(*),
       SUM(CASE WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Date` NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END)
FROM stg_master_raw
UNION ALL
SELECT 'yahoo',
       COUNT(*),
       SUM(CASE WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Date` NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 1 ELSE 0 END)
FROM stg_yahoo_raw;

/* -----------------------------------------------------------------
   5. NUMERIC FIELD VALIDATION (basic pattern checks)
   ----------------------------------------------------------------- */
-- For tables with plain numbers (barchart, investing, marketwatch, master, yahoo)
SELECT 'barchart' AS source,
       SUM(CASE WHEN `Open` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS bad_open,
       SUM(CASE WHEN `High` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS bad_high,
       SUM(CASE WHEN `Low` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS bad_low,
       SUM(CASE WHEN `Close` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS bad_close,
       SUM(CASE WHEN `Volume` NOT REGEXP '^[0-9]+$' THEN 1 ELSE 0 END) AS bad_volume
FROM stg_barchart_raw
UNION ALL
SELECT 'investing',
       SUM(CASE WHEN `Open` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` NOT REGEXP '^[0-9]+$' THEN 1 ELSE 0 END)
FROM stg_investing_raw
UNION ALL
SELECT 'marketwatch',
       SUM(CASE WHEN `Open` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` NOT REGEXP '^[0-9]+$' THEN 1 ELSE 0 END)
FROM stg_marketwatch_raw
UNION ALL
SELECT 'master',
       SUM(CASE WHEN `Open` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` NOT REGEXP '^[0-9]+$' THEN 1 ELSE 0 END)
FROM stg_master_raw
UNION ALL
SELECT 'yahoo',
       SUM(CASE WHEN `Open` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `High` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Low` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Close` NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END),
       SUM(CASE WHEN `Volume` NOT REGEXP '^[0-9]+$' THEN 1 ELSE 0 END)
FROM stg_yahoo_raw;

-- For NASDAQ: just check for non‑null (prices have $ and commas; will be cleaned later)
SELECT 'nasdaq' AS source,
       COUNT(*) AS total_rows,
       SUM(CASE WHEN `Open` IS NULL OR TRIM(`Open`) = '' THEN 1 ELSE 0 END) AS null_open,
       SUM(CASE WHEN `High` IS NULL OR TRIM(`High`) = '' THEN 1 ELSE 0 END) AS null_high,
       SUM(CASE WHEN `Low` IS NULL OR TRIM(`Low`) = '' THEN 1 ELSE 0 END) AS null_low,
       SUM(CASE WHEN `Close` IS NULL OR TRIM(`Close`) = '' THEN 1 ELSE 0 END) AS null_close,
       SUM(CASE WHEN `Volume` IS NULL OR TRIM(`Volume`) = '' THEN 1 ELSE 0 END) AS null_volume
FROM stg_nasdaq_raw;

/* -----------------------------------------------------------------
   6. DUPLICATE ROWS WITHIN EACH TABLE (by date)
   ----------------------------------------------------------------- */
SELECT 'barchart' AS source, `Date`, COUNT(*) AS dup_count
FROM stg_barchart_raw
GROUP BY `Date`
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 5;

SELECT 'investing' AS source, `Date`, COUNT(*) AS dup_count
FROM stg_investing_raw
GROUP BY `Date`
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 5;

SELECT 'marketwatch' AS source, `Date`, COUNT(*) AS dup_count
FROM stg_marketwatch_raw
GROUP BY `Date`
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 5;

SELECT 'master' AS source, `Date`, COUNT(*) AS dup_count
FROM stg_master_raw
GROUP BY `Date`
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 5;

SELECT 'nasdaq' AS source, `Date`, COUNT(*) AS dup_count
FROM stg_nasdaq_raw
GROUP BY `Date`
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 5;

SELECT 'yahoo' AS source, `Date`, COUNT(*) AS dup_count
FROM stg_yahoo_raw
GROUP BY `Date`
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 5;

