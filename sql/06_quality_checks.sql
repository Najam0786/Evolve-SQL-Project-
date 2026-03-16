/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 06_quality_checks.sql
   DESCRIPTION: Data quality validation on core tables
   ============================================================ */

USE gs_stock_project;

/* -----------------------------------------------------------------
   1. NULL CHECKS ON DIMENSIONS
   ----------------------------------------------------------------- */
SELECT 'dim_date' AS table_name,
       SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END) AS null_full_date,
       SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
       SUM(CASE WHEN month IS NULL THEN 1 ELSE 0 END) AS null_month,
       SUM(CASE WHEN day IS NULL THEN 1 ELSE 0 END) AS null_day,
       SUM(CASE WHEN quarter IS NULL THEN 1 ELSE 0 END) AS null_quarter,
       SUM(CASE WHEN day_of_week IS NULL THEN 1 ELSE 0 END) AS null_day_of_week,
       SUM(CASE WHEN is_weekend IS NULL THEN 1 ELSE 0 END) AS null_is_weekend
FROM dim_date;

SELECT 'dim_source' AS table_name,
       SUM(CASE WHEN source_name IS NULL OR TRIM(source_name) = '' THEN 1 ELSE 0 END) AS invalid_source_name
FROM dim_source;

/* -----------------------------------------------------------------
   2. NULL CHECKS ON FACT TABLE (key metrics)
   ----------------------------------------------------------------- */
SELECT
    SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) AS null_date_id,
    SUM(CASE WHEN source_id IS NULL THEN 1 ELSE 0 END) AS null_source_id,
    SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) AS null_open,
    SUM(CASE WHEN high_price IS NULL THEN 1 ELSE 0 END) AS null_high,
    SUM(CASE WHEN low_price IS NULL THEN 1 ELSE 0 END) AS null_low,
    SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) AS null_close,
    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS null_volume
FROM fct_daily_stock;

/* -----------------------------------------------------------------
   3. ORPHAN FOREIGN KEYS (should be zero)
   ----------------------------------------------------------------- */
SELECT COUNT(*) AS orphan_fact_no_date
FROM fct_daily_stock f
LEFT JOIN dim_date d ON d.date_id = f.date_id
WHERE d.date_id IS NULL;

SELECT COUNT(*) AS orphan_fact_no_source
FROM fct_daily_stock f
LEFT JOIN dim_source s ON s.source_id = f.source_id
WHERE s.source_id IS NULL;

/* -----------------------------------------------------------------
   4. DUPLICATES IN FACT TABLE
   Unique constraint on (date_id, source_id) should prevent duplicates.
   Check if any slip through.
   ----------------------------------------------------------------- */
SELECT date_id, source_id, COUNT(*) AS dup_count
FROM fct_daily_stock
GROUP BY date_id, source_id
HAVING COUNT(*) > 1
ORDER BY dup_count DESC;

/* -----------------------------------------------------------------
   5. DATE RANGE COVERAGE
   ----------------------------------------------------------------- */
SELECT
    MIN(full_date) AS earliest_date,
    MAX(full_date) AS latest_date,
    DATEDIFF(MAX(full_date), MIN(full_date)) AS total_days_span,
    COUNT(*) AS total_dates
FROM dim_date;

/* -----------------------------------------------------------------
   6. RANGE CHECKS ON PRICES AND VOLUME
   ----------------------------------------------------------------- */
-- Negative or zero prices (should not exist)
SELECT 'open_price' AS field, COUNT(*) AS negative_or_zero
FROM fct_daily_stock WHERE open_price <= 0
UNION ALL
SELECT 'high_price', COUNT(*) FROM fct_daily_stock WHERE high_price <= 0
UNION ALL
SELECT 'low_price', COUNT(*) FROM fct_daily_stock WHERE low_price <= 0
UNION ALL
SELECT 'close_price', COUNT(*) FROM fct_daily_stock WHERE close_price <= 0
UNION ALL
SELECT 'volume', COUNT(*) FROM fct_daily_stock WHERE volume <= 0;

-- Check price consistency: high >= low
SELECT COUNT(*) AS inconsistent_high_low
FROM fct_daily_stock
WHERE high_price < low_price;

-- Check open price within the day's range (allow small rounding tolerance)
SELECT COUNT(*) AS open_outside_range
FROM fct_daily_stock
WHERE open_price < low_price - 0.01 OR open_price > high_price + 0.01;

/* -----------------------------------------------------------------
   7. OUTLIER DETECTION (using z-score)
   ----------------------------------------------------------------- */
-- Price outliers (|z| > 3)
WITH source_stats AS (
    SELECT source_id,
           AVG(close_price) AS avg_close,
           STDDEV(close_price) AS std_close
    FROM fct_daily_stock
    GROUP BY source_id
)
SELECT f.fact_id, f.source_id, s2.source_name, f.close_price,
       ROUND(s.avg_close,2) AS avg_close_source,
       ROUND(s.std_close,2) AS std_close_source,
       ROUND(ABS(f.close_price - s.avg_close) / NULLIF(s.std_close, 0), 2) AS z_score
FROM fct_daily_stock f
JOIN source_stats s ON s.source_id = f.source_id
JOIN dim_source s2 ON s2.source_id = f.source_id
WHERE ABS(f.close_price - s.avg_close) / NULLIF(s.std_close, 0) > 3
ORDER BY z_score DESC
LIMIT 20;

-- Volume outliers (|z| > 3)
WITH source_vol_stats AS (
    SELECT source_id,
           AVG(volume) AS avg_vol,
           STDDEV(volume) AS std_vol
    FROM fct_daily_stock
    GROUP BY source_id
)
SELECT f.fact_id, f.source_id, s2.source_name, f.volume,
       ROUND(v.avg_vol,0) AS avg_vol_source,
       ROUND(v.std_vol,0) AS std_vol_source,
       ROUND(ABS(f.volume - v.avg_vol) / NULLIF(v.std_vol, 0), 2) AS z_score
FROM fct_daily_stock f
JOIN source_vol_stats v ON v.source_id = f.source_id
JOIN dim_source s2 ON s2.source_id = f.source_id
WHERE ABS(f.volume - v.avg_vol) / NULLIF(v.std_vol, 0) > 3
ORDER BY z_score DESC
LIMIT 20;

/* -----------------------------------------------------------------
   8. CHECK FOR MISSING TRADING DAYS (weekends/holidays)
   ----------------------------------------------------------------- */
-- Number of trading days per month (should be ~20-22)
SELECT year, month, COUNT(*) AS days_count
FROM dim_date
GROUP BY year, month
ORDER BY year, month;

-- Dates missing for a particular source (example: check if 'Yahoo Finance' has all dates)
-- Replace 'Yahoo Finance' with any source name from dim_source.
SELECT d.full_date
FROM dim_date d
LEFT JOIN fct_daily_stock f ON f.date_id = d.date_id
LEFT JOIN dim_source s ON s.source_id = f.source_id
    AND s.source_name = 'Yahoo Finance'   -- change as needed
WHERE f.fact_id IS NULL
ORDER BY d.full_date
LIMIT 20;  -- limit output to avoid flooding

/* -----------------------------------------------------------------
   9. DIVIDEND AND SPLIT CHECKS
   ----------------------------------------------------------------- */
SELECT COUNT(*) AS dividend_days,
       MIN(dividends) AS min_dividend,
       MAX(dividends) AS max_dividend
FROM fct_daily_stock
WHERE dividends > 0;

SELECT COUNT(*) AS split_days,
       MIN(stock_splits) AS min_split,
       MAX(stock_splits) AS max_split
FROM fct_daily_stock
WHERE stock_splits > 0;

/* -----------------------------------------------------------------
   10. SUMMARY STATISTICS FOR FACT TABLE
   ----------------------------------------------------------------- */
SELECT
    COUNT(*) AS total_fact_rows,
    COUNT(DISTINCT date_id) AS distinct_dates,
    COUNT(DISTINCT source_id) AS distinct_sources,
    ROUND(AVG(close_price),2) AS overall_avg_close,
    ROUND(MIN(close_price),2) AS min_close,
    ROUND(MAX(close_price),2) AS max_close,
    SUM(volume) AS total_volume
FROM fct_daily_stock;

/* -----------------------------------------------------------------
   INTERPRETATION:
   - All null counts should be 0.
   - Orphan key counts should be 0.
   - Duplicates should be 0.
   - Negative/zero prices should be 0.
   - Inconsistent high/low should be 0.
   - Outliers are expected; investigate if excessive.
   - Monthly day counts help spot missing periods.
   ----------------------------------------------------------------- */
