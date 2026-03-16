/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 04_semantic_views.sql
   DESCRIPTION: Semantic layer views for analysis
   ============================================================ */

USE gs_stock_project;

/* -----------------------------------------------------------------
   View 1: vw_stock_enriched
   Enriches fact table with date attributes and source name,
   plus calculated daily metrics.
   ----------------------------------------------------------------- */
CREATE OR REPLACE VIEW vw_stock_enriched AS
SELECT
    f.fact_id,
    f.date_id,
    d.full_date,
    d.year,
    d.month,
    d.day,
    d.quarter,
    d.day_of_week,
    d.is_weekend,
    f.source_id,
    s.source_name,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.volume,
    f.dividends,
    f.stock_splits,
    -- Calculated fields
    (f.close_price - f.open_price) AS daily_change,
    ROUND(100 * (f.close_price - f.open_price) / NULLIF(f.open_price, 0), 2) AS daily_pct_change,
    (f.high_price - f.low_price) AS daily_range,
    ROUND(100 * (f.high_price - f.low_price) / NULLIF(f.low_price, 0), 2) AS daily_range_pct,
    CASE
        WHEN f.close_price > f.open_price THEN 'UP'
        WHEN f.close_price < f.open_price THEN 'DOWN'
        ELSE 'FLAT'
    END AS price_direction
FROM fct_daily_stock f
JOIN dim_date d ON d.date_id = f.date_id
JOIN dim_source s ON s.source_id = f.source_id;

SELECT * FROM vw_stock_enriched LIMIT 5;

/* -----------------------------------------------------------------
   View 2: vw_monthly_source_stats
   Monthly aggregates by source: average close, total volume, etc.
   ----------------------------------------------------------------- */
CREATE OR REPLACE VIEW vw_monthly_source_stats AS
SELECT
    d.year,
    d.month,
    DATE_FORMAT(d.full_date, '%Y-%m-01') AS month_start,
    s.source_id,
    s.source_name,
    COUNT(*) AS trading_days,
    ROUND(AVG(f.close_price), 2) AS avg_close_price,
    ROUND(AVG(f.high_price), 2) AS avg_high_price,
    ROUND(AVG(f.low_price), 2) AS avg_low_price,
    SUM(f.volume) AS total_volume,
    ROUND(AVG(f.volume), 0) AS avg_daily_volume,
    SUM(f.dividends) AS total_dividends,
    MAX(f.high_price) AS max_high,
    MIN(f.low_price) AS min_low,
    ROUND(AVG(f.high_price - f.low_price), 2) AS avg_daily_range
FROM fct_daily_stock f
JOIN dim_date d ON d.date_id = f.date_id
JOIN dim_source s ON s.source_id = f.source_id
GROUP BY d.year, d.month, month_start, s.source_id, s.source_name
ORDER BY d.year, d.month, s.source_name;

SELECT * FROM vw_monthly_source_stats vmss  LIMIT 5;

/* -----------------------------------------------------------------
   View 3: vw_source_comparison (optional, but useful)
   Compares closing prices across sources for each date.
   ----------------------------------------------------------------- */
CREATE OR REPLACE VIEW vw_source_comparison AS
SELECT
    d.full_date,
    MAX(CASE WHEN s.source_name LIKE '%Barchart%' THEN f.close_price END) AS barchart_close,
    MAX(CASE WHEN s.source_name LIKE '%Investing%' THEN f.close_price END) AS investing_close,
    MAX(CASE WHEN s.source_name LIKE '%MarketWatch%' THEN f.close_price END) AS marketwatch_close,
    MAX(CASE WHEN s.source_name LIKE '%NASDAQ%' THEN f.close_price END) AS nasdaq_close,
    MAX(CASE WHEN s.source_name LIKE '%Yahoo%' THEN f.close_price END) AS yahoo_close,
    MAX(CASE WHEN s.source_name LIKE '%master%' OR s.source_name = 'Master' THEN f.close_price END) AS master_close
FROM fct_daily_stock f
JOIN dim_date d ON d.date_id = f.date_id
JOIN dim_source s ON s.source_id = f.source_id
GROUP BY d.full_date
ORDER BY d.full_date;

SELECT * FROM vw_source_comparison vsc  LIMIT 5;
