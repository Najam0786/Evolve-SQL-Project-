/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 05_analysis_queries.sql
   DESCRIPTION: Analytical queries answering business questions
   ============================================================ */

USE gs_stock_project;

/* -----------------------------------------------------------------
   Q1: Monthly average closing price trend
   Shows how GS stock performed over the years.
   ----------------------------------------------------------------- */
SELECT
    year,
    month,
    month_start,
    source_name,
    avg_close_price,
    total_volume
FROM vw_monthly_source_stats
ORDER BY source_name, year, month;

/* -----------------------------------------------------------------
   Q2: Top 10 days with highest trading volume (all sources combined)
   Identifies days of unusual activity (earnings, news, etc.)
   ----------------------------------------------------------------- */
SELECT
    full_date,
    source_name,
    volume,
    close_price,
    daily_pct_change
FROM vw_stock_enriched
ORDER BY volume DESC
LIMIT 10;

/* -----------------------------------------------------------------
   Q3: Year-over-year growth in average annual close price
   (Average across all sources for each year – gives a general trend)
   ----------------------------------------------------------------- */
WITH annual_avg AS (
    SELECT
        year,
        AVG(close_price) AS avg_close
    FROM vw_stock_enriched
    GROUP BY year
),
yoy_growth AS (
    SELECT
        year,
        ROUND(avg_close, 2) AS avg_close,
        ROUND(LAG(avg_close) OVER (ORDER BY year), 2) AS prev_year_avg,
        ROUND(100 * (avg_close - LAG(avg_close) OVER (ORDER BY year)) / NULLIF(LAG(avg_close) OVER (ORDER BY year), 0), 2) AS yoy_pct_change
    FROM annual_avg
)
SELECT * FROM yoy_growth
ORDER BY year;

/* -----------------------------------------------------------------
   Q4: Days with largest positive/negative daily percentage change
   (Volatility spikes)
   ----------------------------------------------------------------- */
-- Top 10 gains
SELECT
    full_date,
    source_name,
    close_price,
    daily_pct_change
FROM vw_stock_enriched
ORDER BY daily_pct_change DESC
LIMIT 10;

-- Top 10 losses
SELECT
    full_date,
    source_name,
    close_price,
    daily_pct_change
FROM vw_stock_enriched
ORDER BY daily_pct_change ASC
LIMIT 10;

/* -----------------------------------------------------------------
   Q5: Source consistency – days where sources disagree by >2%
   Identifies potential data discrepancies
   ----------------------------------------------------------------- */
WITH source_prices AS (
    SELECT
        full_date,
        source_name,
        close_price
    FROM vw_stock_enriched
),
price_spread AS (
    SELECT
        full_date,
        MAX(close_price) AS max_price,
        MIN(close_price) AS min_price,
        ROUND(100 * (MAX(close_price) - MIN(close_price)) / NULLIF(MIN(close_price), 0), 2) AS pct_spread
    FROM source_prices
    GROUP BY full_date
    HAVING COUNT(*) > 1  -- at least two sources present
)
SELECT *
FROM price_spread
WHERE pct_spread > 2
ORDER BY pct_spread DESC;

/* -----------------------------------------------------------------
   Q6: Top 5 years by total trading volume (sum of all sources)
   ----------------------------------------------------------------- */
SELECT
    year,
    SUM(volume) AS total_volume
FROM vw_stock_enriched
GROUP BY year
ORDER BY total_volume DESC
LIMIT 5;

/* -----------------------------------------------------------------
   Q7: Monthly volatility (average daily range) for each source
   ----------------------------------------------------------------- */
SELECT
    year,
    month,
    month_start,
    source_name,
    avg_daily_range
FROM vw_monthly_source_stats
ORDER BY year, month, source_name;

/* -----------------------------------------------------------------
   Q8: Dividend impact – compare price change on ex-dividend days
   (Days where dividends > 0)
   ----------------------------------------------------------------- */
SELECT
    full_date,
    source_name,
    dividends,
    close_price,
    daily_pct_change
FROM vw_stock_enriched
WHERE dividends > 0
ORDER BY full_date;

/* -----------------------------------------------------------------
   Q9: Rolling 30-day average closing price (using window function)
   (Uses daily average across all sources to create a single time series)
   ----------------------------------------------------------------- */
WITH daily_avg AS (
    SELECT
        full_date,
        AVG(close_price) AS avg_close
    FROM vw_stock_enriched
    GROUP BY full_date
)
SELECT
    full_date,
    ROUND(avg_close, 2) AS avg_close,
    ROUND(AVG(avg_close) OVER (ORDER BY full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS rolling_30d_avg
FROM daily_avg
ORDER BY full_date;

/* -----------------------------------------------------------------
   Q10: Rank years by average closing price (across all sources)
   ----------------------------------------------------------------- */
SELECT
    year,
    ROUND(AVG(close_price), 2) AS avg_close,
    DENSE_RANK() OVER (ORDER BY AVG(close_price) DESC) AS rank_by_price
FROM vw_stock_enriched
GROUP BY year
ORDER BY avg_close DESC;

/* -----------------------------------------------------------------
   Q11: Percentage of days where stock closed higher than opened (by year)
   (Uses daily average across all sources to determine overall direction)
   ----------------------------------------------------------------- */
WITH daily_direction AS (
    SELECT
        year,
        full_date,
        AVG(close_price) AS avg_close,
        AVG(open_price) AS avg_open
    FROM vw_stock_enriched
    GROUP BY year, full_date
)
SELECT
    year,
    COUNT(*) AS trading_days,
    SUM(CASE WHEN avg_close > avg_open THEN 1 ELSE 0 END) AS up_days,
    ROUND(100 * SUM(CASE WHEN avg_close > avg_open THEN 1 ELSE 0 END) / COUNT(*), 2) AS up_pct
FROM daily_direction
GROUP BY year
ORDER BY year;

/* -----------------------------------------------------------------
   Q12: Top 3 months with highest average daily range (volatility)
   (Uses monthly view which already aggregates across sources per month)
   ----------------------------------------------------------------- */
SELECT
    year,
    month,
    month_start,
    avg_daily_range
FROM vw_monthly_source_stats
ORDER BY avg_daily_range DESC
LIMIT 3;
