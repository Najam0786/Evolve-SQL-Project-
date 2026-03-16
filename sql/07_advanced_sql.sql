/* ============================================================
   PROJECT: Goldman Sachs Stock Data (1999-2026)
   FILE: 07_advanced_sql.sql (FINAL)
   DESCRIPTION: Advanced SQL objects (function, procedure, trigger)
   NOTE: Master dataset omitted to avoid duplicate source conflict.
   ============================================================ */

USE gs_stock_project;

/* -----------------------------------------------------------------
   1. FUNCTION: fn_safe_percent_change
   Safely computes percentage change between two numbers.
   Returns NULL if denominator is zero or NULL.
   ----------------------------------------------------------------- */
DELIMITER $$

DROP FUNCTION IF EXISTS fn_safe_percent_change$$
CREATE FUNCTION fn_safe_percent_change(
    p_new DECIMAL(14,4),
    p_old DECIMAL(14,4)
)
RETURNS DECIMAL(10,2)
DETERMINISTIC
BEGIN
    IF p_old IS NULL OR p_old = 0 THEN
        RETURN NULL;
    END IF;
    RETURN ROUND(100 * (p_new - p_old) / p_old, 2);
END$$

DELIMITER ;

/* -----------------------------------------------------------------
   2. PROCEDURE: sp_refresh_core_from_staging
   Truncates and reloads core tables from staging.
   Uses DELETE instead of TRUNCATE to stay within transaction.
   Master dataset is excluded to avoid duplicate source entries.
   ----------------------------------------------------------------- */
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_refresh_core_from_staging$$
CREATE PROCEDURE sp_refresh_core_from_staging()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Error refreshing core from staging. Transaction rolled back.';
    END;

    START TRANSACTION;

    -- Disable foreign key checks for deletion
    SET FOREIGN_KEY_CHECKS = 0;

    -- Delete all rows from core tables (order matters: fact first)
    DELETE FROM fct_daily_stock;
    DELETE FROM dim_source;
    DELETE FROM dim_date;

    SET FOREIGN_KEY_CHECKS = 1;

    -- Re-populate dim_source (using the actual column name `Source`)
    INSERT INTO dim_source (source_name)
    SELECT DISTINCT `Source` FROM (
        SELECT `Source` FROM stg_barchart_raw
        UNION
        SELECT `Source` FROM stg_investing_raw
        UNION
        SELECT `Source` FROM stg_marketwatch_raw
        -- Master dataset excluded because it duplicates source names
        -- UNION SELECT `Source` FROM stg_master_raw
        UNION
        SELECT `Source` FROM stg_nasdaq_raw
        UNION
        SELECT `Source` FROM stg_yahoo_raw
    ) all_sources
    WHERE `Source` IS NOT NULL AND TRIM(`Source`) != ''
    ORDER BY `Source`;

    -- Re-populate dim_date
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
        -- Master dataset excluded (dates already covered)
        -- UNION SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1))
        -- FROM stg_master_raw
        -- WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
        UNION
        SELECT DISTINCT DATE(SUBSTRING_INDEX(TRIM(`Date`), ' ', 1))
        FROM stg_yahoo_raw
        WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
        UNION
        -- NASDAQ: MM/DD/YYYY
        SELECT DISTINCT STR_TO_DATE(TRIM(`Date`), '%m/%d/%Y') AS trade_date
        FROM stg_nasdaq_raw
        WHERE `Date` IS NOT NULL AND TRIM(`Date`) != ''
    )
    SELECT
        trade_date,
        YEAR(trade_date),
        MONTH(trade_date),
        DAY(trade_date),
        QUARTER(trade_date),
        DAYOFWEEK(trade_date),
        CASE WHEN DAYOFWEEK(trade_date) IN (1,7) THEN TRUE ELSE FALSE END
    FROM all_dates
    WHERE trade_date IS NOT NULL;

    -- Re-populate fct_daily_stock from each staging table (excluding master)

    -- Barchart
    INSERT INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
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
    INSERT INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
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
    INSERT INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
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

    -- NASDAQ (clean $ and commas)
    INSERT INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
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
    INSERT INTO fct_daily_stock (date_id, source_id, open_price, high_price, low_price, close_price, volume, dividends, stock_splits)
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

    COMMIT;

    SELECT COUNT(*) AS fact_rows_after_refresh FROM fct_daily_stock;
END$$

DELIMITER ;

/* -----------------------------------------------------------------
   3. TRIGGER: trg_fct_stock_bi_validate
   BEFORE INSERT on fct_daily_stock to enforce business rules.
   ----------------------------------------------------------------- */
DELIMITER $$

DROP TRIGGER IF EXISTS trg_fct_stock_bi_validate$$
CREATE TRIGGER trg_fct_stock_bi_validate
BEFORE INSERT ON fct_daily_stock
FOR EACH ROW
BEGIN
    IF NEW.open_price IS NULL OR NEW.open_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'open_price must be positive and not NULL';
    END IF;
    IF NEW.high_price IS NULL OR NEW.high_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'high_price must be positive and not NULL';
    END IF;
    IF NEW.low_price IS NULL OR NEW.low_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'low_price must be positive and not NULL';
    END IF;
    IF NEW.close_price IS NULL OR NEW.close_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'close_price must be positive and not NULL';
    END IF;
    IF NEW.high_price < NEW.low_price THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'high_price cannot be less than low_price';
    END IF;
END$$

DELIMITER ;

/* -----------------------------------------------------------------
   4. TRIGGER: trg_fct_stock_bu_validate
   BEFORE UPDATE on fct_daily_stock (similar checks)
   ----------------------------------------------------------------- */
DELIMITER $$

DROP TRIGGER IF EXISTS trg_fct_stock_bu_validate$$
CREATE TRIGGER trg_fct_stock_bu_validate
BEFORE UPDATE ON fct_daily_stock
FOR EACH ROW
BEGIN
    IF NEW.open_price IS NULL OR NEW.open_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'open_price must be positive and not NULL';
    END IF;
    IF NEW.high_price IS NULL OR NEW.high_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'high_price must be positive and not NULL';
    END IF;
    IF NEW.low_price IS NULL OR NEW.low_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'low_price must be positive and not NULL';
    END IF;
    IF NEW.close_price IS NULL OR NEW.close_price <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'close_price must be positive and not NULL';
    END IF;
    IF NEW.high_price < NEW.low_price THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'high_price cannot be less than low_price';
    END IF;
END$$

DELIMITER ;

/* -----------------------------------------------------------------
   5. SMOKE TESTS
   ----------------------------------------------------------------- */
-- Test the function
SELECT fn_safe_percent_change(150, 100) AS pct_gain;  -- should return 50.00
SELECT fn_safe_percent_change(80, 100) AS pct_loss;   -- should return -20.00
SELECT fn_safe_percent_change(100, 0) AS invalid;     -- should return NULL

-- Test the procedure (CAUTION: this will reset core data)
-- CALL sp_refresh_core_from_staging();

-- Note: The trigger will be tested automatically when inserting data via the procedure.