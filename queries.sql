-- =============================================================================
-- queries.sql
-- PURPOSE: Example analytical SQL queries for the crypto_prices database.
-- These demonstrate how to get value out of the data you've collected.
-- Run these in any SQLite client (e.g. DB Browser for SQLite, DBeaver).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. LATEST SNAPSHOT — Most recent price for every coin
-- Useful for a live dashboard showing current prices.
-- -----------------------------------------------------------------------------
SELECT
    coin_id,
    symbol,
    name,
    current_price,
    price_change_percentage_24h,
    market_cap,
    extracted_at
FROM crypto_prices
WHERE extracted_at = (SELECT MAX(extracted_at) FROM crypto_prices)
ORDER BY market_cap DESC;


-- -----------------------------------------------------------------------------
-- 2. TOP 3 GAINERS — Coins with highest 24h price change in latest snapshot
-- -----------------------------------------------------------------------------
SELECT
    name,
    symbol,
    current_price,
    price_change_percentage_24h
FROM crypto_prices
WHERE extracted_at = (SELECT MAX(extracted_at) FROM crypto_prices)
ORDER BY price_change_percentage_24h DESC
LIMIT 3;


-- -----------------------------------------------------------------------------
-- 3. PRICE HISTORY — All recorded prices for Bitcoin over time
-- Only meaningful after running the pipeline multiple times.
-- -----------------------------------------------------------------------------
SELECT
    extracted_at,
    current_price,
    price_change_percentage_24h
FROM crypto_prices
WHERE coin_id = 'bitcoin'
ORDER BY extracted_at DESC;


-- -----------------------------------------------------------------------------
-- 4. PIPELINE RUNS — How many times has the pipeline run, and when?
-- Each unique extracted_at represents one pipeline execution.
-- -----------------------------------------------------------------------------
SELECT
    extracted_at                    AS run_time,
    COUNT(*)                        AS coins_captured
FROM crypto_prices
GROUP BY extracted_at
ORDER BY extracted_at DESC;


-- -----------------------------------------------------------------------------
-- 5. AVERAGE PRICE — Average Bitcoin price across all pipeline runs
-- Useful for understanding price trends over your collection window.
-- -----------------------------------------------------------------------------
SELECT
    coin_id,
    name,
    ROUND(AVG(current_price), 2)        AS avg_price_usd,
    ROUND(MIN(current_price), 2)        AS min_price_usd,
    ROUND(MAX(current_price), 2)        AS max_price_usd,
    COUNT(*)                            AS snapshots_collected
FROM crypto_prices
WHERE coin_id = 'bitcoin'
GROUP BY coin_id, name;


-- -----------------------------------------------------------------------------
-- 6. DATA QUALITY CHECK — Are there any suspicious null or zero prices?
-- A good sanity check to run after every pipeline execution.
-- -----------------------------------------------------------------------------
SELECT
    coin_id,
    extracted_at,
    current_price
FROM crypto_prices
WHERE current_price IS NULL
   OR current_price = 0
ORDER BY extracted_at DESC;


-- -----------------------------------------------------------------------------
-- 7. MARKET DOMINANCE — Bitcoin's share of total tracked market cap
-- Shows BTC dominance relative to the coins we're tracking.
-- -----------------------------------------------------------------------------
SELECT
    ROUND(
        100.0 * SUM(CASE WHEN coin_id = 'bitcoin' THEN market_cap ELSE 0 END)
              / SUM(market_cap),
        2
    ) AS btc_dominance_pct
FROM crypto_prices
WHERE extracted_at = (SELECT MAX(extracted_at) FROM crypto_prices);
