-- =============================================================================
-- schema.sql
-- PURPOSE: Standalone SQL schema definition for the crypto_prices database.
-- Run this manually in any SQL client to inspect or recreate the schema.
-- Compatible with SQLite and (mostly) PostgreSQL.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- TABLE: crypto_prices
-- Stores one row per coin per pipeline run. Over time, this builds up a
-- historical time series you can query for trends and analysis.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS crypto_prices (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Coin identity
    coin_id                     TEXT    NOT NULL,   -- e.g. "bitcoin"
    symbol                      TEXT    NOT NULL,   -- e.g. "BTC"
    name                        TEXT    NOT NULL,   -- e.g. "Bitcoin"

    -- Market data (all prices in USD)
    current_price               REAL    NOT NULL,   -- Current USD price
    market_cap                  REAL,               -- Total market cap
    total_volume                REAL,               -- 24h trading volume
    price_change_percentage_24h REAL,               -- % change in last 24h
    circulating_supply          REAL,               -- Coins currently in circulation

    -- Timestamps
    last_updated                TEXT,               -- Last updated timestamp from API
    extracted_at                TEXT    NOT NULL,   -- When our pipeline ran (UTC)

    -- Prevent inserting the same coin snapshot twice in the same pipeline run
    UNIQUE(coin_id, extracted_at)
);


-- -----------------------------------------------------------------------------
-- INDEXES
-- Speeds up common query patterns (filter by coin, filter by time).
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_coin_id     ON crypto_prices(coin_id);
CREATE INDEX IF NOT EXISTS idx_extracted_at ON crypto_prices(extracted_at);
