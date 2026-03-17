# =============================================================================
# load.py
# PURPOSE: Create the SQLite database schema and load clean records into it.
# Uses SQLite so no external database server is required — perfect for local dev.
# =============================================================================

import sqlite3
import pandas as pd
import os


# --- Database Configuration ---
DB_PATH = "crypto_prices.db"  # SQLite file created in the project directory

# SQL statement to create our main table.
# We use IF NOT EXISTS so re-running doesn't wipe existing data.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id                     TEXT NOT NULL,          -- e.g. "bitcoin"
    symbol                      TEXT NOT NULL,          -- e.g. "BTC"
    name                        TEXT NOT NULL,          -- e.g. "Bitcoin"
    current_price               REAL NOT NULL,          -- USD price
    market_cap                  REAL,                   -- Total market cap in USD
    total_volume                REAL,                   -- 24h trading volume in USD
    price_change_percentage_24h REAL,                   -- % price change last 24h
    circulating_supply          REAL,                   -- Coins in circulation
    last_updated                TEXT,                   -- Timestamp from API
    extracted_at                TEXT NOT NULL,          -- When we fetched it

    -- Prevent loading the same coin snapshot twice
    UNIQUE(coin_id, extracted_at)
);
"""

# Index to speed up queries that filter by coin_id or extracted_at
CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_coin_id ON crypto_prices(coin_id);
CREATE INDEX IF NOT EXISTS idx_extracted_at ON crypto_prices(extracted_at);
"""


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """
    Opens (or creates) a SQLite database connection.

    Args:
        db_path: Path to the .db file. Created automatically if it doesn't exist.

    Returns:
        sqlite3.Connection object
    """
    conn = sqlite3.connect(db_path)
    # Return rows as dict-like objects (easier debugging)
    conn.row_factory = sqlite3.Row
    return conn


def setup_database(conn: sqlite3.Connection) -> None:
    """
    Creates the table and indexes if they don't already exist.
    Safe to call multiple times — uses IF NOT EXISTS.

    Args:
        conn: Active SQLite connection
    """
    print("[LOAD] Setting up database schema...")
    cursor = conn.cursor()

    # Create the main table
    cursor.execute(CREATE_TABLE_SQL)

    # Create indexes (executescript handles multiple statements at once)
    conn.executescript(CREATE_INDEX_SQL)

    conn.commit()
    print(f"[LOAD] Schema ready. Database: {DB_PATH}")


def load_data(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """
    Inserts clean records into the crypto_prices table.

    Uses INSERT OR IGNORE so re-running the pipeline won't create duplicates
    (thanks to the UNIQUE constraint on coin_id + extracted_at).

    Args:
        df:   Clean DataFrame from transform.py
        conn: Active SQLite connection

    Returns:
        int: Number of new rows actually inserted
    """
    print(f"[LOAD] Inserting {len(df)} records into database...")

    # Map DataFrame column names → database column names
    df_to_insert = df.rename(columns={"id": "coin_id"})

    # Only keep columns that exist in our table
    db_columns = [
        "coin_id", "symbol", "name", "current_price", "market_cap",
        "total_volume", "price_change_percentage_24h", "circulating_supply",
        "last_updated", "extracted_at"
    ]
    df_to_insert = df_to_insert[[col for col in db_columns if col in df_to_insert.columns]]

    # Count rows before insert to calculate how many were actually new
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM crypto_prices")
    rows_before = cursor.fetchone()[0]

    # to_sql with if_exists="append" adds rows; method="multi" is faster for batches.
    # We handle duplicate prevention via the UNIQUE constraint + INSERT OR IGNORE.
    try:
        df_to_insert.to_sql(
            name="crypto_prices",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )
    except Exception as e:
        # If to_sql doesn't natively handle IGNORE, fall back to row-by-row insert
        print(f"[LOAD] Batch insert failed ({e}), falling back to row-by-row insert...")
        _insert_with_ignore(df_to_insert, conn)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM crypto_prices")
    rows_after = cursor.fetchone()[0]
    new_rows = rows_after - rows_before

    print(f"[LOAD] Inserted {new_rows} new rows. Total rows in DB: {rows_after}")
    return new_rows


def _insert_with_ignore(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    """
    Fallback: inserts rows one-by-one using INSERT OR IGNORE to skip duplicates.
    Slower but guarantees no constraint violations.
    """
    cols = df.columns.tolist()
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT OR IGNORE INTO crypto_prices ({col_names}) VALUES ({placeholders})"

    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(sql, tuple(row))


def query_latest_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Convenience function: returns the most recent snapshot of each coin.

    Args:
        conn: Active SQLite connection

    Returns:
        pd.DataFrame with the latest price for each coin
    """
    sql = """
        SELECT
            coin_id,
            symbol,
            name,
            current_price,
            price_change_percentage_24h,
            extracted_at
        FROM crypto_prices
        WHERE extracted_at = (SELECT MAX(extracted_at) FROM crypto_prices)
        ORDER BY market_cap DESC;
    """
    return pd.read_sql_query(sql, conn)


# --- Run standalone for testing ---
if __name__ == "__main__":
    # Quick smoke test with one fake row
    sample_df = pd.DataFrame([{
        "id": "bitcoin", "symbol": "BTC", "name": "Bitcoin",
        "current_price": 65000.0, "market_cap": 1_200_000_000_000.0,
        "total_volume": 30_000_000_000.0, "price_change_percentage_24h": 2.5,
        "circulating_supply": 19_500_000.0,
        "last_updated": "2024-01-15 10:00:00",
        "extracted_at": "2024-01-15 10:01:00"
    }])

    conn = get_connection()
    setup_database(conn)
    load_data(sample_df, conn)

    print("\nLatest prices in DB:")
    print(query_latest_prices(conn))
    conn.close()
