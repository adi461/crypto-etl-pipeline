# =============================================================================
# transform.py
# PURPOSE: Clean and shape the raw API data into a structured, analysis-ready
# DataFrame. This is the heart of data quality work in an ETL pipeline.
# =============================================================================

import pandas as pd


# --- Which columns we actually care about (ignore the rest from the API) ---
COLUMNS_TO_KEEP = [
    "id",                              # Unique coin identifier (e.g. "bitcoin")
    "symbol",                          # Ticker symbol (e.g. "btc")
    "name",                            # Human-readable name (e.g. "Bitcoin")
    "current_price",                   # Current price in USD
    "market_cap",                      # Total market capitalization
    "total_volume",                    # 24h trading volume
    "price_change_percentage_24h",     # % change in last 24 hours
    "circulating_supply",              # Number of coins in circulation
    "last_updated",                    # Timestamp from the API
    "extracted_at",                    # Our own extraction timestamp
]


def transform_crypto_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, selects, and formats the raw DataFrame.

    Steps:
      1. Select only the columns we need
      2. Handle missing values
      3. Parse and standardize timestamps
      4. Fix data types
      5. Rename columns to snake_case for DB consistency

    Args:
        raw_df (pd.DataFrame): Raw DataFrame from extract.py

    Returns:
        pd.DataFrame: Clean, ready-to-load DataFrame
    """
    print("[TRANSFORM] Starting transformation...")

    df = raw_df.copy()

    # -------------------------------------------------------------------------
    # STEP 1: Select only the columns we need
    # Some columns from the API (roi, sparkline, etc.) aren't useful here.
    # -------------------------------------------------------------------------
    # Only keep columns that actually exist in the DataFrame (defensive coding)
    available_cols = [col for col in COLUMNS_TO_KEEP if col in df.columns]
    df = df[available_cols]
    print(f"[TRANSFORM] Kept {len(available_cols)} of {len(raw_df.columns)} columns.")

    # -------------------------------------------------------------------------
    # STEP 2: Handle missing values
    # Numeric fields get 0.0 as a safe default; string fields get "unknown".
    # -------------------------------------------------------------------------
    numeric_cols = ["current_price", "market_cap", "total_volume",
                    "price_change_percentage_24h", "circulating_supply"]
    string_cols = ["id", "symbol", "name"]

    for col in numeric_cols:
        if col in df.columns:
            missing = df[col].isna().sum()
            if missing > 0:
                print(f"[TRANSFORM] Filling {missing} missing value(s) in '{col}' with 0.0")
            df[col] = df[col].fillna(0.0)

    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].fillna("unknown")

    # -------------------------------------------------------------------------
    # STEP 3: Parse timestamps
    # The API returns ISO 8601 strings like "2024-01-15T12:34:56.789Z".
    # We convert to clean UTC datetime strings for SQLite storage.
    # -------------------------------------------------------------------------
    for ts_col in ["last_updated", "extracted_at"]:
        if ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
            # Format as a clean string: "2024-01-15 12:34:56"
            df[ts_col] = df[ts_col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # -------------------------------------------------------------------------
    # STEP 4: Enforce correct data types
    # Ensures no string sneaks into a price column, etc.
    # -------------------------------------------------------------------------
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["symbol"] = df["symbol"].str.upper()  # Standardize: "btc" -> "BTC"
    df["name"] = df["name"].str.strip()       # Remove accidental whitespace

    # -------------------------------------------------------------------------
    # STEP 5: Remove exact duplicate rows (same coin ID + same extraction time)
    # -------------------------------------------------------------------------
    before = len(df)
    df = df.drop_duplicates(subset=["id", "extracted_at"])
    after = len(df)
    if before != after:
        print(f"[TRANSFORM] Removed {before - after} duplicate row(s).")

    print(f"[TRANSFORM] Transformation complete. {len(df)} clean records ready.")
    return df


def validate_transformed_data(df: pd.DataFrame) -> bool:
    """
    Basic data quality checks before loading into the database.

    Checks:
      - No null values in critical columns
      - All prices are non-negative
      - No duplicate coin IDs in the same batch

    Args:
        df (pd.DataFrame): Transformed DataFrame

    Returns:
        bool: True if all checks pass, False otherwise.
    """
    print("[VALIDATE] Running data quality checks...")
    passed = True

    # Check 1: No nulls in critical columns
    critical_cols = ["id", "symbol", "name", "current_price", "extracted_at"]
    for col in critical_cols:
        if col in df.columns and df[col].isna().any():
            print(f"[VALIDATE] FAIL — Null values found in critical column: '{col}'")
            passed = False

    # Check 2: Prices must be non-negative
    if "current_price" in df.columns:
        if (df["current_price"] < 0).any():
            print("[VALIDATE] FAIL — Negative prices detected.")
            passed = False

    # Check 3: No duplicate coin IDs in this batch
    if df["id"].duplicated().any():
        dupes = df[df["id"].duplicated()]["id"].tolist()
        print(f"[VALIDATE] FAIL — Duplicate coin IDs in batch: {dupes}")
        passed = False

    if passed:
        print("[VALIDATE] All checks passed ✓")

    return passed


# --- Run standalone for testing ---
if __name__ == "__main__":
    # Simulate with fake data for quick testing without hitting the API
    sample = pd.DataFrame([
        {
            "id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
            "current_price": 65000.0, "market_cap": 1_200_000_000_000,
            "total_volume": 30_000_000_000, "price_change_percentage_24h": 2.5,
            "circulating_supply": 19_500_000, "last_updated": "2024-01-15T10:00:00Z",
            "extracted_at": "2024-01-15T10:01:00"
        }
    ])

    clean_df = transform_crypto_data(sample)
    validate_transformed_data(clean_df)
    print("\nClean DataFrame:")
    print(clean_df)
