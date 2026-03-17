# =============================================================================
# main.py
# PURPOSE: Orchestrate the full ETL pipeline.
# Run this file to execute Extract → Transform → Load in one shot.
#
# Usage:
#   python main.py
# =============================================================================

import sys
from extract import extract_crypto_data
from transform import transform_crypto_data, validate_transformed_data
from load import get_connection, setup_database, load_data, query_latest_prices


def run_pipeline():
    """
    Runs the full ETL pipeline:
      1. EXTRACT  — Fetch raw data from CoinGecko API
      2. TRANSFORM — Clean, validate, and format the data
      3. LOAD      — Write clean records to SQLite database
      4. VERIFY    — Query the DB and print results
    """
    print("=" * 60)
    print("  CRYPTO ETL PIPELINE — STARTING")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # STEP 1: EXTRACT
    # -------------------------------------------------------------------------
    try:
        raw_df = extract_crypto_data()
    except Exception as e:
        print(f"\n[ERROR] Extraction failed: {e}")
        print("[INFO] Check your internet connection or try again later.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STEP 2: TRANSFORM
    # -------------------------------------------------------------------------
    try:
        clean_df = transform_crypto_data(raw_df)
    except Exception as e:
        print(f"\n[ERROR] Transformation failed: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STEP 2b: VALIDATE — Abort if data quality checks fail
    # -------------------------------------------------------------------------
    is_valid = validate_transformed_data(clean_df)
    if not is_valid:
        print("\n[ERROR] Data quality checks failed. Aborting load.")
        print("[INFO] Fix the data issues above before loading into the database.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STEP 3: LOAD
    # -------------------------------------------------------------------------
    try:
        conn = get_connection()
        setup_database(conn)
        new_rows = load_data(clean_df, conn)
    except Exception as e:
        print(f"\n[ERROR] Load failed: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STEP 4: VERIFY — Show what's in the database
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE — LATEST PRICES IN DATABASE")
    print("=" * 60)

    latest = query_latest_prices(conn)
    print(latest.to_string(index=False))

    conn.close()
    print("\n[DONE] Pipeline finished successfully.")


# --- Entry point ---
if __name__ == "__main__":
    run_pipeline()
