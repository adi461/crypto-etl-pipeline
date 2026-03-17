# =============================================================================
# extract.py
# PURPOSE: Fetch raw cryptocurrency price data from the CoinGecko public API.
# No API key required. Returns a raw pandas DataFrame.
# =============================================================================

import requests
import pandas as pd
from datetime import datetime


# --- Configuration ---
# CoinGecko's /coins/markets endpoint returns current market data for a list of coins.
API_URL = "https://api.coingecko.com/api/v3/coins/markets"

# Which coins to track. Add or remove coin IDs from this list to customize.
COIN_IDS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]

# API request parameters (see CoinGecko docs for all options)
PARAMS = {
    "vs_currency": "usd",          # Price currency
    "ids": ",".join(COIN_IDS),     # Comma-separated list of coin IDs
    "order": "market_cap_desc",    # Sort by market cap
    "per_page": 10,                # Max results per page
    "page": 1,
    "sparkline": False,            # We don't need sparkline data
    "price_change_percentage": "24h",
}


def extract_crypto_data() -> pd.DataFrame:
    """
    Calls the CoinGecko API and returns raw data as a pandas DataFrame.

    Returns:
        pd.DataFrame: Raw data with one row per coin.

    Raises:
        Exception: If the API call fails or returns bad data.
    """
    print("[EXTRACT] Fetching data from CoinGecko API...")

    try:
        response = requests.get(API_URL, params=PARAMS, timeout=10)

        # Raise an error for bad HTTP status codes (4xx, 5xx)
        response.raise_for_status()

    except requests.exceptions.Timeout:
        raise Exception("[EXTRACT] Request timed out. Check your internet connection.")
    except requests.exceptions.HTTPError as e:
        raise Exception(f"[EXTRACT] HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"[EXTRACT] Request failed: {e}")

    # Parse the JSON response into a list of dicts
    data = response.json()

    if not data:
        raise Exception("[EXTRACT] API returned empty data.")

    # Convert to DataFrame for easier downstream processing
    raw_df = pd.DataFrame(data)

    # Tag when this snapshot was taken (important for time-series tracking)
    raw_df["extracted_at"] = datetime.utcnow().isoformat()

    print(f"[EXTRACT] Successfully fetched {len(raw_df)} records.")
    return raw_df


# --- Run standalone for testing ---
if __name__ == "__main__":
    df = extract_crypto_data()
    print("\nRaw columns:", df.columns.tolist())
    print("\nSample data:")
    print(df[["id", "symbol", "current_price", "market_cap", "extracted_at"]].head())
