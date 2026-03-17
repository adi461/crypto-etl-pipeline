# Crypto ETL Pipeline 📊

A Python ETL (Extract, Transform, Load) pipeline that fetches live cryptocurrency prices from the CoinGecko API, cleans the data, stores it in a SQLite database, and displays it in a Flask web dashboard.

## What It Does
- Extracts live price data for 5 cryptocurrencies via the CoinGecko public API
- Transforms and validates the data (null handling, deduplication, type formatting)
- Loads clean records into a SQLite database
- Displays prices, 24h changes, and market cap in a web dashboard

## Running the App

**Prerequisites:** Python 3.8+
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the pipeline (fetch + store data)
python main.py

# 3. Start the dashboard
python dashboard.py
# Open http://127.0.0.1:5000
```

## Project Structure
- `extract.py` — Fetches raw data from CoinGecko API
- `transform.py` — Cleans, validates, and formats the data
- `load.py` — Creates DB schema and inserts records
- `main.py` — Orchestrates the full pipeline
- `dashboard.py` — Flask web server serving the dashboard

## Technical Details
- Modular ETL design with clear separation of concerns
- Data quality checks before loading (no nulls, no negatives, no duplicates)
- Idempotent loading — re-running the pipeline never creates duplicate rows
- `UNIQUE(coin_id, extracted_at)` constraint enforced at the database level
- Each pipeline run builds a time-series snapshot for historical analysis

## Technologies Used
- Python 3, pandas, requests
- SQLite
- Flask, Chart.js
