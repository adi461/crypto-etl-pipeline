# Crypto ETL Pipeline

A beginner-friendly ETL (Extract, Transform, Load) pipeline that fetches live
cryptocurrency price data from the CoinGecko public API, cleans it, and stores
it in a local SQLite database.

Built as a data engineering internship portfolio project.

---

## What It Does

```
CoinGecko API  →  extract.py  →  transform.py  →  load.py  →  crypto_prices.db
   (raw JSON)       (DataFrame)    (clean data)    (SQLite)
```

Each time you run the pipeline, it captures a snapshot of current prices for
Bitcoin, Ethereum, Solana, Cardano, and Dogecoin. Run it on a schedule (e.g.
hourly with cron) to build up a historical time series.

---

## Project Structure

```
crypto_etl/
├── extract.py        # Fetches raw data from CoinGecko API
├── transform.py      # Cleans data, handles nulls, formats timestamps
├── load.py           # Creates DB schema and inserts records
├── main.py           # Runs the full pipeline end-to-end
├── schema.sql        # Database schema (reference copy)
├── queries.sql       # Example analytical SQL queries
├── requirements.txt  # Python dependencies
└── README.md         # This file
```

---

## Setup

**1. Clone / download the project**

**2. Create a virtual environment (recommended)**
```bash
python -m venv venv
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

No API key needed — CoinGecko's public API is free and open.

---

## Running the Pipeline

```bash
python main.py
```

**Expected output:**
```
============================================================
  CRYPTO ETL PIPELINE — STARTING
============================================================
[EXTRACT] Fetching data from CoinGecko API...
[EXTRACT] Successfully fetched 5 records.
[TRANSFORM] Starting transformation...
[TRANSFORM] Kept 10 of 26 columns.
[VALIDATE] Running data quality checks...
[VALIDATE] All checks passed ✓
[LOAD] Setting up database schema...
[LOAD] Inserted 5 new rows. Total rows in DB: 5

============================================================
  PIPELINE COMPLETE — LATEST PRICES IN DATABASE
============================================================
 coin_id symbol        name  current_price  price_change_percentage_24h         extracted_at
 bitcoin    BTC     Bitcoin       65432.10                         2.31  2024-01-15 10:01:00
ethereum    ETH    Ethereum        3521.80                         1.45  2024-01-15 10:01:00
  solana    SOL      Solana         110.25                        -0.82  2024-01-15 10:01:00
 cardano    ADA     Cardano           0.63                         0.55  2024-01-15 10:01:00
dogecoin   DOGE    Dogecoin          0.085                         3.20  2024-01-15 10:01:00
```

---

## Querying the Data

Open `crypto_prices.db` in [DB Browser for SQLite](https://sqlitebrowser.org/)
or run queries from the command line:

```bash
sqlite3 crypto_prices.db < queries.sql
```

See `queries.sql` for 7 ready-to-run example queries including:
- Latest prices
- Top 24h gainers
- Bitcoin price history
- Pipeline run audit log
- Data quality checks

---

## Data Quality Checks

Before loading, `transform.py` validates:
- ✅ No null values in critical columns (id, symbol, name, price)
- ✅ No negative prices
- ✅ No duplicate coin IDs in the same batch

The database also enforces a `UNIQUE(coin_id, extracted_at)` constraint so
re-running the pipeline never creates duplicate rows.

---

## Extending the Project

Ideas for taking this further:
- **Schedule it**: Use `cron` (Linux/macOS) or Task Scheduler (Windows) to run hourly
- **Add more coins**: Expand the `COIN_IDS` list in `extract.py`
- **Switch to PostgreSQL**: Replace `sqlite3` in `load.py` with `psycopg2`
- **Add logging**: Replace `print()` calls with Python's `logging` module
- **Visualize**: Load the DB into Jupyter Notebook and plot price trends with matplotlib

---

## Tech Stack

| Tool      | Purpose                        |
|-----------|--------------------------------|
| Python    | Core language                  |
| requests  | HTTP calls to the API          |
| pandas    | Data manipulation              |
| SQLite    | Lightweight local database     |
| CoinGecko | Free crypto price API          |
