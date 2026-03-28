# Project Overview (Simple English)

## 1) What is this project?

Global Commodities Intelligence Platform is a data engineering project that collects commodity prices and turns them into analytics-ready data.

In simple terms:
- It pulls market prices from Yahoo Finance.
- It cleans and organizes the data.
- It stores the final data in a MySQL warehouse.
- It provides SQL checks and notebook charts for analysis.

## 2) Why this project exists

Commodity data is useful for tracking market movement and comparing assets like gold, silver, crude oil, and natural gas.

This project demonstrates how to build a complete pipeline that is:
- repeatable
- structured
- quality-checked
- ready for reporting

## 3) How the data moves (end-to-end)

The pipeline follows a Medallion pattern:

### Bronze (raw layer)
- Script: `etl/extract.py`
- Reads live price data from Yahoo Finance.
- Saves a raw snapshot CSV in `data/bronze/`.
- Also refreshes `result/result.csv` so you can quickly view latest values.

### Silver (clean layer)
- Script: `etl/transform.py`
- Reads the latest Bronze file.
- Cleans bad records and normalizes fields.
- Adds useful columns like year, month, day, and price change percent.
- Saves output in `data/silver/`.

### Gold (analytics layer)
- Script: `etl/load.py`
- Loads Bronze and Silver data into MySQL tables.
- Builds star schema tables for analysis:
  - `dim_commodity`
  - `dim_time`
  - `fact_commodity_price`
- Exports a gold snapshot file to `data/gold/gold_fact_snapshot.csv`.

### Warehouse validation
- Script: `etl/validate_warehouse.py`
- Runs health checks for row counts, nulls, duplicates, orphan keys, and freshness.

## 4) What technologies are used

- Python (ETL)
- Pandas (data handling)
- SQLAlchemy + mysql-connector-python (database loading)
- MySQL (warehouse)
- yfinance (source API)
- Jupyter + Matplotlib + Seaborn (analysis and visualization)

## 5) What data is tracked

The project currently tracks commodities in three groups:
- Precious metals
- Base metals
- Energy commodities

Examples include Gold, Silver, Platinum, Copper, Crude Oil, and Natural Gas.

The exact ticker-to-name mapping is in `etl/commodities_list.py`.

## 6) Database design (high level)

Main schema file: `database/schema_mysql.sql`

Key design points:
- Bronze and Silver store step-by-step pipeline history.
- Gold uses a star schema.
- Unique constraints avoid duplicate analytics records.
- Foreign keys enforce data integrity in Gold.

## 7) What outputs you get

After running the pipeline, you get:
- Bronze snapshots in `data/bronze/`
- Silver snapshots in `data/silver/`
- Gold snapshot in `data/gold/gold_fact_snapshot.csv`
- Latest user-friendly extract in `result/result.csv`

## 8) SQL scripts included

The `scripts/` folder provides ready-to-run SQL for checks and analytics:
- `scripts/bronze/bronze_quality_checks.sql`
- `scripts/silver/silver_quality_checks.sql`
- `scripts/gold/gold_analytics.sql`

These are helpful for manual validation and interview/portfolio demonstrations.

## 9) Notebook analysis

Notebook file: `notebook/analysis.ipynb`

It reads Gold data and produces plots in `notebook/plots/`:
- trend line chart
- latest price bar chart
- correlation heatmap
- price distribution histogram

## 10) How to run quickly

1. Configure `.env` with `DB_URL`
2. Create MySQL tables using `database/schema_mysql.sql`
3. Run ETL in order:
   - `python3 etl/extract.py`
   - `python3 etl/transform.py`
   - `python3 etl/load.py`
4. Run checks:
   - `python3 etl/validate_warehouse.py`

Detailed commands are in `README.md` and `run_commands.txt`.

## 11) Project strengths

- Clear layer-by-layer data architecture
- Code and documentation are aligned
- Includes both Python ETL and SQL quality checks
- Produces practical outputs for analysis and presentation

## 12) Possible next improvements

- Add scheduler/orchestration
- Add automated tests and CI
- Add incremental load strategy
- Add dashboarding layer on top of Gold tables
