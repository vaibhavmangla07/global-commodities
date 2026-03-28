# Global Commodities Intelligence Platform

This project is an end-to-end data engineering pipeline that collects live commodity prices, cleans and enriches the data, loads it into a MySQL warehouse, and prepares analytics-ready outputs.

It follows a Medallion architecture:

- Bronze: raw ingestion snapshot
- Silver: cleaned and standardized data
- Gold: dimensional model (star schema) for analytics

## Project Goals

- Ingest near-real-time commodity market prices from Yahoo Finance.
- Build a reliable and repeatable ETL pipeline using Python.
- Store both raw and curated data for traceability.
- Create a Gold layer optimized for SQL analytics and BI reporting.
- Keep artifacts portfolio-friendly with SQL checks, docs, and notebook visuals.

## What This Project Tracks

The commodity scope is defined in `etl/commodities_list.py`.

Current groups and examples:

- Precious Metal: Gold, Silver, Platinum, Palladium
- Base Metal: Copper, Aluminum, Nickel
- Energy: Crude Oil, Brent Crude, Natural Gas, Rbob Gasoline, Heating Oil

## Architecture Overview

### Source

- Yahoo Finance API via `yfinance`

### Bronze Layer

- File output: `data/bronze/bronze_prices_YYYYMMDD_HHMMSS.csv`
- MySQL table: `bronze_commodity_prices`
- Contains minimally shaped ingestion fields:
  - `commodity_name`
  - `price_usd`
  - `timestamp`
  - `source`

### Silver Layer

- File output: `data/silver/silver_prices_YYYYMMDD_HHMMSS.csv`
- MySQL table: `silver_commodity_prices`
- Applies quality and enrichment:
  - null and invalid filtering
  - normalized names and source text
  - UTC timestamp normalization
  - calendar derivations: `year`, `month`, `day`
  - derived metric: `price_change_pct`

### Gold Layer

- File output: `data/gold/gold_fact_snapshot.csv`
- MySQL tables:
  - `dim_commodity`
  - `dim_time`
  - `fact_commodity_price`
- Built as a star schema for reporting and trend analysis.

## Pipeline Flow

1. `etl/extract.py`

- Pulls latest data per commodity ticker from Yahoo Finance.
- Creates Bronze CSV snapshot.
- Updates `result/result.csv` with a presentation-friendly latest extract.
- Prints full and top-commodity views to console.

2. `etl/transform.py`

- Reads latest Bronze file.
- Cleans and transforms records into Silver format.
- Saves Silver CSV snapshot.

3. `etl/load.py`

- Reads latest Bronze and Silver files.
- Appends to Bronze and Silver MySQL tables.
- Rebuilds Gold dimensional data using `INSERT IGNORE` logic.
- Exports current Gold snapshot CSV.

4. `etl/validate_warehouse.py`

- Runs warehouse checks for:
  - row counts across all layers
  - Silver null quality checks
  - Gold orphan-key integrity
  - Silver duplicate detection
  - freshness by commodity (age in hours)

## Repository Structure

```text
global_commodities_platform
|__ PROJECT_OVERVIEW.md
|__ PROJECT_REPORT.md
|__ README.md
|__ requirements.txt
|__ .gitignore
|__ .gitattributes
|__ .env.example
|__ data
|   |__ bronze
|   |   |__ example.txt
|   |__ silver
|   |   |__ example.txt
|   |__ gold
|       |__ example.txt
|__ database
|   |__ schema_mysql.sql
|__ docs
|   |__ README.md
|   |__ data_architecture.md
|   |__ data_catalog.md
|   |__ naming_conventions.md
|   |__ run_results.md
|   |__ images
|       |__ architecture_overview.svg
|       |__ pipeline_results.svg
|       |__ star_schema.svg
|__ etl
|   |__ commodities_list.py
|   |__ extract.py
|   |__ transform.py
|   |__ load.py
|   |__ validate_warehouse.py
|__ notebook
|   |__ analysis.ipynb
|   |__ plots
|       |__ bar_chart_latest_commodity_price_comparison.png
|       |__ heatmap_commodity_correlation.png
|       |__ histogram_price_distribution_by_commodity.png
|       |__ line_chart_commodity_price_trends.png
|__ result
|   |__ example.txt
|__ scripts
    |__ README.md
    |__ bronze
    |   |__ bronze_quality_checks.sql
    |__ silver
    |   |__ silver_quality_checks.sql
    |__ gold
        |__ gold_analytics.sql
```

  Note:

- `.env` is local-only and should never be committed.
- `.git/` is an internal Git directory and is not shown in repository structure.

## Prerequisites

- Python 3.10+
- MySQL 8+
- Access to Yahoo Finance endpoints

## Installation and Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with:

```env
DB_URL=mysql+mysqlconnector://<user>:<password>@localhost:3306/commodities_db
```

Optional extract loop mode settings:

```env
PIPELINE_MODE=once
PIPELINE_INTERVAL_MINUTES=5
```

4. Create schema tables:

```bash
mysql -u <user> -p -D commodities_db < database/schema_mysql.sql
```

## How To Run

All commands below are also aligned with `run_commands.txt`.

Run extract only:

```bash
set -a && source .env && set +a
python3 etl/extract.py
```

Run full ETL:

```bash
set -a && source .env && set +a
python3 etl/extract.py
python3 etl/transform.py
python3 etl/load.py
```

Run warehouse validation:

```bash
set -a && source .env && set +a
python3 etl/validate_warehouse.py
```

Check the latest extracted result file:

```bash
head -n 20 result/result.csv
```

## SQL Validation and Analytics Scripts

Run SQL checks directly on MySQL:

```bash
mysql -u <user> -p -D commodities_db < scripts/bronze/bronze_quality_checks.sql
mysql -u <user> -p -D commodities_db < scripts/silver/silver_quality_checks.sql
mysql -u <user> -p -D commodities_db < scripts/gold/gold_analytics.sql
```

These scripts cover:

- Bronze row and duplicate checks
- Silver null and duplicate checks
- Gold row counts and latest/top commodity snapshots

## Analytics Notebook

Notebook: `notebook/analysis.ipynb`

What it does:

- Reads Gold layer data from MySQL
- Creates and saves plots in `notebook/plots/`:
  - line chart: commodity trends over time
  - bar chart: latest price comparison
  - heatmap: inter-commodity correlation
  - histogram: price distribution by commodity

## Data Model Notes

Schema file: `database/schema_mysql.sql`

Important constraints:

- Unique commodity name in `dim_commodity`
- Unique full timestamp in `dim_time`
- Unique `(commodity_id, time_id)` in `fact_commodity_price`
- Foreign keys from fact table to both dimensions

This gives strong referential integrity for Gold analytics.

## Operational Outputs

- Latest extract output: `result/result.csv` (overwritten each extract run)
- Bronze snapshots: `data/bronze/`
- Silver snapshots: `data/silver/`
- Gold snapshot: `data/gold/gold_fact_snapshot.csv`

## Documentation Map

- `PROJECT_OVERVIEW.md`: plain-English business and technical summary
- `docs/data_architecture.md`: detailed Medallion flow
- `docs/data_catalog.md`: schema and column-level catalog
- `docs/naming_conventions.md`: naming standards
- `docs/run_results.md`: run evidence template and checklist
- `docs/README.md`: documentation index

## Troubleshooting

- `DB_URL not found in .env`

  - Ensure `.env` exists and is loaded with `set -a && source .env && set +a`.
- `Missing Bronze or Silver file`

  - Run `extract.py` and `transform.py` before `load.py`.
- No rows fetched from Yahoo Finance

  - Retry run; this can happen due to temporary network/API unavailability.
- MySQL table not found errors

  - Apply `database/schema_mysql.sql` before running load and validation.

## Current Scope and Limitations

- Uses Yahoo Finance as a single external source.
- Price refresh frequency depends on script execution schedule.
- Historical depth in each run is intentionally shallow (latest intraday context).
- No orchestration framework yet (manual/scripted execution).

## Suggested Next Enhancements

- Add scheduler orchestration (Airflow, Prefect, or cron).
- Add automated tests for ETL modules.
- Add CI checks for SQL and Python linting.
- Add incremental load and partitioning strategy for scale.
- Add dashboards on top of Gold tables.

## License and Usage

This repository is designed as a learning and portfolio data engineering project.
