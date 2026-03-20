# Global Commodities Intelligence Platform

**Data Engineer Project** for building a local commodity-price warehouse using Python, MySQL, and Medallion architecture (Bronze → Silver → Gold).

## Overview

- Data source: Yahoo Finance via `yfinance`
- Warehouse: MySQL (`commodities_db`)
- Pipeline: `etl/extract.py` → `etl/transform.py` → `etl/load.py`
- Validation: `etl/validate_warehouse.py`
- Analytics: `notebook/analysis.ipynb`
- Latest extract output file: `result/result.csv` (auto-updated on every extract run)

## Commodity Scope (Current)

This project currently tracks **12 commodities**:

- Gold
- Silver
- Platinum
- Palladium
- Copper
- Aluminum
- Nickel
- Crude Oil
- Brent Crude
- Natural Gas
- Rbob Gasoline
- Heating Oil

## Tech Stack

- Python (`pandas`, `SQLAlchemy`, `yfinance`)
- MySQL (`mysql-connector-python`)
- Jupyter (`matplotlib`, `seaborn`)

## Quick Start

1. Create and activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Create database and schema

```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS commodities_db;"
mysql -u root -p commodities_db < database/schema_mysql.sql
```

3. Configure environment

```bash
cp .env.example .env
# then edit .env with your local MySQL credentials
```

4. Run ETL pipeline

```bash
set -a && source .env && set +a
python3 etl/extract.py
python3 etl/transform.py
python3 etl/load.py
```

After `etl/extract.py`, the latest commodity output is automatically written to:

```bash
result/result.csv
```

5. Run validation checks

```bash
python3 etl/validate_warehouse.py
```

## Data Model

- Bronze: `bronze_commodity_prices`
- Silver: `silver_commodity_prices`
- Gold Fact: `fact_commodity_price`
- Gold Dimensions: `dim_commodity`, `dim_time`

## Project Structure

```
global_commodities_platform/
├── etl/
│   ├── commodities_list.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── validate_warehouse.py
├── database/
│   └── schema_mysql.sql
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── result/
│   └── result.csv
├── scripts/
│   ├── bronze/bronze_quality_checks.sql
│   ├── silver/silver_quality_checks.sql
│   └── gold/gold_analytics.sql
├── docs/
│   ├── README.md
│   ├── data_architecture.md
│   ├── data_catalog.md
│   ├── naming_conventions.md
│   └── run_results.md
├── notebook/analysis.ipynb
├── PROJECT_REPORT.md
├── requirements.txt
└── README.md
```

## SQL Script Usage

```bash
mysql -u root -p -D commodities_db < scripts/bronze/bronze_quality_checks.sql
mysql -u root -p -D commodities_db < scripts/silver/silver_quality_checks.sql
mysql -u root -p -D commodities_db < scripts/gold/gold_analytics.sql
```

## Documentation

- Architecture: `docs/data_architecture.md`
- Table catalog: `docs/data_catalog.md`
- Naming rules: `docs/naming_conventions.md`
- Latest validation evidence: `docs/run_results.md`
