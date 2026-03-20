# Global Commodities Intelligence Platform - Project Report

## 1. Project Summary

This is a **Data Engineer project** that builds a local commodity warehouse using Yahoo Finance data, Python ETL, and a MySQL star-schema serving layer.

The implementation follows Medallion architecture:

- Bronze for raw ingestion
- Silver for cleaned and standardized data
- Gold for analytics-ready dimensional modeling

## 2. Business/Portfolio Goal

- Build a clean end-to-end data engineering pipeline
- Preserve data quality through validation and cleanup
- Provide SQL-first analytics access and notebook analysis
- Maintain documentation aligned with actual code and schema

## 3. Scope and Commodity Universe

Current tracked commodity set (12):

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

## 4. Technical Architecture

### 4.1 Pipeline Flow

1. `etl/extract.py`

   - Pulls live prices from Yahoo Finance
   - Writes Bronze CSV file (`data/bronze/bronze_prices_*.csv`)
   - Writes latest tabular result file (`result/result.csv`)
2. `etl/transform.py`

   - Reads latest Bronze file
   - Cleans records and standardizes types
   - Normalizes timestamps to UTC-naive for DB compatibility
   - Derives `year`, `month`, `day`, `price_change_pct`
   - Writes Silver CSV file (`data/silver/silver_prices_*.csv`)
3. `etl/load.py`

   - Loads latest Bronze and Silver files into MySQL
   - Removes unsupported commodities from Bronze/Silver tables
   - Removes duplicates in Bronze/Silver by natural keys
   - Rebuilds Gold dimension/fact tables from Silver
   - Exports Gold snapshot (`data/gold/gold_fact_snapshot.csv`)
4. `etl/validate_warehouse.py`

   - Checks row counts
   - Checks Silver nulls
   - Checks Gold orphan facts
   - Checks Silver duplicates
   - Checks freshness by commodity

### 4.2 Warehouse Design

- Database: `commodities_db`
- Schema definition: `database/schema_mysql.sql`

Gold model is a star schema:

- `dim_commodity`
- `dim_time`
- `fact_commodity_price`

## 5. Data Model (Schema-Accurate)

### Bronze

Table: `bronze_commodity_prices`

- `bronze_id` (PK)
- `commodity_name`
- `price_usd`
- `timestamp`
- `source`
- `ingested_at`

### Silver

Table: `silver_commodity_prices`

- `silver_id` (PK)
- `commodity_name`
- `price_usd`
- `timestamp`
- `source`
- `year`
- `month`
- `day`
- `price_change_pct`
- `transformed_at`

### Gold

Table: `dim_commodity`

- `commodity_id` (PK)
- `commodity_name`

Table: `dim_time`

- `time_id` (PK)
- `full_timestamp`
- `year`, `month`, `day`, `hour`, `minute`

Table: `fact_commodity_price`

- `fact_id` (PK)
- `commodity_id` (FK)
- `time_id` (FK)
- `price_usd`

## 6. Data Quality and Reliability

Implemented controls include:

- Null filtering in transformation stage
- Price positivity filtering (`price_usd > 0`)
- UTC timestamp normalization
- Duplicate cleanup in Bronze and Silver
- Unsupported commodity cleanup based on curated list
- Gold fact uniqueness enforced by `(commodity_id, time_id)` constraint

## 7. SQL Analytics Layer

SQL-first scripts are provided:

- `scripts/bronze/bronze_quality_checks.sql`
- `scripts/silver/silver_quality_checks.sql`
- `scripts/gold/gold_analytics.sql`

This makes the project usable as both:

- Python ETL portfolio project
- SQL warehouse portfolio project

## 8. Environment and Dependencies

- Python dependencies listed in `requirements.txt`
- Config template in `.env.example`
- Runtime config loaded from `.env`

Critical env variable:

- `DB_URL=mysql+mysqlconnector://<user>:<password>@localhost:3306/commodities_db`

## 9. Execution Workflow

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS commodities_db;"
mysql -u root -p commodities_db < database/schema_mysql.sql

set -a && source .env && set +a
python3 etl/extract.py
python3 etl/transform.py
python3 etl/load.py
python3 etl/validate_warehouse.py
```

## 10. Notebook Analytics

Notebook location: `notebook/analysis.ipynb`

The notebook provides:

- Price trend plots
- Latest-price comparison
- Correlation heatmap
- Distribution analysis

## 11. Deliverables in This Repository

- Production-style ETL modules in `etl/`
- MySQL schema in `database/schema_mysql.sql`
- SQL analytics scripts in `scripts/`
- Documentation set in `docs/`
- Latest extract result artifact in `result/result.csv`
- Portfolio-level project summary in `README.md` and this report

## 12. Conclusion

The project is a complete and coherent data engineering portfolio implementation with:

- Well-defined commodity scope
- Verified Medallion architecture
- Rebuildable Gold star schema
- SQL + Python analytics access
- Documentation aligned to source code
