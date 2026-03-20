# Naming Conventions

## General Rules

- Use `snake_case` for file names, table names, and columns
- Keep names explicit and business-readable
- Preserve stable names across pipeline layers where practical

## Layer Prefixes

- Bronze tables: `bronze_<entity>`
- Silver tables: `silver_<entity>`
- Gold dimensions: `dim_<entity>`
- Gold facts: `fact_<entity>`

## Key Naming

- Primary keys use `<entity>_id` or `<table>_id`
- Foreign keys mirror referenced key names
- Example: `commodity_id`, `time_id`

## Metric Naming

- Monetary measures use `_usd` suffix
- Percentage fields use `_pct`
- Temporal derivations use explicit names: `year`, `month`, `day`, `hour`, `minute`

## File Naming

- ETL modules are action-based: `extract.py`, `transform.py`, `load.py`, `validate_warehouse.py`
- SQL scripts are purpose-based:
  - `bronze_quality_checks.sql`
  - `silver_quality_checks.sql`
  - `gold_analytics.sql`

## Documentation Naming

- Use lowercase markdown names in `docs/`
- Keep one concern per document (architecture, catalog, conventions, results)
