# Documentation Index

This folder contains implementation-aligned documentation for the Global Commodities Intelligence Platform.

## Files

- `data_architecture.md` - End-to-end Medallion flow and layer responsibilities
- `data_catalog.md` - Table and column catalog (schema-accurate)
- `naming_conventions.md` - Naming standards for SQL objects and files
- `run_results.md` - Validation checklist and run evidence template

## Operational Output

- Latest extract result file: `result/result.csv`
- This file is refreshed automatically every time `etl/extract.py` runs

## Diagrams

- `images/architecture_overview.svg`
- `images/star_schema.svg`
- `images/pipeline_results.svg`

## Usage

Recommended reading order:

1. `data_architecture.md`
2. `data_catalog.md`
3. `naming_conventions.md`
4. `run_results.md`
