-- Silver quality checks
-- Run with: mysql -u root -p -D commodities_db < scripts/silver/silver_quality_checks.sql

SELECT COUNT(*) AS silver_rows
FROM silver_commodity_prices;

SELECT COUNT(*) AS null_issues
FROM silver_commodity_prices
WHERE commodity_name IS NULL
   OR price_usd IS NULL
   OR timestamp IS NULL
   OR source IS NULL;

SELECT commodity_name, timestamp, COUNT(*) AS duplicate_rows
FROM silver_commodity_prices
GROUP BY commodity_name, timestamp
HAVING COUNT(*) > 1;

SELECT commodity_name, MAX(timestamp) AS latest_ts
FROM silver_commodity_prices
GROUP BY commodity_name
ORDER BY latest_ts DESC;
