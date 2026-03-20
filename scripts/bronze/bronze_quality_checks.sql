-- Bronze quality checks
-- Run with: mysql -u root -p -D commodities_db < scripts/bronze/bronze_quality_checks.sql

SELECT COUNT(*) AS bronze_rows
FROM bronze_commodity_prices;

SELECT commodity_name, COUNT(*) AS rows_count
FROM bronze_commodity_prices
GROUP BY commodity_name
ORDER BY commodity_name;

SELECT commodity_name, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
FROM bronze_commodity_prices
GROUP BY commodity_name
ORDER BY commodity_name;

SELECT commodity_name, timestamp, source, COUNT(*) AS duplicate_rows
FROM bronze_commodity_prices
GROUP BY commodity_name, timestamp, source
HAVING COUNT(*) > 1;
