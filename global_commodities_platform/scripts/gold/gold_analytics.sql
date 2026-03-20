-- Gold analytics queries
-- Run with: mysql -u root -p -D commodities_db < scripts/gold/gold_analytics.sql

-- 1) Gold layer row counts
SELECT 'fact_commodity_price' AS table_name, COUNT(*) AS rows_count FROM fact_commodity_price
UNION ALL
SELECT 'dim_commodity', COUNT(*) FROM dim_commodity
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time;

-- 2) Latest price per commodity from Gold
SELECT
    c.commodity_name,
    t.full_timestamp,
    f.price_usd
FROM fact_commodity_price f
JOIN dim_commodity c ON f.commodity_id = c.commodity_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN (
    SELECT f2.commodity_id, MAX(t2.full_timestamp) AS latest_ts
    FROM fact_commodity_price f2
    JOIN dim_time t2 ON f2.time_id = t2.time_id
    GROUP BY f2.commodity_id
) latest ON latest.commodity_id = f.commodity_id AND latest.latest_ts = t.full_timestamp
ORDER BY c.commodity_name;

-- 3) Top commodities snapshot
SELECT
    c.commodity_name,
    t.full_timestamp,
    f.price_usd
FROM fact_commodity_price f
JOIN dim_commodity c ON f.commodity_id = c.commodity_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE c.commodity_name IN ('Gold', 'Silver', 'Natural Gas', 'Crude Oil')
ORDER BY t.full_timestamp DESC, c.commodity_name;
