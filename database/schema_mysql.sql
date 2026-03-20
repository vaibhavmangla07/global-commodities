CREATE TABLE IF NOT EXISTS bronze_commodity_prices (
    bronze_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    commodity_name VARCHAR(50),
    price_usd DECIMAL(18,6),
    timestamp DATETIME,
    source VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver_commodity_prices (
    silver_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    commodity_name VARCHAR(50) NOT NULL,
    price_usd DECIMAL(18,6) NOT NULL,
    timestamp DATETIME NOT NULL,
    source VARCHAR(50) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    price_change_pct DECIMAL(10,4) NOT NULL,
    transformed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_id INT AUTO_INCREMENT PRIMARY KEY,
    commodity_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT AUTO_INCREMENT PRIMARY KEY,
    full_timestamp DATETIME UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_commodity_price (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    commodity_id INT NOT NULL,
    time_id INT NOT NULL,
    price_usd DECIMAL(18,6) NOT NULL,
    UNIQUE KEY uniq_commodity_time (commodity_id, time_id),
    CONSTRAINT fk_fact_commodity FOREIGN KEY (commodity_id) REFERENCES dim_commodity(commodity_id),
    CONSTRAINT fk_fact_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);