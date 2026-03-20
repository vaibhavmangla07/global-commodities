import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

DB_URL = os.getenv(
    "DB_URL",
    "mysql+mysqlconnector://root:root@localhost:3306/commodities_db",
)


def latest_file(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern))
    if not files:
        return None
    return files[-1]


def load_to_mysql(engine, bronze_df: pd.DataFrame, silver_df: pd.DataFrame) -> None:
    bronze_df.to_sql("bronze_commodity_prices", con=engine, if_exists="append", index=False)
    silver_df.to_sql("silver_commodity_prices", con=engine, if_exists="append", index=False)


def build_gold_layer(engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT IGNORE INTO dim_commodity (commodity_name)
                SELECT DISTINCT commodity_name
                FROM silver_commodity_prices
                """
            )
        )

        connection.execute(
            text(
                """
                INSERT IGNORE INTO dim_time (full_timestamp, year, month, day, hour, minute)
                SELECT DISTINCT
                    timestamp,
                    year,
                    month,
                    day,
                    HOUR(timestamp) AS hour,
                    MINUTE(timestamp) AS minute
                FROM silver_commodity_prices
                """
            )
        )

        connection.execute(
            text(
                """
                INSERT IGNORE INTO fact_commodity_price (commodity_id, time_id, price_usd)
                SELECT
                    c.commodity_id,
                    t.time_id,
                    s.price_usd
                FROM silver_commodity_prices s
                JOIN dim_commodity c ON s.commodity_name = c.commodity_name
                JOIN dim_time t ON s.timestamp = t.full_timestamp
                """
            )
        )


def save_gold_snapshot(engine) -> Path:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    query = """
        SELECT
            c.commodity_name,
            t.full_timestamp,
            f.price_usd
        FROM fact_commodity_price f
        JOIN dim_commodity c ON f.commodity_id = c.commodity_id
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY t.full_timestamp DESC, c.commodity_name
    """
    gold_df = pd.read_sql(query, engine)
    file_path = GOLD_DIR / "gold_fact_snapshot.csv"
    gold_df.to_csv(file_path, index=False)
    return file_path


def run_once() -> None:
    bronze_file = latest_file(BRONZE_DIR, "bronze_prices_*.csv")
    silver_file = latest_file(SILVER_DIR, "silver_prices_*.csv")

    if not bronze_file or not silver_file:
        print("Missing Bronze or Silver file. Run extract.py and transform.py first.")
        return

    bronze_df = pd.read_csv(bronze_file)
    silver_df = pd.read_csv(silver_file)

    engine = create_engine(DB_URL)
    load_to_mysql(engine, bronze_df, silver_df)
    build_gold_layer(engine)
    gold_file = save_gold_snapshot(engine)

    print(f"Loaded Bronze file: {bronze_file}")
    print(f"Loaded Silver file: {silver_file}")
    print(f"Saved Gold snapshot file: {gold_file}")


if __name__ == "__main__":
    run_once()