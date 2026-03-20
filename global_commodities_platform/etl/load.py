import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from commodities_list import ALL_COMMODITY_TICKERS


BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

DB_URL = os.getenv(
    "DB_URL",
    "mysql+mysqlconnector://root:root@localhost:3306/global_commodities",
)
ALLOWED_COMMODITY_NAMES = sorted(set(ALL_COMMODITY_TICKERS.values()))


def latest_file(folder: Path, prefix: str) -> Path | None:
    files = sorted(folder.glob(f"{prefix}_prices_*.csv"))
    if not files:
        return None
    return files[-1]


def load_bronze(engine, bronze_df: pd.DataFrame) -> None:
    bronze_df = bronze_df.copy()
    bronze_df["timestamp"] = pd.to_datetime(bronze_df["timestamp"], utc=True, errors="coerce")
    bronze_df = bronze_df.dropna(subset=["timestamp"])
    bronze_df["timestamp"] = bronze_df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    bronze_df.to_sql("bronze_commodity_prices", engine, if_exists="append", index=False)


def load_silver(engine, silver_df: pd.DataFrame) -> None:
    silver_df.to_sql("silver_commodity_prices", engine, if_exists="append", index=False)


def cleanup_layer_tables(engine) -> None:
    allowed_commodities_csv = ", ".join(f"'{name}'" for name in ALLOWED_COMMODITY_NAMES)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM bronze_commodity_prices
                WHERE commodity_name NOT IN ({allowed_commodities_csv})
                """
            )
        )
        conn.execute(
            text(
                f"""
                DELETE FROM silver_commodity_prices
                WHERE commodity_name NOT IN ({allowed_commodities_csv})
                """
            )
        )
        conn.execute(
            text(
                """
                DELETE b1
                FROM bronze_commodity_prices b1
                JOIN bronze_commodity_prices b2
                  ON b1.commodity_name = b2.commodity_name
                 AND b1.timestamp = b2.timestamp
                 AND b1.source = b2.source
                 AND b1.bronze_id > b2.bronze_id
                """
            )
        )
        conn.execute(
            text(
                """
                DELETE s1
                FROM silver_commodity_prices s1
                JOIN silver_commodity_prices s2
                  ON s1.commodity_name = s2.commodity_name
                 AND s1.timestamp = s2.timestamp
                 AND s1.silver_id > s2.silver_id
                """
            )
        )
        conn.execute(
            text(
                """
                DELETE FROM bronze_commodity_prices
                WHERE timestamp > (UTC_TIMESTAMP() + INTERVAL 10 MINUTE)
                """
            )
        )
        conn.execute(
            text(
                """
                DELETE FROM silver_commodity_prices
                WHERE timestamp > (UTC_TIMESTAMP() + INTERVAL 10 MINUTE)
                """
            )
        )


def rebuild_gold_star_schema(engine) -> None:
    silver_all = pd.read_sql(
        "SELECT commodity_name, price_usd, timestamp FROM silver_commodity_prices",
        engine,
    )

    if silver_all.empty:
        return

    silver_all["timestamp"] = pd.to_datetime(silver_all["timestamp"], utc=True, errors="coerce")
    silver_all = silver_all.dropna(subset=["timestamp", "commodity_name", "price_usd"])
    max_allowed_ts = pd.Timestamp.now(tz="UTC") + pd.Timedelta(minutes=10)
    silver_all = silver_all[silver_all["timestamp"] <= max_allowed_ts]
    silver_all["full_timestamp"] = silver_all["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

    dim_commodity = (
        pd.DataFrame({"commodity_name": sorted(silver_all["commodity_name"].unique())})
        .reset_index(drop=True)
    )
    dim_commodity["commodity_id"] = dim_commodity.index + 1
    dim_commodity = dim_commodity[["commodity_id", "commodity_name"]]

    dim_time = (
        pd.DataFrame({"full_timestamp": silver_all["full_timestamp"].drop_duplicates().sort_values()})
        .reset_index(drop=True)
    )
    dim_time["time_id"] = dim_time.index + 1
    dim_time["year"] = dim_time["full_timestamp"].dt.year
    dim_time["month"] = dim_time["full_timestamp"].dt.month
    dim_time["day"] = dim_time["full_timestamp"].dt.day
    dim_time["hour"] = dim_time["full_timestamp"].dt.hour
    dim_time["minute"] = dim_time["full_timestamp"].dt.minute
    dim_time = dim_time[["time_id", "full_timestamp", "year", "month", "day", "hour", "minute"]]

    fact_df = silver_all.merge(dim_commodity, on="commodity_name", how="left")
    fact_df = fact_df.merge(dim_time, on="full_timestamp", how="left")
    fact_df = fact_df[["commodity_id", "time_id", "price_usd"]]
    fact_df = fact_df.sort_values(["commodity_id", "time_id"]).drop_duplicates(
        subset=["commodity_id", "time_id"], keep="last"
    )

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_commodity_price"))
        conn.execute(text("DELETE FROM dim_time"))
        conn.execute(text("DELETE FROM dim_commodity"))

    dim_commodity.to_sql("dim_commodity", engine, if_exists="append", index=False)
    dim_time.to_sql("dim_time", engine, if_exists="append", index=False)
    fact_df.to_sql("fact_commodity_price", engine, if_exists="append", index=False)


def save_gold_snapshot(engine) -> Path:
    query = """
        SELECT
            f.fact_id,
            c.commodity_name,
            t.full_timestamp,
            t.year,
            t.month,
            t.day,
            f.price_usd
        FROM fact_commodity_price f
        JOIN dim_commodity c ON f.commodity_id = c.commodity_id
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY t.full_timestamp, c.commodity_name
    """
    gold_df = pd.read_sql(query, engine)
    output_file = GOLD_DIR / "gold_fact_snapshot.csv"
    gold_df.to_csv(output_file, index=False)
    return output_file


def run_once() -> None:
    bronze_file = latest_file(BRONZE_DIR, "bronze")
    silver_file = latest_file(SILVER_DIR, "silver")

    if not bronze_file or not silver_file:
        print("Missing Bronze/Silver files. Run extract.py and transform.py first.")
        return

    bronze_df = pd.read_csv(bronze_file)
    silver_df = pd.read_csv(silver_file)

    engine = create_engine(DB_URL)

    load_bronze(engine, bronze_df)
    load_silver(engine, silver_df)
    cleanup_layer_tables(engine)
    rebuild_gold_star_schema(engine)
    gold_snapshot_file = save_gold_snapshot(engine)

    print("Loaded Bronze, Silver, and Gold layers into MySQL.")
    print(f"Saved Gold snapshot: {gold_snapshot_file}")


if __name__ == "__main__":
    run_once()
