from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)


def latest_bronze_file() -> Path | None:
    files = sorted(BRONZE_DIR.glob("bronze_prices_*.csv"))
    if not files:
        return None
    return files[-1]


def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    clean_df = df.copy()

    clean_df = clean_df.dropna(subset=["commodity_name", "price_usd", "timestamp", "source"])
    clean_df["commodity_name"] = clean_df["commodity_name"].str.strip().str.title()
    clean_df["source"] = clean_df["source"].str.strip()
    clean_df["price_usd"] = pd.to_numeric(clean_df["price_usd"], errors="coerce")
    clean_df = clean_df.dropna(subset=["price_usd"])
    clean_df = clean_df[clean_df["price_usd"] > 0]

    clean_df["timestamp"] = pd.to_datetime(clean_df["timestamp"], utc=True, errors="coerce")
    clean_df = clean_df.dropna(subset=["timestamp"])
    clean_df["timestamp"] = clean_df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

    clean_df = clean_df.sort_values(["commodity_name", "timestamp"]).reset_index(drop=True)

    clean_df["year"] = clean_df["timestamp"].dt.year
    clean_df["month"] = clean_df["timestamp"].dt.month
    clean_df["day"] = clean_df["timestamp"].dt.day

    clean_df["price_change_pct"] = (
        clean_df.groupby("commodity_name")["price_usd"].pct_change().fillna(0) * 100
    ).round(4)

    return clean_df


def save_silver(df: pd.DataFrame) -> Path:
    run_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = SILVER_DIR / f"silver_prices_{run_time}.csv"
    df.to_csv(file_path, index=False)
    return file_path


def run_once() -> None:
    bronze_file = latest_bronze_file()
    if not bronze_file:
        print("No Bronze file found. Run extract.py first.")
        return

    bronze_df = pd.read_csv(bronze_file)
    silver_df = clean_and_transform(bronze_df)

    if silver_df.empty:
        print("Silver dataset is empty after cleaning.")
        return

    silver_file = save_silver(silver_df)
    print(silver_df)
    print(f"\nSaved Silver file: {silver_file}")


if __name__ == "__main__":
    run_once()
