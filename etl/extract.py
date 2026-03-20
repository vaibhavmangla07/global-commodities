import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from commodities_list import ALL_COMMODITY_TICKERS, get_commodity_group


BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR = BASE_DIR / "result"
RESULT_DIR.mkdir(parents=True, exist_ok=True)
TOP_COMMODITIES = ["Gold", "Silver", "Natural Gas", "Crude Oil"]


def pick_series(history: pd.DataFrame, field_name: str, ticker: str) -> pd.Series | None:
    if history.empty:
        return None

    if isinstance(history.columns, pd.MultiIndex):
        if (field_name, ticker) in history.columns:
            return history[(field_name, ticker)]

        field_cols = [col for col in history.columns if col[0] == field_name]
        if field_cols:
            return history[field_cols[0]]

    if field_name in history.columns:
        return history[field_name]

    return None

def fetch_yahoo_live_prices() -> list[dict]:
    ticker_map = ALL_COMMODITY_TICKERS

    records = []
    for ticker, commodity_name in ticker_map.items():
        intraday_history = yf.download(
            ticker,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            prepost=True,
        )
        daily_history = yf.download(
            ticker,
            period="5d",
            interval="1d",
            progress=False,
            auto_adjust=False,
        )

        if intraday_history.empty and daily_history.empty:
            continue

        data_source = intraday_history if not intraday_history.empty else daily_history

        close_series = pick_series(data_source, "Close", ticker)
        if close_series is None:
            continue

        close_series = close_series.dropna()
        if close_series.empty:
            continue

        latest_price = float(close_series.iloc[-1])
        high_series = pick_series(data_source, "High", ticker)
        low_series = pick_series(data_source, "Low", ticker)

        if high_series is not None:
            high_series = high_series.dropna()
        if low_series is not None:
            low_series = low_series.dropna()

        if high_series is not None and not high_series.empty:
            today_high = float(high_series.max())
        else:
            today_high = latest_price

        if low_series is not None and not low_series.empty:
            today_low = float(low_series.min())
        else:
            today_low = latest_price

        prev_close_series = pick_series(daily_history, "Close", ticker)
        previous_price = None
        if prev_close_series is not None:
            prev_close_series = prev_close_series.dropna()
            if len(prev_close_series) >= 2:
                previous_price = float(prev_close_series.iloc[-2])
            elif len(prev_close_series) == 1 and len(close_series) >= 2:
                previous_price = float(close_series.iloc[-2])

        price_change_pct = 0.0
        if previous_price and previous_price != 0:
            price_change_pct = ((latest_price - previous_price) / previous_price) * 100

        latest_market_ts = pd.Timestamp(close_series.index[-1])
        if latest_market_ts.tzinfo is None:
            latest_market_ts = latest_market_ts.tz_localize("UTC")
        else:
            latest_market_ts = latest_market_ts.tz_convert("UTC")

        now_utc = datetime.now(timezone.utc)
        records.append(
            {
                "commodity_name": commodity_name,
                "commodity_group": get_commodity_group(commodity_name),
                "price_usd": round(latest_price, 4),
                "today_high": round(today_high, 4),
                "today_low": round(today_low, 4),
                "date": now_utc.strftime("%Y-%m-%d"),
                "time": now_utc.strftime("%H:%M:%S"),
                "price_change_pct": round(price_change_pct, 4),
                "timestamp": latest_market_ts.isoformat(),
                "source": "Yahoo Finance API",
            }
        )

    return records


def extract_live_prices() -> pd.DataFrame:
    records = fetch_yahoo_live_prices()

    df = pd.DataFrame(
        records,
        columns=[
            "commodity_name",
            "commodity_group",
            "price_usd",
            "today_high",
            "today_low",
            "date",
            "time",
            "price_change_pct",
            "timestamp",
            "source",
        ],
    )
    return df


def save_bronze(df: pd.DataFrame) -> Path:
    run_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = BRONZE_DIR / f"bronze_prices_{run_time}.csv"
    bronze_df = df[["commodity_name", "price_usd", "timestamp", "source"]].copy()
    bronze_df.to_csv(file_path, index=False)
    return file_path


def save_result_csv(df: pd.DataFrame) -> Path:
    result_file = RESULT_DIR / "result.csv"
    df.to_csv(result_file, index=False)
    return result_file


def run_once() -> None:
    df = extract_live_prices()
    if df.empty:
        print("No live records fetched from Yahoo Finance API. Please rerun.")
        return

    file_path = save_bronze(df)
    output_df = df.rename(
        columns={
            "commodity_name": "name",
            "commodity_group": "group",
            "price_usd": "current_price",
            "today_high": "today_high",
            "today_low": "today_low",
            "date": "date",
            "time": "time",
            "price_change_pct": "price_change_pct",
        }
    )
    output_df["price_change_pct"] = output_df["price_change_pct"].map(
        lambda value: "0.000" if round(float(value), 4) == 0 else f"{float(value):+.4f}"
    )
    result_file = save_result_csv(output_df)
    print(
        output_df[
            ["name", "group", "current_price", "today_high", "today_low", "date", "time", "price_change_pct"]
        ]
    )

    top_df = output_df[output_df["name"].isin(TOP_COMMODITIES)].copy()
    top_df["name"] = pd.Categorical(top_df["name"], categories=TOP_COMMODITIES, ordered=True)
    top_df = top_df.sort_values("name").reset_index(drop=True)

    print("\nTop Commodities")
    print(top_df[["name", "current_price", "today_high", "today_low", "date", "time", "price_change_pct"]])

    print(f"\nSaved Bronze file: {file_path}")
    print(f"Saved Result file: {result_file}")


def run_loop(interval_minutes: int = 5) -> None:
    while True:
        run_once()
        print(f"Sleeping for {interval_minutes} minutes...")
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    mode = os.getenv("PIPELINE_MODE", "once").lower()
    interval = int(os.getenv("PIPELINE_INTERVAL_MINUTES", "5"))

    if mode == "loop":
        run_loop(interval_minutes=interval)
    else:
        run_once()
