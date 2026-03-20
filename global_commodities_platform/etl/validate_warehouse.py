import os

import pandas as pd
from sqlalchemy import create_engine


DB_URL = os.getenv(
    "DB_URL",
    "mysql+mysqlconnector://root:root@localhost:3306/global_commodities",
)


def run_validation() -> None:
    engine = create_engine(DB_URL)

    checks = {
        "row_counts": """
            SELECT 'bronze_rows' AS check_name, COUNT(*) AS value FROM bronze_commodity_prices
            UNION ALL
            SELECT 'silver_rows', COUNT(*) FROM silver_commodity_prices
            UNION ALL
            SELECT 'gold_fact_rows', COUNT(*) FROM fact_commodity_price
            UNION ALL
            SELECT 'dim_commodity_rows', COUNT(*) FROM dim_commodity
            UNION ALL
            SELECT 'dim_time_rows', COUNT(*) FROM dim_time
        """,
        "silver_nulls": """
            SELECT COUNT(*) AS issues
            FROM silver_commodity_prices
            WHERE commodity_name IS NULL
               OR price_usd IS NULL
               OR timestamp IS NULL
               OR source IS NULL
        """,
        "fact_orphans": """
            SELECT COUNT(*) AS issues
            FROM fact_commodity_price f
            LEFT JOIN dim_commodity c ON f.commodity_id = c.commodity_id
            LEFT JOIN dim_time t ON f.time_id = t.time_id
            WHERE c.commodity_id IS NULL OR t.time_id IS NULL
        """,
        "silver_duplicates": """
            SELECT commodity_name, timestamp, COUNT(*) AS duplicate_rows
            FROM silver_commodity_prices
            GROUP BY commodity_name, timestamp
            HAVING COUNT(*) > 1
            ORDER BY duplicate_rows DESC
            LIMIT 25
        """,
        "freshness_hours": """
            SELECT
                c.commodity_name,
                MAX(t.full_timestamp) AS latest_ts,
                TIMESTAMPDIFF(HOUR, MAX(t.full_timestamp), UTC_TIMESTAMP()) AS age_hours
            FROM fact_commodity_price f
            JOIN dim_commodity c ON f.commodity_id = c.commodity_id
            JOIN dim_time t ON f.time_id = t.time_id
            GROUP BY c.commodity_name
            ORDER BY age_hours DESC, c.commodity_name
        """,
    }

    print("\n=== Warehouse Validation Report ===")
    for name, query in checks.items():
        print(f"\n-- {name} --")
        df = pd.read_sql(query, engine)
        if df.empty:
            print("No issues found.")
        else:
            print(df)


if __name__ == "__main__":
    run_validation()
