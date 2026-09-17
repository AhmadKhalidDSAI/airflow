import os
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task
from pendulum import datetime, duration


# Path to the sample CSV bundled in the project
STEAM_CSV = (
    Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
    / "include"
    / "steam_games_sample.csv"
)


@dag(
    dag_id="project_name_dag",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "TeamName",
        "retries": 1,
        "retry_delay": duration(seconds=10),
    },
    tags=["stage2", "steam"],
    is_paused_upon_creation=False,
)
def project_name_dag():
    @task
    def extract_steam_data() -> dict:
        """Task 1: Read the Steam Games CSV and return key columns."""
        df = pd.read_csv(STEAM_CSV)

        # Keep only the columns we need for analysis
        columns = ["name", "genres", "positive_ratings", "negative_ratings", "price"]
        df = df[columns].dropna()

        # Convert ratings to numeric, coercing errors to NaN then dropping
        df["positive_ratings"] = pd.to_numeric(df["positive_ratings"], errors="coerce")
        df["negative_ratings"] = pd.to_numeric(df["negative_ratings"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df.dropna()

        print(f"Extracted {len(df)} games from Steam dataset.")
        return df.to_dict(orient="list")

    @task
    def transform_and_analyze(data: dict) -> None:
        """Task 2: Compute per-genre statistics from the extracted data."""
        df = pd.DataFrame(data)

        # A game can belong to multiple genres (semicolon-separated)
        # Explode so each genre gets its own row for aggregation
        df["genres"] = df["genres"].str.split(";")
        df = df.explode("genres")
        df["genres"] = df["genres"].str.strip()

        # Compute stats per genre
        stats = (
            df.groupby("genres")
            .agg(
                game_count=("name", "count"),
                avg_price=("price", "mean"),
                avg_positive=("positive_ratings", "mean"),
                avg_negative=("negative_ratings", "mean"),
            )
            .round(2)
        )

        print("=== Steam Games — Per-Genre Summary ===")
        print(stats.to_string())
        print(f"\nTotal genres analyzed: {len(stats)}")

    # Task dependency: extract → transform (sequential)
    raw = extract_steam_data()
    transform_and_analyze(raw)


# Instantiate the DAG
project_name_dag()
