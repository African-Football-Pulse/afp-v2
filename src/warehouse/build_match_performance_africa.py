# src/warehouse/build_match_performance_africa.py

import pandas as pd
from io import BytesIO
from src.storage import azure_blob

CONTAINER = "afp"


def compute_form_score(row):
    goals = row.get("goals", 0)
    assists = row.get("assists", 0)
    yellow = row.get("yellow_cards", 0)
    red = row.get("red_cards", 0)
    minutes = row.get("minutes_played", 0)

    return (
        (3 * goals)
        + (2 * assists)
        - (1 * yellow)
        - (3 * red)
        + (minutes / 90.0)
    )


def main():
    input_path = "warehouse/base/player_match_stats.parquet"
    print(f"[build_match_performance_africa] 🔄 Laddar {input_path}")

    # Läs parquet från Azure
    blob_bytes = (
        azure_blob._client()
        .get_container_client(CONTAINER)
        .get_blob_client(input_path)
        .download_blob()
        .readall()
    )
    df = pd.read_parquet(BytesIO(blob_bytes), engine="pyarrow")

    if df.empty:
        print("[build_match_performance_africa] ⚠️ Ingen data hittades")
        return

    # ✅ Konvertera minutes_played till numerisk (int)
    if "minutes_played" in df.columns:
        df["minutes_played"] = (
            pd.to_numeric(df["minutes_played"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
    else:
        df["minutes_played"] = 0

    # Beräkna form_score per match
    df["form_score"] = df.apply(compute_form_score, axis=1)

    # Sortera och gör rolling window (5, 10, 20 matcher)
    df = df.sort_values(by=["player_id", "season", "match_id"])

    df["form_score_5"] = (
        df.groupby("player_id")["form_score"].transform(lambda x: x.rolling(5, min_periods=1).mean())
    )
    df["form_score_10"] = (
        df.groupby("player_id")["form_score"].transform(lambda x: x.rolling(10, min_periods=1).mean())
    )
    df["form_score_20"] = (
        df.groupby("player_id")["form_score"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    )

    # Långsiktig form = snitt över senaste 3 säsonger
    df["form_score_long"] = df.groupby(["player_id", "season"])["form_score"].transform("mean")

    # 📦 Spara till Parquet
    output_path = "warehouse/metrics/match_performance_africa.parquet"
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(
        container=CONTAINER,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(f"[build_match_performance_africa] ✅ Uploaded {len(df)} rows → {output_path}")

    # 👀 Preview
    print("\n[build_match_performance_africa] 🔎 Sample:")
    print(
        df[["player_id", "season", "form_score", "form_score_5", "form_score_10", "form_scor_]()]()
