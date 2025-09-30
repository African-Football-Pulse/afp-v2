# src/warehouse/build_match_performance_africa.py

import pandas as pd
import numpy as np
from src.storage import azure_blob

CONTAINER = "afp"

def compute_form_score(row):
    """Beräkna enkel match rating per spelare."""
    goals = row.get("goals", 0)
    assists = row.get("assists", 0)
    yc = row.get("yellow_cards", 0)
    rc = row.get("red_cards", 0)
    minutes = row.get("minutes_played", 0)

    return (
        (3 * goals)
        + (2 * assists)
        - (1 * yc)
        - (3 * rc)
        + (minutes / 90.0)
    )

def rolling_form(df, window):
    """Beräkna rullande snitt per spelare över angivet window."""
    return (
        df.groupby("player_id")["form_score"]
        .rolling(window, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

def main():
    input_path = "warehouse/base/player_match_stats.parquet"
    output_path = "warehouse/metrics/match_performance_africa.parquet"

    print(f"[build_match_performance_africa] 🔄 Laddar {input_path}")
    blob_bytes = (
        azure_blob._client()
        .get_container_client(CONTAINER)
        .get_blob_client(input_path)
        .download_blob()
        .readall()
    )
    df = pd.read_parquet(pd.io.common.BytesIO(blob_bytes), engine="pyarrow")

    if df.empty:
        print("[build_match_performance_africa] ⚠️ Ingen data i player_match_stats")
        return

    # Beräkna form score
    df["form_score"] = df.apply(compute_form_score, axis=1)

    # Sortera för rolling
    df = df.sort_values(by=["player_id", "season", "match_id"])

    # Lägg på rullande snitt
    df["form_score_5"] = rolling_form(df, 5)
    df["form_score_10"] = rolling_form(df, 10)
    df["form_score_20"] = rolling_form(df, 20)

    # Long-term form (3 senaste säsonger)
    df["long_term_form"] = (
        df.groupby("player_id")["form_score"]
        .transform(lambda x: x.tail(min(len(x), 60)).mean())  # ca 20 matcher/säsong × 3
    )

    # 📦 Spara till Parquet
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(
        container=CONTAINER,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(f"[build_match_performance_africa] ✅ Uploaded {len(df)} rows → {output_path}")

    # 👀 Preview
    preview = df.groupby("player_id").head(3)
    print("\n[build_match_performance_africa] 🔎 Sample:")
    print(preview[[
        "player_id", "season", "match_id",
        "goals", "assists", "yellow_cards", "red_cards", "minutes_played",
        "form_score", "form_score_5", "form_score_10", "form_score_20", "long_term_form"
    ]].to_string(index=False))

if __name__ == "__main__":
    main()
