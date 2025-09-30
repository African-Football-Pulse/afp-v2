# src/warehouse/build_match_performance_africa.py

import pandas as pd
from src.storage import azure_blob

CONTAINER = "afp"


def compute_form_score(row):
    """Beräkna ett enkelt form-score för en matchrad."""
    goals = row.get("goals", 0)
    assists = row.get("assists", 0)
    minutes = row.get("minutes_played", 0)
    cards = row.get("yellow_cards", 0) + 2 * row.get("red_cards", 0)

    return goals * 4 + assists * 3 + (minutes / 90.0) - cards


def rolling_form(df, window: int, col_name: str):
    """Beräkna rolling form score för ett givet fönster."""
    df[col_name] = (
        df.sort_values("match_id")
        .groupby("player_id")["form_score"]
        .transform(lambda x: x.rolling(window, min_periods=1).mean())
    )
    return df


def main():
    print("[build_match_performance_africa] 🔄 Laddar warehouse/base/player_match_stats.parquet")

    # Ladda player_match_stats
    path = "warehouse/base/player_match_stats.parquet"
    blob_bytes = (
        azure_blob._client()
        .get_container_client(CONTAINER)
        .get_blob_client(path)
        .download_blob()
        .readall()
    )
    df = pd.read_parquet(pd.io.common.BytesIO(blob_bytes), engine="pyarrow")

    if df.empty:
        print("[build_match_performance_africa] ⚠️ Ingen data hittades")
        return

    # Konvertera minutes_played till int
    if "minutes_played" in df.columns:
        df["minutes_played"] = (
            pd.to_numeric(df["minutes_played"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
    else:
        df["minutes_played"] = 0

    # Beräkna grundläggande form_score
    df["form_score"] = df.apply(compute_form_score, axis=1)

    # Rolling windows
    df = rolling_form(df, 5, "form_score_5")      # senaste 5 matcher
    df = rolling_form(df, 10, "form_score_10")    # senaste 10 matcher
    df = rolling_form(df, 100, "form_score_long") # långsiktigt (≈ 3 säsonger)

    # 📦 Spara till Parquet
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")
    output_path = "warehouse/metrics/match_performance_africa.parquet"
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
        df[["player_id", "season", "form_score", "form_score_5", "form_score_10", "form_score_long"]]
        .head(10)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
