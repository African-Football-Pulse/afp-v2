import pandas as pd
from src.storage import azure_blob
from src.warehouse import utils_ids  # 🔑 importera din utils

CONTAINER = "afp"


def main():
    input_path = "warehouse/base/player_match_stats.parquet"
    output_path = "warehouse/base/player_totals.parquet"

    # Ladda matchstats
    print(f"[build_player_totals] 🔄 Laddar {input_path}")
    blob_bytes = (
        azure_blob._client()
        .get_container_client(CONTAINER)
        .get_blob_client(input_path)
        .download_blob()
        .readall()
    )
    df = pd.read_parquet(pd.io.common.BytesIO(blob_bytes), engine="pyarrow")

    if df.empty:
        print("[build_player_totals] ⚠️ Inga rader hittades i player_match_stats.parquet")
        return

    # Normalisera ID-kolumner
    df = utils_ids.normalize_ids(df, cols=["player_id"])

    # Summera per spelare
    totals = df.groupby("player_id").agg(
        apps=("match_id", "nunique"),
        goals=("goals", "sum"),
        assists=("assists", "sum"),
        yellow_cards=("yellow_cards", "sum"),
        red_cards=("red_cards", "sum"),
        minutes_played=("minutes_played", "sum"),
    ).reset_index()

    # 📦 Spara till Parquet
    parquet_bytes = totals.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(
        container=CONTAINER,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(f"[build_player_totals] ✅ Uploaded {len(totals)} rows → {output_path}")

    # 👀 Preview
    print("\n[build_player_totals] 🔎 Sample:")
    print(totals.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
