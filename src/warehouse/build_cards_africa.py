import pandas as pd
from src.storage import azure_blob
import json


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"

    # 📥 Ladda player_match_stats.parquet
    stats_path = "warehouse/base/player_match_stats.parquet"
    parquet_bytes = azure_blob._client().get_container_client(container).get_blob_client(stats_path).download_blob().readall()
    df_stats = pd.read_parquet(pd.io.common.BytesIO(parquet_bytes), engine="pyarrow")

    # 📥 Ladda players_flat för country-info
    players_path = "warehouse/base/players_flat.parquet"
    parquet_bytes = azure_blob._client().get_container_client(container).get_blob_client(players_path).download_blob().readall()
    df_players = pd.read_parquet(pd.io.common.BytesIO(parquet_bytes), engine="pyarrow")
    df_players = df_players[["player_id", "name", "country"]].drop_duplicates()

    # 🔎 Summera kort per spelare × säsong
    df_cards = (
        df_stats.groupby(["player_id", "season"])
        .agg(
            total_yellow=("yellow_cards", "sum"),
            total_red=("red_cards", "sum")
        )
        .reset_index()
    )

    # ➕ Slå ihop med namn + land
    df_cards = df_cards.merge(df_players, on="player_id", how="left")
    df_cards.rename(columns={"name": "player_name"}, inplace=True)

    # ➕ Lägg till total_cards (gult + rött)
    df_cards["total_cards"] = df_cards["total_yellow"] + df_cards["total_red"]

    # 📦 Skriv till Parquet i metrics/
    output_path = "warehouse/metrics/cards_africa.parquet"
    parquet_bytes = df_cards.to_parquet(index=False, engine="pyarrow")
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_cards_africa] ✅ Uploaded {len(df_cards)} rows → {output_path}")
    print("[build_cards_africa] 🔎 Sample:")
    print(df_cards.head(10))


if __name__ == "__main__":
    main()
