# src/warehouse/build_player_match_stats.py

import json
import pandas as pd
from io import BytesIO
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def load_parquet_from_blob(container: str, path: str) -> pd.DataFrame:
    """Ladda parquet från Azure Blob till Pandas."""
    blob_bytes = (
        azure_blob._client()
        .get_container_client(container)
        .get_blob_client(path)
        .download_blob()
        .readall()
    )
    return pd.read_parquet(BytesIO(blob_bytes), engine="pyarrow")


def main():
    container = "afp"

    # 🎯 Masterlista för afrikanska spelare
    master_path = "players/africa/players_africa_master.json"
    master = load_json_from_blob(container, master_path)
    player_ids = {str(p.get("id")) for p in master.get("players", [])}

    rows = []

    # Hämta alla events_flat-filer
    all_files = azure_blob.list_prefix(container, "warehouse/base/events_flat/")
    parquet_files = [f for f in all_files if f.endswith(".parquet")]

    if not parquet_files:
        print("[build_player_match_stats] ⚠️ Inga events_flat-filer hittades")
        return

    for path in parquet_files:
        try:
            df = load_parquet_from_blob(container, path)
        except Exception as e:
            print(f"[build_player_match_stats] ⚠️ Kunde inte läsa {path}: {e}")
            continue

        # Filtrera på spelare i masterlistan
        df = df[df["player_id"].astype(str).isin(player_ids)]
        if df.empty:
            continue

        # Bygg player × match rader
        grouped = df.groupby(["player_id", "match_id", "league_id", "season"]).agg(
            goals=("event_type", lambda x: (x == "goal").sum()),
            assists=("event_type", lambda x: (x == "assist").sum()),
            yellow_cards=("event_type", lambda x: (x == "yellow_card").sum()),
            red_cards=("event_type", lambda x: (x == "red_card").sum()),
            minutes_played=("event_minute", "max"),  # ungefärlig speltid
        ).reset_index()

        rows.append(grouped)

    if not rows:
        print("[build_player_match_stats] ⚠️ Ingen data efter filtrering")
        return

    result = pd.concat(rows, ignore_index=True)

    # 📦 Spara till Parquet (alla spelare × matcher)
    parquet_bytes = result.to_parquet(index=False, engine="pyarrow")
    output_path = "warehouse/base/player_match_stats.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(
        f"[build_player_match_stats] ✅ Uploaded {len(result)} rows → {output_path}"
    )


if __name__ == "__main__":
    main()
