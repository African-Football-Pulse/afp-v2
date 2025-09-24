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


def normalize_id(series: pd.Series) -> pd.Series:
    """Konvertera ID-kolumner från float/NaN till str utan decimal."""
    return series.fillna(0).astype(int).astype(str)


def main():
    container = "afp"

    # 🎯 Masterlista för afrikanska spelare
    master_path = "players/africa/players_africa_master.json"
    master = load_json_from_blob(container, master_path)
    player_ids = {str(int(p.get("id"))) for p in master.get("players", []) if p.get("id")}

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

        # Normalisera ID-kolumner
        df["player_id"] = normalize_id(df["player_id"])
        if "assist_id" in df.columns:
            df["assist_id"] = normalize_id(df["assist_id"])

        # 👀 Debug: visa struktur före filtrering
        print(f"\n[DEBUG] Fil: {path}")
        print(f"[DEBUG] Kolumner: {list(df.columns)}")
        print("[DEBUG] Första 5 rader (innan filtrering):")
        print(df.head().to_string(index=False))

        # Filtrera på spelare i masterlistan
        df = df[df["player_id"].isin(player_ids)]

        if df.empty:
            print(f"[DEBUG] Efter filtrering: 0 rader för {path}")
            continue

        # Bygg player × match rader
        grouped = df.groupby(["player_id", "match_id", "league_id", "season"]).agg(
            goals=("event_type", lambda x: (x == "goal").sum()),
            assists=("event_type", lambda x: (x == "assist").sum()),
            yellow_cards=("event_type", lambda x: (x == "yellow_card").sum()),
            red_cards=("event_type", lambda x: (x == "red_card").sum()),
            minutes_played=("event_minute", "max"),
        ).reset_index()

        rows.append(grouped)

    if not rows:
        print("[build_player_match_stats] ⚠️ Ingen data efter filtrering i någon fil")
        return

    result = pd.concat(rows, ignore_index=True)

    # 📦 Spara till Parquet
    parquet_bytes = result.to_parquet(index=False, engine="pyarrow")
    output_path = "warehouse/base/player_match_stats.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(f"[build_player_match_stats] ✅ Uploaded {len(result)} rows → {output_path}")

    # 👀 Preview per spelare
    print("\n[build_player_match_stats] 🔎 Sample (per spelare):")
    print(result.groupby("player_id").head(3).to_string(index=False))


if __name__ == "__main__":
    main()
