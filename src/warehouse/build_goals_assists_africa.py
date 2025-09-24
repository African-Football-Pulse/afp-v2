# src/warehouse/build_goals_assists_africa.py

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

    # 📥 Masterlista
    master_path = "players/africa/players_africa_master.json"
    master = load_json_from_blob(container, master_path)

    # Endast numeriska ID:n
    players = [p for p in master.get("players", []) if str(p.get("id")).isdigit()]
    id_to_name = {str(p["id"]): p.get("name") for p in players}
    id_to_country = {str(p["id"]): p.get("country") for p in players}
    player_ids = set(id_to_name.keys())

    # 📥 Player match stats (alla spelare × matcher)
    stats_path = "warehouse/base/player_match_stats.parquet"
    try:
        df = load_parquet_from_blob(container, stats_path)
    except Exception as e:
        print(f"[build_goals_assists_africa] ❌ Kunde inte läsa {stats_path}: {e}")
        return

    # Filtrera på våra master-spelare
    df["player_id"] = df["player_id"].astype(str)
    df = df[df["player_id"].isin(player_ids)]

    if df.empty:
        print("[build_goals_assists_africa] ⚠️ Ingen data för masterspelarna")
        return

    # 🔢 Aggregera per spelare × säsong
    grouped = df.groupby(["player_id", "season"]).agg(
        total_goals=("goals", "sum"),
        total_assists=("assists", "sum"),
    ).reset_index()

    grouped["goal_contributions"] = grouped["total_goals"] + grouped["total_assists"]

    # Lägg till namn och land
    grouped["player_name"] = grouped["player_id"].map(id_to_name)
    grouped["country"] = grouped["player_id"].map(id_to_country)

    # 📦 Spara till Parquet
    parquet_bytes = grouped.to_parquet(index=False, engine="pyarrow")
    output_path = "warehouse/metrics/goals_assists_africa.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream",
    )

    print(
        f"[build_goals_assists_africa] ✅ Uploaded {len(grouped)} rows → {output_path}"
    )

    # 👀 Preview
    print("\n[build_goals_assists_africa] 🔎 Sample:")
    print(grouped.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
