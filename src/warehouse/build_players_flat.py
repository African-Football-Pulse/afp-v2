# src/warehouse/build_players_flat.py

import json
import pandas as pd
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    """Helper to load JSON from Azure Blob."""
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"

    # Inputfiler (ligger i players/)
    master_path = "players/africa/players_africa_master.json"
    history_path = "players/africa/players_africa_history.json"

    # Ladda masterfilen
    master = load_json_from_blob(container, master_path)
    players = master.get("players", [])

    # Ladda historikfilen
    history = load_json_from_blob(container, history_path)

    rows = []
    for p in players:
        pid = str(p.get("id"))
        hist = history.get(pid, {}).get("history", [])

        rows.append({
            "player_id": pid,
            "name": p.get("name"),
            "country": p.get("country"),
            "current_club": p.get("club"),
            "aliases": p.get("aliases", []),
            "short_aliases": p.get("short_aliases", []),
            "club_history": hist,   # kan vara JSON-array
            "sources": p.get("sources", {}),
            "parent_club": p.get("parent_club"),
            "loan_status": p.get("loan_status"),
            "transfer_source": p.get("transfer_source"),
            "transfer_updated": p.get("transfer_updated"),
        })

    df = pd.DataFrame(rows)

    # 📦 Spara till Parquet (lokalt i minnet)
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    # Upload till Azure Blob
    output_path = "warehouse/base/players_flat.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_players_flat] ✅ Uploaded {len(df)} players → {output_path}")


if __name__ == "__main__":
    main()
