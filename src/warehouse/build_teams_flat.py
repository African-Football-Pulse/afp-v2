# src/warehouse/build_teams_flat.py

import json
import pandas as pd
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    """Helper to load JSON from Azure Blob."""
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"

    # Alla ligor som finns i Azure → teams/<league_id>/*.json
    leagues_prefix = "teams/"
    team_files = azure_blob.list_prefix(container, leagues_prefix)

    rows = []
    for path in team_files:
        if not path.endswith(".json"):
            continue

        # Exempel: teams/228/4157.json
        parts = path.split("/")
        if len(parts) < 3:
            continue

        league_id = parts[1]
        team_id = parts[2].replace(".json", "")

        try:
            team_data = load_json_from_blob(container, path)
        except Exception as e:
            print(f"[build_teams_flat] ⚠️ Skipping {path}: {e}")
            continue

        stadium = team_data.get("stadium") or {}

        rows.append({
            "team_id": str(team_data.get("id", team_id)),
            "team_name": team_data.get("name"),
            "league_id": league_id,
            "country": (team_data.get("country") or {}).get("name"),
            "stadium_id": stadium.get("id"),
            "stadium_name": stadium.get("name"),
            "stadium_city": stadium.get("city"),
        })

    df = pd.DataFrame(rows)

    # 📦 Spara som Parquet
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    output_path = "warehouse/base/teams_flat.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_teams_flat] ✅ Uploaded {len(df)} teams → {output_path}")


if __name__ == "__main__":
    main()
