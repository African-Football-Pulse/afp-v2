# src/warehouse/build_seasons_flat.py

import os
import json
import pandas as pd
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"

    # Input: leagues.json för att veta vilka ligor vi har
    leagues_path = "meta/leagues.json"
    leagues = load_json_from_blob(container, leagues_path).get("results", [])

    rows = []

    for lg in leagues:
        league_id = str(lg.get("id"))
        league_name = lg.get("name")

        seasons_path = f"meta/seasons_{league_id}.json"
        try:
            seasons_json = load_json_from_blob(container, seasons_path).get("results", [])
        except Exception as e:
            print(f"[build_seasons_flat] ⚠️ Missing or error in {seasons_path}: {e}")
            continue

        for s in seasons_json:
            season = s.get("season", {})
            rows.append({
                "league_id": league_id,
                "league_name": league_name,
                "season_year": season.get("year"),
                "is_active": season.get("is_active"),
                "source": "json"
            })

    df = pd.DataFrame(rows)

    # 📦 Spara till Parquet
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    output_path = "warehouse/base/seasons_flat.parquet"
    azure_blob.put_bytes(
        container=container,
        blob_path=output_path,
        data=parquet_bytes,
        content_type="application/octet-stream"
    )

    print(f"[build_seasons_flat] ✅ Uploaded {len(df)} rows → {output_path}")


if __name__ == "__main__":
    main()
