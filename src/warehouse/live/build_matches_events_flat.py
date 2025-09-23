import os
import json
import pandas as pd
from src.storage import azure_blob

def load_json_from_blob(container, path):
    text = azure_blob.get_text(container, path)
    return json.loads(text)

def main():
    container = os.getenv("AZURE_CONTAINER", "afp")
    base_path = "stats"

    season_filter = os.getenv("SEASON")
    league_filter = os.getenv("LEAGUE")

    rows = []
    match_count = 0

    # Gå igenom alla ligor och säsonger
    seasons = azure_blob.list_files(container, base_path)
    for season_path in seasons:
        season = season_path.split("/")[-2] if "/" in season_path else season_path

        # Filtrera på SEASON om satt
        if season_filter and season != season_filter:
            continue

        league_id = season_path.split("/")[-1]
        if league_filter and league_id != league_filter:
            continue

        match_files = azure_blob.list_files(container, f"{base_path}/{season}/{league_id}")
        for mf in match_files:
            if mf.endswith("manifest.json"):
                continue

            match_data = load_json_from_blob(container, mf)

            # Hoppa över om filen är en lista (manifest istället för match)
            if isinstance(match_data, list):
                continue

            match_count += 1
            if match_count % 100 == 0:
                print(f"[build_matches_events_flat] ({match_count}) Processing {mf}")

            match_id = match_data.get("id")
            league = match_data.get("league_id", league_id)
            season_val = match_data.get("season", season)

            for ev in match_data.get("events", []):
                row = {
                    "match_id": match_id,
                    "league_id": league,
                    "season": season_val,
                    "date": match_data.get("date"),
                    "event_type": ev.get("event_type"),
                    "event_minute": ev.get("event_minute"),
                    "team": ev.get("team"),
                    "player_id": ev.get("player", {}).get("id") if ev.get("player") else None,
                    "player_name": ev.get("player", {}).get("name") if ev.get("player") else None,
                    "assist_id": ev.get("assist_player", {}).get("id") if ev.get("assist_player") else None,
                    "assist_name": ev.get("assist_player", {}).get("name") if ev.get("assist_player") else None,
                }
                rows.append(row)

    if not rows:
        print("[build_matches_events_flat] ⚠️ No rows built, nothing to upload")
        return

    df = pd.DataFrame(rows)
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    # Fullfil
    base_out = "warehouse/base/matches_events_flat.parquet"
    azure_blob.upload_bytes(container, base_out, parquet_bytes)
    print(f"[build_matches_events_flat] ✅ Uploaded {len(rows)} rows → {base_out}")

    # Filtrerad fil om vi kör med flaggor
    if season_filter or league_filter:
        league_str = league_filter if league_filter else "all"
        season_str = season_filter if season_filter else "all"
        out_path = f"warehouse/base/matches_events_flat_{league_str}_{season_str}.parquet"
        azure_blob.upload_bytes(container, out_path, parquet_bytes)
        print(f"[build_matches_events_flat] ✅ Uploaded filtered {len(rows)} rows → {out_path}")


if __name__ == "__main__":
    main()
