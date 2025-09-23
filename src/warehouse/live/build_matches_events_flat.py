import os
import json
import pandas as pd
from src.storage import azure_blob

def load_json_from_blob(container, path):
    text = azure_blob.get_text(container, path)
    return json.loads(text)

def main():
    container = os.getenv("AZURE_CONTAINER", "afp")

    season_filter = os.getenv("SEASON")
    league_filter = os.getenv("LEAGUE")

    if not season_filter or not league_filter:
        raise RuntimeError("SEASON and LEAGUE must be set for live job")

    rows = []
    match_count = 0

    base_path = f"stats/{season_filter}/{league_filter}"
    match_files = azure_blob.list_files(container, base_path)

    for mf in match_files:
        if mf.endswith("manifest.json"):
            continue

        match_data = load_json_from_blob(container, mf)
        if isinstance(match_data, list):
            continue

        match_count += 1
        if match_count % 50 == 0:
            print(f"[build_matches_events_live] ({match_count}) Processing {mf}")

        match_id = match_data.get("id")

        for ev in match_data.get("events", []):
            row = {
                "match_id": match_id,
                "league_id": league_filter,
                "season": season_filter,
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
        print("[build_matches_events_live] ⚠️ No rows built, nothing to upload")
        return

    df = pd.DataFrame(rows)
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    out_path = f"warehouse/live/matches_events_flat_{league_filter}_{season_filter}.parquet"
    azure_blob.upload_bytes(container, out_path, parquet_bytes)

    print(f"[build_matches_events_live] ✅ Uploaded {len(rows)} rows → {out_path}")


if __name__ == "__main__":
    main()
