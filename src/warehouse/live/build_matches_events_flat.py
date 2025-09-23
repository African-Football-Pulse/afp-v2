import os
import json
import pandas as pd
from src.storage import azure_blob

def main():
    container = os.getenv("AZURE_CONTAINER", "afp")
    season = os.getenv("SEASON")
    league = os.getenv("LEAGUE")

    if not season or not league:
        raise RuntimeError("SEASON and LEAGUE must be set as environment variables")

    base_path = f"stats/{season}/{league}/"
    manifest_path = f"{base_path}manifest.json"

    # Hämta manifest
    manifest_text = azure_blob.get_text(container, manifest_path)
    match_ids = json.loads(manifest_text)

    rows = []
    total = len(match_ids)
    for i, match_id in enumerate(match_ids, 1):
        match_path = f"{base_path}{match_id}.json"
        match_text = azure_blob.get_text(container, match_path)
        match = json.loads(match_text)

        print(f"[build_matches_events_flat] ({i}/{total}) Processing {match_path}")

        # Extrahera events från match
        events = match.get("events", [])
        for ev in events:
            row = {
                "match_id": match.get("id"),
                "event_id": ev.get("id"),
                "type": ev.get("type"),
                "minute": ev.get("minute"),
                "team_id": ev.get("team", {}).get("id") if ev.get("team") else None,
                "player_id": ev.get("player", {}).get("id") if ev.get("player") else None,
                "assist_id": ev.get("assist_player", {}).get("id") if ev.get("assist_player") else None,
                "x": ev.get("x"),
                "y": ev.get("y"),
                "expected_goals": ev.get("expected_goals"),
            }
            rows.append(row)

    # Bygg DataFrame
    df = pd.DataFrame(rows)

    # Sparar alltid i live-mappen
    out_path = "warehouse/live/matches_events_flat.parquet"
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")
    azure_blob.upload_bytes(container, out_path, parquet_bytes, content_type="application/octet-stream")

    print(f"[build_matches_events_flat LIVE] ✅ Uploaded {len(df)} rows → {out_path}")

if __name__ == "__main__":
    main()
