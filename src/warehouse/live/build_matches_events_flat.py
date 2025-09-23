import os
import json
import pandas as pd
from src.storage import azure_blob

def load_json_from_blob(container, path):
    text = azure_blob.get_text(container, path)
    return json.loads(text)

def main():
    container = os.environ.get("AZURE_CONTAINER")
    season = os.environ.get("SEASON")
    league = os.environ.get("LEAGUE")

    base_path = f"stats/{season}/{league}"
    files = azure_blob.list_files(container, base_path)

    rows = []
    total = len(files)

    for i, path in enumerate(files, start=1):
        if not path.endswith(".json"):
            continue
        try:
            match = load_json_from_blob(container, path)
        except Exception as e:
            if i % 100 == 0 or i == total:
                print(f"[build_matches_events_flat] ⚠️ Skipping {path}: {e} ({i}/{total})")
            continue

        # hoppa över om filen är en lista (manifest, inte en match)
        if isinstance(match, list):
            continue

        if i % 100 == 0 or i == total:
            print(f"[build_matches_events_flat] Processing {i}/{total} → {path}")

        match_id = match.get("id")
        events = match.get("events", [])

        for ev in events:
            row = {
                "match_id": match_id,
                "event_id": ev.get("id"),
                "type": ev.get("type", {}).get("name"),
                "minute": ev.get("minute"),
                "second": ev.get("second"),
                "team_id": ev.get("team", {}).get("id"),
                "player_id": ev.get("player", {}).get("id"),
                "assist_id": (ev.get("assist_player") or {}).get("id"),
                "x": ev.get("x"),
                "y": ev.get("y"),
                "outcome": ev.get("outcome", {}).get("name"),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    output_path = f"warehouse/live/matches_events_flat.parquet"
    azure_blob.upload_df(container, output_path, df)
    print(f"[build_matches_events_flat] ✅ Uploaded {len(rows)} rows → {output_path}")

if __name__ == "__main__":
    main()
