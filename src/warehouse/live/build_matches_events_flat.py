import os
import json
import pandas as pd
from src.storage import azure_blob

def iter_blob_files(container, prefix):
    """Iterera över alla blobfiler i ett prefix och yielda (path, text)."""
    for path in azure_blob.list_files(container, prefix):
        text = azure_blob.get_text(container, path)
        yield path, text

def main():
    container = os.environ.get("AZURE_CONTAINER", "afp")
    prefix = "stats/"
    rows = []

    # Räkna antal för progress
    all_files = list(azure_blob.list_files(container, prefix))
    total_files = len(all_files)

    for i, (path, text) in enumerate(iter_blob_files(container, prefix), start=1):
        # hoppa över manifestfiler
        if path.endswith("manifest.json"):
            print(f"[build_matches_events_flat] ⏩ Skipping manifest file {path}")
            continue

        print(f"[build_matches_events_flat] ({i}/{total_files}) Processing {path}")

        try:
            match = json.loads(text)
        except Exception as e:
            print(f"[build_matches_events_flat] ⚠️ Failed to parse {path}: {e}")
            continue

        for ev in match.get("events", []):
            row = {
                "match_id": match.get("id"),
                "league_id": match.get("league_id"),
                "season": match.get("season"),
                "date": match.get("date"),
                "event_type": ev.get("event_type"),
                "event_minute": ev.get("event_minute"),
                "team": ev.get("team"),
                "player_id": ev.get("player", {}).get("id") if ev.get("player") else None,
                "player_name": ev.get("player", {}).get("name") if ev.get("player") else None,
                "assist_id": ev.get("assist_player", {}).get("id") if ev.get("assist_player") else None,
                "assist_name": ev.get("assist_player", {}).get("name") if ev.get("assist_player") else None,
                "player_in_id": ev.get("player_in", {}).get("id") if ev.get("player_in") else None,
                "player_in_name": ev.get("player_in", {}).get("name") if ev.get("player_in") else None,
                "player_out_id": ev.get("player_out", {}).get("id") if ev.get("player_out") else None,
                "player_out_name": ev.get("player_out", {}).get("name") if ev.get("player_out") else None,
            }
            rows.append(row)

    if not rows:
        print("[build_matches_events_flat] ⚠️ No rows built, nothing to upload")
        return

    df = pd.DataFrame(rows)
    parquet_bytes = df.to_parquet(index=False, engine="pyarrow")

    out_path = "warehouse/base/matches_events_flat.parquet"
    azure_blob.upload_bytes(container, out_path, parquet_bytes)

    print(f"[build_matches_events_flat] ✅ Uploaded {len(rows)} rows → {out_path}")


if __name__ == "__main__":
    main()
