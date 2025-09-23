import json
import pandas as pd
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def main():
    container = "afp"
    matches_prefix = "stats/"

    all_files = azure_blob.list_prefix(container, matches_prefix)

    match_rows = []
    event_rows = []

    # Filtrera fram bara match-filer (inte players eller manifest)
    match_files = [f for f in all_files if f.endswith(".json") and "/players/" not in f]

    total = len(match_files)
    for i, path in enumerate(match_files, start=1):
        # hoppa över manifest-filer
        if path.endswith("manifest.json"):
            if i % 100 == 0 or i == total:
                print(f"[build_matches_events_flat] ⏩ Skipping manifest file {path} ({i}/{total})")
            continue

        parts = path.split("/")
        if len(parts) < 4:
            continue

        season = parts[1]
        league_id = parts[2]

        try:
            match = load_json_from_blob(container, path)
        except Exception as e:
            if i % 100 == 0 or i == total:
                print(f"[build_matches_events_flat] ⚠️ Skipping {path}: {e} ({i}/{total})")
            continue

        if i % 100 == 0 or i == total:
            print(f"[build_matches_events_flat] Processing {i}/{total} → {path}")

        # --- Matchnivå ---
        match_rows.append({
            "match_id": match.get("id"),
            "date": match.get("date"),
            "time": match.get("time"),
            "league_id": league_id,
            "season": season,
            "home_team_id": (match.get("teams", {}).get("home") or {}).get("id"),
            "home_team_name": (match.get("teams", {}).get("home") or {}).get("name"),
            "away_team_id": (match.get("teams", {}).get("away") or {}).get("id"),
            "away_team_name": (match.get("teams", {}).get("away") or {}).get("name"),
            "status": match.get("status"),
            "winner": match.get("winner"),
            "home_goals": (match.get("goals") or {}).get("home_ft_goals"),
            "away_goals": (match.get("goals") or {}).get("away_ft_goals"),
            "has_extra_time": match.get("has_extra_time"),
            "has_penalties": match.get("has_penalties"),
        })

        # --- Eventnivå ---
        for ev in match.get("events", []):
            row = {
                "match_id": match.get("id"),
                "league_id": league_id,
                "season": season,
                "event_type": ev.get("event_type"),
                "event_minute": ev.get("event_minute"),
                "team": ev.get("team"),
            }

            player = ev.get("player") or {}
            row["player_id"] = player.get("id")
            row["player_name"] = player.get("name")

            assist = ev.get("assist_player") or {}
            row["assist_id"] = assist.get("id")
            row["assist_name"] = assist.get("name")

            pin = ev.get("player_in") or {}
            row["player_in_id"] = pin.get("id")
            row["player_in_name"] = pin.get("name")

            pout = ev.get("player_out") or {}
            row["player_out_id"] = pout.get("id")
            row["player_out_name"] = pout.get("name")

            event_rows.append(row)

    # --- Bygg DataFrames ---
    df_matches = pd.DataFrame(match_rows)
    df_events = pd.DataFrame(event_rows)

    # 📦 Spara Parquet
    parquet_matches = df_matches.to_parquet(index=False, engine="pyarrow")
    parquet_events = df_events.to_parquet(index=False, engine="pyarrow")

    azure_blob.put_bytes(
        container=container,
        blob_path="warehouse/base/matches_flat.parquet",
        data=parquet_matches,
        content_type="application/octet-stream"
    )

    azure_blob.put_bytes(
        container=container,
        blob_path="warehouse/base/events_flat.parquet",
        data=parquet_events,
        content_type="application/octet-stream"
    )

    print(f"[build_matches_events_flat] ✅ Uploaded {len(df_matches)} matches → warehouse/base/matches_flat.parquet")
    print(f"[build_matches_events_flat] ✅ Uploaded {len(df_events)} events → warehouse/base/events_flat.parquet")


if __name__ == "__main__":
    main()
