# src/warehouse/build_matches_events.py

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

    for path in all_files:
        if not path.endswith(".json"):
            continue

        # Vi vill bara ha matchfiler → /stats/<season>/<league>/<match>.json
        parts = path.split("/")
        if len(parts) < 4:
            continue

        season = parts[1]
        league_id = parts[2]
        filename = parts[3].replace(".json", "")

        # Hoppa över "players" undermappar (de hanteras i S_05)
        if "players" in parts:
            continue

        try:
            match = load_json_from_blob(container, path)
        except Exception as e:
            print(f"[build_matches_events] ⚠️ Skipping {path}: {e}")
            continue

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
            # Spelare inblandade
            if "player" in ev:
                row["player_id"] = ev["player"].get("id")
                row["player_name"] = ev["player"].get("name")
            if "assist_player" in ev:
                row["assist_id"] = ev["assist_player"].get("id")
                row["assist_name"] = ev["assist_player"].get("name")
            if "player_in" in ev:
                row["player_in_id"] = ev["player_in"].get("id")
                row["player_in_name"] = ev["player_in"].get("name")
            if "player_out" in ev:
                row["player_out_id"] = ev["player_out"].get("id")
                row["player_out_name"] = ev["player_out"].get("name")

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

    print(f"[build_matches_events] ✅ Uploaded {len(df_matches)} matches → warehouse/base/matches_flat.parquet")
    print(f"[build_matches_events] ✅ Uploaded {len(df_events)} events → warehouse/base/events_flat.parquet")


if __name__ == "__main__":
    main()
