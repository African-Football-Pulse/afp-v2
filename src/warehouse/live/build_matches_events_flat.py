import os
import json
import re
import pandas as pd
from collections import defaultdict
from src.storage import azure_blob


def load_json_from_blob(container: str, path: str):
    text = azure_blob.get_text(container, path)
    return json.loads(text)


def is_date_folder(name: str) -> bool:
    """Returnerar True om strängen är på formatet DD-MM-YYYY."""
    return re.match(r"\d{2}-\d{2}-\d{4}", name) is not None


def main():
    container = "afp"
    matches_prefix = "stats/"

    # Optional filters
    filter_season = os.environ.get("SEASON")
    filter_league = os.environ.get("LEAGUE")

    all_files = azure_blob.list_prefix(container, matches_prefix)

    # Filtrera fram bara match-filer (inte players, manifest eller mappar med datum)
    match_files = [
        f for f in all_files
        if f.endswith(".json")
        and "/players/" not in f
        and not f.endswith("manifest.json")
        and not any(is_date_folder(part) for part in f.split("/"))
    ]

    if filter_season:
        match_files = [f for f in match_files if f.split("/")[1] == filter_season]
    if filter_league:
        match_files = [f for f in match_files if f.split("/")[2] == filter_league]

    total = len(match_files)
    if total == 0:
        print("[build_matches_events_flat:live] ⚠️ No match files found with given filters")
        return

    # Samla rader per (season, league_id)
    matches_by_group = defaultdict(list)
    events_by_group = defaultdict(list)

    for i, path in enumerate(match_files, start=1):
        parts = path.split("/")
        if len(parts) < 4:
            continue

        season = parts[1]
        league_id = parts[2]

        try:
            match = load_json_from_blob(container, path)
        except Exception as e:
            if i % 100 == 0 or i == total:
                print(f"[build_matches_events_flat:live] ⚠️ Skipping {path}: {e} ({i}/{total})")
            continue

        if i % 100 == 0 or i == total:
            print(f"[build_matches_events_flat:live] Processing {i}/{total} → {path}")

        # --- Matchnivå ---
        matches_by_group[(season, league_id)].append({
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

            events_by_group[(season, league_id)].append(row)

    # --- Skriv en parquet per grupp ---
    for (season, league_id), rows in matches_by_group.items():
        df_matches = pd.DataFrame(rows)
        df_events = pd.DataFrame(events_by_group[(season, league_id)])

        path_matches = f"warehouse/live/matches_flat/{season}/{league_id}.parquet"
        path_events = f"warehouse/live/events_flat/{season}/{league_id}.parquet"

        parquet_matches = df_matches.to_parquet(index=False, engine="pyarrow")
        parquet_events = df_events.to_parquet(index=False, engine="pyarrow")

        azure_blob.put_bytes(
            container=container,
            blob_path=path_matches,
            data=parquet_matches,
            content_type="application/octet-stream"
        )

        azure_blob.put_bytes(
            container=container,
            blob_path=path_events,
            data=parquet_events,
            content_type="application/octet-stream"
        )

        print(f"[build_matches_events_flat:live] ✅ Uploaded {len(df_matches)} matches, {len(df_events)} events → {season}/{league_id}")


if __name__ == "__main__":
    main()
