import os
import argparse
from src.storage import azure_blob

HISTORY_PATH = "players/africa/players_africa_history.json"


def load_history(container: str):
    return azure_blob.get_json(container, HISTORY_PATH)


def build_player(player_id: str):
    container = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
    history_all = load_history(container)

    if player_id not in history_all:
        print(f"[build_player_stats] ❌ Player {player_id} not found in history file", flush=True)
        return

    history = history_all[player_id].get("history", [])
    if not history:
        print(f"[build_player_stats] ⚠️ Player {player_id} has no history entries", flush=True)
        return

    totals = {
        "player_id": str(player_id),
        "apps": 0,
        "goals": 0,
        "penalty_goals": 0,
        "assists": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "substitutions_in": 0,
        "substitutions_out": 0,
    }

    per_season_stats = {}

    for entry in history:
        season = entry["season"]
        league_id = entry["league_id"]
        path = f"stats/{season}/{league_id}/players/{player_id}.json"

        try:
            stats = azure_blob.get_json(container, path)
        except Exception:
            print(f"[build_player_stats] ⚠️ Missing stats file: {path}", flush=True)
            continue

        # summera totals
        for key in totals.keys():
            if key == "player_id":
                continue
            totals[key] += stats.get(key, 0)

        # lagra per-säsong
        if season not in per_season_stats:
            per_season_stats[season] = {
                "apps": 0,
                "goals": 0,
                "penalty_goals": 0,
                "assists": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "substitutions_in": 0,
                "substitutions_out": 0,
            }

        for key in per_season_stats[season].keys():
            per_season_stats[season][key] += stats.get(key, 0)

    # ✅ skriv per-säsong-filer
    for season, season_stats in per_season_stats.items():
        out_path = f"stats/players/{player_id}/{season}.json"
        azure_blob.upload_json(container, out_path, season_stats)
        print(f"[build_player_stats] Uploaded season stats → {out_path}", flush=True)

    # ✅ skriv totals
    out_totals = f"stats/players/{player_id}/totals.json"
    azure_blob.upload_json(container, out_totals, totals)
    print(f"[build_player_stats] Uploaded totals → {out_totals}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player", required=True, help="Player ID to build stats for")
    args = parser.parse_args()

    build_player(args.player)


if __name__ == "__main__":
    main()
