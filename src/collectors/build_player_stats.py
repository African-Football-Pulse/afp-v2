import os
import argparse
from src.storage import azure_blob
from src.collectors.collect_player_stats import collect_player_stats

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

    for entry in history:
        league_id = entry["league_id"]
        season = entry["season"]

        print(f"[build_player_stats] Running collect_player_stats for {player_id}, {season}, league {league_id}", flush=True)
        try:
            collect_player_stats(str(player_id), str(league_id), season)
        except Exception as e:
            print(f"[build_player_stats] ❌ Error collecting stats for {player_id}, {league_id}, {season}: {e}", flush=True)

    print(f"[build_player_stats] ✅ Done for player {player_id}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player", required=True, help="Player ID to build stats for")
    args = parser.parse_args()

    build_player(args.player)


if __name__ == "__main__":
    main()
