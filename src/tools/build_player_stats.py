import argparse
import os
import subprocess
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def load_history(player_id: str):
    path = "players/africa/players_africa_history.json"
    history_all = azure_blob.get_json(CONTAINER, path)
    return history_all.get(player_id, {}).get("history", [])

def run_collect_stats(player_id: str, league_id: str, season: str):
    print(f"[build_player_stats] Running collect_player_stats for {player_id}, {season}, league {league_id}")
    subprocess.run(
        [
            "python", "-m", "src.tools.collect_player_stats",
            "--player", player_id,
            "--league", league_id,
            "--season", season
        ],
        check=True
    )

def build_player(player_id: str):
    history = load_history(player_id)
    if not history:
        print(f"[build_player_stats] No history for player {player_id}")
        return

    for entry in history:
        run_collect_stats(player_id, entry["league_id"], entry["season"])

    print(f"✔ Finished building stats for {player_id}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player", required=True, help="Player ID to build stats for")
    args = parser.parse_args()
    build_player(args.player)

if __name__ == "__main__":
    main()
