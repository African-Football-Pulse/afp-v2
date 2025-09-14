import argparse
import os
import subprocess
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

if not CONTAINER or not CONTAINER.strip():
    raise RuntimeError("AZURE_STORAGE_CONTAINER is missing or empty")


def load_history(player_id: str):
    """Load player history from master file"""
    path = "players/africa/players_africa_history.json"
    history_all = azure_blob.get_json(CONTAINER, path)
    return history_all.get(player_id, {}).get("history", [])


def run_collect_stats(player_id: str, league_id: str, season: str):
    """Run collect_player_stats.py for given player/league/season"""
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


def finalize_player(player_id: str, history: list):
    """Copy per-season stats into player folder and build totals.json"""
    totals = {
        "player_id": player_id,
        "apps": 0,
        "goals": 0,
        "penalty_goals": 0,
        "assists": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "substitutions_in": 0,
        "substitutions_out": 0,
        "seasons": []
    }

    for entry in history:
        season = entry["season"]
        league_id = entry["league_id"]
        src_path = f"stats/{season}/{league_id}/players/{player_id}.json"
        dst_path = f"stats/players/{player_id}/{season}.json"

        if azure_blob.exists(CONTAINER, src_path):
            stats = azure_blob.get_json(CONTAINER, src_path)
            azure_blob.upload_json(CONTAINER, dst_path, stats)
            print(f"[build_player_stats] Copied {src_path} → {dst_path}")

            totals["apps"] += stats.get("apps", 0)
            totals["goals"] += stats.get("goals", 0)
            totals["penalty_goals"] += stats.get("penalty_goals", 0)
            totals["assists"] += stats.get("assists", 0)
            totals["yellow_cards"] += stats.get("yellow_cards", 0)
            totals["red_cards"] += stats.get("red_cards", 0)
            totals["substitutions_in"] += stats.get("substitutions_in", 0)
            totals["substitutions_out"] += stats.get("substitutions_out", 0)
            totals["seasons"].append(season)
        else:
            print(f"[build_player_stats] Missing stats file: {src_path}")

    totals_path = f"stats/players/{player_id}/totals.json"
    azure_blob.upload_json(CONTAINER, totals_path, totals)
    print(f"[build_player_stats] Uploaded totals → {totals_path}")


def build_player(player_id: str):
    history = load_history(player_id)
    if not history:
        print(f"[build_player_stats] No history found for {player_id}")
        return

    # 1. Kör collect_player_stats för varje säsong i historiken
    for entry in history:
        run_collect_stats(player_id, entry["league_id"], entry["season"])

    # 2. Kopiera till spelarmapp och bygg totals
    finalize_player(player_id, history)
    print(f"✔ Finished building stats for {player_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player", required=True, help="Player ID to build stats for")
    args = parser.parse_args()
    build_player(args.player)


if __name__ == "__main__":
    main()
