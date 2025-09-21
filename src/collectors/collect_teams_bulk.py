import os
import argparse
from src.storage import azure_blob
import json

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
HISTORY_PATH = "players/africa/players_africa_history.json"
MASTER_PATH = "players/africa/players_africa_master.json"


def load_json(container: str, path: str):
    try:
        return azure_blob.get_json(container, path)
    except Exception as e:
        print(f"[collect_teams_bulk] ⚠️ Could not load {path}: {e}", flush=True)
        return {}


def collect_teams(container: str, season: str):
    # Läs master och history
    master = load_json(container, MASTER_PATH)
    history_all = load_json(container, HISTORY_PATH)

    teams_by_league = {}

    for pid, pdata in history_all.items():
        player = master.get(pid, {"id": pid, "name": "Unknown"})
        for entry in pdata.get("history", []):
            if entry["season"] != season:
                continue

            league_id = entry["league_id"]
            team_id = entry.get("team_id")
            if not team_id:
                continue

            teams_by_league.setdefault(league_id, {})
            league_teams = teams_by_league[league_id]

            league_teams.setdefault(team_id, {"players": []})
            league_teams[team_id]["players"].append({
                "id": pid,
                "name": player.get("name"),
                "country": player.get("country")
            })

    # Ladda upp en fil per liga
    total_teams = 0
    for league_id, teams in teams_by_league.items():
        out_path = f"meta/{season}/teams_{league_id}.json"
        azure_blob.upload_json(container, out_path, teams)
        print(f"[collect_teams_bulk] Uploaded → {out_path} ({len(teams)} teams)", flush=True)
        total_teams += len(teams)

    return total_teams


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season, e.g. 2024-2025")
    args = parser.parse_args()

    container = CONTAINER
    season = args.season

    print(f"[collect_teams_bulk] Starting team grouping for season {season}", flush=True)

    total = collect_teams(container, season)

    print(f"[collect_teams_bulk] DONE. Total teams with African players this season: {total}", flush=True)


if __name__ == "__main__":
    main()
