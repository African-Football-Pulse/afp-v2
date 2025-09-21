import os
import argparse
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
HISTORY_PATH = "players/africa/players_africa_history.json"
MASTER_PATH = "players/africa/players_africa_master.json"


def load_json(container: str, path: str):
    try:
        return azure_blob.get_json(container, path)
    except Exception as e:
        print(f"[collect_teams_bulk] ⚠️ Could not load {path}: {e}", flush=True)
        return {}


def build_master_lookup(container: str):
    """Bygg en lookup {player_id: {name, country, club}} från masterfilen"""
    data = load_json(container, MASTER_PATH)
    lookup = {}
    for pid, info in data.items():
        lookup[str(pid)] = {
            "name": info.get("name"),
            "country": info.get("country"),
            "club": info.get("club")
        }
    return lookup


def collect_teams(container: str, season: str):
    master_lookup = build_master_lookup(container)
    history_all = load_json(container, HISTORY_PATH)

    teams_by_league = {}
    processed = 0

    for pid, pdata in history_all.items():
        for entry in pdata.get("history", []):
            if entry["season"] != season:
                continue

            league_id = entry["league_id"]
            club_id = entry.get("club_id")
            club_name = entry.get("club_name")
            if not club_id:
                continue

            teams_by_league.setdefault(league_id, {})
            league_teams = teams_by_league[league_id]

            league_teams.setdefault(club_id, {"club_name": club_name, "players": []})

            info = master_lookup.get(pid, {"name": "Unknown", "country": None, "club": None})

            league_teams[club_id]["players"].append({
                "id": pid,
                "name": info["name"],
                "country": info["country"],
                "club": info["club"]
            })
            processed += 1

    total_teams = 0
    for league_id, teams in teams_by_league.items():
        out_path = f"meta/{season}/teams_{league_id}.json"
        azure_blob.upload_json(container, out_path, teams)
        print(f"[collect_teams_bulk] Uploaded → {out_path} ({len(teams)} teams)", flush=True)
        total_teams += len(teams)

    print(f"[collect_teams_bulk] Processed players with history: {processed}", flush=True)
    return total_teams


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season, e.g. 2024-2025")
    args = parser.parse_args()

    print(f"[collect_teams_bulk] Starting team grouping for season {args.season}", flush=True)
    total = collect_teams(CONTAINER, args.season)
    print(f"[collect_teams_bulk] DONE. Total teams with African players this season: {total}", flush=True)


if __name__ == "__main__":
    main()
