import os
import argparse
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
MASTER_PATH = "players/africa/players_africa_master.json"

def load_json(container: str, path: str):
    try:
        return azure_blob.get_json(container, path)
    except Exception:
        return None

def collect_teams(container: str, season: str):
    master = load_json(container, MASTER_PATH)
    if not master:
        print(f"[collect_teams_bulk] ⚠️ Could not load master file", flush=True)
        return 0

    teams_by_league = {}
    processed = 0

    for pid, pdata in master.items():
        stats_path = f"stats/players/{pid}/{season}.json"
        stats = load_json(container, stats_path)
        if not stats:
            continue

        league_id = stats.get("league_id")
        team_id = stats.get("team_id")
        team_name = stats.get("team_name")

        if not league_id or not team_id:
            continue

        teams_by_league.setdefault(league_id, {})
        league_teams = teams_by_league[league_id]

        league_teams.setdefault(team_id, {"team_name": team_name, "players": []})
        league_teams[team_id]["players"].append({
            "id": pid,
            "name": pdata.get("name"),
            "country": pdata.get("country")
        })
        processed += 1

    # Ladda upp en fil per liga
    total_teams = 0
    for league_id, teams in teams_by_league.items():
        out_path = f"meta/{season}/teams_{league_id}.json"
        azure_blob.upload_json(container, out_path, teams)
        print(f"[collect_teams_bulk] Uploaded → {out_path} ({len(teams)} teams)", flush=True)
        total_teams += len(teams)

    print(f"[collect_teams_bulk] Processed players with stats: {processed}", flush=True)
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
