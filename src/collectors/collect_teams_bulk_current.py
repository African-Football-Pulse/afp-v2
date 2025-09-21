import os
import argparse
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
MASTER_PATH = "players/africa/players_africa_master.json"


def load_json(container: str, path: str):
    try:
        return azure_blob.get_json(container, path)
    except Exception as e:
        print(f"[collect_teams_bulk_current] ⚠️ Could not load {path}: {e}", flush=True)
        return {}


def collect_current_teams(container: str, season: str):
    data = load_json(container, MASTER_PATH)
    players = data.get("players", [])

    teams = {}
    processed = 0

    for player in players:
        pid = str(player.get("id"))
        club_name = player.get("club")
        if not club_name:
            continue

        teams.setdefault(club_name, {"club_name": club_name, "players": []})
        teams[club_name]["players"].append({
            "id": pid,
            "name": player.get("name"),
            "country": player.get("country")
        })
        processed += 1

    out_path = f"meta/{season}/teams_current.json"
    azure_blob.upload_json(container, out_path, teams)
    print(f"[collect_teams_bulk_current] Uploaded → {out_path} ({len(teams)} teams)", flush=True)
    print(f"[collect_teams_bulk_current] Processed players: {processed}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season, e.g. 2025-2026")
    args = parser.parse_args()

    print(f"[collect_teams_bulk_current] Starting current season team grouping for {args.season}", flush=True)
    collect_current_teams(CONTAINER, args.season)
    print(f"[collect_teams_bulk_current] DONE", flush=True)


if __name__ == "__main__":
    main()
