import os
import argparse
import requests
from src.storage import azure_blob
from src.utils.league_loader import load_leagues
from src.utils.season_loader import get_active_season

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/transfers/"
AUTH_TOKEN = os.getenv("SOCCERDATA_AUTH_TOKEN")

def fetch_transfers(team_id):
    params = {"team_id": team_id, "auth_token": AUTH_TOKEN}
    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}
    r = requests.get(API_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def collect_transfers(container, league_id, season):
    manifest_path = f"teams/{league_id}/manifest.json"
    teams_manifest = azure_blob.get_json(container, manifest_path)
    if not teams_manifest:
        print(f"[collect_transfers_bulk] ⚠️ No team manifest for league {league_id}")
        return 0

    exported = 0
    all_entries = []

    for team in teams_manifest.get("teams", []):
        team_id = team.get("id")
        try:
            transfers = fetch_transfers(team_id)
            if transfers:
                out_path = f"transfers/{season}/{league_id}/{team_id}.json"
                azure_blob.upload_json(container, out_path, transfers)
                print(f"[collect_transfers_bulk] Uploaded transfers → {out_path}")
                all_entries.append({"team_id": team_id, "file": out_path})
                exported += 1
        except Exception as e:
            print(f"[collect_transfers_bulk] ⚠️ Could not fetch transfers for team {team_id}: {e}")

    # manifest för ligan
    manifest_out = {
        "league_id": league_id,
        "season": season,
        "teams": all_entries
    }
    out_manifest_path = f"transfers/{season}/{league_id}/manifest.json"
    azure_blob.upload_json(container, out_manifest_path, manifest_out)
    print(f"[collect_transfers_bulk] Uploaded manifest → {out_manifest_path}")
    return exported

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit number of teams to process")
    args = parser.parse_args()

    leagues = load_leagues()
    total = 0

    for league_id in leagues:
        season = get_active_season(CONTAINER, league_id)
        if not season:
            print(f"[collect_transfers_bulk] ⚠️ No active season for league {league_id}")
            continue

        print(f"[collect_transfers_bulk] league_id={league_id}, active season={season}")
        exported = collect_transfers(CONTAINER, league_id, season)
        print(f"[collect_transfers_bulk] Exported {exported} teams for league {league_id}, season {season}")
        total += exported

        if args.limit and total >= args.limit:
            print("[collect_transfers_bulk] Limit reached, stopping.")
            break

    print(f"[collect_transfers_bulk] DONE. Total teams processed: {total}")

if __name__ == "__main__":
    main()
