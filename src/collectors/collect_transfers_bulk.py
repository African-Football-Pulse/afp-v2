import os
import argparse
import requests
import yaml
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/transfers/"
AUTH_TOKEN = os.getenv("SOCCERDATA_AUTH_TOKEN")

def load_leagues_from_config():
    """Load leagues from config/leagues.yaml"""
    path = "config/leagues.yaml"
    try:
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        return [lid for lid, active in cfg.get("leagues", {}).items() if active]
    except Exception as e:
        print(f"[collect_transfers_bulk] ⚠️ Could not load leagues from {path}: {e}")
        return []

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
    parser.add_argument("--season", required=True, help="Season, e.g. 2025-2026")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of teams to process")
    args = parser.parse_args()

    leagues = load_leagues_from_config()
    total = 0

    for league_id in leagues:
        print(f"[collect_transfers_bulk] league_id={league_id}, season={args.season}")
        exported = collect_transfers(CONTAINER, league_id, args.season)
        print(f"[collect_transfers_bulk] Exported {exported} teams for league {league_id}, season {args.season}")
        total += exported

        if args.limit and total >= args.limit:
            print("[collect_transfers_bulk] Limit reached, stopping.")
            break

    print(f"[collect_transfers_bulk] DONE. Total teams processed: {total}")

if __name__ == "__main__":
    main()
