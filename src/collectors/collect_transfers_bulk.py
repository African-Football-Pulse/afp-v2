import os
import json
import argparse
import requests
from collections import defaultdict
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_BASE = "https://api.soccerdataapi.com"

def fetch_transfers(team_id, token):
    url = f"{API_BASE}/transfers/"
    params = {"team_id": team_id, "auth_token": token}
    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def collect_transfers_for_league(container, league_id, season, token, limit=None):
    manifest_path = f"meta/{season}/league_{league_id}.json"
    manifest = azure_blob.get_json(container, manifest_path)

    if not manifest or "stage" not in manifest[0]:
        print(f"[collect_transfers_bulk] ⚠️ No manifest found for league {league_id}, season {season}")
        return 0

    stage = manifest[0]["stage"][0]
    teams = set()
    for m in stage.get("matches", []):
        for side in ["home", "away"]:
            tid = m["teams"][side]["id"]
            teams.add(tid)

    teams = list(teams)
    if limit:
        teams = teams[:limit]

    league_dir = f"transfers/{season}/{league_id}"
    collected = 0
    manifest_out = []

    for tid in teams:
        try:
            data = fetch_transfers(tid, token)
            out_path = f"{league_dir}/{tid}.json"
            azure_blob.upload_json(container, out_path, data)
            manifest_out.append({"team_id": tid, "file": out_path})
            collected += 1
            print(f"[collect_transfers_bulk] Uploaded → {out_path}")
        except Exception as e:
            print(f"[collect_transfers_bulk] ⚠️ Could not fetch transfers for team {tid}: {e}")

    # skriv manifest för ligan
    out_manifest_path = f"{league_dir}/manifest.json"
    azure_blob.upload_json(container, out_manifest_path, manifest_out)
    print(f"[collect_transfers_bulk] Uploaded → {out_manifest_path}")

    return collected

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season to process, e.g. 2024-2025")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of teams (for testing)")
    args = parser.parse_args()

    token = os.getenv("SOCCERDATA_API_KEY")
    if not token:
        raise RuntimeError("[collect_transfers_bulk] Missing SOCCERDATA_API_KEY")

    leagues = [
        228, 229, 230, 310, 326, 198, 235, 241, 253, 268, 297
    ]  # samma som övriga collectors

    print(f"[collect_transfers_bulk] Starting transfer collection for season {args.season}")
    total = 0
    for league_id in leagues:
        count = collect_transfers_for_league(CONTAINER, league_id, args.season, token, args.limit)
        print(f"[collect_transfers_bulk] league_id={league_id}, collected={count}")
        total += count

    print(f"[collect_transfers_bulk] DONE. Total transfers collected: {total}")

if __name__ == "__main__":
    main()
