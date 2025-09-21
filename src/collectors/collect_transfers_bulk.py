# src/collectors/collect_transfers_bulk.py

import os
import sys
import requests
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_KEY = os.getenv("SOCCERDATA_AUTH_KEY")

API_URL = "https://api.soccerdataapi.com/transfers/"

def fetch_transfers(team_id):
    """Fetch transfers for a given team from the SoccerData API"""
    params = {
        "team_id": team_id,
        "auth_token": API_KEY,
    }
    headers = {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }
    resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def collect_transfers(container, leagues):
    total = 0
    for league_id, season, teams in leagues:
        print(f"[collect_transfers_bulk] league_id={league_id}, season={season}")
        for tid in teams:
            try:
                data = fetch_transfers(tid)
                out_path = f"transfers/teams/{tid}.json"
                azure_blob.upload_json(container, out_path, data)
                print(f"[collect_transfers_bulk] Uploaded → {out_path}")
                total += 1
            except Exception as e:
                print(f"[collect_transfers_bulk] ⚠️ Could not fetch team {tid}: {e}")
    return total

def main():
    if not API_KEY:
        raise RuntimeError("[collect_transfers_bulk] Missing SOCCERDATA_AUTH_KEY")

    print("[collect_transfers_bulk] Starting transfer collection...")

    # Här kan vi hämta team-IDs från meta/teams_{league_id}.json (som byggts i steg 10)
    # och loopa igenom alla lag
    container = CONTAINER
    leagues = []

    # Läs in teams-filer
    prefix = "meta/2025-2026/"
    team_files = azure_blob.list_blobs(container, prefix)
    for path in team_files:
        if path.endswith(".json") and "teams_" in path:
            league_id = path.split("_")[-1].replace(".json", "")
            data = azure_blob.get_json(container, path)
            season = "2025-2026"
            teams = [int(t["id"]) for t in data] if isinstance(data, list) else []
            leagues.append((league_id, season, teams))

    total = collect_transfers(container, leagues)
    print(f"[collect_transfers_bulk] DONE. Total teams processed: {total}")

if __name__ == "__main__":
    main()
