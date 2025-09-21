# src/collectors/collect_transfers_bulk.py
import os
import json
import requests
from src.storage import azure_blob
from src.utils.config_loader import load_leagues

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/transfers/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")

def fetch_transfers_for_team(team_id):
    params = {"team_id": team_id, "auth_token": AUTH_KEY}
    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}
    resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def collect_transfers(container, season):
    leagues = load_leagues()
    total = 0

    for league in leagues:
        if not league.get("enabled", False) or league.get("is_cup", False):
            continue
        league_id = league["id"]
        teams_path = f"meta/{season}/teams_{league_id}.json"

        try:
            teams = azure_blob.get_json(container, teams_path)
        except Exception:
            print(f"[collect_transfers_bulk] ⚠️ Missing teams file: {teams_path}")
            continue

        league_count = 0
        for tid, tinfo in teams.items():
            try:
                data = fetch_transfers_for_team(tid)
                out_path = f"transfers/{season}/{league_id}/team_{tid}.json"
                azure_blob.upload_json(container, out_path, data)
                league_count += 1
            except Exception as e:
                print(f"[collect_transfers_bulk] ⚠️ Could not fetch transfers for team {tid}: {e}")

        print(f"[collect_transfers_bulk] Uploaded {league_count} teams for league {league_id}, season {season}")
        total += league_count

    return total

def main():
    season = os.getenv("SEASON")
    if not season:
        raise RuntimeError("[collect_transfers_bulk] Missing SEASON env var")

    print("[collect_transfers_bulk] Starting transfer collection...")
    total = collect_transfers(CONTAINER, season)
    print(f"[collect_transfers_bulk] DONE. Total teams processed: {total}")

if __name__ == "__main__":
    main()
