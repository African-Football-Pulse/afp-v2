# src/collectors/collect_transfers_bulk.py
import os
import requests
import yaml
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/transfers/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")

CONFIG_PATH = "config/leagues.yaml"

def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return data.get("leagues", [])

def fetch_transfers_for_team(team_id):
    params = {"team_id": team_id, "auth_token": AUTH_KEY}
    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}
    resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def collect_transfers(container):
    leagues = load_leagues()
    total = 0

    for league in leagues:
        if not league.get("enabled", False) or league.get("is_cup", False):
            continue
        league_id = league["id"]
        teams_path = f"meta/2025-2026/teams_{league_id}.json"  # använder senaste laglistorna

        try:
            teams = azure_blob.get_json(container, teams_path)
        except Exception:
            print(f"[collect_transfers_bulk] ⚠️ Missing teams file: {teams_path}")
            continue

        league_count = 0
        for tid, _ in teams.items():
            try:
                data = fetch_transfers_for_team(tid)
                out_path = f"transfers/teams/team_{tid}.json"
                azure_blob.upload_json(container, out_path, data)
                league_count += 1
            except Exception as e:
                print(f"[collect_transfers_bulk] ⚠️ Could not fetch transfers for team {tid}: {e}")

        print(f"[collect_transfers_bulk] Uploaded {league_count} teams for league {league_id}")
        total += league_count

    return total

def main():
    if not AUTH_KEY:
        raise RuntimeError("[collect_transfers_bulk] Missing SOCCERDATA_AUTH_KEY")

    print("[collect_transfers_bulk] Starting transfer collection...")
    total = collect_transfers(CONTAINER)
    print(f"[collect_transfers_bulk] DONE. Total teams processed: {total}")

if __name__ == "__main__":
    main()
