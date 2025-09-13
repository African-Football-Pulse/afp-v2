# src/collectors/collect_teams.py

import argparse
import requests
from src.storage import azure_blob

API_URL = "https://api.soccerdataapi.com/team/"
CONTAINER = "afp"

def fetch_team(team_id, token):
    params = {"team_id": team_id, "auth_token": token}
    headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
    resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def save_team(league_id, team_id, data):
    path = f"teams/{league_id}/{team_id}.json"
    azure_blob.upload_json(CONTAINER, path, data)

def save_manifest(league_id, team_ids):
    path = f"teams/{league_id}/manifest.json"
    azure_blob.upload_json(CONTAINER, path, {"teams": sorted(team_ids)})

def collect_teams_for_league(league_id: int, season: str, token: str):
    # Läs match-manifest från Azure
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    match_manifest = azure_blob.get_json(CONTAINER, manifest_path)

    # Extrahera unika team_id
    team_ids = set()
    for m in match_manifest.get("matches", []):
        if "home_team" in m and "id" in m["home_team"]:
            team_ids.add(m["home_team"]["id"])
        if "away_team" in m and "id" in m["away_team"]:
            team_ids.add(m["away_team"]["id"])

    # Hämta & spara varje team
    for tid in sorted(team_ids):
        try:
            team_data = fetch_team(tid, token)
            save_team(league_id, tid, team_data)
            print(f"✅ Saved team {tid} for league {league_id}")
        except Exception as e:
            print(f"⚠️ Failed to fetch/save team {tid}: {e}")

    # Spara manifest
    save_manifest(league_id, team_ids)
    print(f"League {league_id}: {len(team_ids)} teams saved")

def main():
    parser = argparse.ArgumentParser(description="Collect teams for a league/season")
    parser.add_argument("--league_id", type=int, required=True, help="League ID (e.g. 228 for Premier League)")
    parser.add_argument("--season", type=str, required=True, help="Season string (e.g. 2024-2025)")
    parser.add_argument("--token", type=str, required=True, help="Auth token for SoccerData API")
    args = parser.parse_args()

    collect_teams_for_league(args.league_id, args.season, args.token)

if __name__ == "__main__":
    main()
