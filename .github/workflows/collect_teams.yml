import os
import json
import requests
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/team/"

def collect_teams_from_matches(league_id: int, season: str):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Lista alla matchfiler
    prefix = f"stats/{season}/{league_id}/"
    blobs = azure_blob.list_prefix(container, prefix)
    match_files = [b for b in blobs if b.endswith(".json") and "manifest" not in b]

    print(f"[collect_teams] Found {len(match_files)} match files in {prefix}")

    team_ids = set()
    for blob_path in match_files:
        match = azure_blob.get_json(container, blob_path)
        if "teams" in match:
            home = match["teams"].get("home", {})
            away = match["teams"].get("away", {})
            if "id" in home: team_ids.add(home["id"])
            if "id" in away: team_ids.add(away["id"])

    print(f"[collect_teams] Extracted {len(team_ids)} unique teams for league {league_id}, season {season}")

    # Hämta & spara varje team
    for tid in sorted(team_ids):
        params = {"team_id": tid, "auth_token": token}
        headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            team_data = resp.json()

            team_path = f"teams/{league_id}/{tid}.json"
            azure_blob.put_text(container, team_path, json.dumps(team_data, indent=2, ensure_ascii=False))
            print(f"[collect_teams] Uploaded team {tid} → {team_path}")
        except Exception as e:
            print(f"[collect_teams] ⚠️ Failed to fetch/save team {tid}: {e}")

# Wrapper för bakåtkompatibilitet
def collect_teams(league_id: int, season: str):
    return collect_teams_from_matches(league_id, season)

if __name__ == "__main__":
    # Exempel: Premier League 2024-2025
    collect_teams(228, "2024-2025")
