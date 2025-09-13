import os
import json
import requests
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/team/"

def collect_teams(league_id: int, season: str):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs match-manifest från Azure
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    print(f"[collect_teams] Loading match manifest from {manifest_path} ...")
    match_manifest = azure_blob.get_json(container, manifest_path)

    # Extrahera unika team_id
    team_ids = set()
    for m in match_manifest.get("matches", []):
        if "home_team" in m and "id" in m["home_team"]:
            team_ids.add(m["home_team"]["id"])
        if "away_team" in m and "id" in m["away_team"]:
            team_ids.add(m["away_team"]["id"])

    print(f"[collect_teams] Found {len(team_ids)} unique teams in league {league_id}")

    # Hämta och spara varje team
    for tid in sorted(team_ids):
        params = {"team_id": tid, "auth_token": token}
        headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            team_data = resp.json()

            path = f"teams/{league_id}/{tid}.json"
            azure_blob.put_text(container, path, json.dumps(team_data, indent=2, ensure_ascii=False))
            print(f"[collect_teams] Uploaded team {tid} → {path}")
        except Exception as e:
            print(f"[collect_teams] ⚠️ Failed to fetch/save team {tid}: {e}")

    # Spara manifest
    manifest_out = f"teams/{league_id}/manifest.json"
    azure_blob.put_text(container, manifest_out, json.dumps({"teams": sorted(team_ids)}, indent=2))
    print(f"[collect_teams] Uploaded manifest → {manifest_out}")


if __name__ == "__main__":
    # Exempel: Premier League 2024-2025
    collect_teams(league_id=228, season="2024-2025")
