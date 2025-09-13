import os
import json
import requests
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/team/"

def collect_teams(league_id: int, season: str):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs match-manifest (kan vara lista eller dict med "matches")
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    print(f"[collect_teams] Loading matches from {manifest_path} ...")
    match_manifest = azure_blob.get_json(container, manifest_path)

    if isinstance(match_manifest, dict) and "matches" in match_manifest:
        matches = match_manifest["matches"]
    elif isinstance(match_manifest, list):
        matches = match_manifest
    else:
        raise ValueError(f"Unexpected manifest format: {type(match_manifest)}")

    # Extrahera unika team_id med stöd för flera format
    team_ids = set()
    for m in matches:
        # Nyare format: m["teams"]["home"]["id"], m["teams"]["away"]["id"]
        if "teams" in m:
            if "home" in m["teams"] and "id" in m["teams"]["home"]:
                team_ids.add(m["teams"]["home"]["id"])
            if "away" in m["teams"] and "id" in m["teams"]["away"]:
                team_ids.add(m["teams"]["away"]["id"])

        # Äldre/alternativt format: m["home"]["id"], m["away"]["id"]
        if "home" in m and isinstance(m["home"], dict) and "id" in m["home"]:
            team_ids.add(m["home"]["id"])
        if "away" in m and isinstance(m["away"], dict) and "id" in m["away"]:
            team_ids.add(m["away"]["id"])

    print(f"[collect_teams] Found {len(team_ids)} unique teams for league {league_id}, season {season}")

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


if __name__ == "__main__":
    # Exempel: Premier League 2024-2025
    collect_teams(228, "2024-2025")
