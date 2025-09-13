import os
import json
import requests
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/team/"

def collect_teams(league_id: int):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Läs säsongsinfo
    seasons_path = f"meta/seasons_{league_id}.json"
    print(f"[collect_teams] Loading seasons from {seasons_path} ...")
    seasons_meta = azure_blob.get_json(container, seasons_path)

    # Hantera olika format (list eller dict med "results")
    if isinstance(seasons_meta, dict) and "results" in seasons_meta:
        seasons = seasons_meta["results"]
    elif isinstance(seasons_meta, list):
        seasons = seasons_meta
    else:
        raise ValueError(f"Unexpected seasons format: {type(seasons_meta)}")

    # Filtrera aktiva säsonger
    active_seasons = [s["season"]["year"] for s in seasons if s.get("season", {}).get("is_active")]
    if not active_seasons:
        print(f"[collect_teams] No active seasons found for league {league_id}")
        return

    for season in active_seasons:
        print(f"[collect_teams] Processing league {league_id}, season {season} ...")

        # Läs match-manifest
        manifest_path = f"stats/{season}/{league_id}/manifest.json"
        match_manifest = azure_blob.get_json(container, manifest_path)

        # Extrahera unika team_id
        team_ids = set()
        for m in match_manifest.get("matches", []):
            if "home_team" in m and "id" in m["home_team"]:
                team_ids.add(m["home_team"]["id"])
            if "away_team" in m and "id" in m["away_team"]:
                team_ids.add(m["away_team"]["id"])

        print(f"[collect_teams] Found {len(team_ids)} unique teams")

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
    # Exempel: Premier League
    collect_teams(228)
