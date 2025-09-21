import os
import json
import argparse
import requests
from src.storage import azure_blob
from src.utils.config_loader import load_leagues

BASE_URL = "https://api.soccerdataapi.com/team/"


def collect_teams_for_league(container: str, league_id: int, season: str, token: str):
    print(f"[collect_teams_bulk] league_id={league_id}, season={season} ...", flush=True)

    # Läs manifest för säsongen
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        match_manifest = azure_blob.get_json(container, manifest_path)
    except Exception:
        print(f"[collect_teams_bulk] ⚠️ No manifest found for league {league_id}, season {season}", flush=True)
        return None

    # Extrahera unika team_id
    team_ids = set()
    matches = match_manifest.get("matches", [])
    for m in matches:
        if "home_team" in m and "id" in m["home_team"]:
            team_ids.add(m["home_team"]["id"])
        if "away_team" in m and "id" in m["away_team"]:
            team_ids.add(m["away_team"]["id"])

    if not team_ids:
        print(f"[collect_teams_bulk] ⚠️ No teams found in manifest {manifest_path}", flush=True)
        return None

    print(f"[collect_teams_bulk] Found {len(team_ids)} teams", flush=True)

    teams_data = {}
    for tid in sorted(team_ids):
        params = {"team_id": tid, "auth_token": token}
        headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            team_data = resp.json()
            teams_data[tid] = team_data
        except Exception as e:
            print(f"[collect_teams_bulk] ⚠️ Failed to fetch team {tid}: {e}", flush=True)

    # Spara som en JSON per liga/säsong
    out_path = f"meta/{season}/teams_{league_id}.json"
    azure_blob.upload_json(container, out_path, teams_data)
    print(f"[collect_teams_bulk] Uploaded teams → {out_path}", flush=True)

    return teams_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season, e.g. 2024-2025")
    args = parser.parse_args()

    container = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    leagues = load_leagues()
    season = args.season

    print(f"[collect_teams_bulk] Starting team collection for season {season}", flush=True)

    total_teams = 0
    for league in leagues:
        if not league.get("enabled", False):
            continue
        league_id = league["id"]
        teams_data = collect_teams_for_league(container, league_id, season, token)
        if teams_data:
            total_teams += len(teams_data)

    print(f"[collect_teams_bulk] DONE. Total teams collected this season: {total_teams}", flush=True)


if __name__ == "__main__":
    main()
