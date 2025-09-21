import os
import argparse
import requests
import json
import yaml
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
API_BASE = "https://api.soccerdataapi.com"
AUTH_KEY = os.environ.get("SOCCERDATA_AUTH_KEY")


def load_leagues():
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return [l for l in cfg.get("leagues", []) if l.get("enabled", False)]


def load_matches(manifest):
    """
    Samma logik som i 05_collect_player_history_bulk:
    Hanterar både liga- och cupformat.
    """
    matches = []
    if isinstance(manifest, dict):
        if "results" in manifest:  # dict med results
            results = manifest["results"]
            if isinstance(results, dict) and "stage" in results:
                for stage in results["stage"]:
                    matches.extend(stage.get("matches", []))
            elif isinstance(results, list):  # cupformat
                for league_data in results:
                    for stage in league_data.get("stage", []):
                        matches.extend(stage.get("matches", []))
    elif isinstance(manifest, list):  # ibland är hela manifestet en lista
        for league_data in manifest:
            for stage in league_data.get("stage", []):
                matches.extend(stage.get("matches", []))
    return matches


def fetch_team_info(team_id: int):
    """Hämta lagdetaljer från SoccerData API"""
    url = f"{API_BASE}/team/{team_id}"
    headers = {"Authorization": f"Bearer {AUTH_KEY}"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def collect_team_info(container: str, league_id: int, season: str):
    """Läs manifest, samla team_ids, hämta info från API och ladda upp."""
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        manifest = azure_blob.get_json(container, manifest_path)
    except Exception:
        print(f"[collect_team_info_bulk] ⚠️ No manifest found for league {league_id}, season {season}", flush=True)
        return 0

    matches = load_matches(manifest)
    if not matches:
        print(f"[collect_team_info_bulk] ⚠️ No matches found in manifest for league {league_id}", flush=True)
        return 0

    # samla team_ids
    team_ids = set()
    for m in matches:
        teams = m.get("teams", {})
        if "home" in teams:
            team_ids.add(teams["home"]["id"])
        if "away" in teams:
            team_ids.add(teams["away"]["id"])

    if not team_ids:
        print(f"[collect_team_info_bulk] ⚠️ No teams extracted for league {league_id}", flush=True)
        return 0

    out_prefix = f"teams/{league_id}/"
    processed = 0

    for tid in team_ids:
        try:
            data = fetch_team_info(tid)
            azure_blob.upload_json(container, f"{out_prefix}{tid}.json", data)
            processed += 1
        except Exception as e:
            print(f"[collect_team_info_bulk] ⚠️ Could not fetch team {tid}: {e}", flush=True)

    # skriv manifest (listan med ids)
    azure_blob.upload_json(container, f"{out_prefix}manifest.json", list(team_ids))

    print(f"[collect_team_info_bulk] Uploaded {processed} teams for league {league_id}", flush=True)
    return processed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help="Season to process (e.g. 2024-2025)")
    args = parser.parse_args()

    leagues = load_leagues()
    container = CONTAINER
    total = 0

    print("[collect_team_info_bulk] Starting team info collection...", flush=True)

    for league in leagues:
        n = collect_team_info(container, league["id"], args.season)
        total += n

    print(f"[collect_team_info_bulk] DONE. Total teams processed: {total}", flush=True)


if __name__ == "__main__":
    main()
