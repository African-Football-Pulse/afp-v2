import os
import argparse
import requests
import json
import yaml
from src.storage import azure_blob

CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER") or "afp"
API_BASE = "https://api.soccerdataapi.com"
AUTH_KEY = os.environ.get("SOCCERDATA_AUTH_KEY")


def load_leagues():
    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return [l for l in cfg.get("leagues", []) if l.get("enabled", False)]


def load_team_ids(container: str, league_id: int):
    """Hämta listan med team_ids från teams/{league_id}/manifest.json (skapad i 09)."""
    path = f"teams/{league_id}/manifest.json"
    try:
        return azure_blob.get_json(container, path)
    except Exception:
        print(f"[collect_team_info_bulk] ⚠️ No team manifest for league {league_id}", flush=True)
        return []


def fetch_team_info(team_id: int):
    """Hämta lagdetaljer från SoccerData API"""
    url = f"{API_BASE}/team/{team_id}"
    headers = {"Authorization": f"Bearer {AUTH_KEY}"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def collect_team_info(container: str, league_id: int):
    team_ids = load_team_ids(container, league_id)
    if not team_ids:
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

    print(f"[collect_team_info_bulk] Uploaded {processed} teams for league {league_id}", flush=True)
    return processed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=False, help="Season, optional (not used for API call)")
    args = parser.parse_args()

    leagues = load_leagues()
    container = CONTAINER
    total = 0

    print("[collect_team_info_bulk] Starting team info collection...", flush=True)

    for league in leagues:
        n = collect_team_info(container, league["id"])
        total += n

    print(f"[collect_team_info_bulk] DONE. Total teams processed: {total}", flush=True)


if __name__ == "__main__":
    main()
