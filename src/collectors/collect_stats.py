import os
import argparse
import requests
from datetime import datetime, timezone
from src.collectors import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/matches/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def run(league_id: int, mode: str = "weekly", date: str = None, season: str = None):
    """
    Hämtar matcher från SoccerData API och sparar manifest i Azure.
    """
    if not AUTH_KEY:
        raise RuntimeError("Missing SOCCERDATA_AUTH_KEY in environment")

    params = {"league_id": league_id, "auth_token": AUTH_KEY}
    if mode == "weekly":
        params["date"] = date or today_str()
    elif mode == "fullseason" and season:
        params["season"] = season

    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}

    print(f"[collect_stats] Requesting matches for league={league_id}, mode={mode}, params={params}")
    resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if isinstance(data, dict):
        data = [data]

    matches = []
    for league in data:
        matches.extend(league.get("matches", []))

    if mode == "fullseason":
        if not season:
            raise ValueError("Season must be provided in fullseason mode")
        blob_path = f"stats/{season}/{league_id}/manifest.json"
    else:
        date_str = date or today_str()
        blob_path = f"stats/weekly/{date_str}/{league_id}/manifest.json"

    manifest = {
        "league_id": league_id,
        "mode": mode,
        "date": date or today_str(),
        "matches": matches
    }

    utils.upload_json_debug(blob_path, manifest)
    print(f"[collect_stats] ✅ Uploaded manifest with {len(matches)} matches → {blob_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True)
    parser.add_argument("--mode", choices=["weekly", "fullseason"], default="weekly")
    parser.add_argument("--date", type=str, required=False)
    parser.add_argument("--season", type=str, required=False)
    args = parser.parse_args()

    run(args.league_id, mode=args.mode, date=args.date, season=args.season)
