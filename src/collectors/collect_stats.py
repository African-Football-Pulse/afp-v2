import os
import argparse
import requests
import yaml
from datetime import datetime, timezone

from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/matches/"


def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def collect_stats(league_id: int, season: str = None, date: str = None, smoke: bool = False, mode: str = "weekly"):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    params = {
        "league_id": league_id,
        "auth_token": token
    }
    if season:
        params["season"] = season
    if date and mode == "weekly":
        params["date"] = date

    print(f"[collect_stats] Fetching data for league_id={league_id}, season={season}, date={date}, mode={mode}")

    try:
        resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[collect_stats] ERROR fetching data: {e}")
        return False

    if smoke and isinstance(data, list) and len(data) > 0 and "matches" in data[0]:
        data[0]["matches"] = data[0]["matches"][:5]

    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Sätt path beroende på mode
    if mode == "fullseason":
        if not season:
            raise ValueError("Season must be provided in fullseason mode")
        blob_path = f"stats/{season}/{league_id}/manifest.json"
    else:  # weekly
        date_str = date or today_str()
        blob_path = f"stats/{date_str}/{league_id}/manifest.json"

    azure_blob.upload_json(container, blob_path, data)
    return True


def run_from_config(config_path: str, smoke: bool = False, season: str = None, mode: str = "weekly"):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    for entry in leagues:
        if not entry.get("enabled", False):
            continue
        league_id = entry["id"]
        league_name = entry.get("name", str(league_id))
        print(f"[collect_stats] Running {mode} for {league_name} (id={league_id}, season={season})")
        collect_stats(league_id, season=season, smoke=smoke, mode=mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, help="Numeric league_id from SoccerData API")
    parser.add_argument("--season", type=str, help="Season string, e.g. 2024-2025")
    parser.add_argument("--date", type=str, help="Optional date in format YYYY-MM-DD (weekly only)")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--config", type=str, help="Path to YAML config (overrides CLI)")
    parser.add_argument("--mode", type=str, choices=["weekly", "fullseason"], default="weekly")

    args = parser.parse_args()

    if args.config:
        run_from_config(args.config, args.smoke, args.season, args.mode)
    elif args.league_id:
        collect_stats(args.league_id, args.season, args.date, args.smoke, args.mode)
    else:
        print("[collect_stats] ERROR: must specify --league_id or --config")
