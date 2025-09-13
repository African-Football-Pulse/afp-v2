# src/collectors/collect_stats.py

import os
import json
import argparse
import requests
import yaml
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.common.blob_io import get_container_client

TZ = ZoneInfo("Europe/Stockholm")
BASE_URL = "https://api.soccerdataapi.com/matches/"


def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()


def upload_json(container_client, path: str, obj):
    if container_client is None:
        raise RuntimeError("BLOB_CONTAINER_SAS_URL must be set – Collector requires Azure Blob access.")
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    container_client.upload_blob(name=path, data=data, overwrite=True)
    print(f"[collect_stats] Uploaded to Azure: {path}")


def collect_stats(league_id: int, season: str = None, date: str = None, smoke: bool = False):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    params = {
        "league_id": league_id,
        "auth_token": token
    }
    if season:
        params["season"] = season
    if date:
        params["date"] = date

    print(f"[collect_stats] Fetching data for league_id={league_id}, season={season}, date={date}")

    try:
        resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[collect_stats] ERROR fetching data: {e}")
        return False

    if smoke and isinstance(data, list) and len(data) > 0 and "matches" in data[0]:
        data[0]["matches"] = data[0]["matches"][:5]

    date_str = today_str()
    container_client = get_container_client()

    blob_path = f"stats/{date_str}/{league_id}/manifest.json"
    upload_json(container_client, blob_path, data)

    return True


def run_from_config(config_path: str, smoke: bool = False):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    for entry in leagues:
        if not entry.get("enabled", False):
            continue
        league_id = entry["id"]
        season = entry.get("season")
        collect_stats(league_id, season, smoke=smoke)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, help="Numeric league_id from SoccerData API")
    parser.add_argument("--season", type=str, help="Optional season string, e.g. 2024-2025")
    parser.add_argument("--date", type=str, help="Optional date in format YYYY-MM-DD")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--config", type=str, help="Path to YAML config (overrides CLI)")

    args = parser.parse_args()

    if args.config:
        run_from_config(args.config, args.smoke)
    elif args.league_id:
        collect_stats(args.league_id, args.season, args.date, args.smoke)
    else:
        print("[collect_stats] ERROR: must specify --league_id or --config")
