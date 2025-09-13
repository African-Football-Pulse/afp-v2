# src/collectors/collect_stats.py

import os
import json
import argparse
import requests
import yaml
from datetime import datetime
from azure.storage.blob import BlobServiceClient


def upload_to_azure(local_file: str, blob_path: str):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        )
        container = os.environ.get("AZURE_STORAGE_CONTAINER", "producer")
        blob_client = blob_service_client.get_blob_client(container, blob_path)

        with open(local_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"[collect_stats] Uploaded to Azure: {container}/{blob_path}")
    except Exception as e:
        print(f"[collect_stats] ERROR uploading to Azure: {e}")


def collect_stats(league_id: int, season: str = None, date: str = None, smoke: bool = False):
    base_url = "https://api.soccerdataapi.com/matches/"
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
        resp = requests.get(base_url, headers={"Content-Type": "application/json"}, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[collect_stats] ERROR fetching data: {e}")
        return False

    # Begränsa antal matcher i smoke-mode
    if smoke and isinstance(data, list) and len(data) > 0 and "matches" in data[0]:
        data[0]["matches"] = data[0]["matches"][:5]

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = f"outputs/stats/{date_str}/{league_id}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "manifest.json")

    with open(out_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"[collect_stats] Saved locally: {out_file}")

    blob_path = f"stats/{date_str}/{league_id}/manifest.json"
    upload_to_azure(out_file, blob_path)

    return True


def run_from_config(config_path: str, smoke: bool = False):
    with open(config_path, "r") as f:
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
