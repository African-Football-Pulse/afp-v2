# src/collectors/collect_stats.py

import os
import json
import argparse
import yaml
from datetime import datetime
from soccerdata import FBref
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


def collect_stats(league: str, season: str, smoke: bool = False):
    fbref = FBref(leagues=[league], seasons=[season])
    print(f"[collect_stats] Fetching data for {league} {season}...")

    try:
        matches = fbref.read_matchlist(league=league, season=season)
        if smoke:
            matches = matches.head(5)
        print(f"[collect_stats] Retrieved {len(matches)} matches")
    except Exception as e:
        print(f"[collect_stats] ERROR fetching data: {e}")
        return False

    date = datetime.utcnow().strftime("%Y-%m-%d")
    payload = {
        "league": league,
        "season": season,
        "date": date,
        "count": len(matches),
        "matches": matches.to_dict(orient="records"),
    }

    out_dir = f"outputs/stats/{date}/{league}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "manifest.json")

    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"[collect_stats] Saved locally: {out_file}")

    blob_path = f"stats/{date}/{league}/manifest.json"
    upload_to_azure(out_file, blob_path)

    return True


def run_from_config(config_path: str, smoke: bool = False):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    for entry in leagues:
        if not entry.get("enabled", False):
            continue
        league = entry["code"]
        season = entry.get("season", "2024-2025")
        collect_stats(league, season, smoke)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, help="League code (e.g. ENG-Premier League)")
    parser.add_argument("--season", type=str, default="2024-2025")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--config", type=str, help="Path to YAML config (overrides league/season)")

    args = parser.parse_args()

    if args.config:
        run_from_config(args.config, args.smoke)
    elif args.league:
        collect_stats(args.league, args.season, args.smoke)
    else:
        print("[collect_stats] ERROR: must specify --league or --config")
