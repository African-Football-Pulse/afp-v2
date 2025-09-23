# src/tools/get_latest_date.py
import argparse
import json
import yaml
from pathlib import Path
from src.collectors.utils import get_latest_finished_date
from src.storage import azure_blob

def get_league_config(league_id: int, config_path: str = "config/leagues.yaml") -> dict | None:
    with open(config_path, "r", encoding="utf-8") as f:
        leagues = yaml.safe_load(f)
    for league in leagues:
        if int(league["id"]) == int(league_id):
            return league
    return None

def get_latest_match_date_for_league(league_id: int, container: str, config_path: str = "config/leagues.yaml") -> str | None:
    league = get_league_config(league_id, config_path)
    if not league:
        raise ValueError(f"League ID {league_id} not found in {config_path}")

    season = league["season"]
    blob_path = f"stats/{season}/{league_id}/manifest.json"

    if not azure_blob.exists(container, blob_path):
        return None

    manifest = azure_blob.get_json(container, blob_path)
    return get_latest_finished_date(manifest)

def main():
    parser = argparse.ArgumentParser(description="Get latest finished match date for a league.")
    parser.add_argument("league_id", type=int, help="League ID (e.g. 228 for Premier League)")
    parser.add_argument("--container", type=str, default="afp", help="Azure container name")
    parser.add_argument("--config", type=str, default="config/leagues.yaml", help="Path to leagues.yaml")
    args = parser.parse_args()

    date = get_latest_match_date_for_league(args.league_id, container=args.container, config_path=args.config)
    if date:
        print(date)
    else:
        print("No finished matches found")

if __name__ == "__main__":
    main()
