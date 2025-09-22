import os
import json
import argparse
from datetime import datetime, timezone
from src.collectors import utils


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def run(league_id: int, mode: str = "weekly", date: str = None, season: str = None):
    """
    Hämtar matcher från SoccerData API (eller mock) och sparar manifest i Azure.
    - league_id: numeric league_id
    - mode: 'weekly' eller 'fullseason'
    - date: YYYY-MM-DD (endast weekly)
    - season: 'YYYY-YYYY' (endast fullseason)
    """
    if mode == "fullseason":
        if not season:
            raise ValueError("Season must be provided in fullseason mode")
        blob_path = f"stats/{season}/{league_id}/manifest.json"
    else:  # weekly
        date_str = date or today_str()
        blob_path = f"stats/weekly/{date_str}/{league_id}/manifest.json"

    # ⚠️ Här borde du ha API-koden, just nu mockar vi manifest
    manifest = {
        "league_id": league_id,
        "mode": mode,
        "date": date or today_str(),
        "matches": []
    }

    utils.upload_json_debug(blob_path, manifest)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True)
    parser.add_argument("--mode", choices=["weekly", "fullseason"], default="weekly")
    parser.add_argument("--date", type=str, required=False)
    parser.add_argument("--season", type=str, required=False)
    args = parser.parse_args()

    run(args.league_id, mode=args.mode, date=args.date, season=args.season)
