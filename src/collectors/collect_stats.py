# src/collector/collect_stats.py

import os
import json
import argparse
from datetime import datetime
from soccerdata import FBref
from azure.storage.blob import BlobServiceClient

def collect_stats(league: str, season: str, smoke: bool = False):
    # Initiera SoccerData
    fbref = FBref(leagues=[league], seasons=[season])

    print(f"[collect_stats] Fetching data for {league} {season}...")

    try:
        matches = fbref.read_matchlist(league=league, season=season)
        if smoke:
            matches = matches.head(5)  # Begränsa vid smoke-mode
        print(f"[collect_stats] Retrieved {len(matches)} matches")
    except Exception as e:
        print(f"[collect_stats] ERROR fetching data: {e}")
        return False

    # Gör datumstämplad mapp
    date = datetime.utcnow().strftime("%Y-%m-%d")
    payload = {
        "league": league,
        "season": season,
        "date": date,
        "count": len(matches),
        "matches": matches.to_dict(orient="records")
    }

    # Spara lokalt
    out_dir = f"outputs/stats/{date}/{league}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "manifest.json")

    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"[collect_stats] Saved locally: {out_file}")

    # Azure-upload
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        )
        container = os.environ.get("AZURE_STORAGE_CONTAINER", "producer")

        blob_path = f"stats/{date}/{league}/manifest.json"
        blob_client = blob_service_client.get_blob_client(container, blob_path)

        with open(out_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"[collect_stats] Uploaded to Azure: {container}/{blob_path}")

    except Exception as e:
        print(f"[collect_stats] ERROR uploading to Azure: {e}")
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, required=True)
    parser.add_argument("--season", type=str, default="2024-2025")
    parser.add_argument("--smoke", action="store_true")

    args = parser.parse_args()
    collect_stats(args.league, args.season, args.smoke)
