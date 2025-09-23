# src/tools/get_latest_date.py
import argparse
from src.collectors.utils import get_latest_finished_date
from src.storage import azure_blob

def get_latest_match_date_for_league(league_id: int, season: str, container: str) -> str | None:
    """
    Returnera senaste färdigspelade matchdatum från Azure Blob Storage.
    """
    blob_path = f"stats/{season}/{league_id}/manifest.json"

    if not azure_blob.exists(container, blob_path):
        return None

    manifest = azure_blob.get_json(container, blob_path)
    return get_latest_finished_date(manifest)

def main():
    parser = argparse.ArgumentParser(description="Get latest finished match date for a league from Azure.")
    parser.add_argument("league_id", type=int, help="League ID (e.g. 228 for Premier League)")
    parser.add_argument("--season", type=str, required=True, help="Season folder (e.g. 2025-2026)")
    parser.add_argument("--container", type=str, default="your-container", help="Azure Blob container name")
    args = parser.parse_args()

    date = get_latest_match_date_for_league(args.league_id, args.season, args.container)
    if date:
        print(date)
    else:
        print("No finished matches found")

if __name__ == "__main__":
    main()
