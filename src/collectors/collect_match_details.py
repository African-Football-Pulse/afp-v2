import os
import json
import requests
from datetime import datetime, timezone
from src.storage import azure_blob
from src.collectors import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/matches/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")

def today_str():
    return datetime.now(timezone.utc).date().isoformat()

def run(league_id: int, manifest_path: str, with_api: bool = False, mode: str = "weekly", season: str = None):
    """
    Läser manifest (från Azure Blob) och sparar matcher som separata filer.
    Om with_api=True → hämtar detaljer för varje match från SoccerData API.
    """
    if not AUTH_KEY and with_api:
        raise RuntimeError("Missing SOCCERDATA_AUTH_KEY in environment")

    manifest_text = azure_blob.get_text(CONTAINER, manifest_path)
    manifest = json.loads(manifest_text)

    matches = manifest.get("matches", [])
    print(f"[collect_match_details] Found {len(matches)} matches in manifest (league {league_id}).")

    for match in matches:
        match_id = match["id"]

        if mode == "fullseason":
            if not season:
                raise ValueError("Season must be provided in fullseason mode")
            blob_path = f"stats/{season}/{league_id}/{match_id}.json"
        else:  # weekly
            date_str = today_str()
            blob_path = f"stats/weekly/{date_str}/{league_id}/{match_id}.json"

        if with_api:
            # Fetch detailed data for match
            params = {"match_id": match_id, "auth_token": AUTH_KEY}
            headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}
            print(f"[collect_match_details] Fetching details for match {match_id} (league {league_id})...")
            resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            match_data = resp.json()
        else:
            # Just use the summary match object from manifest
            match_data = match

        utils.upload_json_debug(blob_path, match_data)

    print(f"[collect_match_details] ✅ Done. Uploaded {len(matches)} matches for league {league_id}.")
