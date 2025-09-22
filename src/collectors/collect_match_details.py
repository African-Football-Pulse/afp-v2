import os
import json
from datetime import datetime, timezone
from src.storage import azure_blob
from src.collectors import utils


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def run(league_id: int, manifest_path: str, with_api: bool = False, mode: str = "weekly", season: str = None):
    """
    Läser manifest (från Azure Blob) och sparar matcher som separata filer.
    - league_id: numeric league_id
    - manifest_path: blob path till manifest.json
    - with_api: om True → hämta detaljer per match via API
    - mode: 'weekly' eller 'fullseason'
    - season: krävs om mode='fullseason'
    """
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    manifest_text = azure_blob.get_text(container, manifest_path)
    manifest = json.loads(manifest_text)

    matches = []
    for league in manifest:
        for stage in league.get("stage", []):
            matches.extend(stage.get("matches", []))

    print(f"[collect_match_details] Found {len(matches)} matches in manifest.")

    for match in matches:
        match_id = match["id"]

        # Sätt rätt path beroende på mode
        if mode == "fullseason":
            if not season:
                raise ValueError("Season must be provided in fullseason mode")
            blob_path = f"stats/{season}/{league_id}/{match_id}.json"
        else:  # weekly
            date_str = today_str()
            blob_path = f"stats/weekly/{date_str}/{league_id}/{match_id}.json"

        utils.upload_json_debug(blob_path, match)
