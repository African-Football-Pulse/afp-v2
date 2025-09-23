import os
import requests
from datetime import datetime
from src.storage import azure_blob
from src.collectors import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/matches/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")


def fetch_and_store_stats(league_id: int, season: str, date: str = None, mode: str = "weekly"):
    """
    Hämtar statistik för en liga, antingen för en specifik dag eller via manifest.
    Sparar resultatet till Azure Blob Storage.
    """

    if not AUTH_KEY:
        raise RuntimeError("SOCCERDATA_AUTH_KEY saknas i environment")

    # --- bygg parametrar ---
    params = {
        "league_id": league_id,
        "auth_token": AUTH_KEY,
    }
    if season:
        params["season"] = season
    if date:
        params["date"] = date

    print(f"[collect_stats] Requesting matches for league={league_id}, mode={mode}, params={params}")

    headers = {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }

    resp = requests.get(API_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if not data or (isinstance(data, list) and len(data) == 0):
        raise RuntimeError(f"No data returned for league {league_id}")

    # --- filtrera finished matcher ---
    matches = []
    for block in data:
        for m in block.get("matches", []):
            if m.get("status") == "finished":
                matches.append(m)

    if not matches:
        raise RuntimeError(f"No finished matches found for league {league_id}")

    # --- spara till Azure ---
    out_path = f"stats/{season}/{league_id}/{mode}_{date or 'latest'}.json"
    azure_blob.upload_json(CONTAINER, out_path, {"matches": matches})
    print(f"[collect_stats] ✅ Uploaded {len(matches)} matches → {out_path}")

    # --- uppdatera manifest ---
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        manifest = azure_blob.get_json(CONTAINER, manifest_path)
    except Exception:
        manifest = {"matches": []}

    # merge nya matcher
    seen_ids = {m["id"] for m in manifest["matches"]}
    for m in matches:
        if m["id"] not in seen_ids:
            manifest["matches"].append(m)

    azure_blob.upload_json(CONTAINER, manifest_path, manifest)
    print(f"[collect_stats] 📌 Manifest updated → {manifest_path}")
