import os
import requests
from datetime import datetime, timezone
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/matches/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")


def run(league_id: int, season: str, match_date: str, mode: str = "weekly"):
    """
    Hämtar statistik från SoccerData API för given liga, säsong och datum.
    Skriver resultat till Azure Blob.
    """
    params = {"league_id": league_id, "auth_token": AUTH_KEY}
    if season:
        params["season"] = season
    if match_date:
        params["date"] = match_date

    headers = {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json"
    }

    print(f"[collect_stats] Requesting matches for league={league_id}, mode={mode}, params={params}")

    try:
        resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"API request failed for league {league_id}: {e}")

    # Spara till Azure
    out_path = f"stats/{season}/{league_id}/{match_date}/matches.json"
    try:
        azure_blob.upload_json(CONTAINER, out_path, data)
        print(f"[collect_stats] ✅ Uploaded {out_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload stats for league {league_id}: {e}")
