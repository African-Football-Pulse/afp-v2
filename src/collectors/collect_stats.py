import os
import requests
import urllib.parse
from datetime import datetime, timezone
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/matches/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")


def run(league_id: int, season: str, match_date: str = None, mode: str = "weekly"):
    """
    Hämtar statistik från SoccerData API för given liga, säsong och ev. datum.
    Skriver resultat till Azure Blob och returnerar datat som Python-objekt.
    """
    params = {"league_id": league_id, "auth_token": AUTH_KEY}

    # Lägg bara till season om vi inte använder date
    if match_date:
        params["date"] = match_date
    else:
        if season:
            params["season"] = season

    headers = {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json"
    }

    print(f"[collect_stats] Requesting matches for league={league_id}, mode={mode}, params={params}")

    # 👇 Bygg och logga full URL
    full_url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    print(f"[collect_stats] Full URL: {full_url}")

    try:
        resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"API request failed for league {league_id}: {e}")

    # Spara till Azure
    out_path = f"stats/{season}/{league_id}"
    if match_date:
        out_path = f"{out_path}/{match_date}"
    out_path = f"{out_path}/matches.json"

    try:
        azure_blob.upload_json(CONTAINER, out_path, data)
        print(f"[collect_stats] ✅ Uploaded {out_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload stats for league {league_id}: {e}")

    return data  # 👈 Viktigt! Returnera datat så att fullseason kan använda det


# 👇 Wrapper för bakåtkompatibilitet
def collect_stats(league_id: int, season: str, match_date: str = None, mode: str = "fullseason"):
    """
    Wrapper för bakåtkompatibilitet. Anropar run().
    """
    return run(league_id, season, match_date, mode)
