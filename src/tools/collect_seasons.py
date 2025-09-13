import os
import json
import requests
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/season/"

def collect_seasons(league_id: int):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    params = {
        "auth_token": token,
        "league_id": league_id,
    }

    print(f"[collect_seasons] Fetching season metadata for league_id={league_id}...")
    resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
    resp.raise_for_status()
    seasons = resp.json()

    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = f"meta/seasons_{league_id}.json"
    azure_blob.put_text(container, blob_path, json.dumps(seasons, indent=2, ensure_ascii=False))
    print(f"[collect_seasons] Uploaded to Azure: {blob_path}")


if __name__ == "__main__":
    # Exempel: Premier League
    collect_seasons(228)
