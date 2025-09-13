import os
import json
import requests
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/league/"

def collect_leagues(country_id: int = None):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    params = {"auth_token": token}
    if country_id:
        params["country_id"] = country_id

    print("[collect_leagues] Fetching league metadata...")
    resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
    resp.raise_for_status()
    leagues = resp.json()

    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = "meta/leagues.json"
    azure_blob.put_text(container, blob_path, json.dumps(leagues, indent=2, ensure_ascii=False))
    print(f"[collect_leagues] Uploaded to Azure: {blob_path}")


if __name__ == "__main__":
    # Om du vill begränsa till ett land: collect_leagues(country_id=8)  # England
    collect_leagues()
