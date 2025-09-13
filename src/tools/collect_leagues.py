import os
import json
import requests
from src.storage import azure_blob

def collect_leagues():
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    url = "https://api.soccerdataapi.com/leagues/"

    print("[collect_leagues] Fetching league metadata...")
    resp = requests.get(url, params={"auth_token": token}, timeout=30)
    resp.raise_for_status()
    leagues = resp.json()

    # Lagra i Azure
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = "meta/leagues.json"
    azure_blob.put_text(container, blob_path, json.dumps(leagues, indent=2, ensure_ascii=False))
    print(f"[collect_leagues] Uploaded to Azure: {blob_path}")


if __name__ == "__main__":
    collect_leagues()
