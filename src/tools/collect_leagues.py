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

    # Lagra hela svaret i Azure
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")
    blob_path = "meta/leagues.json"
    azure_blob.put_text(container, blob_path, json.dumps(leagues, indent=2, ensure_ascii=False))
    print(f"[collect_leagues] Uploaded to Azure: {blob_path}")

    # Logga en tabell med de viktigaste fälten
    print("\n=== Available Leagues ===")
    results = leagues.get("results", [])
    for league in results:
        lid = league.get("id")
        name = league.get("name")
        country = league.get("country", {}).get("name")
        is_cup = league.get("is_cup")
        print(f"{lid:<5} | {name:<30} | {country:<15} | Cup={is_cup}")


if __name__ == "__main__":
    # Default: hämta alla ligor
    collect_leagues()
