import os
import requests
import yaml
from src.storage import azure_blob

SOCCERDATA_AUTH_KEY = os.environ.get("SOCCERDATA_AUTH_KEY")

def fetch_seasons(league_id: int):
    """Hämta alla säsonger för en viss liga från SoccerData API."""
    url = "https://api.soccerdataapi.com/season/"
    params = {"league_id": league_id, "auth_token": SOCCERDATA_AUTH_KEY}
    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}

    print(f"[collect_all_seasons] Fetching seasons for league_id={league_id}...")
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def run_from_config(config_path: str):
    """Läs config/leagues.yaml och hämta alla säsonger för varje aktiv liga."""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        name = league["name"]
        country = league.get("country", "N/A")

        data = fetch_seasons(league_id)
        results = data.get("results", [])

        if results:
            seasons = []
            for s in results:
                year = s.get("year")
                sid = s.get("id")
                active = s.get("is_active", False)
                seasons.append(f"{year} (id={sid}, active={active})")

            print(f"[collect_all_seasons] Seasons for {name} ({country}): {', '.join(seasons)}")
        else:
            print(f"[collect_all_seasons] ⚠️ No seasons returned for {name} ({country})")

        # Spara alltid hela svaret i Azure
        blob_path = f"meta/seasons_{league_id}.json"
        azure_blob.upload_json(container, blob_path, data)
        print(f"[collect_all_seasons] Uploaded to Azure: {blob_path}")

if __name__ == "__main__":
    run_from_config("config/leagues.yaml")
