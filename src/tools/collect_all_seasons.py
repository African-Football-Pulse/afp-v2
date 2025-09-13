import os
import requests
import yaml
from src.storage import azure_blob

SOCCERDATA_AUTH_KEY = os.environ.get("SOCCERDATA_AUTH_KEY")
BLOB_CONTAINER_SAS_URL = os.environ.get("BLOB_CONTAINER_SAS_URL")

def fetch_seasons(league_id: int):
    """Hämta alla säsonger för en viss liga från SoccerData API."""
    url = "https://api.soccerdataapi.com/season/"
    params = {"league_id": league_id, "auth_token": SOCCERDATA_AUTH_KEY}
    headers = {
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }

    print(f"[collect_all_seasons] Fetching seasons for league_id={league_id}...")
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json()

def run_from_config(config_path: str):
    """Läs config/leagues.yaml och hämta alla säsonger för varje aktiv liga."""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    service_client = azure_blob.get_blob_service_client(BLOB_CONTAINER_SAS_URL)
    container = service_client.get_container_client("afp")


    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        name = league["name"]
        country = league.get("country", "N/A")

        data = fetch_seasons(league_id)

        # Filtrera ut None från "year"
        years = [s.get("year") for s in data.get("results", []) if s.get("year")]
        if years:
            print(f"[collect_all_seasons] Seasons found for {name} ({country}): {', '.join(years)}")
        else:
            print(f"[collect_all_seasons] No valid seasons found for {name} ({country})")

        # Spara hela svaret i Azure
        blob_path = f"meta/seasons_{league_id}.json"
        azure_blob.upload_json(container, blob_path, data)
        print(f"[collect_all_seasons] Uploaded to Azure: {blob_path}")

if __name__ == "__main__":
    run_from_config("config/leagues.yaml")
