import os
import json
import requests
import yaml
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/season/"
import os
import json
import requests
import yaml
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/season/"

def collect_seasons_for_league(league_id: int):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    params = {
        "auth_token": token,
        "league_id": league_id,
    }

    resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def run_from_config(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        league_name = league.get("name", str(league_id))

        print(f"\n[collect_all_seasons] Fetching seasons for {league_name} (id={league_id})...")
        data = collect_seasons_for_league(league_id)

        # Lagra i Azure
        blob_path = f"meta/seasons_{league_id}.json"
        azure_blob.put_text(container, blob_path, json.dumps(data, indent=2, ensure_ascii=False))
        print(f"[collect_all_seasons] Uploaded to Azure: {blob_path}")

        # Logga ut alla säsonger direkt
        results = data.get("results", [])
        if not results:
            print(f"[collect_all_seasons] ⚠️ No seasons found for {league_name}")
        else:
            years = [s.get("year") for s in results]
            active = [s.get("year") for s in results if s.get("is_active")]
            print(f"[collect_all_seasons] Seasons found: {', '.join(years)}")
            if active:
                print(f"[collect_all_seasons] Active season(s): {', '.join(active)}")


if __name__ == "__main__":
    run_from_config("config/leagues.yaml")

def collect_seasons_for_league(league_id: int):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    params = {
        "auth_token": token,
        "league_id": league_id,
    }

    resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def run_from_config(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        league_name = league.get("name", str(league_id))

        print(f"[collect_all_seasons] Fetching seasons for {league_name} (id={league_id})...")
        data = collect_seasons_for_league(league_id)

        blob_path = f"meta/seasons_{league_id}.json"
        azure_blob.put_text(container, blob_path, json.dumps(data, indent=2, ensure_ascii=False))
        print(f"[collect_all_seasons] Uploaded to Azure: {blob_path}")


if __name__ == "__main__":
    run_from_config("config/leagues.yaml")
