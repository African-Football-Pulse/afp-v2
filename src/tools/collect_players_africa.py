import os
import json
import requests
from src.storage import azure_blob

API_URL = "https://api.soccerdataapi.com/player/"

def collect_players_africa(league_id: int, season: str, whitelist: dict):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    prefix = f"stats/{season}/{league_id}/"
    blobs = azure_blob.list_prefix(container, prefix)
    match_files = [b for b in blobs if b.endswith(".json") and "manifest" not in b]

    print(f"[collect_players_africa] Found {len(match_files)} match files in {prefix}")

    found_ids = set()

    for blob_path in match_files:
        match = azure_blob.get_json(container, blob_path)

        # Leta i möjliga sektioner som innehåller spelare
        for section in ["players", "lineups", "events"]:
            if section not in match:
                continue
            for p in match[section]:
                pid = p.get("id")
                name = p.get("name", "")
                if not pid or not name:
                    continue

                # Matcha mot whitelist
                for af in whitelist["players"]:
                    if name == af["name"] or name in af.get("aliases", []):
                        found_ids.add(pid)

    print(f"[collect_players_africa] Found {len(found_ids)} African players in league {league_id}")

    # Hämta & spara spelare via API
    for pid in sorted(found_ids):
        params = {"player_id": pid, "auth_token": token}
        headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
        try:
            resp = requests.get(API_URL, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            player_data = resp.json()

            path = f"players/{league_id}/{pid}.json"
            azure_blob.put_text(container, path, json.dumps(player_data, indent=2, ensure_ascii=False))
            print(f"[collect_players_africa] Uploaded player {pid} → {path}")
        except Exception as e:
            print(f"[collect_players_africa] ⚠️ Failed to fetch/save player {pid}: {e}")


if __name__ == "__main__":
    # Exempel: Premier League 2024-2025
    with open("players_africa.json", "r", encoding="utf-8") as f:
        whitelist = json.load(f)
    collect_players_africa(228, "2024-2025", whitelist)
