import os
import json
import requests
from src.storage import azure_blob

API_URL = "https://api.soccerdataapi.com/player/"

def collect_players_africa(league_id: int, season: str, whitelist: dict):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    print(f"[collect_players_africa] Loading manifest {manifest_path} ...")
    manifest = azure_blob.get_json(container, manifest_path)

    # Ibland är manifest en lista, ibland dict med "matches"
    if isinstance(manifest, dict) and "matches" in manifest:
        matches = manifest["matches"]
    elif isinstance(manifest, list):
        matches = manifest
    else:
        raise ValueError(f"Unexpected manifest format: {type(manifest)}")

    found_ids = set()

    for m in matches:
        for ev in m.get("events", []):
            # event.player
            if "player" in ev and isinstance(ev["player"], dict):
                pid = ev["player"].get("id")
                name = ev["player"].get("name", "")
                if pid and name and is_african(name, whitelist):
                    found_ids.add(pid)

            # event.assist_player
            if "assist_player" in ev and isinstance(ev["assist_player"], dict):
                pid = ev["assist_player"].get("id")
                name = ev["assist_player"].get("name", "")
                if pid and name and is_african(name, whitelist):
                    found_ids.add(pid)

    print(f"[collect_players_africa] Found {len(found_ids)} African players in league {league_id}, season {season}")

    # Hämta & spara via API
    for pid in sorted(found_ids):
        try:
            params = {"player_id": pid, "auth_token": token}
            headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
            resp = requests.get(API_URL, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            player_data = resp.json()

            path = f"players/{league_id}/{pid}.json"
            azure_blob.put_text(container, path, json.dumps(player_data, indent=2, ensure_ascii=False))
            print(f"[collect_players_africa] Uploaded player {pid} → {path}")
        except Exception as e:
            print(f"[collect_players_africa] ⚠️ Failed to fetch/save player {pid}: {e}")


def is_african(name: str, whitelist: dict) -> bool:
    for af in whitelist.get("players", []):
        if name == af["name"] or name in af.get("aliases", []):
            return True
    return False


if __name__ == "__main__":
    with open("players_africa.json", "r", encoding="utf-8") as f:
        whitelist = json.load(f)
    collect_players_africa(228, "2024-2025", whitelist)
