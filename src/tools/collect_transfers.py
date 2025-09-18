import os
import json
import requests
from datetime import datetime, timezone
from src.storage import azure_blob

BASE_URL = "https://api.soccerdataapi.com/transfers/"

def collect_transfers_from_teams(league_id: int, season: str):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    prefix = f"teams/{league_id}/"
    blobs = azure_blob.list_prefix(container, prefix)
    team_files = [b for b in blobs if b.endswith(".json")]

    print(f"[collect_transfers] Found {len(team_files)} team files in {prefix}")

    manifest = {
        "league_id": league_id,
        "season": season,
        "teams": []
    }

    for blob_path in team_files:
        team_data = azure_blob.get_json(container, blob_path)
        team_id = team_data.get("id")
        team_name = team_data.get("name")
        if not team_id:
            print(f"[collect_transfers] ⚠️ Missing id in {blob_path}")
            continue

        params = {"team_id": team_id, "auth_token": token}
        headers = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}

        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            transfers_data = resp.json()

            transfer_path = f"transfers/{league_id}/{team_id}.json"
            azure_blob.put_text(container, transfer_path, json.dumps(transfers_data, indent=2, ensure_ascii=False))
            print(f"[collect_transfers] Uploaded transfers for team {team_id} → {transfer_path}")

            manifest["teams"].append({
                "id": team_id,
                "name": team_name,
                "path": transfer_path,
                "collected_at": datetime.now(timezone.utc).isoformat()
            })

        except Exception as e:
            print(f"[collect_transfers] ⚠️ Failed to fetch/save transfers for team {team_id}: {e}")

    # Spara manifest
    manifest_path = f"transfers/{league_id}/manifest.json"
    azure_blob.put_text(container, manifest_path, json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"[collect_transfers] Wrote manifest → {manifest_path}")

def collect_transfers(league_id: int, season: str):
    return collect_transfers_from_teams(league_id, season)

if __name__ == "__main__":
    # Exempel: Premier League 2024-2025
    collect_transfers(228, "2024-2025")
