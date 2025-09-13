# src/collectors/collect_match_details.py

import os
import json
import argparse
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.storage import azure_blob

TZ = ZoneInfo("Europe/Stockholm")
BASE_URL = "https://api.soccerdataapi.com/match/"


def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()


def upload_json(container: str, path: str, obj):
    data = json.dumps(obj, ensure_ascii=False, indent=2)
    azure_blob.put_text(container, path, data, content_type="application/json")
    print(f"[collect_match_details] Uploaded to Azure: {container}/{path}")


def fetch_match_details(match_id: int, token: str):
    params = {"id": match_id, "auth_token": token}
    try:
        resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[collect_match_details] ERROR fetching match {match_id}: {e}")
        return None


def run(league_id: int, manifest_blob_path: str):
    token = os.environ["SOCCERDATA_AUTH_KEY"]
    # 🔹 Defaulta nu till 'afp' istället för 'producer'
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "afp")

    # Hämta manifest från Blob
    manifest_text = azure_blob.get_text(container, manifest_blob_path)
    manifest = json.loads(manifest_text)

    matches = []
    if isinstance(manifest, list) and len(manifest) > 0 and "matches" in manifest[0]:
        matches = manifest[0]["matches"]

    print(f"[collect_match_details] Found {len(matches)} matches in manifest.")

    date_str = today_str()
    for m in matches:
        match_id = m.get("id")
        if not match_id:
            continue

        details = fetch_match_details(match_id, token)
        if not details:
            continue

        blob_path = f"stats/{date_str}/{league_id}/{match_id}.json"
        upload_json(container, blob_path, details)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True, help="Numeric league_id from SoccerData API")
    parser.add_argument("--manifest", type=str, required=True, help="Blob path to manifest.json (from collect_stats)")
    args = parser.parse_args()

    run(args.league_id, args.manifest)
