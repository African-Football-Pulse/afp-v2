# src/collectors/collect_match_details.py

import os
import json
import argparse
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.common.blob_io import get_container_client

TZ = ZoneInfo("Europe/Stockholm")
BASE_URL = "https://api.soccerdataapi.com/match/"


def today_str():
    return datetime.now(timezone.utc).astimezone(TZ).date().isoformat()


def upload_json(container_client, path: str, obj):
    if container_client is None:
        raise RuntimeError("BLOB_CONTAINER_SAS_URL must be set – Collector requires Azure Blob access.")
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    container_client.upload_blob(name=path, data=data, overwrite=True)
    print(f"[collect_match_details] Uploaded to Azure: {path}")


def fetch_match_details(match_id: int, token: str):
    params = {
        "id": match_id,
        "auth_token": token
    }
    try:
        resp = requests.get(BASE_URL, headers={"Content-Type": "application/json"}, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[collect_match_details] ERROR fetching match {match_id}: {e}")
        return None


def run(league_id: int, manifest_path: str):
    token = os.environ["SOCCERDATA_AUTH_KEY"]

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    matches = []
    if isinstance(manifest, list) and len(manifest) > 0 and "matches" in manifest[0]:
        matches = manifest[0]["matches"]

    print(f"[collect_match_details] Found {len(matches)} matches in manifest.")

    date_str = today_str()
    container_client = get_container_client()

    for m in matches:
        match_id = m.get("id")
        if not match_id:
            continue

        details = fetch_match_details(match_id, token)
        if not details:
            continue

        blob_path = f"stats/{date_str}/{league_id}/{match_id}.json"
        upload_json(container_client, blob_path, details)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True, help="Numeric league_id from SoccerData API")
    parser.add_argument("--manifest", type=str, required=True, help="Path to manifest.json for the league")
    args = parser.parse_args()

    run(args.league_id, args.manifest)
