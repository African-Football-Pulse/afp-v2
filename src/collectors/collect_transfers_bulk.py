import os
import requests
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

def collect_transfers_for_league(league_id: int, season: str, token: str):
    url = "https://api.soccerdataapi.com/transfers/"
    params = {
        "league_id": league_id,
        "season": season,
        "auth_token": token,
    }

    print(f"[collect_transfers_bulk] Fetching transfers for league {league_id}, season {season}")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    if not data or "data" not in data:
        print(f"[collect_transfers_bulk] ⚠️ No data returned for league {league_id}, season {season}")
        return 0

    out_path = f"meta/{season}/transfers_{league_id}.json"
    azure_blob.upload_json(CONTAINER, out_path, data["data"])

    print(f"[collect_transfers_bulk] Uploaded → {out_path} ({len(data['data'])} transfers)")
    return len(data["data"])

def main():
    token = os.getenv("SOCCERDATA_AUTH_KEY")
    if not token:
        raise RuntimeError("[collect_transfers_bulk] Missing SOCCERDATA_AUTH_KEY")

    leagues = [
        228, 229, 230, 310, 326, 198, 235, 241, 253, 268, 297
    ]
    season = os.getenv("SEASON", "2025-2026")

    print("[collect_transfers_bulk] Starting transfer collection...")
    total = 0
    for league_id in leagues:
        try:
            total += collect_transfers_for_league(league_id, season, token)
        except Exception as e:
            print(f"[collect_transfers_bulk] ⚠️ Error fetching transfers for {league_id}: {e}")

    print(f"[collect_transfers_bulk] DONE. Total transfers collected: {total}")

if __name__ == "__main__":
    main()
