import os
import argparse
import requests
from datetime import datetime, timezone
from collections import defaultdict
from src.collectors import utils

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")
API_URL = "https://api.soccerdataapi.com/matches/"
AUTH_KEY = os.getenv("SOCCERDATA_AUTH_KEY")


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def run(league_id: int, mode: str = "weekly", season: str = None):
    """
    Hämtar matcher från SoccerData API och sparar manifest i Azure.
    - weekly: hämtar ALLA matcher för aktiv säsong (ingen season i params),
              plockar senaste datum med finished-matcher.
    - fullseason: hämtar ALLA matcher för given season och sparar allt.
    """
    if not AUTH_KEY:
        raise RuntimeError("Missing SOCCERDATA_AUTH_KEY in environment")

    params = {"league_id": league_id, "auth_token": AUTH_KEY}


    if date:
        params["date"] = date
    if season:
        params["season"] = season

    headers = {"Accept-Encoding": "gzip", "Content-Type": "application/json"}

    print(f"[collect_stats] Requesting matches for league={league_id}, mode={mode}, params={params}")
    resp = requests.get(API_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    if isinstance(data, dict):
        data = [data]

    matches = []
    for league in data:
        matches.extend(league.get("matches", []))

    if mode == "weekly":
        # Gruppér matcher per datum där status == finished
        finished_by_date = defaultdict(list)
        for m in matches:
            status = (m.get("status") or "").lower()
            if status in ("finished", "ft", "ended", "full time"):
                finished_by_date[m.get("date")].append(m)

        if not finished_by_date:
            raise RuntimeError(f"No finished matches found for league {league_id}")

        # Hitta senaste datum (API ger oftast DD/MM/YYYY)
        parsed = []
        for d, ms in finished_by_date.items():
            try:
                dt = datetime.strptime(d, "%d/%m/%Y").date()
            except ValueError:
                dt = datetime.strptime(d, "%Y-%m-%d").date()
            parsed.append((dt, d, ms))

        parsed.sort(key=lambda x: x[0], reverse=True)
        latest_date, latest_date_str, latest_matches = parsed[0]

        blob_path = f"stats/weekly/{latest_date.isoformat()}/{league_id}/manifest.json"
        manifest = {
            "league_id": league_id,
            "mode": "weekly",
            "date": latest_date.isoformat(),
            "matches": latest_matches,
        }
        utils.upload_json_debug(blob_path, manifest)
        print(f"[collect_stats] ✅ Uploaded weekly manifest with {len(latest_matches)} matches "
              f"({latest_date_str}) → {blob_path}")

    elif mode == "fullseason":
        if not season:
            raise ValueError("Season must be provided in fullseason mode")
        blob_path = f"stats/{season}/{league_id}/manifest.json"
        manifest = {
            "league_id": league_id,
            "mode": "fullseason",
            "season": season,
            "matches": matches,
        }
        utils.upload_json_debug(blob_path, manifest)
        print(f"[collect_stats] ✅ Uploaded fullseason manifest with {len(matches)} matches → {blob_path}")

    else:
        raise ValueError(f"Unsupported mode: {mode}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True)
    parser.add_argument("--mode", choices=["weekly", "fullseason"], default="weekly")
    parser.add_argument("--season", type=str, required=False)
    args = parser.parse_args()

    run(args.league_id, mode=args.mode, season=args.season)
