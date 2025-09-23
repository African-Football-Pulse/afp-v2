import os
import yaml
from src.storage import azure_blob
from src.tools.get_latest_date import get_latest_match_date_for_league


def filter_latest_round_for_league(league: dict, season: str, container: str):
    league_id = league["id"]
    league_name = league["name"]

    # 1. Hämta senaste datum från manifest
    latest_date = get_latest_match_date_for_league(league_id, season=season, container=container)
    if not latest_date:
        print(f"[filter_matches_latest] ⚠️ Inget giltigt matchdatum hittades för {league_name} ({league_id})")
        return

    # 2. Hämta fullseason matches.json
    src_path = f"stats/{season}/{league_id}/matches.json"
    data = azure_blob.get_json(container, src_path)
    if not data:
        print(f"[filter_matches_latest] ❌ Kunde inte hämta {src_path} från Azure")
        return

    # 3. Filtrera fram matcherna för latest_date
    matches = []
    leagues = data if isinstance(data, list) else [data]

    for lg in leagues:
        for stage in lg.get("stage", []):
            for m in stage.get("matches", []):
                if m.get("date") == latest_date:
                    matches.append(m)

    if not matches:
        print(f"[filter_matches_latest] ⚠️ Hittade inga matcher för {latest_date} i {league_name} ({league_id})")
        return

    # 4. Ladda upp filtrerade matcher
    safe_date = latest_date.replace("/", "-")
    out_path = f"stats/{season}/{league_id}/{safe_date}/matches.json"
    azure_blob.upload_json(container, out_path, matches)
    print(f"[filter_matches_latest] ✅ Uploaded {len(matches)} matches for {league_name} ({league_id}) on {latest_date} → {out_path}")


def main():
    season = os.getenv("SEASON", "2025-2026")
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    print(f"Running filter_latest for season={season}, container={container}")

    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for league in config.get("leagues", []):
        if not league.get("enabled", False):
            continue
        filter_latest_round_for_league(league, season, container)


if __name__ == "__main__":
    main()
