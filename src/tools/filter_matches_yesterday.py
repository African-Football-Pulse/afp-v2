import os
import yaml
import datetime
from src.storage import azure_blob


def filter_yesterday_for_league(league: dict, season: str, container: str):
    league_id = league["id"]
    league_name = league["name"]

    # 1. Räkna ut gårdagens datum
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    date_str = yesterday.strftime("%d/%m/%Y")
    safe_date = yesterday.strftime("%d-%m-%Y")

    # 2. Hämta fullseason matches.json
    src_path = f"stats/{season}/{league_id}/matches.json"
    data = azure_blob.get_json(container, src_path)
    if not data:
        print(f"[filter_matches_yesterday] ❌ Kunde inte hämta {src_path} från Azure")
        return

    # 3. Filtrera fram matcherna för gårdagens datum
    matches = []
    leagues = data if isinstance(data, list) else [data]

    for lg in leagues:
        for stage in lg.get("stage", []):
            for m in stage.get("matches", []):
                if m.get("date") == date_str:
                    matches.append(m)

    if not matches:
        print(f"[filter_matches_yesterday] ⏩ Inga matcher spelades i {league_name} ({league_id}) på {date_str}")
        return

    # 4. Ladda upp filtrerade matcher
    out_path = f"stats/{season}/{league_id}/{safe_date}/matches.json"
    azure_blob.upload_json(container, out_path, matches)
    print(f"[filter_matches_yesterday] ✅ Uploaded {len(matches)} matches for {league_name} ({league_id}) → {out_path}")


def main():
    season = os.getenv("SEASON", "2025-2026")
    container = os.getenv("AZURE_STORAGE_CONTAINER", "afp")

    print(f"Running filter_yesterday for season={season}, container={container}")

    with open("config/leagues.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for league in config.get("leagues", []):
        if not league.get("enabled", False):
            continue
        filter_yesterday_for_league(league, season, container)


if __name__ == "__main__":
    main()
