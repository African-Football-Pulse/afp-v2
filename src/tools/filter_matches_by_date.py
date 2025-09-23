import os
import yaml
import datetime
from src.storage import azure_blob


def filter_latest_round_for_league(league: dict, season: str, container: str):
    league_id = league["id"]
    league_name = league["name"]

    # 1. Hämta fullseason matches.json
    src_path = f"stats/{season}/{league_id}/matches.json"
    data = azure_blob.get_json(container, src_path)
    if not data:
        print(f"[filter_matches_by_date] ❌ Kunde inte hämta {src_path} från Azure")
        return

    # 2. Extrahera alla datum
    dates = []
    leagues = data if isinstance(data, list) else [data]

    for lg in leagues:
        for stage in lg.get("stage", []):
            for m in stage.get("matches", []):
                d = m.get("date")
                try:
                    dt = datetime.datetime.strptime(d, "%d/%m/%Y")
                    if dt.date() < datetime.date.today():
                        dates.append(dt.date())
                except Exception:
                    continue

    if not dates:
        print(f"[filter_matches_by_date] ⚠️ Inga giltiga datum hittades i {src_path}")
        return

    latest_date = max(dates)
    latest_date_str = latest_date.strftime("%d/%m/%Y")
    safe_date = latest_date.strftime("%d-%m-%Y")

    print(f"[filter_matches_by_date] ✅ Valde senaste datum {latest_date_str} för {league_name} ({league_id})")

    # 3. Filtrera fram matcherna för latest_date
    matches = []
    for lg in leagues:
        for stage in lg.get("stage", []):
            for m in stage.get("matches", []):
                if m.get("date") == latest_date_str:
                    matches.append(m)

    if not matches:
        print(f"[filter_matches_by_date] ⚠️ Hittade inga matcher för {latest_date_str} i {league_name} ({league_id})")
        return

    # 4. Ladda upp filtrerade matcher
    out_path = f"stats/{season}/{league_id}/{safe_date}/matches.json"
    azure_blob.upload_json(container, out_path, matches)
    print(f"[filter_matches_by_date] ✅ Uploaded {len(matches)} matches for {league_name} ({league_id}) → {out_path}")


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
