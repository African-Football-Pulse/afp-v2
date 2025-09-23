import os
from datetime import datetime, timezone
import yaml
from src.collectors import collect_stats
from src.collectors import utils

CONFIG_PATH = "config/leagues.yaml"


def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return [lg for lg in data.get("leagues", []) if lg.get("enabled", False)]


def main():
    leagues = load_leagues()
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for league in leagues:
        league_id = league["id"]
        season = league["season"]
        name = league["name"]

        print(f"[collect_stats_weekly] Processing {name} ({league_id}) for {today}...")

        # Ladda manifest från Azure
        blob_path = f"stats/{season}/{league_id}/manifest.json"
        manifest = utils.download_json_debug(blob_path)

        if not manifest:
            print(f"[collect_stats_weekly] ❌ Failed for {name} ({league_id}): No manifest found")
            continue

        # Hitta senaste färdiga matchdatum
        match_date = utils.get_latest_finished_date(manifest)
        if not match_date:
            print(f"[collect_stats_weekly] ❌ Failed for {name} ({league_id}): No finished matches found in manifest")
            continue

        try:
            collect_stats.run(league_id, season=season, mode="weekly", match_date=match_date)
        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {name} ({league_id}): {e}")


if __name__ == "__main__":
    main()
