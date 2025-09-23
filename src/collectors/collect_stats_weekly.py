import os
from datetime import datetime, timezone
import yaml
from src.collectors import collect_stats, utils

CONFIG_PATH = "config/leagues.yaml"


def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return [lg for lg in data.get("leagues", []) if lg.get("enabled", False)]


def main():
    leagues = load_leagues()
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        league_id = league["id"]
        season = league.get("season")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        print(f"[collect_stats_weekly] Processing {league['name']} ({league_id}) for {today}...")

        manifest_path = f"stats/{season}/{league_id}/manifest.json"
        manifest = utils.download_json_debug(manifest_path)

        if not manifest:
            print(f"[collect_stats_weekly] ❌ Failed for {league['name']} ({league_id}): No manifest found")
            continue

        match_date = utils.get_latest_finished_date(manifest)
        if not match_date:
            print(f"[collect_stats_weekly] ❌ Failed for {league['name']} ({league_id}): No valid match dates found before today")
            continue

        print(f"[collect_stats_weekly] ➡️ Using match date {match_date} for {league['name']} ({league_id})")

        try:
            collect_stats.run(league_id, season, match_date, mode="weekly")
        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {league['name']} ({league_id}): {e}")


if __name__ == "__main__":
    main()
