import os
from datetime import datetime, timezone
import yaml
from src.collectors import collect_stats

CONFIG_PATH = "config/leagues.yaml"


def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return data.get("leagues", [])


def today_str():
    return datetime.now(timezone.utc).date().isoformat()


def run_all():
    leagues = load_leagues()
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        if not league.get("enabled", False):
            continue
        try:
            league_id = league["id"]
            name = league.get("name", league["key"])
            season = league.get("season")
            print(f"[collect_stats_weekly] Processing {name} ({league_id}) for {today_str()}...")

            collect_stats.run(league_id, mode="weekly", season=season)

        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {league.get('name', league.get('key'))} "
                  f"({league.get('id')}): {e}")


if __name__ == "__main__":
    run_all()
