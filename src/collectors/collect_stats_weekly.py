import os
import yaml
from src.collectors import collect_stats   # ✅ rätt import
from src.collectors import utils           # ✅ nytt: för get_latest_finished_date
from src.storage import azure_blob

CONFIG_PATH = "config/leagues.yaml"


def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return data.get("leagues", [])


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

            # Hämta manifest från Azure för aktuell liga/säsong
            manifest_path = f"stats/{season}/{league_id}/manifest.json"
            manifest = azure_blob.download_json(
                os.getenv("AZURE_STORAGE_CONTAINER", "afp"),
                manifest_path
            )

            # Plocka fram senaste färdigspelade datum
            latest_date = utils.get_latest_finished_date(manifest)

            print(f"[collect_stats_weekly] Processing {name} ({league_id}) for {latest_date}...")

            # ✅ kör collect_stats i weekly-mode, med season + latest_date
            collect_stats.run(league_id, mode="weekly", season=season, date=latest_date)

        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {league.get('name', league.get('key'))} "
                  f"({league.get('id')}): {e}")


if __name__ == "__main__":
    run_all()
