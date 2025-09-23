import os
from datetime import datetime, timezone
import yaml
from src.collectors import collect_stats
from src.storage import azure_blob
from src.collectors import utils

CONFIG_PATH = "config/leagues.yaml"
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return [lg for lg in data.get("leagues", []) if lg.get("enabled", False)]


def find_latest_finished_date(league_id: int, season: str) -> str | None:
    """
    Läs manifestet från Azure och hitta senaste matchdag med färdiga matcher.
    """
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        manifest = azure_blob.get_json(CONTAINER, manifest_path)
    except Exception as e:
        print(f"[collect_stats_weekly] ⚠️ Kunde inte läsa manifest {manifest_path}: {e}")
        return None

    latest_date = None
    for m in manifest.get("matches", []):
        if m.get("status") == "finished":
            dt = datetime.strptime(m["date"], "%d/%m/%Y")
            if latest_date is None or dt > latest_date:
                latest_date = dt

    if latest_date:
        return latest_date.strftime("%Y-%m-%d")
    return None


def main():
    leagues = load_leagues()
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        league_id = league["id"]
        season = league.get("season")
        league_name = league["name"]

        match_date = find_latest_finished_date(league_id, season)
        if not match_date:
            print(f"[collect_stats_weekly] ⚠️ Skipping {league_name} ({league_id}): Hittade inget färdigt matchdatum")
            continue

        print(f"[collect_stats_weekly] Processing {league_name} ({league_id}) for {match_date}...")

        try:
            collect_stats.fetch_and_store_stats(
                league_id=league_id,
                season=season,
                date=match_date,
                mode="weekly",
            )
        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {league_name} ({league_id}): {e}")


if __name__ == "__main__":
    main()
