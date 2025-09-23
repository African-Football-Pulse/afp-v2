import os
import yaml
from datetime import datetime, timezone
from src.collectors import collect_stats
from src.collectors import utils

CONFIG_PATH = "config/leagues.yaml"
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def load_leagues():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return [l for l in data.get("leagues", []) if l.get("enabled", False)]


def find_latest_finished_date(league_id: int, season: str) -> str:
    """
    Läs manifest för ligan och returnera senaste 'finished' matchdatum (DD/MM/YYYY).
    Manifestet är en lista med matchobjekt.
    """
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    try:
        matches = utils.download_json_debug(manifest_path)
    except Exception as e:
        print(f"[collect_stats_weekly] ⚠️ No manifest for league {league_id}: {e}")
        return None

    if not isinstance(matches, list):
        print(f"[collect_stats_weekly] ⚠️ Unexpected manifest format for league {league_id}")
        return None

    finished = [m for m in matches if m.get("status") == "finished" and "date" in m]
    if not finished:
        return None

    finished.sort(key=lambda m: datetime.strptime(m["date"], "%d/%m/%Y"), reverse=True)
    return finished[0]["date"]


def main():
    leagues = load_leagues()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"[collect_stats_weekly] Found {len(leagues)} leagues in config.")

    for league in leagues:
        league_id = league["id"]
        season = league.get("season", "")
        name = league.get("name", "")
        print(f"[collect_stats_weekly] Processing {name} ({league_id}) for {today}...")

        match_date = find_latest_finished_date(league_id, season)
        if not match_date:
            print(f"[collect_stats_weekly] ❌ Failed for {name} ({league_id}): No finished matches found in manifest")
            continue

        try:
            collect_stats.run(league_id, season, mode="weekly", match_date=match_date)
        except Exception as e:
            print(f"[collect_stats_weekly] ❌ Failed for {name} ({league_id}): {e}")


if __name__ == "__main__":
    main()
