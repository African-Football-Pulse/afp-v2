import argparse
import json
from pathlib import Path
from src.utils import get_latest_finished_date  # ✅ rätt import

def get_latest_match_date_for_league(league_id: int, stats_dir: str = "stats") -> str | None:
    seasons = sorted(Path(stats_dir).iterdir(), key=lambda p: p.name, reverse=True)
    if not seasons:
        return None

    latest_season = seasons[0].name
    manifest_path = Path(stats_dir) / latest_season / str(league_id) / "manifest.json"

    if not manifest_path.exists():
        return None

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    return get_latest_finished_date(manifest)  # ✅ använd funktionen

def main():
    parser = argparse.ArgumentParser(description="Get latest finished match date for a league.")
    parser.add_argument("league_id", type=int, help="League ID (e.g. 228 for Premier League)")
    parser.add_argument("--stats_dir", type=str, default="stats", help="Base stats directory")
    args = parser.parse_args()

    date = get_latest_match_date_for_league(args.league_id, stats_dir=args.stats_dir)
    if date:
        print(date)
    else:
        print("No finished matches found")

if __name__ == "__main__":
    main()
