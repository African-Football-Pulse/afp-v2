# src/tools/get_latest_date.py
import argparse
from src.collectors.get_latest_match_date import get_latest_match_date_for_league

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
