import argparse
from datetime import date
from src.collectors.collect_stats import collect_stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True)
    parser.add_argument("--season", type=str, required=True)
    args = parser.parse_args()

    # Använd dagens datum (måndag) för att bara ta senaste omgången
    today = date.today().isoformat()
    collect_stats(args.league_id, season=args.season, date=today)
