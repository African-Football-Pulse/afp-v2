import argparse
from src.collectors.collect_stats import collect_stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True, help="Numeric league_id")
    parser.add_argument("--season", type=str, required=True, help="Season string, e.g. 2024-2025")
    args = parser.parse_args()

    # Kör en gång för att hämta allt spelat i säsongen
    collect_stats(args.league_id, season=args.season)
