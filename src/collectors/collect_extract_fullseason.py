import argparse
from src.collectors.collect_match_details import run


def run_single_league(league_id: int, season: str, with_api: bool = False):
    """
    Kör extract för EN liga, baserat på league_id + season.
    """
    manifest_path = f"stats/{season}/{league_id}/manifest.json"
    print(f"[collect_extract_fullseason] Extracting matches for league_id={league_id}, season={season}")
    run(league_id, manifest_path, with_api, mode="fullseason", season=season)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--league_id", type=int, required=True, help="Numeric league_id (e.g. 228)")
    parser.add_argument("--season", type=str, required=True, help="Season string, e.g. 2024-2025")
    parser.add_argument("--with_api", action="store_true", help="If set, fetch match details from API")
    args = parser.parse_args()

    run_single_league(args.league_id, args.season, args.with_api)
