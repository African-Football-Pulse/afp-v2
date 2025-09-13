import argparse
import yaml
from pathlib import Path
from src.collectors.collect_match_details import run


def run_from_config(config_path: str, with_api: bool = False):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        season = league["season"]
        name = league["name"]

        # Manifestets path följer samma struktur som collect_stats använder
        manifest_path = f"stats/{season}/{league_id}/manifest.json"

        print(f"[collect_extract_fullseason] Extracting matches for {name} (id={league_id}, season={season})")
        run(league_id, manifest_path, with_api)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        default="config/leagues.yaml",
        help="Path to YAML config with leagues",
    )
    parser.add_argument(
        "--with_api",
        action="store_true",
        help="Fetch each match from API instead of only using manifest",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    run_from_config(config_path, args.with_api)
