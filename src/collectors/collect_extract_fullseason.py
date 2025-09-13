import argparse
import yaml
from pathlib import Path
from src.collectors.collect_match_details import run


def run_from_config(config_path: str, season: str, with_api: bool = False):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        name = league["name"]

        # Fullseason ska alltid peka på säsong, inte datum
        manifest_path = f"stats/{season}/{league_id}/manifest.json"

        print(f"[collect_extract_fullseason] Extracting matches for {name} (id={league_id}, season={season})")
        run(league_id, manifest_path, with_api, mode="fullseason", season=season)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config/leagues.yaml")
    parser.add_argument("--season", type=str, required=True)
    parser.add_argument("--with_api", action="store_true")
    args = parser.parse_args()

    config_path = Path(args.config)
    run_from_config(config_path, args.season, args.with_api)
