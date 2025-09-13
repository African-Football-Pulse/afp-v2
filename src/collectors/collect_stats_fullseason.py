import argparse
import yaml
from pathlib import Path
from src.collectors.collect_stats import collect_stats


def run_from_config(config_path: str, season: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    leagues = cfg.get("leagues", [])
    for league in leagues:
        if not league.get("enabled", False):
            continue

        league_id = league["id"]
        name = league["name"]

        print(f"[collect_stats_fullseason] Fetching full season for {name} (id={league_id}, season={season})")
        collect_stats(league_id, season=season, mode="fullseason")   # 👈 viktig ändring


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        default="config/leagues.yaml",
        help="Path to YAML config with leagues",
    )
    parser.add_argument(
        "--season",
        type=str,
        required=True,
        help="Season string, e.g. 2024-2025",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    run_from_config(config_path, args.season)
