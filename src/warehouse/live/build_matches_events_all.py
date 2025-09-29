import os
import yaml
import subprocess

with open("config/leagues.yaml", "r", encoding="utf-8") as f:
    leagues = yaml.safe_load(f)

for league in leagues.get("leagues", []):
    if not league.get("enabled", False):
        continue

    season = league["season"]
    key = league["key"]

    print(f"▶️ Running build_matches_events_flat for {key} ({season})")

    subprocess.run([
        "python", "-m", "src.warehouse.live.build_matches_events_flat"
    ], env={**os.environ, "SEASON": season, "LEAGUE": key}, check=True)
