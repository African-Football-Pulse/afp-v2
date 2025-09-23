import argparse
import json
from pathlib import Path

def filter_matches(league_id: int, season: str, date: str, stats_dir: str = "stats"):
    path = Path(stats_dir) / season / str(league_id) / "matches.json"
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    matches = []
    for league in data if isinstance(data, list) else [data]:
        for stage in league.get("stage", []):
            for m in stage.get("matches", []):
                if m.get("date") == date:
                    matches.append(m)

    print(f"✅ Found {len(matches)} matches for {date} in league {league_id}")
    for m in matches:
        home = m["teams"]["home"]["name"]
        away = m["teams"]["away"]["name"]
        score = f'{m["goals"]["home_ft_goals"]}-{m["goals"]["away_ft_goals"]}'
        print(f"- {home} vs {away} ({score})")

    return matches

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("league_id", type=int)
    parser.add_argument("season", type=str)
    parser.add_argument("date", type=str, help="format: DD/MM/YYYY")
    args = parser.parse_args()
    filter_matches(args.league_id, args.season, args.date)

if __name__ == "__main__":
    main()
