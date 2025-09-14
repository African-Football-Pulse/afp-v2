import json
from pathlib import Path

# Repo root = två nivåer upp från detta script (src/tools/.. → repo root)
BASE_DIR = Path(__file__).resolve().parents[2]

# Paths
BASE_PATH = BASE_DIR / "afp" / "stats" / "players"
HISTORY_FILE = BASE_DIR / "afp" / "players" / "africa" / "players_africa_history.json"

def load_history(player_id: str):
    if not HISTORY_FILE.exists():
        raise FileNotFoundError(f"History file not found: {HISTORY_FILE}")
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        history = json.load(f)
    return history.get(player_id, {}).get("history", [])

def load_season_stats(season: str, league_id: str, player_id: str):
    path = BASE_DIR / "stats" / season / league_id / "players" / f"{player_id}.json"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_player_season(player_id: str, season: str, stats: dict):
    outdir = BASE_PATH / player_id
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"{season}.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

def update_totals(player_id: str):
    player_dir = BASE_PATH / player_id
    totals = {
        "player_id": player_id,
        "apps": 0,
        "goals": 0,
        "assists": 0,
        "yellow_cards": 0,
        "red_cards": 0,
        "substitutions_in": 0,
        "substitutions_out": 0,
        "seasons": []
    }

    for season_file in player_dir.glob("*.json"):
        if season_file.name == "totals.json":
            continue
        with open(season_file, "r", encoding="utf-8") as f:
            season_stats = json.load(f)
        totals["apps"] += season_stats.get("apps", 0)
        totals["goals"] += season_stats.get("goals", 0)
        totals["assists"] += season_stats.get("assists", 0)
        totals["yellow_cards"] += season_stats.get("yellow_cards", 0)
        totals["red_cards"] += season_stats.get("red_cards", 0)
        totals["substitutions_in"] += season_stats.get("substitutions_in", 0)
        totals["substitutions_out"] += season_stats.get("substitutions_out", 0)
        totals["seasons"].append(season_file.stem)

    with open(player_dir / "totals.json", "w", encoding="utf-8") as f:
        json.dump(totals, f, indent=2, ensure_ascii=False)

def build_player(player_id: str):
    history = load_history(player_id)
    if not history:
        print(f"No history found for {player_id}")
        return

    for entry in history:
        season = entry["season"]
        league_id = entry["league_id"]
        stats = load_season_stats(season, league_id, player_id)
        if stats:
            save_player_season(player_id, season, stats)

    update_totals(player_id)
    print(f"✔ Built stats for player {player_id}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--player", required=True, help="Player ID to build stats for")
    args = parser.parse_args()
    build_player(args.player)
