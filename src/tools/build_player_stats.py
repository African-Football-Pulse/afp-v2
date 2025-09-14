import argparse
import os
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_CONTAINER", "afp")


def load_history(player_id: str):
    """Load players_africa_history.json from Azure"""
    path = "players/africa/players_africa_history.json"
    history_all = azure_blob.get_json(CONTAINER, path)
    return history_all.get(player_id, {}).get("history", [])


def load_season_stats(season: str, league_id: str, player_id: str):
    """Load season stats file from Azure"""
    path = f"stats/{season}/{league_id}/players/{player_id}.json"
    if azure_blob.exists(CONTAINER, path):
        return azure_blob.get_json(CONTAINER, path)
    return None


def save_player_season(player_id: str, season: str, stats: dict):
    """Upload stats to stats/players/<player_id>/<season>.json"""
    path = f"stats/players/{player_id}/{season}.json"
    azure_blob.upload_json(CONTAINER, path, stats)
    print(f"[build_player_stats] Uploaded {path}")


def update_totals(player_id: str):
    """Aggregate all season stats into totals.json"""
    prefix = f"stats/players/{player_id}/"
    blobs = azure_blob.list_prefix(CONTAINER, prefix)

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

    for blob_name in blobs:
        if not blob_name.endswith(".json") or blob_name.endswith("totals.json"):
            continue
        season_stats = azure_blob.get_json(CONTAINER, blob_name)
        totals["apps"] += season_stats.get("apps", 0)
        totals["goals"] += season_stats.get("goals", 0)
        totals["assists"] += season_stats.get("assists", 0)
        totals["yellow_cards"] += season_stats.get("yellow_cards", 0)
        totals["red_cards"] += season_stats.get("red_cards", 0)
        totals["substitutions_in"] += season_stats.get("substitutions_in", 0)
        totals["substitutions_out"] += season_stats.get("substitutions_out", 0)
        totals["seasons"].append(season_stats.get("season"))

    outpath = f"stats/players/{player_id}/totals.json"
    azure_blob.upload_json(CONTAINER, outpath, totals)
    print(f"[build_player_stats] Uploaded {outpath}")


def build_player(player_id: str):
    history = load_history(player_id)
    if not history:
        print(f"[build_player_stats] No history found for {player_id}")
        return

    for entry in history:
        season = entry["season"]
        league_id = entry["league_id"]
        stats = load_season_stats(season, league_id, player_id)
        if stats:
            save_player_season(player_id, season, stats)

    update_totals(player_id)
    print(f"✔ Built stats for player {player_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player", required=True, help="Player ID to build stats for")
    args = parser.parse_args()
    build_player(args.player)


if __name__ == "__main__":
    main()
