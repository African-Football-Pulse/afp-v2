import os
import json
import argparse
from src.storage import azure_blob

CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "afp")


def load_manifest(season: str, league_id: str):
    path = f"stats/{season}/{league_id}/manifest.json"
    print(f"[collect_player_stats] Loading matches from {path} ...")
    return azure_blob.get_json(CONTAINER, path)


def collect_player_stats(player_id: str, league_id: str, season: str):
    manifest = load_manifest(season, league_id)

    if not isinstance(manifest, list):
        print("[collect_player_stats] Unexpected manifest format")
        return

    apps = 0
    goals = 0

    player_id_int = int(player_id)

    for match in manifest:
        events = match.get("events", [])
        appeared = False

        for ev in events:
            if ev.get("player", {}).get("id") == player_id_int:
                appeared = True
                if ev.get("event_type") == "goal":
                    goals += 1
            if ev.get("player_in", {}).get("id") == player_id_int:
                appeared = True

        if appeared:
            apps += 1

    stats = {
        "player_id": player_id,
        "league_id": league_id,
        "season": season,
        "apps": apps,
        "goals": goals,
    }

    out_path = f"stats/players/{season}/{league_id}/{player_id}.json"
    azure_blob.upload_json(CONTAINER, out_path, stats)
    print(f"[collect_player_stats] Uploaded stats for {player_id} → {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-id", required=True, help="Player ID")
    parser.add_argument("--league-id", required=True, help="League ID")
    parser.add_argument("--season", required=True, help="Season (e.g., 2024-2025)")
    args = parser.parse_args()

    collect_player_stats(args.player_id, args.league_id, args.season)


if __name__ == "__main__":
    main()
